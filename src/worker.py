"""
IP Worker — creates floating IPs on a hunter reglet until target IP is found.

Flow (per attempt):
  1. Acquire one quota slot (semaphore = concurrency).
  2. client.create_address()  — returns a new IP (first call = primary IP,
     subsequent calls allocate floating IPs via POST /v1/ips).
  3. If it matches target_ip -> mark found, fire callback, stop.
  4. Otherwise client.delete_address_direct(ip) — synchronously so the
     quota slot is free before the semaphore is released.
"""

import asyncio
import ipaddress
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Deque

from .regru_client import RegRuRateLimitError

_ATTEMPT_TIMEOUT = 600.0   # full cycle incl. slot waiting + create + wait-active
_DELETE_TIMEOUT  = 360.0   # per-attempt delete budget (incl. 404 polling)


# ───────────────────────── IP matching ─────────────────────────

def _ip_matches(ip: str, patterns: str) -> bool:
    """Does `ip` match ANY comma-separated `patterns`?

    Supported forms:
      84.201.1.2          exact
      51.250              prefix (must end at an octet boundary)
      84.*.*.2            wildcard (exactly 4 octets, * = anything)
      79.174.92.0/24      CIDR subnet
    """
    ip = ip.strip()
    if not ip:
        return False
    try:
        ip_obj = ipaddress.ip_address(ip)
    except ValueError:
        ip_obj = None

    for pat in patterns.split(","):
        pat = pat.strip()
        if not pat:
            continue

        if "/" in pat and ip_obj is not None:
            try:
                if ip_obj in ipaddress.ip_network(pat, strict=False):
                    return True
            except ValueError:
                pass
            continue

        if "*" in pat:
            pp, ip_p = pat.split("."), ip.split(".")
            if len(pp) == 4 and len(ip_p) == 4 \
               and all(p == "*" or p == i for p, i in zip(pp, ip_p)):
                return True
            continue

        if len(pat.split(".")) == 4:
            if ip == pat:
                return True
            continue

        if ip == pat or ip.startswith(pat + "."):
            return True

    return False


# ───────────────────────── Data ─────────────────────────

@dataclass
class WorkerStats:
    account_name: str
    target_ip:    str
    attempts:     int   = 0
    errors:       int   = 0
    rate_limits:  int   = 0
    last_ip:      str   = "-"
    last_error:   str   = ""
    start_time:   float = field(default_factory=time.time)
    found:        bool  = False
    found_ip:     str   = ""
    found_id:     str   = ""
    running:      bool  = False
    speed:        float = 0.0
    current_rpm:  int   = 0
    paused_until: float = 0.0
    no_rl_wait:   bool  = False


OnFoundCallback = Callable[[WorkerStats], Awaitable[None]]


class _AdaptiveRate:
    """Target rpm with 429-driven halving backoff."""

    def __init__(self, target_rpm: int):
        self._target  = max(target_rpm, 1)
        self._min     = max(int(self._target * 0.03), 1)
        self._current = float(self._target)
        self._lock    = asyncio.Lock()

    @property
    def interval(self) -> float:
        return 60.0 / max(self._current, 1.0)

    @property
    def current_rpm(self) -> int:
        return int(self._current)

    async def on_success(self) -> None:
        async with self._lock:
            self._current = min(self._current + 2.0, float(self._target))

    async def on_rate_limit(self) -> None:
        async with self._lock:
            self._current = max(self._current * 0.5, float(self._min))


# ───────────────────────── Worker ─────────────────────────

class IPWorker:
    def __init__(
        self,
        client,
        stats:               WorkerStats,
        attempts_per_minute: int,
        concurrency:         int,
        on_found:            OnFoundCallback,
        error_backoff:       float = 5.0,
        rate_limit_wait:     float = 10.0,
        no_rl_wait:          bool  = False,
    ):
        self.client          = client
        self.stats           = stats
        self.concurrency     = max(concurrency, 1)
        self.on_found        = on_found
        self.error_backoff   = error_backoff
        self.rate_limit_wait = rate_limit_wait
        self.no_rl_wait      = no_rl_wait
        self.stats.no_rl_wait = no_rl_wait

        self._rate = _AdaptiveRate(attempts_per_minute)
        self._done = asyncio.Event()
        self._ts:   Deque[float] = deque()

    async def _tick(self, ip: str = "", error: str = "", rate_limit: bool = False) -> None:
        now = time.monotonic()
        if ip:
            self._ts.append(now)
            self.stats.attempts += 1
            self.stats.last_ip = ip
        cutoff = now - 60.0
        while self._ts and self._ts[0] < cutoff:
            self._ts.popleft()
        self.stats.speed       = float(len(self._ts))
        self.stats.current_rpm = self._rate.current_rpm
        if error:
            self.stats.errors   += 1
            self.stats.last_error = error
        if rate_limit:
            self.stats.rate_limits += 1

    async def _safe_delete(self, reglet_id: str) -> None:
        """Delete a reglet AND wait for the quota slot to free.

        Must block long enough for the API to return 404 on the reglet —
        otherwise the next attempt upends on 'server limit reached'.
        """
        try:
            await asyncio.wait_for(
                self.client.delete_address_direct(reglet_id),
                timeout=_DELETE_TIMEOUT,
            )
        except Exception as e:
            print(f"[worker/{self.stats.account_name}] delete err ({reglet_id}): {e}")

    async def _attempt(self, sem: asyncio.Semaphore) -> None:
        """One hunting attempt: create reglet -> check IP -> delete (all sync)."""
        async with sem:
            if self._done.is_set():
                return
            rid, ip = "", ""
            try:
                rid, ip = await asyncio.wait_for(
                    self.client.create_address(),
                    timeout=_ATTEMPT_TIMEOUT,
                )
                await self._tick(ip=ip)
                await self._rate.on_success()

                if _ip_matches(ip, self.stats.target_ip):
                    if not self._done.is_set():
                        self.stats.found    = True
                        self.stats.found_ip = ip
                        self.stats.found_id = rid
                        self._done.set()
                        try:
                            await self.on_found(self.stats)
                        except Exception as e:
                            print(f"[worker] on_found err: {e}")
                else:
                    await self._safe_delete(rid)

            except asyncio.TimeoutError:
                if rid:
                    await self._safe_delete(rid)
                await self._tick(error="Timeout")
                await asyncio.sleep(3.0)

            except RegRuRateLimitError:
                await self._rate.on_rate_limit()
                if rid:
                    await self._safe_delete(rid)
                if self.no_rl_wait:
                    await self._tick(error="Rate limit (skip)", rate_limit=True)
                else:
                    wait = self.rate_limit_wait
                    self.stats.paused_until = time.time() + wait
                    await self._tick(error=f"Rate limit {wait:.0f}s", rate_limit=True)
                    await asyncio.sleep(wait)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                if rid:
                    await self._safe_delete(rid)
                await self._tick(error=str(e)[:100])
                if self.error_backoff > 0:
                    await asyncio.sleep(min(self.error_backoff, 10.0))

    async def run(self) -> None:
        self.stats.running    = True
        self.stats.start_time = time.time()
        self._ts.clear()

        sem     = asyncio.Semaphore(self.concurrency)
        pending: set[asyncio.Task] = set()
        next_t  = time.monotonic()

        try:
            while not self._done.is_set():
                now = time.monotonic()
                wait = next_t - now
                if wait > 0:
                    await asyncio.sleep(min(wait, 0.1))
                    continue
                if len(pending) >= self.concurrency:
                    await asyncio.sleep(0.05)
                    continue
                next_t = max(next_t + self._rate.interval, time.monotonic())
                task = asyncio.create_task(self._attempt(sem))
                pending.add(task)
                task.add_done_callback(pending.discard)
        finally:
            for t in list(pending):
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self.stats.running = False

    def stop(self) -> None:
        self._done.set()
