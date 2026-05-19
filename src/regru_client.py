"""
Async Reg.ru Cloud API client for IP hunting.

API constraint: POST /v1/ips does NOT work in openstack-* regions —
it returns "HTTP 400: invalid server region" for every body variant.
Floating IP rotation isn't available via the public API.

So we use the classic flow — create reglet, check its primary IP,
delete it, repeat.  The delete is fully awaited (polled until 404)
so the quota slot is guaranteed free before the next POST /v1/reglets.

Catalog: V2 (region-aware).
"""

import asyncio
import time
import uuid
from typing import Optional

import aiohttp


# ──────── Global per-token quota semaphore ────────
# Multiple RegRuClient instances can share the same API token (e.g. when
# hunting multiple regions of the same account).  Reg.ru enforces a HARD
# server-count quota per account — this semaphore mirrors it on our side.
#
# If you create N RegRuClient objects with the same token, at most one
# will hold a "create" slot at any moment.  Prevents "HTTP 400: server
# limit reached" errors entirely.

_token_quota: dict[str, int]                 = {}  # token -> quota (max servers)
_token_sem:   dict[str, asyncio.Semaphore]   = {}
_token_live:  dict[str, int]                 = {}  # token -> currently held slots

# Global registry of reglets WE believe are legitimately in-flight.
# Indexed by (token, region).  Anything NOT in here but named hunt-* is
# a leak — the quota guard task kills it.
_active_reglets: dict[tuple[str, str], set[str]] = {}

# Snapshot of hunt-* reglets that existed BEFORE the hunt started.
# These are NEVER touched by the quota guard or emergency cleanup.
# Populated by bot at hunt start; cleared on reset.
_preexisting: dict[tuple[str, str], set[str]] = {}


def _track_active(token: str, region: str, rid: str) -> None:
    _active_reglets.setdefault((token, region), set()).add(rid)

def _untrack_active(token: str, region: str, rid: str) -> None:
    s = _active_reglets.get((token, region))
    if s: s.discard(rid)

def get_active_reglets(token: str, region: str) -> set[str]:
    return set(_active_reglets.get((token, region), set()))

def add_preexisting(token: str, region: str, rids: set[str]) -> None:
    """Mark these reglet IDs as pre-existing — they won't be auto-deleted."""
    if rids:
        _preexisting.setdefault((token, region), set()).update(rids)

def is_preexisting(token: str, region: str, rid: str) -> bool:
    return rid in _preexisting.get((token, region), set())

def _set_token_quota(token: str, quota: int) -> None:
    """Set / raise the live-server limit for this token's semaphore.
    Idempotent: if called again with bigger quota, we expand the semaphore."""
    quota = max(1, int(quota))
    cur = _token_quota.get(token, 0)
    if token not in _token_sem:
        _token_sem[token]   = asyncio.Semaphore(quota)
        _token_quota[token] = quota
    elif quota > cur:
        # Expand existing semaphore by releasing extra slots
        for _ in range(quota - cur):
            _token_sem[token].release()
        _token_quota[token] = quota


def reset_token_quota(token: str) -> None:
    """Forget everything we know about this token's quota.  Use between
    hunt runs to avoid stuck slots from a crashed previous session."""
    _token_sem.pop(token, None)
    _token_quota.pop(token, None)
    _token_live.pop(token, None)
    # Also clear preexisting / active for this token (any region)
    for key in list(_preexisting.keys()):
        if key[0] == token:
            _preexisting.pop(key, None)
    for key in list(_active_reglets.keys()):
        if key[0] == token:
            _active_reglets.pop(key, None)


class _TokenSlot:
    """Async context manager: hold one quota slot for the duration of
    a full create->check->delete cycle."""
    def __init__(self, token: str):
        self.token = token
    async def __aenter__(self):
        if self.token in _token_sem:
            await _token_sem[self.token].acquire()
            _token_live[self.token] = _token_live.get(self.token, 0) + 1
        return self
    async def __aexit__(self, *exc):
        if self.token in _token_sem:
            _token_sem[self.token].release()
            _token_live[self.token] = max(0, _token_live.get(self.token, 1) - 1)


# ───────────────────────── Errors ─────────────────────────

class RegRuError(Exception):
    pass


class RegRuRateLimitError(Exception):
    def __init__(self, retry_after: float = 5.0):
        self.retry_after = retry_after


# ───────────────────────── Client ─────────────────────────

class RegRuClient:
    V1 = "https://api.cloudvps.reg.ru/v1"
    V2 = "https://api.cloudvps.reg.ru/v2"

    _ALL_REGIONS = [
        "openstack-msk1", "openstack-msk2",
        "openstack-spb1", "openstack-sam1",
        "openstack-fz1",  "msk1",
    ]

    def __init__(
        self,
        api_token: str,
        region: str = "openstack-msk1",
        operation_timeout: float = 180.0,
        quota: int = 1,
    ):
        self._token      = api_token
        self._region     = region
        self._op_timeout = operation_timeout
        # Register/extend the shared per-token semaphore.
        _set_token_quota(api_token, quota)
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Catalog (filled once, shared across attempts of this client)
        self._plan_slug:  str = ""
        self._image_slug: str = ""
        self._catalog_ok: bool = False
        self._catalog_lock = asyncio.Lock()

    # ───── Session ─────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            async with self._session_lock:
                if self._session is None or self._session.closed:
                    self._session = aiohttp.ClientSession(
                        connector=aiohttp.TCPConnector(
                            limit=50, limit_per_host=30,
                            ttl_dns_cache=600, enable_cleanup_closed=True,
                        ),
                        timeout=aiohttp.ClientTimeout(
                            total=30, connect=5, sock_read=25),
                        headers={
                            "Authorization": f"Bearer {self._token}",
                            "Content-Type":  "application/json",
                        },
                    )
        return self._session

    async def _reset_session(self) -> None:
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = None

    # ───── HTTP ─────

    async def _request(self, method: str, url: str,
                       json_body: dict | None = None) -> dict | None:
        for attempt in range(3):
            sess = await self._get_session()
            try:
                async with sess.request(method, url, json=json_body) as resp:
                    if resp.status == 429:
                        raise RegRuRateLimitError(
                            float(resp.headers.get("Retry-After", "5")))
                    if resp.status == 204:
                        return None
                    body = await resp.json(content_type=None)
                    if resp.status >= 400:
                        msg = (body.get("message") or body.get("error")
                               or body.get("detail") or str(body)
                               ) if isinstance(body, dict) else str(body)
                        raise RegRuError(f"HTTP {resp.status}: {msg}")
                    return body
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == 2:
                    raise
                await self._reset_session()
                await asyncio.sleep(0.5)
        return None

    async def _get(self, url: str) -> dict:
        return await self._request("GET", url) or {}

    async def _post(self, url: str, body: dict) -> dict:
        return await self._request("POST", url, body) or {}

    async def _delete(self, url: str) -> None:
        await self._request("DELETE", url)

    # ───── Catalog (V2, region-aware) ─────

    async def _ensure_catalog(self) -> None:
        if self._catalog_ok:
            return
        async with self._catalog_lock:
            if self._catalog_ok:
                return
            r = self._region

            plans = (await self._get(
                f"{self.V2}/plans?region={r}&page=1&items_per_page=100"
            )).get("plans", [])
            if not plans:
                raise RegRuError(
                    f"Нет тарифов в {r}. Проверьте регион/идентификацию.")
            self._plan_slug = min(plans, key=_plan_price).get("slug", "")
            if not self._plan_slug:
                raise RegRuError("Тариф без slug")

            images = (await self._get(
                f"{self.V2}/images?region={r}&page=1&items_per_page=100"
                f"&type=distribution"
            )).get("images", [])
            if not images:
                images = (await self._get(
                    f"{self.V2}/images?region={r}&page=1&items_per_page=100"
                )).get("images", [])
            if not images:
                raise RegRuError(f"Нет образов в {r}")
            chosen = next((i for i in images
                           if "ubuntu" in i.get("slug", "").lower()), images[0])
            self._image_slug = chosen.get("slug", "") or str(chosen.get("id", ""))
            if not self._image_slug:
                raise RegRuError("Образ без slug/id")

            self._catalog_ok = True
            print(f"[regru/{r}] Plan: {self._plan_slug}  Image: {self._image_slug}")

    async def setup(self) -> None:
        """Compat shim for bot.py — just pre-warm the catalog."""
        await self._ensure_catalog()

    # ───── Hot loop: create / check / delete reglet ─────

    async def create_address(self) -> tuple[str, str]:
        """Create a reglet, wait until active+IP, return (reglet_id, ip).

        Uses the per-token quota semaphore — only `quota` workers may
        hold a slot at once across ALL RegRuClient instances using the
        same token.  Slot is released ONLY after the server is fully
        deleted (worker calls delete_address_direct → _release_slot).
        """
        await self._ensure_catalog()

        # Acquire a quota slot.  Blocks if all `quota` slots are taken
        # by siblings of this token.
        await _token_sem[self._token].acquire()
        _token_live[self._token] = _token_live.get(self._token, 0) + 1

        try:
            body = {
                "size":        self._plan_slug,
                "image":       self._image_slug,
                "region_slug": self._region,
                "name":        f"hunt-{uuid.uuid4().hex[:8]}",
                "floating_ip": True,
            }
            data = None
            # Even with our semaphore, server-side may disagree briefly
            # (stale archive).  Retry with aggressive slot-freeing.
            for attempt in range(30):
                try:
                    data = await self._post(f"{self.V1}/reglets", body)
                    break
                except RegRuError as e:
                    m = str(e).lower()
                    if "limit" in m and "reach" in m:
                        print(f"[regru/{self._region}] API says quota busy "
                              f"(try {attempt+1}/30), kicking archived...")
                        await self._free_one_slot()
                        continue
                    raise
            if data is None:
                # Couldn't get past "limit reached" — release our slot
                self._release_slot()
                raise RegRuError(
                    "Лимит серверов Reg.ru даже после чистки. "
                    "Проверьте идентификацию аккаунта.")

            rid = str(data.get("reglet", {}).get("id") or "")
            if not rid:
                self._release_slot()
                raise RegRuError(f"Не создан сервер: {data}")

            # Track as actively used — quota guard won't touch it
            _track_active(self._token, self._region, rid)

            try:
                ip = await self._wait_active(rid)
            except Exception:
                # Activation failed — slot release + untrack
                _untrack_active(self._token, self._region, rid)
                self._release_slot()
                raise
            return (rid, ip)
        except Exception:
            raise

    def _release_slot(self) -> None:
        """Release one per-token quota slot.  Idempotent-safe via tracking."""
        sem = _token_sem.get(self._token)
        if sem is None:
            return
        try:
            sem.release()
            _token_live[self._token] = max(0, _token_live.get(self._token, 1) - 1)
        except ValueError:
            pass   # already released, no-op

    async def _free_one_slot(self) -> None:
        """Try to free at least one quota slot — kick archived hunt-*
        reglets and wait until the visible count goes down."""
        try:
            reglets = await self.list_reglets()
        except Exception:
            await asyncio.sleep(5.0)
            return
        # Re-DELETE any of our hunters that are stuck in archive
        for r in reglets:
            if (r.get("name", "").startswith("hunt-")
                    and (r.get("status") or "").lower() == "archive"):
                try:
                    await self._delete(f"{self.V1}/reglets/{r['id']}")
                    print(f"[regru/{self._region}] Re-DELETE stuck archive {r['id']}")
                except Exception:
                    pass
        # Wait a bit and re-check
        await asyncio.sleep(6.0)

    async def _wait_active(self, reglet_id: str) -> str:
        """Poll reglet until status=active + ip present.

        Reg.ru creation takes 60-120s reliably, so we wait max(op_timeout, 180s).
        """
        wait_limit = max(self._op_timeout, 180.0)
        deadline = time.monotonic() + wait_limit
        while time.monotonic() < deadline:
            await asyncio.sleep(4.0)
            r = (await self._get(f"{self.V1}/reglets/{reglet_id}")
                 ).get("reglet", {})
            status = (r.get("status") or "").lower()
            ip = r.get("ip") or ""
            if status == "active" and ip:
                return ip
            if status in ("suspended", "archive"):
                await self._delete_and_wait(reglet_id)
                raise RegRuError(f"Сервер -> {status}")
        await self._delete_and_wait(reglet_id)
        raise RegRuError(f"Таймаут активации ({wait_limit:.0f}с)")

    async def delete_address_direct(self, reglet_id: str) -> None:
        """Delete a reglet, wait until API slot is free, then release
        our local per-token quota slot and untrack the reglet."""
        try:
            await self._delete_and_wait(reglet_id)
        finally:
            _untrack_active(self._token, self._region, reglet_id)
            self._release_slot()

    async def _delete_and_wait(self, reglet_id: str) -> None:
        """DELETE + poll until 404.  Agressively re-DELETE if stuck in archive.

        Archived reglets still occupy the Reg.ru quota, so we MUST wait
        for full disappearance (404) before releasing the slot.
        """
        if not reglet_id:
            return
        # Initial DELETE (retry if server is still provisioning)
        for attempt in range(12):
            try:
                await self._delete(f"{self.V1}/reglets/{reglet_id}")
                break
            except RegRuError as e:
                if "404" in str(e):
                    return
                if attempt == 11:
                    print(f"[regru/{self._region}] DELETE {reglet_id} give up: {e}")
                    break   # still try to poll — may disappear anyway
                await asyncio.sleep(4.0)
            except Exception:
                await asyncio.sleep(3.0)

        # Poll until 404 — up to 5 minutes.  Re-DELETE every 15s if stuck.
        deadline = time.monotonic() + 300.0
        last_nudge = time.monotonic()
        while time.monotonic() < deadline:
            await asyncio.sleep(2.5)
            try:
                info = await self._get(f"{self.V1}/reglets/{reglet_id}")
                status = (info.get("reglet", {}).get("status") or "").lower()
                if status == "archive" and time.monotonic() - last_nudge > 15.0:
                    try: await self._delete(f"{self.V1}/reglets/{reglet_id}")
                    except Exception: pass
                    last_nudge = time.monotonic()
            except RegRuError as e:
                if "404" in str(e):
                    return   # slot free
            except Exception:
                pass
        print(f"[regru/{self._region}] ⚠ {reglet_id} still visible after 300s!")

    # ───── Cleanup (no persistent state, kept for bot compat) ─────

    async def cleanup(self) -> None:
        """No persistent state to clean up (hot-path already cleans itself)."""
        pass

    async def emergency_cleanup(self, keep_ids: set[str] | None = None) -> int:
        """Delete hunt-* reglets CREATED BY THIS HUNT RUN.

        Pre-existing reglets (present before hunt started) are NEVER touched.
        Reglets in keep_ids are preserved (user found the target IP).
        """
        keep = keep_ids or set()
        try:
            reglets = await self.list_reglets()
        except Exception as e:
            print(f"[regru/{self._region}] emergency list err: {e}")
            return 0
        to_kill = []
        for r in reglets:
            if not r.get("name", "").startswith("hunt-"):
                continue
            if r["id"] in keep:
                continue
            if is_preexisting(self._token, self._region, r["id"]):
                continue   # pre-existing — keep it
            rs = r.get("region_slug", "")
            if rs and rs != self._region and rs in self._ALL_REGIONS:
                continue   # another region
            to_kill.append(r["id"])
        if not to_kill:
            return 0
        print(f"[regru/{self._region}] 🔨 emergency_cleanup: killing {len(to_kill)}")
        await asyncio.gather(
            *(self._delete_and_wait(rid) for rid in to_kill),
            return_exceptions=True,
        )
        return len(to_kill)

    async def guard_sweep(self) -> int:
        """Periodic background sweep during hunt.

        Kills hunt-* reglets that are neither actively tracked by a worker
        nor pre-existing (snapshot at hunt start).
        """
        try:
            reglets = await self.list_reglets()
        except Exception:
            return 0
        active = get_active_reglets(self._token, self._region)
        leaks = []
        for r in reglets:
            if not r.get("name", "").startswith("hunt-"):
                continue
            rs = r.get("region_slug", "")
            if rs and rs != self._region and rs in self._ALL_REGIONS:
                continue
            if r["id"] in active:
                continue
            if is_preexisting(self._token, self._region, r["id"]):
                continue   # pre-existing — leave alone
            leaks.append(r["id"])
        if leaks:
            print(f"[regru/{self._region}] 🧹 guard_sweep: {len(leaks)} leak(s)")
            for rid in leaks:
                asyncio.create_task(self._delete_and_wait(rid))
        return len(leaks)

    async def cleanup_stale_hunters(self) -> int:
        """Delete leftover hunt-* reglets from previous runs.

        Called ONCE by the bot before workers start — wipes orphan servers
        that would otherwise occupy the quota.

        Filter logic:
          * Skip reglets in OTHER known regions (so multi-region setups
            don't kill each other's servers).
          * Reglets without region_slug, or with a region matching ours,
            are considered ours and deleted.
        """
        try:
            reglets = await self.list_reglets()
        except Exception as e:
            print(f"[regru/{self._region}] cleanup list err: {e}")
            return 0
        stale = []
        for r in reglets:
            if not r.get("name", "").startswith("hunt-"):
                continue
            rs = r.get("region_slug", "")
            # Only skip if it's clearly in ANOTHER region we know about
            if rs and rs != self._region and rs in self._ALL_REGIONS:
                continue
            stale.append(r)
        if not stale:
            return 0
        print(f"[regru/{self._region}] Found {len(stale)} stale hunter(s): "
              f"{[r['id'] for r in stale]} — cleaning...")
        await asyncio.gather(
            *(self._delete_and_wait(r["id"]) for r in stale),
            return_exceptions=True,
        )
        print(f"[regru/{self._region}] Stale cleanup done")
        return len(stale)

    # ───── List / direct reglet ops (used by bot UI) ─────

    async def list_reglets(self) -> list[dict]:
        data = await self._get(f"{self.V1}/reglets")
        return [{
            "id":          str(r.get("id", "")),
            "ip":          r.get("ip") or "",
            "name":        r.get("name", ""),
            "status":      r.get("status", ""),
            "region_slug": r.get("region_slug", ""),
        } for r in data.get("reglets", [])]

    async def delete_reglet(self, reglet_id: str) -> None:
        """Best-effort reglet delete + wait for slot free."""
        await self._delete_and_wait(reglet_id)

    # ───── Region discovery ─────

    async def discover_regions(self) -> list[str]:
        working: list[str] = []
        for r in self._ALL_REGIONS:
            try:
                if (await self._get(
                    f"{self.V2}/plans?region={r}&page=1&items_per_page=10"
                )).get("plans"):
                    working.append(r)
            except Exception:
                pass
        return working

    # ───── Close ─────

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


# ───────────────────────── helpers ─────────────────────────

def _plan_price(p: dict) -> float:
    """Cheapest plan = lowest price_per_hour (fall back to month/720)."""
    pph = p.get("price_per_hour")
    try:
        if pph is not None:
            v = float(pph)
            if v > 0:
                return v
    except (ValueError, TypeError):
        pass
    ppm = p.get("price_per_month")
    try:
        if isinstance(ppm, (int, float)) and ppm > 0:
            return float(ppm) / 720.0
    except (ValueError, TypeError):
        pass
    return 99999.0
