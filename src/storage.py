"""
Persistent JSON storage for accounts and settings.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

DATA_FILE = Path("data/config.json")

DEFAULT_SETTINGS: dict[str, Any] = {
    # Reg.ru quota for non-identified accounts = 1 server.
    # concurrency_per_account MUST equal your quota to avoid "limit reached".
    "attempts_per_minute":     30,
    "concurrency_per_account": 1,    # = your Reg.ru quota
    "servers_per_region":      1,    # hunter workers per region
    "update_interval":         5.0,
    "operation_timeout":       180.0,
    "error_backoff":           5.0,
    "rate_limit_wait":         10.0,
    "no_rl_wait":              False,
    "show_errors":             False,
}

_lock = asyncio.Lock()


def _load_raw() -> dict:
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"settings": DEFAULT_SETTINGS.copy(), "accounts": []}


def _save_raw(data: dict) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = DATA_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(DATA_FILE)


async def load() -> dict:
    async with _lock:
        return _load_raw()


async def save(data: dict) -> None:
    async with _lock:
        _save_raw(data)


async def get_accounts() -> list[dict]:
    d = await load()
    return d.get("accounts", [])


async def get_settings() -> dict:
    d = await load()
    merged = DEFAULT_SETTINGS.copy()
    merged.update(d.get("settings", {}))
    return merged


async def upsert_account(account: dict) -> None:
    async with _lock:
        d = _load_raw()
        accounts: list = d.get("accounts", [])
        for i, a in enumerate(accounts):
            if a["name"] == account["name"]:
                accounts[i] = account
                d["accounts"] = accounts
                _save_raw(d)
                return
        accounts.append(account)
        d["accounts"] = accounts
        _save_raw(d)


async def rename_account(old_name: str, account: dict) -> None:
    async with _lock:
        d = _load_raw()
        accounts: list = d.get("accounts", [])
        d["accounts"] = [account if a["name"] == old_name else a for a in accounts]
        _save_raw(d)


async def delete_account(name: str) -> None:
    async with _lock:
        d = _load_raw()
        d["accounts"] = [a for a in d.get("accounts", []) if a["name"] != name]
        _save_raw(d)


async def update_setting(key: str, value: Any) -> None:
    async with _lock:
        d = _load_raw()
        settings = d.get("settings", {})
        settings[key] = value
        d["settings"] = settings
        _save_raw(d)


# ───────────────────────── Per-account settings ─────────────────────────

# Keys that can be overridden per-account.  All other settings stay global.
PER_ACCOUNT_KEYS: tuple[str, ...] = (
    "attempts_per_minute",
    "concurrency_per_account",
    "servers_per_region",
    "rate_limit_wait",
    "error_backoff",
    "no_rl_wait",
)


# Named presets — "one-click" configuration.
PRESETS: dict[str, dict[str, Any]] = {
    "unverified": {
        "label":       "🟡 Без верификации",
        "hint":        "Квота 1 сервер. Безопасный дефолт для свежего аккаунта.",
        "attempts_per_minute":     30,
        "concurrency_per_account": 1,
        "servers_per_region":      1,
        "rate_limit_wait":         10.0,
        "error_backoff":           5.0,
        "no_rl_wait":              False,
    },
    "verified": {
        "label":       "🟢 С верификацией (квота 5)",
        "hint":        "Квота 5 серверов. Нужна идентификация в cloud.reg.ru.",
        "attempts_per_minute":     60,
        "concurrency_per_account": 5,
        "servers_per_region":      1,
        "rate_limit_wait":         5.0,
        "error_backoff":           3.0,
        "no_rl_wait":              False,
    },
    "max_speed": {
        "label":       "⚡ Макс. скорость (квота 20+)",
        "hint":        "Для аккаунтов с увеличенной квотой. Агрессивные параметры.",
        "attempts_per_minute":     120,
        "concurrency_per_account": 10,
        "servers_per_region":      2,
        "rate_limit_wait":         3.0,
        "error_backoff":           2.0,
        "no_rl_wait":              True,
    },
}


def effective_settings(account: dict, globals_: dict) -> dict:
    """Merge globals + account's 'settings' dict.  Account overrides win."""
    out = dict(globals_)
    overrides = account.get("settings") or {}
    for k, v in overrides.items():
        if v is not None:
            out[k] = v
    return out


async def set_account_setting(acc_name: str, key: str, value: Any) -> None:
    """Set a per-account override.  value=None removes the override."""
    async with _lock:
        d = _load_raw()
        accounts = d.get("accounts", [])
        for a in accounts:
            if a["name"] == acc_name:
                s = a.get("settings") or {}
                if value is None:
                    s.pop(key, None)
                else:
                    s[key] = value
                a["settings"] = s
                _save_raw(d)
                return


async def apply_preset(acc_name: str, preset_key: str) -> None:
    """Apply one of PRESETS to an account (overwrites per-account settings)."""
    preset = PRESETS.get(preset_key)
    if not preset:
        return
    async with _lock:
        d = _load_raw()
        for a in d.get("accounts", []):
            if a["name"] == acc_name:
                s = {k: v for k, v in preset.items()
                     if k in PER_ACCOUNT_KEYS}
                a["settings"] = s
                _save_raw(d)
                return


async def clear_account_settings(acc_name: str) -> None:
    """Remove all per-account overrides (use globals again)."""
    async with _lock:
        d = _load_raw()
        for a in d.get("accounts", []):
            if a["name"] == acc_name:
                a.pop("settings", None)
                _save_raw(d)
                return
