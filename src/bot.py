"""
Telegram bot UI for Reg.ru IP Hunter.
"""
from __future__ import annotations

import asyncio
import ipaddress
import time
from dataclasses import dataclass, field
from typing import Optional

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from . import storage
from .worker import IPWorker, WorkerStats
from .regru_client import RegRuClient, reset_token_quota, add_preexisting

# ───────────────────────── Regions ─────────────────────────

REGION_NAMES: dict[str, str] = {
    "openstack-msk1": "Москва 1",
    "openstack-msk2": "Москва 2",
    "openstack-spb1": "Петербург",
    "openstack-sam1": "Самара",
    "openstack-fz1":  "Франкфурт",
    "msk1":           "Москва (legacy)",
}
_DEFAULT_REGIONS = ["openstack-msk1", "openstack-msk2", "openstack-spb1", "openstack-sam1"]

# Discovered once from API (replaces the default list when known)
_live_regions: list[str] = []


def _rlabel(slug: str) -> str:
    return f"📍 {REGION_NAMES.get(slug, slug)}"


def _rshort(slug: str) -> str:
    return REGION_NAMES.get(slug, slug)


def _acc_regions(acc: dict) -> list[str]:
    """Regions of an account.  Handles old `region` and new `regions` fields."""
    rs = acc.get("regions")
    if isinstance(rs, list) and rs:
        return rs
    return [acc.get("region", "openstack-msk1")]


def _regions_compact(acc: dict) -> str:
    rs = _acc_regions(acc)
    return _rshort(rs[0]) if len(rs) == 1 else " + ".join(_rshort(r) for r in rs)


async def _fetch_live_regions(token: str) -> None:
    """Discover regions that actually work for the given token (once)."""
    global _live_regions
    if _live_regions:
        return
    try:
        c = RegRuClient(api_token=token)
        _live_regions = await c.discover_regions()
        await c.close()
        for r in _live_regions:
            REGION_NAMES.setdefault(r, r)
        print(f"[bot] Live regions: {_live_regions}")
    except Exception as e:
        print(f"[bot] Region discovery failed: {e}")


# ───────────────────────── Settings meta ─────────────────────────

SETTINGS_META: dict[str, tuple[str, type, str]] = {
    "attempts_per_minute": ("🔄 Попыток/мин", int,
        "Попыток в минуту на воркера (рекомендуется 30)."),
    "concurrency_per_account": ("🔀 Параллельно", int,
        "ВАЖНО: должно равняться <b>квоте Reg.ru</b>!\n"
        "• Без идентификации = <b>1</b>\n"
        "• С идентификацией = <b>5</b>\n"
        "Превышение квоты = HTTP 400 'server limit reached'."),
    "servers_per_region": ("🖥 Серверов на регион", int,
        "Сколько воркеров создать в каждом регионе.\n"
        "Каждый воркер использует <b>concurrency</b> слотов квоты.\n"
        "Итого: <code>servers × concurrency × регионов ≤ квота</code>."),
    "update_interval":   ("🕐 Обновление",      float, "Сек между обновлениями карточки (мин 5)."),
    "operation_timeout": ("⏰ Таймаут",          float, "Таймаут создания сервера (сек)."),
    "error_backoff":     ("⚡ Пауза ошибки",    float, "Пауза после ошибки API (сек)."),
    "rate_limit_wait":   ("🚫 Пауза RL",        float, "Пауза при 429 (сек)."),
}

IP_HINT = (
    "Введите IP для поиска:\n\n"
    "<code>84.201.1.2</code>       — точный\n"
    "<code>51.250</code>            — префикс\n"
    "<code>84.*.*.2</code>         — wildcard\n"
    "<code>79.174.92.0/24</code>  — подсеть (CIDR)\n\n"
    "Несколько через запятую."
)

# ───────────────────────── FSM ─────────────────────────

class AddAccount(StatesGroup):
    name      = State()
    api_token = State()
    region    = State()   # multi-select
    target_ip = State()

class EditField(StatesGroup):
    waiting = State()

class EditRegions(StatesGroup):
    waiting = State()

class EditSetting(StatesGroup):
    waiting = State()

class HuntSelect(StatesGroup):
    choosing = State()

# ───────────────────────── Hunt state ─────────────────────────

@dataclass
class _Hunt:
    active:  bool = False
    workers: list[IPWorker]     = field(default_factory=list)
    tasks:   list[asyncio.Task] = field(default_factory=list)
    stats:   list[WorkerStats]  = field(default_factory=list)
    clients: list[RegRuClient]  = field(default_factory=list)
    updater:     Optional[asyncio.Task] = None
    supervisor:  Optional[asyncio.Task] = None
    quota_guard: Optional[asyncio.Task] = None
    chat_id: Optional[int] = None
    msg_id:  Optional[int] = None
    target_rpm: int      = 3
    update_interval: float = 5.0
    show_errors: bool    = False

_hunt = _Hunt()
_OWNER: int = 0

def _ok(ev) -> bool:
    return bool(ev.from_user and ev.from_user.id == _OWNER)

def _btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


# ───────────────────────── Keyboards ─────────────────────────

def kb_main(active: bool) -> InlineKeyboardMarkup:
    h = _btn("⏹ Остановить", "hunt:stop") if active else _btn("🎯 Охота", "hunt:start")
    return InlineKeyboardMarkup(inline_keyboard=[
        [h],
        [_btn("👥 Аккаунты", "menu:accounts"), _btn("⚙️ Настройки", "menu:settings")],
    ])

def kb_accounts(accounts: list, active: bool) -> InlineKeyboardMarkup:
    rows = []
    for i, a in enumerate(accounts):
        rs = _acc_regions(a)
        suffix = f"×{len(rs)}" if len(rs) > 1 else _rshort(rs[0])
        rows.append([_btn(f"👤 {a['name']}  ·  {suffix}", f"acc:view:{i}"),
                     _btn("🗑", f"acc:del:{i}")])
    if not active:
        rows.append([_btn("➕ Добавить", "acc:add")])
    rows.append([_btn("◀ Меню", "menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_acc_detail(idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("📛 Имя",       f"acc:edit:{idx}:name"),
         _btn("🔑 Токен",     f"acc:edit:{idx}:api_token")],
        [_btn("📍 Регионы",   f"acc:editreg:{idx}"),
         _btn("🎯 IP",        f"acc:edit:{idx}:target_ip")],
        [_btn("⚙️ Настройки", f"acc:settings:{idx}"),
         _btn("🖥 Серверы",   f"ips:list:{idx}")],
        [_btn("🗑 Удалить",   f"acc:del:{idx}"),
         _btn("◀ Аккаунты",   "menu:accounts")],
    ])


def kb_acc_settings(idx: int, acc: dict, globals_: dict) -> InlineKeyboardMarkup:
    """Per-account settings screen: per-parameter override only."""
    overrides = acc.get("settings") or {}
    rows: list[list[InlineKeyboardButton]] = []
    for key in storage.PER_ACCOUNT_KEYS:
        meta = SETTINGS_META.get(key)
        if not meta:
            continue
        label = meta[0]
        if key in overrides and overrides[key] is not None:
            badge = f"🟢 {overrides[key]}"
        else:
            badge = f"⚪ {globals_.get(key, '—')}"
        rows.append([_btn(f"{label}: {badge}", f"acc:sp:{idx}:{key}")])
    rows.append([_btn("🔄 Сбросить к глобальным", f"acc:reset:{idx}")])
    rows.append([_btn("◀ К аккаунту", f"acc:view:{idx}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_regions_multi(prefix: str, selected: list[str],
                     done_cb: str, cancel_cb: str = "menu:accounts") -> InlineKeyboardMarkup:
    """Multi-select region picker with checkboxes."""
    slugs = _live_regions or _DEFAULT_REGIONS
    sel = set(selected)
    rows = [
        [_btn(f"{'✅' if s in sel else '⬜'} 📍 {REGION_NAMES.get(s, s)}  ({s})",
              f"{prefix}:{s}")]
        for s in slugs
    ]
    n = len(sel)
    rows.append([_btn(f"✔️ Готово ({n})" if n else "✔️ Выберите", done_cb)])
    rows.append([_btn("❌ Отмена", cancel_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_settings(s: dict) -> InlineKeyboardMarkup:
    rows = [[_btn(f"{m[0]}: {s.get(k, '—')}", f"set:{k}")]
            for k, m in SETTINGS_META.items()]
    rows.append([
        _btn(f"{'🟢' if s.get('no_rl_wait') else '🔴'} Игнор RL", "toggle:no_rl_wait"),
        _btn(f"{'🟢' if s.get('show_errors') else '🔴'} Ошибки",  "toggle:show_errors"),
    ])
    rows.append([_btn("◀ Меню", "menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_del_acc(idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        _btn("✅ Да", f"acc:del:{idx}:yes"),
        _btn("❌ Нет", "menu:accounts"),
    ]])

def kb_cancel(target: str = "menu:accounts") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[_btn("❌ Отмена", target)]])

def kb_back_only(target: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[_btn("◀", target)]])

def kb_ip_list(ips: list[dict], idx: int, page: int,
               selected: set[str] | None = None,
               ps: int = 8) -> InlineKeyboardMarkup:
    start = page * ps
    sel = selected or set()
    rows = []
    for ip in ips[start:start + ps]:
        st  = {"active": "🟢", "off": "🔴"}.get(ip["status"], "⏳")
        chk = "☑" if ip["id"] in sel else "⬜"
        label = f"{chk} {st} {ip['ip']}" + (f"  {ip['name']}" if ip.get("name") else "")
        rows.append([_btn(label, f"ips:tog:{idx}:{ip['id']}"),
                     _btn("🗑", f"ip:del:{idx}:{ip['id']}:{ip['ip']}")])
    nav = []
    if page > 0:               nav.append(_btn("◀", f"ips:page:{idx}:{page - 1}"))
    if start + ps < len(ips):  nav.append(_btn("▶", f"ips:page:{idx}:{page + 1}"))
    if nav: rows.append(nav)
    # Selection actions
    n = len(sel)
    rows.append([
        _btn("☑ Все",     f"ips:selall:{idx}"),
        _btn("☐ Сброс",   f"ips:selnone:{idx}"),
        _btn("🎯 hunt-*", f"ips:selhunt:{idx}"),
    ])
    del_label = f"🗑 Удалить выбранные ({n})" if n else "🗑 Удалить всё"
    del_cb    = f"ips:delsel:{idx}" if n else f"ips:delall:{idx}"
    rows.append([_btn(del_label, del_cb)])
    rows.append([_btn("🔄", f"ips:list:{idx}"),
                 _btn("◀ Аккаунт", f"acc:view:{idx}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_del_ip(idx: int, ip_id: str, ip_addr: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        _btn("✅ Удалить", f"ip:del:{idx}:{ip_id}:{ip_addr}:yes"),
        _btn("❌ Нет",     f"ips:list:{idx}"),
    ]])

def kb_confirm_bulk(idx: int, action: str, count: int) -> InlineKeyboardMarkup:
    """action: 'all' | 'sel' | 'hunt'"""
    return InlineKeyboardMarkup(inline_keyboard=[[
        _btn(f"✅ Удалить {count}", f"ips:bulkok:{idx}:{action}"),
        _btn("❌ Отмена",            f"ips:list:{idx}"),
    ]])

def kb_hunt_sel(accounts: list, sel: list[str]) -> InlineKeyboardMarkup:
    s = set(sel)
    rows = [[_btn(f"{'✅' if a['name'] in s else '⬜'} {a['name']}  ·  🎯 {a['target_ip']}",
                  f"hunt:toggle:{i}")]
            for i, a in enumerate(accounts)]
    rows.append([_btn("☑ Все", "hunt:selall"), _btn("☐ Сброс", "hunt:selnone")])
    n = sum(1 for a in accounts if a["name"] in s)
    rows.append([_btn(f"🚀 Старт ({n})" if n else "🚀 Выберите", "hunt:go")])
    rows.append([_btn("◀ Назад", "menu:main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ───────────────────────── Text builders ─────────────────────────

_HR = "━━━━━━━━━━━━━━━━━━━━━"

def _mask(t: str) -> str:
    return t[:8] + "..." + t[-4:] if len(t) > 12 else "***"

def _dur(secs: float) -> str:
    s = int(secs)
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"

def _num(n: int) -> str:
    return f"{n:,}".replace(",", " ")

def txt_main(accounts: list, s: dict, active: bool) -> str:
    workers = sum(len(_acc_regions(a)) for a in accounts)
    st = "🟢 Охота" if active else "⚪ Ожидание"
    lines = [
        "🌐 <b>Reg.ru IP Hunter</b>",
        _HR,
        f"{st}  ·  👥 {len(accounts)}  ·  🔀 {workers}"
        f"  ·  ⚡ {s.get('attempts_per_minute', '—')} rpm",
    ]
    for a in accounts[:5]:
        lines.append(f"  👤 <b>{a['name']}</b> · {_regions_compact(a)}"
                     f" · 🎯 <code>{a['target_ip']}</code>")
    if len(accounts) > 5:
        lines.append(f"  ...и ещё {len(accounts) - 5}")
    return "\n".join(lines)

def txt_accs(accounts: list) -> str:
    if not accounts:
        return f"👥 <b>Аккаунты</b>\n{_HR}\n<i>Пусто</i>"
    lines = [f"👥 <b>Аккаунты</b> ({len(accounts)})", _HR]
    for a in accounts:
        lines.append(f"👤 <b>{a['name']}</b> · 📍 {_regions_compact(a)}"
                     f" · 🎯 <code>{a['target_ip']}</code>")
    return "\n".join(lines)

def txt_acc(a: dict) -> str:
    rs = _acc_regions(a)
    if len(rs) == 1:
        regions_line = _rlabel(rs[0])
    else:
        regions_line = f"📍 Регионы ({len(rs)}):\n  " + "\n  ".join(
            f"• {_rshort(r)}  <code>{r}</code>" for r in rs)
    overrides = a.get("settings") or {}
    settings_line = ""
    if overrides:
        vals = ", ".join(f"{k}={v}" for k, v in overrides.items())
        settings_line = f"\n⚙️ <b>Свои настройки</b>: {vals}"
    return (
        f"👤 <b>{a['name']}</b>\n{_HR}\n"
        f"🔑 <code>{_mask(a['api_token'])}</code>\n"
        f"{regions_line}\n"
        f"🎯 <code>{a['target_ip']}</code>"
        f"{settings_line}"
    )

def txt_ips(ips: list, name: str, page: int, ps: int = 8) -> str:
    if not ips:
        return f"🖥 <b>{name}</b>\n{_HR}\n<i>Нет серверов</i>"
    pages = (len(ips) - 1) // ps + 1
    return f"🖥 <b>{name}</b>  ·  {len(ips)} серв.  ·  стр {page + 1}/{pages}"

def txt_hunt_select(accounts: list, sel: list[str], rpm: int, conc: int) -> str:
    n = sum(1 for a in accounts if a["name"] in set(sel))
    return (
        f"🎯 <b>Запуск охоты</b>\n{_HR}\n"
        f"Выбрано: <b>{n}</b> / {len(accounts)}\n"
        f"⚡ {rpm} rpm  ·  🔀 x{conc}"
    )

def _acc_block(st: WorkerStats, rpm: int, show_err: bool) -> str:
    elapsed = time.time() - st.start_time
    if st.found:
        badge = f"✅ <b>НАЙДЕН {st.found_ip}</b>"
    elif st.paused_until > time.time():
        badge = f"⏸ RL ({int(st.paused_until - time.time())}с)"
    elif st.running and st.attempts == 0 and st.errors == 0:
        badge = "⏳ Создаю первый сервер..."
    elif st.running:
        badge = "🔵"
    else:
        badge = "⚫"
    norl = " 🔕" if st.no_rl_wait else ""
    lines = [
        f"{badge} <b>{st.account_name}</b>{norl}",
        f"  🎯 <code>{st.target_ip}</code> · 📡 <code>{st.last_ip}</code>",
        f"  🔄 {_num(st.attempts)} · ⚡ {st.speed:.0f}/{st.current_rpm or rpm} rpm"
        f" · ⏱ {_dur(elapsed)}",
    ]
    if show_err and (st.errors or st.last_error):
        rl_part = f" 🚫 {st.rate_limits}" if st.rate_limits else ""
        lines.append(f"  ❌ {st.errors}{rl_part}")
        if st.last_error:
            lines.append(f"  ⚠️ <code>{st.last_error[:60]}</code>")
    return "\n".join(lines)

def txt_card(all_st: list[WorkerStats], rpm: int, show_err: bool = False) -> str:
    blocks = [_acc_block(s, rpm, show_err) for s in all_st]
    total = sum(s.attempts for s in all_st)
    spd = sum(s.speed for s in all_st)
    elapsed = time.time() - min((s.start_time for s in all_st), default=time.time())
    nf = sum(1 for s in all_st if s.found)
    done = all(s.found or not s.running for s in all_st)
    if done and nf:
        h = f"✅ <b>Завершено</b> · {nf}/{len(all_st)}"
    elif nf:
        h = f"🔍 <b>Охота</b> · ✅ {nf}/{len(all_st)}"
    else:
        h = f"🔍 <b>Охота</b> · {len(all_st)} воркеров"
    return (
        f"{h}\n{_HR}\n"
        + "\n\n".join(blocks)
        + f"\n{_HR}\n"
        f"📊 {_num(total)} · ⚡ {spd:.0f} rpm · ⏱ {_dur(elapsed)}\n"
        f"<i>{time.strftime('%H:%M:%S')}</i>"
    )

def txt_found(st: WorkerStats) -> str:
    lines = [
        "🎉 <b>IP НАЙДЕН!</b>", _HR,
        f"🌐 <code>{st.found_ip}</code>",
        f"🎯 <code>{st.target_ip}</code>",
        f"👤 {st.account_name}",
        f"🔄 {_num(st.attempts)} · ⏱ {_dur(time.time() - st.start_time)}",
    ]
    if st.found_id:
        lines.append(f"🖥 Reglet ID: <code>{st.found_id}</code>")
    lines += [
        _HR,
        "💡 <i>Сервер с нужным IP сохранён в Reg.ru.\n"
        "Остальные hunt-* серверы будут удалены.</i>",
    ]
    return "\n".join(lines)


# ───────────────────────── Router / helpers ─────────────────────────

router = Router()

class _ErrMw(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception as e:
            import traceback
            traceback.print_exc()
            try:
                await event.answer(f"⚠️ {str(e)[:150]}", show_alert=True)
            except Exception:
                pass

router.callback_query.middleware(_ErrMw())


async def _safe_edit(bot: Bot, cid: int, mid: int, text: str,
                     kb: Optional[InlineKeyboardMarkup] = None) -> None:
    """Edit a bot message, tolerating RL / not-modified errors."""
    async def _do():
        await bot.edit_message_text(text, chat_id=cid, message_id=mid,
                                    parse_mode="HTML", reply_markup=kb)
    try:
        await _do()
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        try: await _do()
        except Exception: pass
    except TelegramBadRequest as e:
        if "not modified" not in str(e).lower():
            print(f"[bot] edit err: {e}")
    except Exception as e:
        print(f"[bot] edit err: {e}")


async def _cb_edit(cb: CallbackQuery, text: str, **kw) -> None:
    kw.setdefault("parse_mode", "HTML")
    try:
        await cb.message.edit_text(text, **kw)
    except TelegramBadRequest as e:
        if "not modified" in str(e).lower():
            return
        try: await cb.message.answer(text, **kw)
        except Exception: pass
    except Exception:
        try: await cb.message.answer(text, **kw)
        except Exception: pass


def _valid_ip(p: str) -> bool:
    if not p.strip():
        return False
    for part in p.split(","):
        part = part.strip()
        if not part or " " in part:
            return False
        if "/" in part:
            try: ipaddress.ip_network(part, strict=False)
            except ValueError: return False
            continue
        if "*" in part:
            octs = part.split(".")
            if len(octs) != 4:
                return False
            for x in octs:
                if x == "*":
                    continue
                try:
                    if not 0 <= int(x) <= 255:
                        return False
                except ValueError:
                    return False
    return True


def _make_client(acc: dict) -> RegRuClient:
    """Fresh client for one-off API calls (IP list / delete)."""
    return RegRuClient(api_token=acc["api_token"], region=_acc_regions(acc)[0])


# ───────────────────────── /start, /cancel ─────────────────────────

async def _render_main(sender, state: FSMContext) -> None:
    """Send or edit the main screen."""
    await state.clear()
    accounts = await storage.get_accounts()
    if accounts and not _live_regions:
        await _fetch_live_regions(accounts[0]["api_token"])
    s = await storage.get_settings()
    txt = txt_main(accounts, s, _hunt.active)
    kb = kb_main(_hunt.active)
    if isinstance(sender, CallbackQuery):
        await _cb_edit(sender, txt, reply_markup=kb)
        await sender.answer()
    else:
        await sender.answer(txt, reply_markup=kb, parse_mode="HTML")


@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext) -> None:
    if not _ok(msg): return
    await _render_main(msg, state)

@router.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext) -> None:
    if not _ok(msg): return
    await _render_main(msg, state)

@router.callback_query(F.data == "menu:main")
async def cb_main(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    await _render_main(cb, state)

@router.callback_query(F.data == "menu:accounts")
async def cb_accs(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    await state.clear()
    accs = await storage.get_accounts()
    await _cb_edit(cb, txt_accs(accs), reply_markup=kb_accounts(accs, _hunt.active))
    await cb.answer()


# ───────────────────────── Account: view / add / edit / delete ─────────────────────────

@router.callback_query(F.data.startswith("acc:view:"))
async def cb_acc_view(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    await state.clear()
    idx = int(cb.data.split(":")[2])
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Не найден", show_alert=True); return
    await _cb_edit(cb, txt_acc(accs[idx]), reply_markup=kb_acc_detail(idx))
    await cb.answer()

# -- Add flow --

@router.callback_query(F.data == "acc:add")
async def cb_add(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    if _hunt.active:
        await cb.answer("Нельзя во время охоты", show_alert=True); return
    await state.clear()
    await state.set_state(AddAccount.name)
    await _cb_edit(cb,
        f"➕ <b>Новый аккаунт</b>\n{_HR}\nШаг <b>1/4</b> — Название\n\n"
        "Введите имя:", reply_markup=kb_cancel())
    await cb.answer()

@router.message(AddAccount.name)
async def add_name(msg: Message, state: FSMContext) -> None:
    if not _ok(msg): return
    name = (msg.text or "").strip()
    if not name:
        await msg.answer("❌ Пустое имя"); return
    if any(a["name"] == name for a in await storage.get_accounts()):
        await msg.answer("❌ Уже есть"); return
    await state.update_data(name=name)
    await state.set_state(AddAccount.api_token)
    await msg.answer(
        f"✅ Имя: <b>{name}</b>\n\nШаг <b>2/4</b> — API токен\n\n"
        f"Введите токен Reg.ru Cloud:\n<i>Панель → Облачные серверы → API</i>",
        reply_markup=kb_cancel(), parse_mode="HTML")

@router.message(AddAccount.api_token)
async def add_token(msg: Message, state: FSMContext) -> None:
    if not _ok(msg): return
    token = (msg.text or "").strip()
    if not token:
        await msg.answer("❌ Пустой токен"); return
    await state.update_data(api_token=token, selected_regions=[])
    await state.set_state(AddAccount.region)
    await _fetch_live_regions(token)
    await msg.answer(
        "✅ Токен OK\n\nШаг <b>3/4</b> — Регионы\n\n"
        "Отметьте зоны. На каждую создаётся отдельный воркер.\n"
        "<i>Учтите лимиты — без идентификации обычно 1 сервер на аккаунт.</i>",
        reply_markup=kb_regions_multi("addregm", [], "addregm:DONE"),
        parse_mode="HTML")

@router.callback_query(F.data.startswith("addregm:"))
async def cb_addregm(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    if await state.get_state() != AddAccount.region.state:
        await cb.answer("Устарело", show_alert=True); return
    slug = cb.data.split(":", 1)[1]
    data = await state.get_data()
    sel: list[str] = list(data.get("selected_regions", []))

    if slug == "DONE":
        if not sel:
            await cb.answer("Выберите хотя бы один", show_alert=True); return
        await state.update_data(regions=sel, region=sel[0])
        await state.set_state(AddAccount.target_ip)
        summary = ", ".join(_rshort(s) for s in sel)
        await _cb_edit(cb,
            f"✅ Регионы ({len(sel)}): {summary}\n\nШаг <b>4/4</b> — Цель\n\n{IP_HINT}",
            reply_markup=kb_cancel())
        await cb.answer(); return

    if slug in sel: sel.remove(slug)
    else:           sel.append(slug)
    await state.update_data(selected_regions=sel)
    await _cb_edit(cb,
        "✅ Токен OK\n\nШаг <b>3/4</b> — Регионы\n\n"
        "Отметьте зоны. На каждую создаётся отдельный воркер.",
        reply_markup=kb_regions_multi("addregm", sel, "addregm:DONE"))
    await cb.answer()

@router.message(AddAccount.target_ip)
async def add_ip(msg: Message, state: FSMContext) -> None:
    if not _ok(msg): return
    ip = (msg.text or "").strip()
    if not _valid_ip(ip):
        await msg.answer(f"❌ Формат.\n{IP_HINT}", parse_mode="HTML"); return
    data = await state.get_data()
    if not all(k in data for k in ("name", "api_token", "region")):
        await state.clear()
        await msg.answer("❌ Данные потеряны, /start"); return
    regions = data.get("regions") or [data["region"]]
    acc = {
        "name":      data["name"],
        "api_token": data["api_token"],
        "region":    regions[0],
        "regions":   regions,
        "target_ip": ip,
    }
    await storage.upsert_account(acc)
    await state.clear()
    accs = await storage.get_accounts()
    rs = ", ".join(_rshort(r) for r in regions)
    await msg.answer(
        f"✅ <b>Добавлен!</b>\n\n📛 <b>{acc['name']}</b>\n"
        f"📍 Регионов: {len(regions)} — {rs}\n🎯 <code>{ip}</code>",
        reply_markup=kb_accounts(accs, _hunt.active), parse_mode="HTML")

# -- Edit regions (multi) --

@router.callback_query(F.data.startswith("acc:editreg:"))
async def cb_editreg(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    await _fetch_live_regions(accs[idx]["api_token"])
    current = _acc_regions(accs[idx])
    await state.clear()
    await state.set_state(EditRegions.waiting)
    await state.update_data(edit_idx=idx, selected_regions=list(current))
    await _cb_edit(cb,
        f"📍 <b>Регионы: {accs[idx]['name']}</b>\n"
        f"Сейчас: {', '.join(_rshort(r) for r in current)}\n\n"
        f"Отметьте нужные зоны:",
        reply_markup=kb_regions_multi(
            f"setregm:{idx}", list(current),
            f"setregm:{idx}:DONE", f"acc:view:{idx}"))
    await cb.answer()

@router.callback_query(F.data.startswith("setregm:"))
async def cb_setregm(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    parts = cb.data.split(":")
    idx  = int(parts[1])
    slug = parts[2]
    data = await state.get_data()
    sel: list[str] = list(data.get("selected_regions", []))

    if slug == "DONE":
        if not sel:
            await cb.answer("Выберите хотя бы один", show_alert=True); return
        accs = await storage.get_accounts()
        if idx >= len(accs):
            await cb.answer("Нет", show_alert=True); return
        acc = dict(accs[idx])
        acc["regions"] = sel
        acc["region"]  = sel[0]
        await storage.upsert_account(acc)
        await state.clear()
        accs = await storage.get_accounts()
        ni = next((i for i, a in enumerate(accs) if a["name"] == acc["name"]), 0)
        await _cb_edit(cb, txt_acc(accs[ni]), reply_markup=kb_acc_detail(ni))
        await cb.answer(f"✅ Регионов: {len(sel)}"); return

    if slug in sel: sel.remove(slug)
    else:           sel.append(slug)
    await state.update_data(selected_regions=sel)
    accs = await storage.get_accounts()
    name = accs[idx]["name"] if idx < len(accs) else ""
    await _cb_edit(cb,
        f"📍 <b>Регионы: {name}</b>\n\nОтметьте нужные зоны:",
        reply_markup=kb_regions_multi(
            f"setregm:{idx}", sel,
            f"setregm:{idx}:DONE", f"acc:view:{idx}"))
    await cb.answer()

# -- Edit single field (name / token / target_ip) --

_EDIT_LABELS = {
    "name":      "Имя",
    "api_token": "API токен",
    "target_ip": "Целевой IP",
}

@router.callback_query(F.data.startswith("acc:edit:"))
async def cb_edit(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    _, _, idx_s, fld = cb.data.split(":", 3)
    idx = int(idx_s)
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    await state.clear()
    await state.set_state(EditField.waiting)
    await state.update_data(edit_idx=idx, edit_field=fld, old_name=accs[idx]["name"])
    cur  = accs[idx].get(fld, "")
    disp = _mask(cur) if fld == "api_token" else cur
    label = _EDIT_LABELS.get(fld, fld)
    extra = f"\n\n{IP_HINT}" if fld == "target_ip" else "\n\nВведите новое значение:"
    await _cb_edit(cb,
        f"✏️ <b>{label}</b>\nСейчас: <code>{disp}</code>{extra}",
        reply_markup=kb_cancel(f"acc:view:{idx}"))
    await cb.answer()

@router.message(EditField.waiting)
async def edit_val(msg: Message, state: FSMContext) -> None:
    if not _ok(msg): return
    data = await state.get_data()
    idx      = data.get("edit_idx", 0)
    fld      = data.get("edit_field", "")
    old_name = data.get("old_name", "")
    val = (msg.text or "").strip()
    if not val:
        await msg.answer("❌ Пусто"); return
    if fld == "target_ip" and not _valid_ip(val):
        await msg.answer(f"❌ Формат.\n{IP_HINT}", parse_mode="HTML"); return
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await state.clear(); await msg.answer("❌ Нет"); return
    acc = dict(accs[idx])
    if fld == "name" and val != acc["name"]:
        if any(a["name"] == val for a in accs):
            await msg.answer("❌ Уже есть"); return
        acc["name"] = val
        await storage.rename_account(old_name, acc)
    else:
        acc[fld] = val
        await storage.upsert_account(acc)
    await state.clear()
    accs = await storage.get_accounts()
    ni = next((i for i, a in enumerate(accs) if a["name"] == acc["name"]), 0)
    await msg.answer(txt_acc(accs[ni]),
                     reply_markup=kb_acc_detail(ni), parse_mode="HTML")

# -- Per-account settings / presets --

async def _render_acc_settings(cb: CallbackQuery, idx: int) -> None:
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    acc = accs[idx]
    globals_ = await storage.get_settings()
    overrides = acc.get("settings") or {}
    ov_lines = ""
    if overrides:
        ov_lines = "\n" + "\n".join(
            f"  • <code>{k}</code> = <b>{v}</b>" for k, v in overrides.items())
    body = (
        f"⚙️ <b>Настройки: {acc['name']}</b>\n{_HR}\n"
        f"🟢 — переопределено для аккаунта\n"
        f"⚪ — наследуется из глобальных\n"
        f"{ov_lines}"
    )
    await _cb_edit(cb, body, reply_markup=kb_acc_settings(idx, acc, globals_))

@router.callback_query(F.data.startswith("acc:settings:"))
async def cb_acc_settings_open(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    await state.clear()
    idx = int(cb.data.split(":")[2])
    await _render_acc_settings(cb, idx)
    await cb.answer()

@router.callback_query(F.data.startswith("acc:reset:"))
async def cb_acc_reset(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    await storage.clear_account_settings(accs[idx]["name"])
    await _render_acc_settings(cb, idx)
    await cb.answer("🔄 Сброшено")

@router.callback_query(F.data.startswith("acc:sp:"))
async def cb_acc_sp(cb: CallbackQuery, state: FSMContext) -> None:
    """Prompt to set one specific per-account override value."""
    if not _ok(cb): return
    _, _, idx_s, key = cb.data.split(":", 3)
    idx = int(idx_s)
    if key not in storage.PER_ACCOUNT_KEYS or key not in SETTINGS_META:
        await cb.answer(); return
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    acc = accs[idx]
    overrides = acc.get("settings") or {}
    globals_ = await storage.get_settings()
    label, _, hint = SETTINGS_META[key]
    cur_str = (f"<b>{overrides[key]}</b> (override)" if key in overrides
               else f"{globals_.get(key, '—')} (из глобальных)")
    await state.clear()
    await state.set_state(EditSetting.waiting)
    await state.update_data(setting_key=key, acc_idx=idx,
                            acc_name=acc["name"], for_account=True)
    await _cb_edit(cb,
        f"⚙️ <b>{label}</b> · {acc['name']}\n\n"
        f"Сейчас: {cur_str}\n\n{hint}\n\n"
        f"Пришлите новое число или <code>0</code> чтобы сбросить к глобальному.",
        reply_markup=kb_cancel(f"acc:settings:{idx}"))
    await cb.answer()

# -- Delete account --

@router.callback_query(F.data.startswith("acc:del:"))
async def cb_del(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    parts = cb.data.split(":")
    idx = int(parts[2])
    confirmed = len(parts) == 4 and parts[3] == "yes"
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    if confirmed:
        await storage.delete_account(accs[idx]["name"])
        accs = await storage.get_accounts()
        await _cb_edit(cb, txt_accs(accs), reply_markup=kb_accounts(accs, _hunt.active))
        await cb.answer("🗑 OK"); return
    if _hunt.active:
        await cb.answer("Нельзя во время охоты", show_alert=True); return
    await _cb_edit(cb,
        f"🗑 Удалить <b>{accs[idx]['name']}</b>?",
        reply_markup=kb_del_acc(idx))
    await cb.answer()


# ───────────────────────── Reglets list (manual management) ─────────────────────────

async def _render_ip_list(cb: CallbackQuery, state: FSMContext,
                          idx: int, page: int = 0,
                          fresh_fetch: bool = False) -> None:
    """Render the reglets list for account `idx`."""
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    acc  = accs[idx]
    data = await state.get_data()
    ips  = data.get("ip_cache", [])
    if fresh_fetch or not ips:
        c = _make_client(acc)
        try:
            ips = await c.list_reglets()
        finally:
            await c.close()
    sel = set(data.get("ip_sel", []))
    # Drop selected IDs that no longer exist
    valid_ids = {i["id"] for i in ips}
    sel &= valid_ids
    await state.update_data(ip_cache=ips, ip_sel=list(sel))
    await _cb_edit(cb, txt_ips(ips, acc["name"], page),
                   reply_markup=kb_ip_list(ips, idx, page, sel))


@router.callback_query(F.data.startswith("ips:list:"))
async def cb_ips(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    await cb.answer("⏳")
    try:
        # Fresh fetch + reset selection
        await state.update_data(ip_sel=[])
        await _render_ip_list(cb, state, idx, page=0, fresh_fetch=True)
    except Exception as e:
        await _cb_edit(cb, f"❌ <code>{e}</code>",
                       reply_markup=kb_back_only(f"acc:view:{idx}"))


@router.callback_query(F.data.startswith("ips:page:"))
async def cb_ips_page(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    _, _, idx_s, page_s = cb.data.split(":")
    await _render_ip_list(cb, state, int(idx_s), page=int(page_s))
    await cb.answer()


@router.callback_query(F.data.startswith("ips:tog:"))
async def cb_ips_tog(cb: CallbackQuery, state: FSMContext) -> None:
    """Toggle a single reglet's selection."""
    if not _ok(cb): return
    _, _, idx_s, ip_id = cb.data.split(":", 3)
    idx = int(idx_s)
    data = await state.get_data()
    sel = set(data.get("ip_sel", []))
    if ip_id in sel: sel.remove(ip_id)
    else:            sel.add(ip_id)
    await state.update_data(ip_sel=list(sel))
    await _render_ip_list(cb, state, idx, page=0)
    await cb.answer()


@router.callback_query(F.data.startswith("ips:selall:"))
async def cb_ips_selall(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    data = await state.get_data()
    ips = data.get("ip_cache", [])
    await state.update_data(ip_sel=[i["id"] for i in ips])
    await _render_ip_list(cb, state, idx, page=0)
    await cb.answer()


@router.callback_query(F.data.startswith("ips:selnone:"))
async def cb_ips_selnone(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    await state.update_data(ip_sel=[])
    await _render_ip_list(cb, state, idx, page=0)
    await cb.answer()


@router.callback_query(F.data.startswith("ips:selhunt:"))
async def cb_ips_selhunt(cb: CallbackQuery, state: FSMContext) -> None:
    """Select all reglets whose name starts with 'hunt-'."""
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    data = await state.get_data()
    ips = data.get("ip_cache", [])
    sel = [i["id"] for i in ips if i.get("name", "").startswith("hunt-")]
    await state.update_data(ip_sel=sel)
    await _render_ip_list(cb, state, idx, page=0)
    await cb.answer(f"☑ {len(sel)} hunt-* серверов")


# ─── Bulk delete: confirmation + execution ───

@router.callback_query(F.data.startswith("ips:delsel:"))
async def cb_ips_delsel(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    data = await state.get_data()
    sel = list(data.get("ip_sel", []))
    if not sel:
        await cb.answer("Ничего не выбрано", show_alert=True); return
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    await _cb_edit(cb,
        f"🗑 Удалить <b>{len(sel)}</b> серверов аккаунта <b>{accs[idx]['name']}</b>?\n\n"
        f"<i>Операция параллельная и необратимая.</i>",
        reply_markup=kb_confirm_bulk(idx, "sel", len(sel)))
    await cb.answer()


@router.callback_query(F.data.startswith("ips:delall:"))
async def cb_ips_delall(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    data = await state.get_data()
    ips = data.get("ip_cache", [])
    if not ips:
        await cb.answer("Список пуст", show_alert=True); return
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    await _cb_edit(cb,
        f"🗑 Удалить ВСЕ <b>{len(ips)}</b> серверов аккаунта "
        f"<b>{accs[idx]['name']}</b>?",
        reply_markup=kb_confirm_bulk(idx, "all", len(ips)))
    await cb.answer()


@router.callback_query(F.data.startswith("ips:bulkok:"))
async def cb_ips_bulkok(cb: CallbackQuery, state: FSMContext) -> None:
    """Execute bulk delete: action ∈ {sel, all, hunt}."""
    if not _ok(cb): return
    _, _, idx_s, action = cb.data.split(":", 3)
    idx = int(idx_s)
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    data = await state.get_data()
    ips = data.get("ip_cache", [])

    if action == "sel":
        sel_set = set(data.get("ip_sel", []))
        targets = [i["id"] for i in ips if i["id"] in sel_set]
    elif action == "all":
        targets = [i["id"] for i in ips]
    elif action == "hunt":
        targets = [i["id"] for i in ips if i.get("name", "").startswith("hunt-")]
    else:
        targets = []

    if not targets:
        await cb.answer("Нечего удалять", show_alert=True); return

    await cb.answer(f"⏳ Удаляю {len(targets)}...")
    await _cb_edit(cb,
        f"⏳ <b>Удаление {len(targets)} серверов</b>\n{_HR}\n"
        f"Это может занять до 2 минут (ждём освобождения квоты).")

    acc = accs[idx]
    c = _make_client(acc)
    failed = 0
    try:
        # Parallel delete with concurrency cap to avoid hammering the API
        sem = asyncio.Semaphore(5)
        async def _one(rid: str) -> bool:
            async with sem:
                try:
                    await c.delete_reglet(rid)
                    return True
                except Exception as e:
                    print(f"[bot] bulk delete {rid} err: {e}")
                    return False
        results = await asyncio.gather(*(_one(r) for r in targets),
                                       return_exceptions=True)
        failed = sum(1 for r in results if r is not True)
        # Refresh
        try:
            ips = await c.list_reglets()
        except Exception:
            pass
    finally:
        await c.close()

    await state.update_data(ip_cache=ips, ip_sel=[])
    ok = len(targets) - failed
    head = f"✅ Удалено: <b>{ok}</b>" + (f"  ·  ❌ Ошибок: {failed}" if failed else "")
    await _cb_edit(cb,
        f"{head}\n\n{txt_ips(ips, acc['name'], 0)}",
        reply_markup=kb_ip_list(ips, idx, 0, set()))


@router.callback_query(F.data.startswith("ip:del:"))
async def cb_ip_del(cb: CallbackQuery, state: FSMContext) -> None:
    """Single-IP delete (existing flow)."""
    if not _ok(cb): return
    parts = cb.data.split(":")
    idx, ip_id, ip_addr = int(parts[2]), parts[3], parts[4]
    confirmed = len(parts) == 6 and parts[5] == "yes"
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer("Нет", show_alert=True); return
    if not confirmed:
        await _cb_edit(cb, f"🗑 Удалить сервер <code>{ip_addr}</code>?",
                       reply_markup=kb_del_ip(idx, ip_id, ip_addr))
        await cb.answer(); return
    await cb.answer("⏳")
    acc = accs[idx]
    c = _make_client(acc)
    try:
        await c.delete_reglet(ip_id)
        ips = await c.list_reglets()
        data = await state.get_data()
        sel = set(data.get("ip_sel", []))
        sel.discard(ip_id)
        await state.update_data(ip_cache=ips, ip_sel=list(sel))
        await _cb_edit(cb, f"✅ Удален\n\n{txt_ips(ips, acc['name'], 0)}",
                       reply_markup=kb_ip_list(ips, idx, 0, sel))
    except Exception as e:
        await _cb_edit(cb, f"❌ <code>{e}</code>",
                       reply_markup=kb_back_only(f"ips:list:{idx}"))
    finally:
        await c.close()


# ───────────────────────── Settings ─────────────────────────

_SETTINGS_HDR = f"⚙️ <b>Настройки</b>\n{_HR}"

async def _render_settings(cb: CallbackQuery) -> None:
    s = await storage.get_settings()
    await _cb_edit(cb, _SETTINGS_HDR, reply_markup=kb_settings(s))

@router.callback_query(F.data == "menu:settings")
async def cb_settings(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    await state.clear()
    await _render_settings(cb)
    await cb.answer()

@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery) -> None:
    await cb.answer()

@router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(cb: CallbackQuery) -> None:
    if not _ok(cb): return
    key = cb.data.split(":", 1)[1]
    if key not in ("no_rl_wait", "show_errors"):
        await cb.answer(); return
    s = await storage.get_settings()
    new_val = not bool(s.get(key, False))
    await storage.update_setting(key, new_val)
    if _hunt.active and key == "show_errors":
        _hunt.show_errors = new_val
    await _render_settings(cb)
    await cb.answer("🟢 Вкл" if new_val else "🔴 Выкл")

@router.callback_query(F.data.startswith("set:"))
async def cb_set(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    key = cb.data[4:]
    meta = SETTINGS_META.get(key)
    if not meta:
        await cb.answer(); return
    label, _, hint = meta
    s = await storage.get_settings()
    await state.clear()
    await state.set_state(EditSetting.waiting)
    await state.update_data(setting_key=key)
    await _cb_edit(cb,
        f"⚙️ <b>{label}</b>\nСейчас: <code>{s.get(key, '—')}</code>\n\n{hint}",
        reply_markup=kb_cancel("menu:settings"))
    await cb.answer()

@router.message(EditSetting.waiting)
async def set_val(msg: Message, state: FSMContext) -> None:
    if not _ok(msg): return
    data = await state.get_data()
    key: str = data.get("setting_key", "")
    meta = SETTINGS_META.get(key)
    if not meta:
        await state.clear(); return
    label, typ, _ = meta
    raw = (msg.text or "").strip()
    for_account = bool(data.get("for_account"))
    acc_name = data.get("acc_name", "")
    acc_idx = int(data.get("acc_idx", 0))

    # "0" in per-account mode = clear override (inherit from globals)
    if for_account and raw == "0":
        await storage.set_account_setting(acc_name, key, None)
        await state.clear()
        await msg.answer(f"🔄 {label}: сброшено к глобальному",
                         reply_markup=kb_back_only(f"acc:settings:{acc_idx}"),
                         parse_mode="HTML")
        return

    try:
        v = typ(raw)
        if v <= 0: raise ValueError
    except (ValueError, TypeError):
        await msg.answer("❌ Положительное число"); return
    if key == "update_interval" and v < 5:
        v = typ(5)

    if for_account:
        await storage.set_account_setting(acc_name, key, v)
        await state.clear()
        await msg.answer(f"✅ {label} → <code>{v}</code> (для {acc_name})",
                         reply_markup=kb_back_only(f"acc:settings:{acc_idx}"),
                         parse_mode="HTML")
    else:
        await storage.update_setting(key, v)
        if _hunt.active:
            if key == "attempts_per_minute": _hunt.target_rpm = int(v)
            elif key == "update_interval":    _hunt.update_interval = float(v)
        await state.clear()
        s = await storage.get_settings()
        await msg.answer(f"✅ {label} → <code>{v}</code>",
                         reply_markup=kb_settings(s), parse_mode="HTML")


# ───────────────────────── Hunt ─────────────────────────

async def _render_hunt_select(cb: CallbackQuery, state: FSMContext, sel: list[str]) -> None:
    accs = await storage.get_accounts()
    s = await storage.get_settings()
    await state.update_data(selected=sel)
    await _cb_edit(cb,
        txt_hunt_select(accs, sel,
                        int(s.get("attempts_per_minute", 3)),
                        int(s.get("concurrency_per_account", 1))),
        reply_markup=kb_hunt_sel(accs, sel))

@router.callback_query(F.data == "hunt:start")
async def cb_hunt_start(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    if _hunt.active:
        await cb.answer("Уже идёт", show_alert=True); return
    accs = await storage.get_accounts()
    if not accs:
        await cb.answer("Нет аккаунтов", show_alert=True); return
    sel = [a["name"] for a in accs]
    await state.set_state(HuntSelect.choosing)
    await _render_hunt_select(cb, state, sel)
    await cb.answer()

@router.callback_query(F.data.startswith("hunt:toggle:"))
async def cb_htoggle(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    idx = int(cb.data.split(":")[2])
    accs = await storage.get_accounts()
    if idx >= len(accs):
        await cb.answer(); return
    data = await state.get_data()
    sel: list[str] = list(data.get("selected", []))
    name = accs[idx]["name"]
    if name in sel: sel.remove(name)
    else:           sel.append(name)
    await _render_hunt_select(cb, state, sel)
    await cb.answer()

@router.callback_query(F.data == "hunt:selall")
async def cb_hselall(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    accs = await storage.get_accounts()
    await _render_hunt_select(cb, state, [a["name"] for a in accs])
    await cb.answer()

@router.callback_query(F.data == "hunt:selnone")
async def cb_hselnone(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    await _render_hunt_select(cb, state, [])
    await cb.answer()

@router.callback_query(F.data == "hunt:go")
async def cb_hunt_go(cb: CallbackQuery, state: FSMContext) -> None:
    if not _ok(cb): return
    if _hunt.active:
        await cb.answer("Уже идёт", show_alert=True); return
    data = await state.get_data()
    sel_names = set(data.get("selected", []))
    accs = await storage.get_accounts()
    chosen = [a for a in accs if a["name"] in sel_names]
    if not chosen:
        await cb.answer("Выберите аккаунты", show_alert=True); return

    s = await storage.get_settings()
    # Global-only settings (used outside per-worker context)
    upd      = float(s["update_interval"])
    tout     = float(s["operation_timeout"])

    await state.clear()
    await cb.answer("🚀")

    _hunt.workers.clear(); _hunt.tasks.clear(); _hunt.stats.clear()
    _hunt.clients.clear(); _hunt.msg_id = None
    _hunt.chat_id         = cb.message.chat.id
    _hunt.target_rpm      = int(s["attempts_per_minute"])
    _hunt.update_interval = upd
    _hunt.show_errors     = bool(s.get("show_errors", False))
    _hunt.active          = True

    bot = cb.message.bot

    async def on_found(st: WorkerStats) -> None:
        try:
            await bot.send_message(_hunt.chat_id, txt_found(st), parse_mode="HTML")
        except Exception as e:
            print(f"[bot] found alert err: {e}")

    # Reset per-token quota semaphores — avoids stuck slots from a
    # previous (possibly crashed) hunt run.
    for acc in chosen:
        reset_token_quota(acc["api_token"])

    # nsrv workers per (account, region) — each with its OWN effective settings
    # (per-account overrides win over globals).
    #
    # IMPORTANT: the per-token quota semaphore enforces Reg.ru's hard limit.
    # Even if the user misconfigures concurrency * regions * nsrv > quota,
    # only `quota` workers will actually create a server at a time.
    for acc in chosen:
        eff  = storage.effective_settings(acc, s)
        rpm  = int(eff["attempts_per_minute"])
        conc = int(eff["concurrency_per_account"])
        nsrv = max(1, int(eff.get("servers_per_region", 1)))
        back = float(eff["error_backoff"])
        rlw  = float(eff.get("rate_limit_wait", 10.0))
        norl = bool(eff.get("no_rl_wait", False))

        # Account quota = concurrency × servers × regions (upper bound).
        # User controls this via concurrency_per_account — it's the KEY knob.
        acc_quota = conc

        regs = _acc_regions(acc)
        for region in regs:
            for i in range(nsrv):
                client = RegRuClient(
                    api_token=acc["api_token"],
                    region=region,
                    operation_timeout=tout,
                    quota=acc_quota,
                )
                _hunt.clients.append(client)
                parts = [acc["name"]]
                if len(regs) > 1:
                    parts.append(_rshort(region))
                if nsrv > 1:
                    parts.append(f"#{i+1}")
                wname = "  ·  ".join(parts)
                st = WorkerStats(account_name=wname, target_ip=acc["target_ip"])
                _hunt.stats.append(st)
                _hunt.workers.append(IPWorker(
                    client=client, stats=st,
                    attempts_per_minute=rpm, concurrency=conc,
                    on_found=on_found, error_backoff=back,
                    rate_limit_wait=rlw, no_rl_wait=norl,
                ))

    # Startup message — each account shows its effective settings
    def _acc_summary(a: dict) -> str:
        eff = storage.effective_settings(a, s)
        badge = " 🟢" if a.get("settings") else ""
        return (f"  👤 {a['name']}{badge} · {_regions_compact(a)}"
                f" · 🎯 {a['target_ip']}"
                f" · ⚡{eff['attempts_per_minute']}rpm"
                f" · 🔀{eff['concurrency_per_account']}")
    lines = "\n".join(_acc_summary(a) for a in chosen)
    nworkers = len(_hunt.workers)
    await bot.send_message(_hunt.chat_id,
        f"🚀 <b>Охота</b>\n{_HR}\n"
        f"👥 {len(chosen)} акк · 🖥 {nworkers} воркеров\n"
        f"{lines}",
        parse_mode="HTML")

    # Snapshot of pre-existing hunt-* reglets — one call per unique
    # (token, region).  These are PROTECTED: quota guard won't kill
    # them and emergency cleanup won't touch them.
    seen_keys: set[tuple[str, str]] = set()
    uniq_clients: list[RegRuClient] = []
    for c in _hunt.clients:
        key = (c._token, c._region)          # type: ignore[attr-defined]
        if key in seen_keys:
            continue
        seen_keys.add(key)
        uniq_clients.append(c)

    async def _snapshot(c: RegRuClient) -> int:
        try:
            reglets = await c.list_reglets()
        except Exception as e:
            print(f"[bot] snapshot err: {e}")
            return 0
        pre = {
            r["id"] for r in reglets
            if r.get("name", "").startswith("hunt-")
            and (not r.get("region_slug") or r["region_slug"] == c._region)
        }
        if pre:
            add_preexisting(c._token, c._region, pre)   # type: ignore[attr-defined]
            print(f"[bot] Pre-existing in {c._region}: {len(pre)} — will NOT be deleted")
        return len(pre)

    snap_counts = await asyncio.gather(
        *(_snapshot(c) for c in uniq_clients), return_exceptions=True)
    total_pre = sum(n for n in snap_counts if isinstance(n, int))
    if total_pre:
        await bot.send_message(_hunt.chat_id,
            f"🔒 Найдено <b>{total_pre}</b> старых серверов — они сохранены.",
            parse_mode="HTML")

    # Card
    try:
        m = await bot.send_message(_hunt.chat_id,
            txt_card(_hunt.stats, rpm, _hunt.show_errors),
            parse_mode="HTML")
        _hunt.msg_id = m.message_id
    except Exception as e:
        print(f"[bot] card init err: {e}")

    # Start
    for w in _hunt.workers:
        _hunt.tasks.append(asyncio.create_task(w.run()))
    _hunt.updater     = asyncio.create_task(_updater(bot))
    _hunt.supervisor  = asyncio.create_task(_supervisor(bot))
    _hunt.quota_guard = asyncio.create_task(_quota_guard_loop())

    await _cb_edit(cb, txt_main(accs, s, True), reply_markup=kb_main(True))

@router.callback_query(F.data == "hunt:stop")
async def cb_hunt_stop(cb: CallbackQuery) -> None:
    if not _ok(cb): return
    if not _hunt.active:
        await cb.answer("Не запущена", show_alert=True); return
    await _stop_hunt(cb.message.bot)
    accs = await storage.get_accounts()
    s = await storage.get_settings()
    await _cb_edit(cb, txt_main(accs, s, False), reply_markup=kb_main(False))
    await cb.answer("⏹")


# ───────────────────────── Hunt internals ─────────────────────────

async def _stop_hunt(bot: Optional[Bot] = None) -> None:
    """Signal workers to stop.  Cleanup happens in supervisor."""
    _hunt.active = False
    for w in _hunt.workers:
        w.stop()
    if _hunt.updater:
        _hunt.updater.cancel()
    if _hunt.quota_guard:
        _hunt.quota_guard.cancel()
    if bot and _hunt.chat_id and _hunt.msg_id:
        await _safe_edit(bot, _hunt.chat_id, _hunt.msg_id,
                         txt_card(_hunt.stats, _hunt.target_rpm, _hunt.show_errors))

async def _updater(bot: Bot) -> None:
    while _hunt.active:
        await asyncio.sleep(_hunt.update_interval)
        if not _hunt.active: break
        if _hunt.msg_id and _hunt.chat_id:
            await _safe_edit(bot, _hunt.chat_id, _hunt.msg_id,
                             txt_card(_hunt.stats, _hunt.target_rpm, _hunt.show_errors))

async def _quota_guard_loop() -> None:
    """Every 45s: scan each (token, region) and kill leaked hunt-* reglets.

    Catches leaks from worker crashes, network flakes, or delete polling
    giving up.  A "leak" = reglet that exists in Reg.ru but isn't in our
    active registry.
    """
    try:
        await asyncio.sleep(30.0)   # initial grace
        while _hunt.active:
            seen: set[tuple[str, str]] = set()
            for c in _hunt.clients:
                key = (c._token, c._region)   # type: ignore[attr-defined]
                if key in seen:
                    continue
                seen.add(key)
                try:
                    await c.guard_sweep()
                except Exception as e:
                    print(f"[quota-guard] err: {e}")
                if not _hunt.active:
                    return
            # 45s between scans
            for _ in range(45):
                if not _hunt.active:
                    return
                await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        pass

async def _supervisor(bot: Bot) -> None:
    try:
        await asyncio.gather(*_hunt.tasks, return_exceptions=True)
    finally:
        was = _hunt.active
        _hunt.active = False
        if _hunt.updater:
            _hunt.updater.cancel()
        if _hunt.quota_guard:
            _hunt.quota_guard.cancel()

        # IDs to keep: reglets where the target IP was found
        keep_by_key: dict[tuple[str, str], set[str]] = {}
        for c, st in zip(_hunt.clients, _hunt.stats):
            if st.found and st.found_id:
                k = (c._token, c._region)   # type: ignore[attr-defined]
                keep_by_key.setdefault(k, set()).add(st.found_id)

        # Emergency cleanup — ONE call per unique (token, region),
        # killing ALL hunt-* except "found" ones.  Idempotent + waits
        # for full disappearance.
        seen: set[tuple[str, str]] = set()
        for c in _hunt.clients:
            k = (c._token, c._region)   # type: ignore[attr-defined]
            if k in seen:
                continue
            seen.add(k)
            try:
                n = await c.emergency_cleanup(keep_ids=keep_by_key.get(k, set()))
                if n:
                    print(f"[bot] emergency_cleanup {k}: killed {n}")
            except Exception as e:
                print(f"[bot] emergency cleanup err: {e}")

        if was and _hunt.chat_id and _hunt.msg_id:
            await _safe_edit(bot, _hunt.chat_id, _hunt.msg_id,
                             txt_card(_hunt.stats, _hunt.target_rpm, _hunt.show_errors))
        for c in _hunt.clients:
            try: await c.close()
            except Exception: pass


# ───────────────────────── Fallback + build ─────────────────────────

@router.callback_query()
async def cb_fallback(cb: CallbackQuery) -> None:
    await cb.answer()

def build(token: str, user_id: int) -> tuple[Bot, Dispatcher]:
    global _OWNER
    _OWNER = user_id
    bot = Bot(token=token)
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    return bot, dp
