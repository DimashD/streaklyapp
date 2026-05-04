"""
Streakly Bot v2 — aiogram 3.x
Добавлено: Kaspi QR оплата, ИИ-инсайты, реферальная программа, Strava подключение
"""
import asyncio
import logging
import os
import secrets
from datetime import datetime, timedelta, date
from typing import Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove,
    LabeledPrice, PreCheckoutQuery,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncpg
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379")
WEBAPP_URL   = os.getenv("WEBAPP_URL", "https://streakly.app")
API_URL      = os.getenv("API_URL", "https://api.streakly.app")
ADMIN_IDS    = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

HABIT_DEFS = {
    "run":       {"icon": "🏃", "name": "Бег",       "defTime": "07:00"},
    "book":      {"icon": "📚", "name": "Чтение",    "defTime": "21:00"},
    "water":     {"icon": "💧", "name": "Вода",      "defTime": "09:00"},
    "meditate":  {"icon": "🧘", "name": "Медитация", "defTime": "06:30"},
    "sleep":     {"icon": "😴", "name": "Сон",       "defTime": "22:30"},
    "journal":   {"icon": "✍️", "name": "Дневник",  "defTime": "22:00"},
    "nutrition": {"icon": "🥗", "name": "Питание",   "defTime": "12:00"},
}

PLANS = {
    "monthly": {"price": 2990,  "days": 30,  "label": "Стандарт",  "desc": "2 990 ₸/месяц"},
    "yearly":  {"price": 1990,  "days": 365, "label": "Годовой",   "desc": "1 990 ₸/мес · скидка 33%"},
    "family":  {"price": 5990,  "days": 30,  "label": "Семья × 5", "desc": "5 990 ₸/месяц"},
}

# ══════════════════════════════════════════════════════
# FSM STATES
# ══════════════════════════════════════════════════════
class Onboarding(StatesGroup):
    consent       = State()
    habits        = State()
    phone         = State()

class RunLog(StatesGroup):
    distance  = State()
    duration  = State()

class BookLog(StatesGroup):
    action    = State()  # 'pages' | 'new_title'
    pages     = State()
    new_title = State()
    new_pages = State()

class Support(StatesGroup):
    message = State()

# ══════════════════════════════════════════════════════
# DB HELPERS
# ══════════════════════════════════════════════════════
pool: Optional[asyncpg.Pool] = None

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              BIGINT PRIMARY KEY,
                username        TEXT,
                first_name      TEXT,
                phone           TEXT,
                is_pro          BOOLEAN DEFAULT FALSE,
                pro_until       TIMESTAMPTZ,
                pro_plan        TEXT,
                referral_code   TEXT UNIQUE,
                referred_by     BIGINT,
                consent_given   BOOLEAN DEFAULT FALSE,
                consent_at      TIMESTAMPTZ,
                strava_id       TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS habits (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                type            TEXT NOT NULL,
                name            TEXT NOT NULL,
                icon            TEXT,
                reminder_time   TIME,
                reminder_on     BOOLEAN DEFAULT TRUE,
                streak          INT DEFAULT 0,
                best_streak     INT DEFAULT 0,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS checkins (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                habit_id        INT REFERENCES habits(id) ON DELETE CASCADE,
                date            DATE NOT NULL,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(habit_id, date)
            );
            CREATE TABLE IF NOT EXISTS run_log (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                date            DATE NOT NULL,
                distance_km     NUMERIC(6,2),
                duration_sec    INT,
                source          TEXT DEFAULT 'manual',
                strava_id       TEXT UNIQUE,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS books (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                title           TEXT NOT NULL,
                author          TEXT,
                total_pages     INT,
                pages_read      INT DEFAULT 0,
                is_done         BOOLEAN DEFAULT FALSE,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS payments (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                plan            TEXT NOT NULL,
                amount          INT NOT NULL,
                status          TEXT DEFAULT 'pending',
                kaspi_order_id  TEXT UNIQUE,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                paid_at         TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS referral_rewards (
                id              SERIAL PRIMARY KEY,
                referrer_id     BIGINT REFERENCES users(id) ON DELETE CASCADE,
                referred_id     BIGINT REFERENCES users(id) ON DELETE CASCADE,
                days_added      INT DEFAULT 7,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(referred_id)
            );
            CREATE TABLE IF NOT EXISTS ai_insights (
                user_id         BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                content         TEXT NOT NULL,
                generated_at    TIMESTAMPTZ DEFAULT NOW(),
                valid_until     TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_checkins_user_date ON checkins(user_id, date);
            CREATE INDEX IF NOT EXISTS idx_habits_user        ON habits(user_id);
        """)
    logger.info("✅ Database initialized")

async def get_or_create_user(tg_user) -> dict:
    ref_code = secrets.token_urlsafe(8)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO users (id, username, first_name, referral_code)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE
                SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, updated_at=NOW()
            RETURNING *
        """, tg_user.id, tg_user.username, tg_user.first_name, ref_code)
    return dict(row)

async def get_habits(user_id: int) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM habits WHERE user_id=$1 ORDER BY created_at", user_id)
    return [dict(r) for r in rows]

async def get_today_done(user_id: int) -> set:
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT habit_id FROM checkins WHERE user_id=$1 AND date=CURRENT_DATE
        """, user_id)
    return {r["habit_id"] for r in rows}

async def do_checkin(user_id: int, habit_id: int) -> int:
    """Чекин + пересчёт стрика. Возвращает новый стрик."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO checkins (user_id, habit_id, date)
            VALUES ($1, $2, CURRENT_DATE) ON CONFLICT DO NOTHING
        """, user_id, habit_id)
        rows  = await conn.fetch(
            "SELECT date FROM checkins WHERE habit_id=$1 ORDER BY date DESC", habit_id
        )
        dates = [r["date"] for r in rows]
        streak = _streak(dates)
        await conn.execute("""
            UPDATE habits SET streak=$1, best_streak=GREATEST(best_streak,$1)
            WHERE id=$2
        """, streak, habit_id)
    return streak

def _streak(dates: list) -> int:
    if not dates:
        return 0
    today, yesterday = date.today(), date.today() - timedelta(days=1)
    if dates[0] != today and dates[0] != yesterday:
        return 0
    n, exp = 0, dates[0]
    for d in dates:
        if d == exp:
            n += 1
            exp = exp - timedelta(days=1)
        else:
            break
    return n

# ══════════════════════════════════════════════════════
# KEYBOARDS
# ══════════════════════════════════════════════════════
def kb_today(habits: list, done: set) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for h in habits:
        ok   = h["id"] in done
        fire = f" 🔥{h['streak']}" if h["streak"] > 0 else ""
        b.button(
            text=f"{'✅' if ok else '⬜'} {h['icon']} {h['name']}{fire}",
            callback_data=f"ci:{h['id']}"
        )
    b.button(text="📊 Статистика",     callback_data="stats")
    b.button(text="⚔️ Рейтинг",        callback_data="social")
    b.button(text="🌐 Открыть приложение", url=WEBAPP_URL)
    b.adjust(1, 1, 2, 1)
    return b.as_markup()

def kb_main() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Отметить привычки", callback_data="today")
    b.button(text="📊 Статистика",        callback_data="stats")
    b.button(text="⚔️ Рейтинг",           callback_data="social")
    b.button(text="✨ Pro-подписка",       callback_data="upgrade")
    b.button(text="🔗 Подключить Strava",  callback_data="strava_connect")
    b.button(text="🤖 ИИ-инсайт",         callback_data="insight")
    b.button(text="🌐 Приложение",         url=WEBAPP_URL)
    b.adjust(1, 2, 2, 1, 1)
    return b.as_markup()

def kb_habits_select(selected: list) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for key, d in HABIT_DEFS.items():
        mark = "✅" if key in selected else "⬜"
        b.button(text=f"{mark} {d['icon']} {d['name']}", callback_data=f"hs:{key}")
    b.button(text=f"✅ Готово ({len(selected)} выбрано)", callback_data="hs:done")
    b.adjust(2, 2, 2, 1, 1)
    return b.as_markup()

def kb_plans() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for key, p in PLANS.items():
        b.button(text=f"{'⭐ ' if key=='yearly' else ''}{p['label']} — {p['desc']}", callback_data=f"pay:{key}")
    b.button(text="← Назад", callback_data="today")
    b.adjust(1, 1, 1, 1)
    return b.as_markup()

def kb_phone() -> ReplyKeyboardMarkup:
    b = ReplyKeyboardBuilder()
    b.button(text="📱 Поделиться номером", request_contact=True)
    b.button(text="Пропустить →")
    b.adjust(1)
    return b.as_markup(resize_keyboard=True, one_time_keyboard=True)

# ══════════════════════════════════════════════════════
# ROUTER
# ══════════════════════════════════════════════════════
router = Router()

# ── /start ──
@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    user = await get_or_create_user(msg.from_user)
    habits = await get_habits(msg.from_user.id)

    # Проверяем реферальный код в deep link: /start ref_CODE
    args = msg.text.split(" ", 1)
    if len(args) > 1 and args[1].startswith("ref_"):
        ref_code = args[1][4:]
        await state.update_data(ref_code=ref_code)

    if user.get("consent_given") and habits:
        # Повторный пользователь
        done  = await get_today_done(msg.from_user.id)
        total = len(habits)
        done_n = len(done)
        await msg.answer(
            f"🔥 С возвращением, {msg.from_user.first_name}!\n\n"
            f"Сегодня выполнено: {done_n}/{total}",
            reply_markup=kb_today(habits, done)
        )
    else:
        # Новый пользователь — онбординг
        await state.set_state(Onboarding.consent)
        await msg.answer(
            f"👋 Привет, *{msg.from_user.first_name}*!\n\n"
            "Я — *Streakly* 🔥 Твой трекер привычек.\n\n"
            "Помогу выработать нужные привычки за 21 день:\n"
            "ежедневные чекины, стрики, соревнования с друзьями\n"
            "и автоматическая синхронизация со Strava.\n\n"
            "━━━━━━━━━━━━━━\n"
            "Для работы мне нужно сохранить:\n"
            "• Твой Telegram ID и имя\n"
            "• Данные о привычках и чекинах\n"
            "• Время напоминаний\n\n"
            f"[Политика конфиденциальности]({WEBAPP_URL}/privacy.html) · "
            f"[Условия использования]({WEBAPP_URL}/terms.html)",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Принимаю и продолжаю", callback_data="consent_yes"),
                InlineKeyboardButton(text="❌ Отказаться",            callback_data="consent_no"),
            ]])
        )

@router.callback_query(F.data == "consent_yes", Onboarding.consent)
async def consent_yes(call: CallbackQuery, state: FSMContext):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET consent_given=TRUE, consent_at=NOW() WHERE id=$1
        """, call.from_user.id)
    await state.update_data(selected=[])
    await state.set_state(Onboarding.habits)
    await call.message.edit_text(
        "🎯 *Выбери привычки* которые хочешь отслеживать:\n\n"
        "_Можно добавить больше позже_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_habits_select([])
    )

@router.callback_query(F.data == "consent_no")
async def consent_no(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text(
        "Понял. Без согласия использование невозможно.\n"
        "Если передумаешь — напиши /start 🙂"
    )

@router.callback_query(F.data.startswith("hs:"), Onboarding.habits)
async def habit_select(call: CallbackQuery, state: FSMContext):
    key  = call.data[3:]
    data = await state.get_data()
    sel  = data.get("selected", [])

    if key == "done":
        if not sel:
            await call.answer("Выбери хотя бы одну привычку!", show_alert=True)
            return
        # Сохраняем привычки
        async with pool.acquire() as conn:
            for htype in sel:
                d = HABIT_DEFS[htype]
                t = datetime.strptime(d["defTime"], "%H:%M").time()
                await conn.execute("""
                    INSERT INTO habits (user_id, type, name, icon, reminder_time)
                    VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING
                """, call.from_user.id, htype, d["name"], d["icon"], t)
        await state.set_state(Onboarding.phone)
        await call.message.edit_text(
            f"🎯 Отлично! Выбрано {len(sel)} привычек.\n\n"
            "📱 *Поделись номером телефона* (необязательно)\n\n"
            "Это нужно для:\n"
            "• Восстановления доступа при потере Telegram\n"
            "• Синхронизации между устройствами\n\n"
            "_Мы не используем номер для рекламы_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=None
        )
        await call.message.answer(
            "Поделись номером или пропусти:",
            reply_markup=kb_phone()
        )
    else:
        if key in sel:
            sel.remove(key)
        else:
            sel.append(key)
        await state.update_data(selected=sel)
        await call.message.edit_reply_markup(reply_markup=kb_habits_select(sel))

@router.message(Onboarding.phone, F.contact)
async def phone_got(msg: Message, state: FSMContext):
    phone = msg.contact.phone_number
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET phone=$1 WHERE id=$2", phone, msg.from_user.id)
    await msg.answer("✅ Номер сохранён!", reply_markup=ReplyKeyboardRemove())
    await finish_onboarding(msg, state)

@router.message(Onboarding.phone)
async def phone_skip(msg: Message, state: FSMContext):
    await msg.answer("Хорошо, пропускаем.", reply_markup=ReplyKeyboardRemove())
    await finish_onboarding(msg, state)

async def finish_onboarding(msg: Message, state: FSMContext):
    data   = await state.get_data()
    ref_code = data.get("ref_code")
    await state.clear()

    # Применяем реф. код если был
    if ref_code:
        async with pool.acquire() as conn:
            referrer = await conn.fetchrow(
                "SELECT id, first_name FROM users WHERE referral_code=$1 AND id!=$2",
                ref_code, msg.from_user.id
            )
            if referrer:
                existing = await conn.fetchrow(
                    "SELECT id FROM referral_rewards WHERE referred_id=$1", msg.from_user.id
                )
                if not existing:
                    for uid in [msg.from_user.id, referrer["id"]]:
                        cur = await conn.fetchrow("SELECT pro_until FROM users WHERE id=$1", uid)
                        base = max(cur["pro_until"], datetime.now()) if cur["pro_until"] else datetime.now()
                        await conn.execute("""
                            UPDATE users SET is_pro=TRUE, pro_until=$1, updated_at=NOW() WHERE id=$2
                        """, base + timedelta(days=7), uid)
                    await conn.execute("""
                        INSERT INTO referral_rewards (referrer_id, referred_id, days_added)
                        VALUES ($1, $2, 7) ON CONFLICT DO NOTHING
                    """, referrer["id"], msg.from_user.id)
                    # Уведомляем реферера
                    try:
                        bot = Bot(token=BOT_TOKEN)
                        await bot.send_message(
                            referrer["id"],
                            f"🎁 *+7 дней Pro!*\n\nПо твоей ссылке зарегистрировался новый пользователь.\n🔥",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        await bot.session.close()
                    except Exception:
                        pass

    habits = await get_habits(msg.from_user.id)
    list_str = "\n".join(f"  {h['icon']} {h['name']} — ⏰ {h['reminder_time'].strftime('%H:%M')}" for h in habits)

    await msg.answer(
        f"🚀 *Добро пожаловать в Streakly!*\n\n"
        f"Твои привычки:\n{list_str}\n\n"
        f"🎁 *Первые 7 дней Pro — бесплатно!*\n"
        f"Напоминания придут в установленное время.\n\n"
        f"Начнём прямо сейчас?",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Отметить первые привычки!", callback_data="today")
        ]])
    )

# ── /today и чекины ──
@router.message(Command("today"))
@router.callback_query(F.data == "today")
async def show_today(event, **kw):
    msg     = event if isinstance(event, Message) else event.message
    uid     = event.from_user.id
    habits  = await get_habits(uid)
    if not habits:
        await msg.answer("У тебя нет привычек. Напиши /start")
        return
    done    = await get_today_done(uid)
    done_n  = len(done)
    total   = len(habits)
    today_s = datetime.now().strftime("%-d %B").lower()
    text = (
        f"🗓 *{today_s.capitalize()}*\n"
        f"Выполнено: {done_n}/{total}"
        + (" 🏆" if done_n == total else " 🔥" if done_n > 0 else "")
        + ("\n\n🎉 *Идеальный день! Все привычки выполнены!*" if done_n == total else "\n\nОтметь выполненные:")
    )
    kb = kb_today(habits, done)
    if isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        except Exception:
            await event.message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    else:
        await msg.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

@router.callback_query(F.data.startswith("ci:"))
async def do_ci(call: CallbackQuery):
    habit_id = int(call.data[3:])
    streak   = await do_checkin(call.from_user.id, habit_id)
    habits   = await get_habits(call.from_user.id)
    done     = await get_today_done(call.from_user.id)
    done_n   = len(done)
    total    = len(habits)

    h = next((x for x in habits if x["id"] == habit_id), None)
    msg_text = f"✅ {h['icon']} {h['name']} — стрик 🔥{streak} дн!" if h else "✅ Отмечено!"
    if done_n == total:
        msg_text = "🏆 ВСЕ привычки выполнены! Идеальный день!"
    await call.answer(msg_text, show_alert=(done_n == total))

    today_s = datetime.now().strftime("%-d %B").lower()
    text = (
        f"🗓 *{today_s.capitalize()}*\n"
        f"Выполнено: {done_n}/{total}"
        + (" 🏆" if done_n == total else " 🔥")
        + ("\n\n🎉 *Идеальный день!*" if done_n == total else "\n\nОтметь выполненные:")
    )
    try:
        await call.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb_today(habits, done))
    except Exception:
        pass

# ── /stats ──
@router.message(Command("stats"))
@router.callback_query(F.data == "stats")
async def show_stats(event, **kw):
    msg = event if isinstance(event, Message) else event.message
    uid = event.from_user.id
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT h.name, h.icon, h.streak, h.best_streak,
                   COUNT(c.id) AS done30
            FROM habits h
            LEFT JOIN checkins c ON c.habit_id=h.id
                AND c.date >= CURRENT_DATE - 30
            WHERE h.user_id=$1
            GROUP BY h.id, h.name, h.icon, h.streak, h.best_streak
            ORDER BY h.streak DESC
        """, uid)
    if not rows:
        await msg.answer("Нет привычек. Напиши /start")
        return
    max_streak     = max(r["streak"] for r in rows)
    total_checkins = sum(r["done30"] for r in rows)
    text = (
        f"📊 *Твоя статистика*\n\n"
        f"🔥 Лучший стрик: *{max_streak} дней*\n"
        f"✅ Чекинов за 30 дней: *{total_checkins}*\n\n"
    )
    for r in rows:
        pct = round(r["done30"] / 30 * 100)
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        text += f"{r['icon']} *{r['name']}*\n`{bar}` {pct}%  🔥{r['streak']} дн\n\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="← Назад", callback_data="today"),
        InlineKeyboardButton(text="🤖 ИИ-инсайт", callback_data="insight"),
    ]])
    if isinstance(event, CallbackQuery):
        await event.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    else:
        await msg.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

# ── /social ──
@router.message(Command("social"))
@router.callback_query(F.data == "social")
async def show_social(event, **kw):
    msg = event if isinstance(event, Message) else event.message
    uid = event.from_user.id
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT u.first_name, h.type, h.icon, h.name, h.streak, (u.id=$1) AS is_me
            FROM habits h JOIN users u ON u.id=h.user_id
            WHERE h.type IN (SELECT type FROM habits WHERE user_id=$1)
            ORDER BY h.type, h.streak DESC
        """, uid)
    if not rows:
        await msg.answer("Нет данных. Добавь привычки командой /start")
        return

    by_type: dict = {}
    for r in rows:
        by_type.setdefault(r["type"], []).append(r)

    medals = ["🥇", "🥈", "🥉"]
    text   = "⚔️ *Рейтинг по привычкам*\n\n"
    for htype, entries in by_type.items():
        text += f"{entries[0]['icon']} *{entries[0]['name']}*\n"
        for i, e in enumerate(entries[:5]):
            you   = " 👈 _ты_" if e["is_me"] else ""
            medal = medals[i] if i < 3 else f"{i+1}."
            text += f"  {medal} {e['first_name']} — 🔥{e['streak']} дн{you}\n"
        text += "\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="← Назад",        callback_data="today"),
        InlineKeyboardButton(text="👥 Пригласить",  switch_inline_query=""),
    ]])
    if isinstance(event, CallbackQuery):
        await event.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    else:
        await msg.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

# ── /run ──
@router.message(Command("run"))
async def cmd_run(msg: Message, state: FSMContext):
    await state.set_state(RunLog.distance)
    await msg.answer(
        "🏃 *Записать пробежку*\n\nВведи дистанцию в км:\n_Например: `5.2`_",
        parse_mode=ParseMode.MARKDOWN
    )

@router.message(RunLog.distance)
async def run_dist(msg: Message, state: FSMContext):
    try:
        dist = float(msg.text.replace(",", "."))
        if dist <= 0 or dist > 500:
            raise ValueError
    except ValueError:
        await msg.answer("⚠️ Введи число, например `5.2`")
        return
    await state.update_data(dist=dist)
    await state.set_state(RunLog.duration)
    await msg.answer(
        f"✅ {dist} км\n\nВведи время в формате `мм:сс` или `ч:мм:сс`:\n"
        "_Например: `28:30` или пропусти /skip_",
        parse_mode=ParseMode.MARKDOWN
    )

@router.message(RunLog.duration)
async def run_dur(msg: Message, state: FSMContext):
    data = await state.get_data()
    dist = data["dist"]
    raw  = msg.text.strip()

    dur_sec  = 0
    pace_str = "—"
    if raw and raw != "/skip":
        try:
            parts   = list(map(int, raw.split(":")))
            dur_sec = parts[-1] + parts[-2] * 60 + (parts[-3] * 3600 if len(parts) == 3 else 0)
            ppm     = dur_sec / dist
            pace_str = f"{int(ppm//60)}:{int(ppm%60):02d}"
        except Exception:
            await msg.answer("⚠️ Неверный формат. Введи `28:30` или /skip")
            return

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO run_log (user_id, date, distance_km, duration_sec)
            VALUES ($1, CURRENT_DATE, $2, $3)
        """, msg.from_user.id, dist, dur_sec or None)
        habit = await conn.fetchrow(
            "SELECT id FROM habits WHERE user_id=$1 AND type='run'", msg.from_user.id
        )
        if habit:
            streak = await do_checkin(msg.from_user.id, habit["id"])
        else:
            streak = 0

    await state.clear()
    await msg.answer(
        f"🏃 *Пробежка записана!*\n\n"
        f"📏 {dist} км" + (f" · ⏱ {raw}" if raw != "/skip" else "") + f" · ⚡ {pace_str}'/км\n"
        f"{'✅ Стрик «Бег»: 🔥' + str(streak) + ' дн!' if streak else ''}",
        parse_mode=ParseMode.MARKDOWN
    )

@router.message(Command("skip"), RunLog.duration)
async def run_skip(msg: Message, state: FSMContext):
    data = await state.get_data()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO run_log (user_id, date, distance_km) VALUES ($1, CURRENT_DATE, $2)
        """, msg.from_user.id, data["dist"])
        habit = await conn.fetchrow(
            "SELECT id FROM habits WHERE user_id=$1 AND type='run'", msg.from_user.id
        )
        if habit:
            await do_checkin(msg.from_user.id, habit["id"])
    await state.clear()
    await msg.answer(f"✅ {data['dist']} км записано!")

# ── ИИ-инсайты ──
@router.message(Command("insight"))
@router.callback_query(F.data == "insight")
async def show_insight(event, **kw):
    msg = event if isinstance(event, Message) else event.message
    uid = event.from_user.id

    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT is_pro FROM users WHERE id=$1", uid)

    if not user or not user["is_pro"]:
        text = (
            "🤖 *ИИ-инсайты* — функция Pro-плана\n\n"
            "Каждую неделю GPT-4o анализирует твои паттерны и даёт персональные советы:\n"
            "• В какие дни чаще пропускаешь\n"
            "• Что мешает регулярности\n"
            "• Конкретный план улучшения\n\n"
            "Попробуй 7 дней бесплатно 👇"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✨ Попробовать Pro бесплатно", callback_data="upgrade")
        ]])
        if isinstance(event, CallbackQuery):
            await event.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        else:
            await msg.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        return

    # Запрашиваем инсайт от бэкенда
    wait_msg = await msg.answer("🤖 Анализирую твои данные за 30 дней...")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Бэкенд авторизуется через user_id напрямую (internal call)
            resp = await client.get(
                f"{API_URL}/insights",
                headers={"X-Internal-User-Id": str(uid)}
            )
        if resp.status_code == 200:
            data    = resp.json()
            content = data["insights"]
        else:
            raise Exception(f"API returned {resp.status_code}")
    except Exception as e:
        logger.error(f"Insight error: {e}")
        content = (
            "📊 *Твой анализ за 30 дней*\n\n"
            "Ты хорошо справляешься с утренними привычками! "
            "Обрати внимание на вечерние — статистика показывает больше пропусков после 20:00. "
            "Попробуй упростить одну вечернюю привычку на следующей неделе — это сильнее чем добавлять новые."
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔄 Обновить", callback_data="insight_refresh"),
        InlineKeyboardButton(text="← Назад",    callback_data="today"),
    ]])
    try:
        await wait_msg.edit_text(
            f"🤖 *ИИ-инсайт недели*\n\n{content}",
            parse_mode=ParseMode.MARKDOWN, reply_markup=kb
        )
    except Exception:
        await msg.answer(f"🤖 *ИИ-инсайт недели*\n\n{content}", parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

@router.callback_query(F.data == "insight_refresh")
async def insight_refresh(call: CallbackQuery):
    """Сбросить кеш и получить новый инсайт"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{API_URL}/insights/refresh",
                              headers={"X-Internal-User-Id": str(call.from_user.id)})
    except Exception:
        pass
    await call.answer("🔄 Генерирую новый инсайт...")
    await show_insight(call)

# ── Оплата Kaspi ──
@router.message(Command("upgrade"))
@router.callback_query(F.data == "upgrade")
async def show_upgrade(event, **kw):
    msg = event if isinstance(event, Message) else event.message
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT is_pro, pro_until, pro_plan FROM users WHERE id=$1", event.from_user.id)

    if user and user["is_pro"] and user["pro_until"] and user["pro_until"] > datetime.now():
        until = user["pro_until"].strftime("%d.%m.%Y")
        text  = (
            f"✨ *У тебя уже есть Pro!*\n\n"
            f"Тариф: {user['pro_plan'] or 'monthly'}\n"
            f"Действует до: {until}\n\n"
            f"Все функции разблокированы 🚀"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="← Назад", callback_data="today")
        ]])
    else:
        text = (
            "✨ *Streakly Pro*\n\n"
            "Что открывается:\n"
            "🔗 Интеграция со Strava — авточекин при пробежке\n"
            "🤖 ИИ-инсайты каждую неделю\n"
            "📊 Тепловая карта и аналитика за год\n"
            "🔄 Синхронизация между устройствами\n"
            "⚔️ Рейтинг с друзьями по каждой привычке\n\n"
            "Выбери тариф:"
        )
        kb = kb_plans()

    if isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
        except Exception:
            await event.message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    else:
        await msg.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

@router.callback_query(F.data.startswith("pay:"))
async def initiate_payment(call: CallbackQuery):
    plan = call.data[4:]
    if plan not in PLANS:
        await call.answer("Неверный тариф", show_alert=True)
        return

    p = PLANS[plan]
    await call.answer()

    # Создаём платёж через бэкенд
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{API_URL}/payments/create",
                json={"plan": plan},
                headers={"X-Internal-User-Id": str(call.from_user.id)}
            )
        data = resp.json()
    except Exception as e:
        logger.error(f"Payment create error: {e}")
        await call.message.answer(
            f"⚠️ Временная ошибка. Попробуй позже или напиши в поддержку.",
        )
        return

    payment_id = data["paymentId"]
    amount     = data["amount"]
    kaspi_url  = data.get("kaspiQrUrl", "")

    text = (
        f"💳 *Оплата — {p['label']}*\n\n"
        f"Сумма: *{amount:,} ₸*\n"
        f"Номер заказа: `streakly-{payment_id}`\n\n"
        f"*Способы оплаты:*\n\n"
        f"1️⃣ *Kaspi.kz* — перейди по ссылке:\n{kaspi_url}\n\n"
        f"2️⃣ *Kaspi Pay* — переведи на номер продавца:\n"
        f"`+7 700 000 0000`\n"
        f"Комментарий: `streakly-{payment_id}`\n\n"
        f"После оплаты нажми *«Я оплатил»* — проверим вручную."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Открыть Kaspi QR", url=kaspi_url)],
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"paid_check:{payment_id}")],
        [InlineKeyboardButton(text="← Назад",             callback_data="upgrade")],
    ])
    await call.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

@router.callback_query(F.data.startswith("paid_check:"))
async def check_payment(call: CallbackQuery):
    payment_id = int(call.data.split(":")[1])
    await call.answer("🔍 Проверяем оплату...")

    async with pool.acquire() as conn:
        payment = await conn.fetchrow("SELECT * FROM payments WHERE id=$1", payment_id)

    if payment and payment["status"] == "paid":
        await call.message.edit_text(
            "🎉 *Оплата подтверждена! Pro активирован!*\n\n"
            "Все функции разблокированы. Начинай пользоваться 🚀",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🏠 Главная", callback_data="today")
            ]])
        )
    else:
        # Уведомляем админа для ручной проверки
        for admin_id in ADMIN_IDS:
            try:
                bot = call.message.bot
                await bot.send_message(
                    admin_id,
                    f"⚠️ *Запрос подтверждения оплаты*\n\n"
                    f"Пользователь: @{call.from_user.username or call.from_user.id}\n"
                    f"Payment ID: `{payment_id}`\n"
                    f"Сумма: {payment['amount'] if payment else '?'} ₸\n\n"
                    f"Проверь Kaspi и подтверди:",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm:{payment_id}:{call.from_user.id}")
                    ]])
                )
            except Exception:
                pass

        await call.message.edit_text(
            "⏳ *Оплата на проверке*\n\n"
            "Мы получили запрос и проверяем оплату вручную.\n"
            "Подписка будет активирована в течение 15 минут.\n\n"
            "Если что-то пошло не так — напиши в /support",
            parse_mode=ParseMode.MARKDOWN,
        )

@router.callback_query(F.data.startswith("admin_confirm:"))
async def admin_confirm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Нет доступа", show_alert=True)
        return
    _, payment_id, user_id = call.data.split(":")
    payment_id, user_id = int(payment_id), int(user_id)

    async with pool.acquire() as conn:
        payment = await conn.fetchrow("SELECT * FROM payments WHERE id=$1", payment_id)
        if not payment:
            await call.answer("Платёж не найден", show_alert=True)
            return
        plan      = payment["plan"]
        days      = PLANS.get(plan, PLANS["monthly"])["days"]
        pro_until = datetime.now() + timedelta(days=days)
        await conn.execute("""
            UPDATE users SET is_pro=TRUE, pro_until=$1, pro_plan=$2, updated_at=NOW() WHERE id=$3
        """, pro_until, plan, user_id)
        await conn.execute("""
            UPDATE payments SET status='paid', paid_at=NOW() WHERE id=$1
        """, payment_id)

    # Уведомляем пользователя
    try:
        p = PLANS[plan]
        await call.message.bot.send_message(
            user_id,
            f"🎉 *Pro активирован!*\n\n"
            f"Тариф: {p['label']}\n"
            f"До: {pro_until.strftime('%d.%m.%Y')}\n\n"
            f"Все функции разблокированы! 🚀",
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception:
        pass

    await call.message.edit_text(f"✅ Подписка активирована для user_id={user_id}")

# ── Strava ──
@router.callback_query(F.data == "strava_connect")
async def strava_connect(call: CallbackQuery):
    async with pool.acquire() as conn:
        token_row = await conn.fetchrow(
            "SELECT * FROM strava_tokens WHERE user_id=$1", call.from_user.id
        )
    if token_row:
        await call.message.edit_text(
            "🟠 *Strava уже подключена!*\n\n"
            "Пробежки автоматически засчитываются в привычку «Бег».",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Синхронизировать сейчас", callback_data="strava_sync")],
                [InlineKeyboardButton(text="🔌 Отключить Strava",        callback_data="strava_disconnect")],
                [InlineKeyboardButton(text="← Назад",                    callback_data="today")],
            ])
        )
    else:
        strava_url = (
            f"https://www.strava.com/oauth/authorize"
            f"?client_id={os.getenv('STRAVA_CLIENT_ID', '')}"
            f"&redirect_uri={API_URL}/auth/strava/callback"
            f"&response_type=code&approval_prompt=auto&scope=activity:read_all"
            f"&state={call.from_user.id}"
        )
        await call.message.edit_text(
            "🟠 *Подключить Strava*\n\n"
            "После подключения каждая пробежка в Strava будет автоматически:\n"
            "• Отмечать привычку «Бег»\n"
            "• Записывать км, время и темп\n"
            "• Присылать тебе уведомление\n\n"
            "Нажми кнопку ниже и разреши доступ:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🟠 Подключить Strava", url=strava_url)],
                [InlineKeyboardButton(text="← Назад",               callback_data="today")],
            ])
        )

@router.callback_query(F.data == "strava_sync")
async def strava_sync(call: CallbackQuery):
    await call.answer("🔄 Синхронизирую...")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            await client.post(f"{API_URL}/strava/sync",
                              headers={"X-Internal-User-Id": str(call.from_user.id)})
        await call.message.answer("✅ Strava синхронизирована!")
    except Exception:
        await call.message.answer("⚠️ Ошибка синхронизации. Попробуй позже.")

@router.callback_query(F.data == "strava_disconnect")
async def strava_disconnect(call: CallbackQuery):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM strava_tokens WHERE user_id=$1", call.from_user.id)
        await conn.execute("UPDATE users SET strava_id=NULL WHERE id=$1", call.from_user.id)
    await call.answer("Strava отключена")
    await show_today(call)

# ── Реферальная программа ──
@router.message(Command("refer"))
async def cmd_refer(msg: Message):
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT referral_code FROM users WHERE id=$1", msg.from_user.id)
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM referral_rewards WHERE referrer_id=$1", msg.from_user.id
        )
    ref_code = user["referral_code"] if user else "—"
    ref_link = f"https://t.me/{(await msg.bot.me()).username}?start=ref_{ref_code}"

    await msg.answer(
        f"🤝 *Реферальная программа*\n\n"
        f"Приглашай друзей — оба получаете *+7 дней Pro* 🎁\n\n"
        f"Твоя ссылка:\n`{ref_link}`\n\n"
        f"Друзей приглашено: *{count}*\n"
        f"Дней Pro заработано: *{count * 7}*\n\n"
        f"_Поделись ссылкой в чате, сторис или личке_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📤 Поделиться ссылкой", switch_inline_query=f"Присоединяйся к Streakly — трекеру привычек! {ref_link}")
        ]])
    )

# ── /export ──
@router.message(Command("export"))
@router.callback_query(F.data == "export")
async def export_data(event, **kw):
    msg = event if isinstance(event, Message) else event.message
    uid = event.from_user.id
    import json as jsonlib
    async with pool.acquire() as conn:
        habits   = await conn.fetch("SELECT * FROM habits WHERE user_id=$1", uid)
        checkins = await conn.fetch("SELECT * FROM checkins WHERE user_id=$1 ORDER BY date DESC", uid)
        runs     = await conn.fetch("SELECT * FROM run_log WHERE user_id=$1 ORDER BY date DESC", uid)
        books    = await conn.fetch("SELECT * FROM books WHERE user_id=$1", uid)

    def ser(rows):
        result = []
        for r in rows:
            d = dict(r)
            for k, v in d.items():
                if hasattr(v, 'isoformat'):
                    d[k] = v.isoformat()
            result.append(d)
        return result

    data = {
        "exported_at": datetime.now().isoformat(),
        "app": "Streakly v2",
        "habits":   ser(habits),
        "checkins": ser(checkins),
        "run_log":  ser(runs),
        "books":    ser(books),
    }
    json_bytes = jsonlib.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    from aiogram.types import BufferedInputFile
    await msg.answer_document(
        BufferedInputFile(json_bytes, filename=f"streakly_{date.today()}.json"),
        caption=f"✅ Экспорт: {len(checkins)} чекинов, {len(habits)} привычек, {len(runs)} пробежек"
    )

# ── /settings ──
@router.message(Command("settings"))
async def cmd_settings(msg: Message):
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE id=$1", msg.from_user.id)
    phone_s = f"✅ {user['phone']}" if user and user["phone"] else "❌ Не указан"
    pro_s   = f"✅ Pro до {user['pro_until'].strftime('%d.%m.%Y')}" if user and user["is_pro"] else "❌ Базовый план"
    await msg.answer(
        f"⚙️ *Настройки аккаунта*\n\n"
        f"👤 Имя: {msg.from_user.first_name}\n"
        f"📱 Телефон: {phone_s}\n"
        f"✨ Тариф: {pro_s}\n"
        f"📅 С нами с: {user['created_at'].strftime('%d.%m.%Y') if user else '—'}\n",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✨ Улучшить план",     callback_data="upgrade")],
            [InlineKeyboardButton(text="🤝 Пригласить друга",  callback_data="refer")],
            [InlineKeyboardButton(text="📤 Экспорт данных",    callback_data="export")],
            [InlineKeyboardButton(text="🔒 Политика конф.",    url=f"{WEBAPP_URL}/privacy.html")],
            [InlineKeyboardButton(text="🗑 Удалить аккаунт",   callback_data="delete_account")],
        ])
    )

@router.callback_query(F.data == "refer")
async def refer_cb(call: CallbackQuery):
    await cmd_refer(call.message)

@router.callback_query(F.data == "delete_account")
async def delete_confirm(call: CallbackQuery):
    await call.message.edit_text(
        "⚠️ *Удаление аккаунта*\n\n"
        "Это удалит ВСЕ твои данные:\n"
        "• Профиль, настройки\n"
        "• Все привычки и стрики\n"
        "• Историю чекинов\n"
        "• Пробежки и книги\n\n"
        "Перед удалением сделай /export\n\n"
        "Данные удалятся безвозвратно.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ Да, удалить всё",   callback_data="delete_confirmed")],
            [InlineKeyboardButton(text="← Отмена",              callback_data="today")],
        ])
    )

@router.callback_query(F.data == "delete_confirmed")
async def delete_confirmed(call: CallbackQuery):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE id=$1", call.from_user.id)
    await call.message.edit_text(
        "✅ Все данные удалены.\n\n"
        "Спасибо что пользовался Streakly!\n"
        "Если захочешь вернуться — /start 🙂"
    )

# ── /help ──
@router.message(Command("help"))
async def cmd_help(msg: Message):
    await msg.answer(
        "📖 *Команды Streakly*\n\n"
        "/today — отметить привычки\n"
        "/stats — моя статистика\n"
        "/social — рейтинг с друзьями\n"
        "/run — записать пробежку\n"
        "/insight — ИИ-анализ (Pro)\n"
        "/refer — пригласить друга (+7 дней Pro)\n"
        "/upgrade — Pro-подписка\n"
        "/settings — настройки аккаунта\n"
        "/export — скачать все данные\n"
        "/help — эта справка\n\n"
        f"🌐 [Открыть приложение]({WEBAPP_URL})",
        parse_mode=ParseMode.MARKDOWN
    )

# ══════════════════════════════════════════════════════
# SCHEDULER — напоминания
# ══════════════════════════════════════════════════════
async def send_reminders(bot: Bot):
    """Каждую минуту проверяем кому слать напоминание"""
    now_time = datetime.now().strftime("%H:%M")
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT h.user_id, h.id, h.name, h.icon, h.streak, u.first_name
            FROM habits h
            JOIN users u ON u.id=h.user_id
            WHERE h.reminder_on=TRUE
              AND TO_CHAR(h.reminder_time, 'HH24:MI')=$1
              AND h.id NOT IN (
                  SELECT habit_id FROM checkins WHERE date=CURRENT_DATE
              )
        """, now_time)

    for r in rows:
        try:
            await bot.send_message(
                r["user_id"],
                f"🔔 *{r['icon']} {r['name']}*\n\n"
                f"Привет, {r['first_name']}! Время выполнить привычку 💪\n"
                f"Стрик: 🔥{r['streak']} дней — не прерывай!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text=f"✅ Отметить {r['icon']}", callback_data=f"ci:{r['id']}"),
                    InlineKeyboardButton(text="⏰ Позже",                  callback_data="dismiss"),
                ]])
            )
        except Exception as e:
            logger.warning(f"Reminder failed for {r['user_id']}: {e}")

@router.callback_query(F.data == "dismiss")
async def dismiss_reminder(call: CallbackQuery):
    await call.answer("Напомним позже")
    await call.message.delete()

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
async def main():
    await init_db()

    bot     = Bot(token=BOT_TOKEN)
    storage = RedisStorage.from_url(REDIS_URL)
    dp      = Dispatcher(storage=storage)
    dp.include_router(router)

    scheduler = AsyncIOScheduler(timezone="Asia/Almaty")
    scheduler.add_job(
        send_reminders,
        trigger=CronTrigger(minute="*"),
        args=[bot]
    )
    scheduler.start()

    logger.info("🚀 Streakly Bot v2 starting...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
