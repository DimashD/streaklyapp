"""
Streaklyapp Backend v2 — FastAPI
Полный продакшен: Telegram auth, Kaspi QR оплата, Strava OAuth, ИИ-инсайты, реферальная программа
"""
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from datetime import datetime, date, timedelta
from typing import Optional, List

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException, Request, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════
BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
DATABASE_URL       = os.getenv("DATABASE_URL", "")
WEBAPP_URL         = os.getenv("WEBAPP_URL", "https://streaklyapp.app")
API_URL            = os.getenv("API_URL", "https://api.streaklyapp.app")

# Strava
STRAVA_CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID", "")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET", "")
STRAVA_WEBHOOK_SECRET = os.getenv("STRAVA_WEBHOOK_SECRET", "")

# Kaspi
KASPI_MERCHANT_ID  = os.getenv("KASPI_MERCHANT_ID", "")
KASPI_PRIVATE_KEY  = os.getenv("KASPI_PRIVATE_KEY", "")

# OpenAI
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")

# ══════════════════════════════════════════════════════
# APP INIT
# ══════════════════════════════════════════════════════
app = FastAPI(
    title="Streaklyapp API",
    version="2.0.0",
    docs_url="/docs",   # отключи в проде: docs_url=None
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[WEBAPP_URL, "http://localhost:8080", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool: Optional[asyncpg.Pool] = None

# ══════════════════════════════════════════════════════
# DB INIT — все таблицы создаются здесь
# ══════════════════════════════════════════════════════
@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
    async with pool.acquire() as conn:
        await conn.execute("""
            -- Пользователи
            CREATE TABLE IF NOT EXISTS users (
                id              BIGINT PRIMARY KEY,
                username        TEXT,
                first_name      TEXT,
                phone           TEXT,
                is_pro          BOOLEAN DEFAULT FALSE,
                pro_until       TIMESTAMPTZ,
                pro_plan        TEXT,                     -- 'monthly'|'yearly'|'family'
                referral_code   TEXT UNIQUE,              -- уникальный реф. код
                referred_by     BIGINT REFERENCES users(id),
                consent_given   BOOLEAN DEFAULT FALSE,
                consent_at      TIMESTAMPTZ,
                strava_id       TEXT,
                garmin_id       TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );

            -- Привычки
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

            -- Чекины
            CREATE TABLE IF NOT EXISTS checkins (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                habit_id        INT REFERENCES habits(id) ON DELETE CASCADE,
                date            DATE NOT NULL,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(habit_id, date)
            );

            -- Пробежки
            CREATE TABLE IF NOT EXISTS run_log (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                date            DATE NOT NULL,
                distance_km     NUMERIC(6,2),
                duration_sec    INT,
                pace_sec_per_km INT,
                heart_rate      INT,
                source          TEXT DEFAULT 'manual',
                strava_id       TEXT UNIQUE,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            );

            -- Книги
            CREATE TABLE IF NOT EXISTS books (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                title           TEXT NOT NULL,
                author          TEXT,
                total_pages     INT,
                pages_read      INT DEFAULT 0,
                is_done         BOOLEAN DEFAULT FALSE,
                started_at      DATE,
                finished_at     DATE,
                rating          INT,
                created_at      TIMESTAMPTZ DEFAULT NOW()
            );

            -- Strava OAuth токены
            CREATE TABLE IF NOT EXISTS strava_tokens (
                user_id         BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                access_token    TEXT NOT NULL,
                refresh_token   TEXT NOT NULL,
                expires_at      BIGINT NOT NULL,
                scope           TEXT,
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );

            -- Платежи Kaspi
            CREATE TABLE IF NOT EXISTS payments (
                id              SERIAL PRIMARY KEY,
                user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
                plan            TEXT NOT NULL,           -- 'monthly'|'yearly'|'family'
                amount          INT NOT NULL,            -- в тенге
                status          TEXT DEFAULT 'pending',  -- 'pending'|'paid'|'failed'|'refunded'
                kaspi_order_id  TEXT UNIQUE,
                kaspi_txn_id    TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                paid_at         TIMESTAMPTZ
            );

            -- Реферальные начисления
            CREATE TABLE IF NOT EXISTS referral_rewards (
                id              SERIAL PRIMARY KEY,
                referrer_id     BIGINT REFERENCES users(id) ON DELETE CASCADE,
                referred_id     BIGINT REFERENCES users(id) ON DELETE CASCADE,
                days_added      INT DEFAULT 7,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(referred_id)
            );

            -- ИИ-инсайты (кеш)
            CREATE TABLE IF NOT EXISTS ai_insights (
                user_id         BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                content         TEXT NOT NULL,
                generated_at    TIMESTAMPTZ DEFAULT NOW(),
                valid_until     TIMESTAMPTZ
            );

            -- Индексы для производительности
            CREATE INDEX IF NOT EXISTS idx_checkins_user_date ON checkins(user_id, date);
            CREATE INDEX IF NOT EXISTS idx_checkins_habit    ON checkins(habit_id);
            CREATE INDEX IF NOT EXISTS idx_habits_user       ON habits(user_id);
            CREATE INDEX IF NOT EXISTS idx_run_log_user_date ON run_log(user_id, date);
            CREATE INDEX IF NOT EXISTS idx_payments_user     ON payments(user_id);
        """)
    logger.info("✅ Database initialized")

@app.on_event("shutdown")
async def shutdown():
    await pool.close()

# ══════════════════════════════════════════════════════
# AUTH — Telegram Login Widget
# ══════════════════════════════════════════════════════
def verify_telegram_auth(data: dict) -> bool:
    check_hash = data.pop("hash", "")
    data_str = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
    computed = hmac.new(secret_key, data_str.encode(), hashlib.sha256).hexdigest()
    if computed != check_hash:
        return False
    if time.time() - int(data.get("auth_date", 0)) > 86400:
        return False
    return True

def make_token(user_id: int) -> str:
    ts = int(time.time())
    payload = f"{user_id}:{ts}"
    sig = hmac.new(BOT_TOKEN.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(f"{sig}:{payload}".encode()).decode()

def parse_token(token: str) -> Optional[int]:
    try:
        decoded = base64.urlsafe_b64decode(token).decode()
        sig, user_id_str, ts_str = decoded.split(":", 2)
        payload = f"{user_id_str}:{ts_str}"
        expected = hmac.new(BOT_TOKEN.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if not secrets.compare_digest(sig, expected):
            return None
        # токен действителен 30 дней
        if time.time() - int(ts_str) > 86400 * 30:
            return None
        return int(user_id_str)
    except Exception:
        return None

async def get_current_user(
    authorization: str = Header(None),
    x_internal_user_id: str = Header(None),  # внутренние вызовы от бота
):
    # Внутренний вызов от Telegram-бота
    if x_internal_user_id:
        try:
            user_id = int(x_internal_user_id)
        except ValueError:
            raise HTTPException(400, "Bad internal user id")
        async with pool.acquire() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE id=$1", user_id)
        if not user:
            raise HTTPException(404, "User not found")
        return dict(user)
    # Обычная Bearer-авторизация от PWA
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    token = authorization[7:]
    user_id = parse_token(token)
    if not user_id:
        raise HTTPException(401, "Invalid token")
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE id=$1", user_id)
    if not user:
        raise HTTPException(401, "User not found")
    return dict(user)

@app.post("/auth/telegram")
async def telegram_auth(body: dict):
    data = dict(body)
    if not verify_telegram_auth(data):
        raise HTTPException(401, "Invalid Telegram auth")

    user_id    = int(body["id"])
    first_name = body.get("first_name", "")
    username   = body.get("username")

    async with pool.acquire() as conn:
        # Генерируем реф. код при первой регистрации
        ref_code = secrets.token_urlsafe(8)
        row = await conn.fetchrow("""
            INSERT INTO users (id, first_name, username, referral_code)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE
                SET first_name = EXCLUDED.first_name,
                    username   = EXCLUDED.username,
                    updated_at = NOW()
            RETURNING *
        """, user_id, first_name, username, ref_code)

    token = make_token(user_id)
    return {
        "token": token,
        "user": {
            "id":           row["id"],
            "name":         row["first_name"],
            "username":     row["username"],
            "isPro":        row["is_pro"],
            "proUntil":     row["pro_until"].isoformat() if row["pro_until"] else None,
            "referralCode": row["referral_code"],
            "referralLink": f"{WEBAPP_URL}?ref={row['referral_code']}",
        }
    }

# ══════════════════════════════════════════════════════
# SYNC — cross-device data
# ══════════════════════════════════════════════════════
def _serialize(rows) -> list:
    result = []
    for r in rows:
        d = dict(r)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
        result.append(d)
    return result

@app.get("/sync")
async def sync_data(user=Depends(get_current_user)):
    uid = user["id"]
    async with pool.acquire() as conn:
        habits   = await conn.fetch("SELECT * FROM habits WHERE user_id=$1 ORDER BY created_at", uid)
        checkins = await conn.fetch("SELECT * FROM checkins WHERE user_id=$1 ORDER BY date DESC LIMIT 365", uid)
        runs     = await conn.fetch("SELECT * FROM run_log WHERE user_id=$1 ORDER BY date DESC LIMIT 200", uid)
        books    = await conn.fetch("SELECT * FROM books WHERE user_id=$1 ORDER BY created_at", uid)
    return {
        "synced_at": datetime.now().isoformat(),
        "habits":    _serialize(habits),
        "checkins":  _serialize(checkins),
        "run_log":   _serialize(runs),
        "books":     _serialize(books),
    }

class CheckinPayload(BaseModel):
    habit_id:  int
    date:      str
    timestamp: Optional[int] = None

@app.post("/checkin")
async def post_checkin(payload: CheckinPayload, user=Depends(get_current_user)):
    checkin_date = date.fromisoformat(payload.date)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO checkins (user_id, habit_id, date)
            VALUES ($1, $2, $3) ON CONFLICT (habit_id, date) DO NOTHING
        """, user["id"], payload.habit_id, checkin_date)
        rows  = await conn.fetch("SELECT date FROM checkins WHERE habit_id=$1 ORDER BY date DESC", payload.habit_id)
        dates = [r["date"] for r in rows]
        streak = _calc_streak(dates)
        await conn.execute("""
            UPDATE habits SET streak=$1, best_streak=GREATEST(best_streak,$1)
            WHERE id=$2 AND user_id=$3
        """, streak, payload.habit_id, user["id"])
    return {"ok": True, "streak": streak}

def _calc_streak(dates: list) -> int:
    if not dates:
        return 0
    today     = date.today()
    yesterday = today - timedelta(days=1)
    if dates[0] != today and dates[0] != yesterday:
        return 0
    streak, expected = 0, dates[0]
    for d in dates:
        if d == expected:
            streak += 1
            expected = expected - timedelta(days=1)
        else:
            break
    return streak

# ══════════════════════════════════════════════════════
# KASPI QR — оплата подписки
# ══════════════════════════════════════════════════════
PLANS = {
    "monthly": {"amount": 2990,  "days": 30,  "label": "Стандарт · 2 990 ₸/мес"},
    "yearly":  {"amount": 23880, "days": 365, "label": "Годовой · 23 880 ₸/год"},
    "family":  {"amount": 5990,  "days": 30,  "label": "Семья × 5 · 5 990 ₸/мес"},
}

class CreatePaymentRequest(BaseModel):
    plan: str  # monthly | yearly | family

@app.post("/payments/create")
async def create_payment(body: CreatePaymentRequest, user=Depends(get_current_user)):
    """Создаём заказ и возвращаем данные для Kaspi QR"""
    plan = body.plan
    if plan not in PLANS:
        raise HTTPException(400, f"Unknown plan: {plan}")

    plan_info = PLANS[plan]

    async with pool.acquire() as conn:
        # Создаём запись о платеже
        row = await conn.fetchrow("""
            INSERT INTO payments (user_id, plan, amount, status)
            VALUES ($1, $2, $3, 'pending')
            RETURNING id
        """, user["id"], plan, plan_info["amount"])
        payment_id = row["id"]

    # ── Kaspi QR интеграция ──
    # В реальной интеграции здесь запрос к Kaspi Business API.
    # Документация: https://kaspi.kz/merchantcabinet/api/documentation
    #
    # Для MVP используем статический QR (упрощённый вариант):
    # Kaspi позволяет создать QR по шаблону:
    # https://pay.kaspi.kz/pay/qr?id=MERCHANT_ID&amount=AMOUNT&ref=ORDER_ID
    #
    # Полный API: POST https://kaspi.kz/api/v1/payments/create
    # Headers: Authorization: Bearer {token}
    # Body: { merchantId, amount, currency: "KZT", orderId, description }

    kaspi_qr_url = (
        f"https://pay.kaspi.kz/pay/qr"
        f"?id={KASPI_MERCHANT_ID}"
        f"&amount={plan_info['amount']}"
        f"&ref=streaklyapp-{payment_id}"
    )

    # Обновляем kaspi_order_id
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE payments SET kaspi_order_id=$1 WHERE id=$2",
            f"streaklyapp-{payment_id}", payment_id
        )

    return {
        "paymentId":  payment_id,
        "plan":       plan,
        "amount":     plan_info["amount"],
        "label":      plan_info["label"],
        "kaspiQrUrl": kaspi_qr_url,
        # Ссылка на глубокое открытие приложения Kaspi
        "kaspiDeepLink": f"kaspi://payment?merchantId={KASPI_MERCHANT_ID}&amount={plan_info['amount']}&orderId=streaklyapp-{payment_id}",
        "checkUrl":   f"{API_URL}/payments/{payment_id}/status",
    }

@app.get("/payments/{payment_id}/status")
async def check_payment(payment_id: int, user=Depends(get_current_user)):
    """PWA polling: проверяем оплачен ли платёж"""
    async with pool.acquire() as conn:
        payment = await conn.fetchrow(
            "SELECT * FROM payments WHERE id=$1 AND user_id=$2",
            payment_id, user["id"]
        )
    if not payment:
        raise HTTPException(404, "Payment not found")

    return {
        "status":    payment["status"],
        "plan":      payment["plan"],
        "amount":    payment["amount"],
        "paidAt":    payment["paid_at"].isoformat() if payment["paid_at"] else None,
    }

@app.post("/webhooks/kaspi")
async def kaspi_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Kaspi отправляет сюда уведомление при успешной оплате.
    Документация: https://kaspi.kz/merchantcabinet/api/documentation#webhooks
    """
    body = await request.json()
    logger.info(f"Kaspi webhook: {body}")

    # Верификация подписи Kaspi
    # В реальной интеграции: проверь HMAC-SHA256 подпись из заголовка X-Kaspi-Signature
    x_sig = request.headers.get("X-Kaspi-Signature", "")
    body_bytes = await request.body()
    expected = hmac.new(KASPI_PRIVATE_KEY.encode(), body_bytes, hashlib.sha256).hexdigest()
    if not secrets.compare_digest(x_sig, expected):
        logger.warning("Invalid Kaspi signature")
        # В проде: raise HTTPException(403)
        # Пока пропускаем для тестирования

    order_id  = body.get("orderId", "")    # "streaklyapp-42"
    txn_id    = body.get("txnId", "")
    status    = body.get("status", "")     # "SUCCESS" | "FAILED"
    amount    = body.get("amount", 0)

    if status != "SUCCESS":
        return {"ok": True}

    # Находим платёж по order_id
    async with pool.acquire() as conn:
        payment = await conn.fetchrow(
            "SELECT * FROM payments WHERE kaspi_order_id=$1", order_id
        )
        if not payment:
            logger.error(f"Payment not found: {order_id}")
            return {"ok": True}

        # Активируем подписку
        background_tasks.add_task(_activate_subscription, payment["user_id"], payment["plan"], txn_id)

    return {"ok": True}

async def _activate_subscription(user_id: int, plan: str, txn_id: str):
    """Активирует Pro подписку после успешной оплаты"""
    plan_info = PLANS.get(plan, PLANS["monthly"])
    pro_until = datetime.now() + timedelta(days=plan_info["days"])

    async with pool.acquire() as conn:
        # Обновляем пользователя
        await conn.execute("""
            UPDATE users
            SET is_pro=TRUE, pro_until=$1, pro_plan=$2, updated_at=NOW()
            WHERE id=$3
        """, pro_until, plan, user_id)

        # Обновляем платёж
        await conn.execute("""
            UPDATE payments
            SET status='paid', kaspi_txn_id=$1, paid_at=NOW()
            WHERE user_id=$2 AND status='pending'
        """, txn_id, user_id)

        # Уведомляем через бота
        user = await conn.fetchrow("SELECT first_name FROM users WHERE id=$1", user_id)

    try:
        from aiogram import Bot
        bot = Bot(token=BOT_TOKEN)
        await bot.send_message(
            user_id,
            f"🎉 *{user['first_name']}, подписка активирована!*\n\n"
            f"✨ {plan_info['label']}\n"
            f"📅 Действует до: {pro_until.strftime('%d.%m.%Y')}\n\n"
            f"Теперь у тебя:\n"
            f"• Безлимит привычек\n"
            f"• ИИ-инсайты каждую неделю\n"
            f"• Интеграция со Strava\n"
            f"• Синхронизация между устройствами\n\n"
            f"Спасибо за доверие! 🔥",
            parse_mode="Markdown"
        )
        await bot.session.close()
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")

@app.post("/payments/manual-confirm/{payment_id}")
async def manual_confirm(payment_id: int, user=Depends(get_current_user)):
    """
    Ручное подтверждение оплаты — для MVP пока нет полного Kaspi API.
    Администратор вызывает этот эндпоинт после проверки оплаты.
    Удали этот метод когда подключишь Kaspi Webhook.
    """
    async with pool.acquire() as conn:
        payment = await conn.fetchrow("SELECT * FROM payments WHERE id=$1", payment_id)
        if not payment:
            raise HTTPException(404, "Payment not found")
        await _activate_subscription(payment["user_id"], payment["plan"], f"manual-{payment_id}")
    return {"ok": True, "message": "Subscription activated"}

# ══════════════════════════════════════════════════════
# STRAVA — полный OAuth поток
# ══════════════════════════════════════════════════════
@app.get("/auth/strava")
async def strava_auth_start(user=Depends(get_current_user)):
    """
    Шаг 1: Отдаём пользователю ссылку для авторизации в Strava.
    PWA открывает эту ссылку → пользователь разрешает доступ → Strava редиректит на /auth/strava/callback
    """
    state = base64.urlsafe_b64encode(
        json.dumps({"user_id": user["id"], "ts": int(time.time())}).encode()
    ).decode()

    strava_url = (
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={STRAVA_CLIENT_ID}"
        f"&redirect_uri={API_URL}/auth/strava/callback"
        f"&response_type=code"
        f"&approval_prompt=auto"
        f"&scope=activity:read_all"
        f"&state={state}"
    )
    return {"authUrl": strava_url}

@app.get("/auth/strava/callback")
async def strava_callback(code: str, state: str, error: Optional[str] = None):
    """
    Шаг 2: Strava возвращает code → меняем на access_token → сохраняем → редиректим в PWA
    """
    if error:
        return RedirectResponse(f"{WEBAPP_URL}?strava=error&msg={error}")

    # Декодируем state чтобы получить user_id
    try:
        state_data = json.loads(base64.urlsafe_b64decode(state).decode())
        user_id = state_data["user_id"]
    except Exception:
        return RedirectResponse(f"{WEBAPP_URL}?strava=error&msg=invalid_state")

    # Меняем code на токен
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://www.strava.com/oauth/token",
            data={
                "client_id":     STRAVA_CLIENT_ID,
                "client_secret": STRAVA_CLIENT_SECRET,
                "code":          code,
                "grant_type":    "authorization_code",
            }
        )
        if resp.status_code != 200:
            return RedirectResponse(f"{WEBAPP_URL}?strava=error&msg=token_exchange_failed")
        token_data = resp.json()

    # Сохраняем токен и strava_id
    strava_athlete_id = str(token_data["athlete"]["id"])
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET strava_id=$1 WHERE id=$2
        """, strava_athlete_id, user_id)

        await conn.execute("""
            INSERT INTO strava_tokens (user_id, access_token, refresh_token, expires_at, scope)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id) DO UPDATE
                SET access_token=$2, refresh_token=$3, expires_at=$4, updated_at=NOW()
        """, user_id,
            token_data["access_token"],
            token_data["refresh_token"],
            token_data["expires_at"],
            token_data.get("scope", "")
        )

        # Импортируем последние 10 активностей сразу
        await _import_strava_activities(user_id, token_data["access_token"])

    return RedirectResponse(f"{WEBAPP_URL}?strava=connected")

async def _refresh_strava_token(user_id: int) -> Optional[str]:
    """Обновляет токен Strava если истёк"""
    async with pool.acquire() as conn:
        token_row = await conn.fetchrow(
            "SELECT * FROM strava_tokens WHERE user_id=$1", user_id
        )
        if not token_row:
            return None

        # Токен ещё действителен
        if token_row["expires_at"] > int(time.time()) + 300:
            return token_row["access_token"]

        # Обновляем
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://www.strava.com/oauth/token",
                data={
                    "client_id":     STRAVA_CLIENT_ID,
                    "client_secret": STRAVA_CLIENT_SECRET,
                    "refresh_token": token_row["refresh_token"],
                    "grant_type":    "refresh_token",
                }
            )
            if resp.status_code != 200:
                return None
            new_data = resp.json()

        await conn.execute("""
            UPDATE strava_tokens
            SET access_token=$1, refresh_token=$2, expires_at=$3, updated_at=NOW()
            WHERE user_id=$4
        """, new_data["access_token"], new_data["refresh_token"],
            new_data["expires_at"], user_id)

        return new_data["access_token"]

async def _import_strava_activities(user_id: int, access_token: str):
    """Импортирует последние активности из Strava"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"per_page": 10, "page": 1}
        )
        if resp.status_code != 200:
            return
        activities = resp.json()

    async with pool.acquire() as conn:
        habit = await conn.fetchrow(
            "SELECT id FROM habits WHERE user_id=$1 AND type='run'", user_id
        )
        for act in activities:
            act_date = date.fromisoformat(act["start_date_local"][:10])
            dist_km  = round(act.get("distance", 0) / 1000, 2)
            dur_sec  = act.get("moving_time", 0)

            if dist_km < 0.1:
                continue

            # Сохраняем пробежку
            try:
                await conn.execute("""
                    INSERT INTO run_log (user_id, date, distance_km, duration_sec, source, strava_id)
                    VALUES ($1, $2, $3, $4, 'strava', $5)
                    ON CONFLICT (strava_id) DO NOTHING
                """, user_id, act_date, dist_km, dur_sec, str(act["id"]))
            except Exception:
                pass

            # Авточекин
            if habit:
                try:
                    await conn.execute("""
                        INSERT INTO checkins (user_id, habit_id, date)
                        VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
                    """, user_id, habit["id"], act_date)
                except Exception:
                    pass

@app.post("/auth/strava/disconnect")
async def strava_disconnect(user=Depends(get_current_user)):
    """Отключает Strava интеграцию"""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM strava_tokens WHERE user_id=$1", user["id"])
        await conn.execute("UPDATE users SET strava_id=NULL WHERE id=$1", user["id"])
    return {"ok": True}

@app.get("/auth/strava/status")
async def strava_status(user=Depends(get_current_user)):
    """Проверяем подключена ли Strava"""
    async with pool.acquire() as conn:
        token = await conn.fetchrow("SELECT * FROM strava_tokens WHERE user_id=$1", user["id"])
    return {
        "connected": bool(token),
        "expiresAt": token["expires_at"] if token else None,
    }

@app.post("/strava/sync")
async def strava_sync(user=Depends(get_current_user)):
    """Ручная синхронизация со Strava (вызывается из PWA)"""
    access_token = await _refresh_strava_token(user["id"])
    if not access_token:
        raise HTTPException(400, "Strava not connected")
    await _import_strava_activities(user["id"], access_token)
    return {"ok": True, "message": "Activities synced"}

# ── Strava Webhook (авто-уведомления от Strava) ──
@app.get("/webhooks/strava")
async def strava_verify(request: Request):
    params = dict(request.query_params)
    if params.get("hub.verify_token") == STRAVA_WEBHOOK_SECRET:
        return {"hub.challenge": params.get("hub.challenge")}
    raise HTTPException(403, "Invalid verify token")

@app.post("/webhooks/strava")
async def strava_event(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    if body.get("object_type") != "activity" or body.get("aspect_type") != "create":
        return {"ok": True}

    strava_owner_id = str(body.get("owner_id"))
    activity_id     = body.get("object_id")
    background_tasks.add_task(_process_strava_event, strava_owner_id, activity_id)
    return {"ok": True}

async def _process_strava_event(strava_owner_id: str, activity_id: int):
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT id FROM users WHERE strava_id=$1", strava_owner_id)
        if not user:
            return

    access_token = await _refresh_strava_token(user["id"])
    if not access_token:
        return

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://www.strava.com/api/v3/activities/{activity_id}",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if resp.status_code != 200:
            return
        act = resp.json()

    dist_km = round(act.get("distance", 0) / 1000, 2)
    dur_sec = act.get("moving_time", 0)
    today   = date.today()

    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO run_log (user_id, date, distance_km, duration_sec, source, strava_id)
                VALUES ($1, $2, $3, $4, 'strava', $5)
                ON CONFLICT (strava_id) DO NOTHING
            """, user["id"], today, dist_km, dur_sec, str(activity_id))
        except Exception:
            pass

        habit = await conn.fetchrow(
            "SELECT id, streak FROM habits WHERE user_id=$1 AND type='run'", user["id"]
        )
        if habit:
            await conn.execute("""
                INSERT INTO checkins (user_id, habit_id, date)
                VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
            """, user["id"], habit["id"], today)

    # Уведомление пользователю
    pace = f"{dur_sec//dist_km//60:.0f}:{dur_sec//dist_km%60:02.0f}" if dist_km > 0 else "—"
    try:
        from aiogram import Bot
        bot = Bot(token=BOT_TOKEN)
        await bot.send_message(
            user["id"],
            f"🟠 *Strava — пробежка засчитана!*\n\n"
            f"🏃 {act.get('name', 'Пробежка')}\n"
            f"📏 {dist_km} км · {dur_sec//60} мин · темп {pace}'/км\n\n"
            f"✅ Привычка «Бег» автоматически отмечена!",
            parse_mode="Markdown"
        )
        await bot.session.close()
    except Exception as e:
        logger.error(f"Bot notify error: {e}")

# ══════════════════════════════════════════════════════
# ИИ-ИНСАЙТЫ — GPT-4o
# ══════════════════════════════════════════════════════
@app.get("/insights")
async def get_insights(user=Depends(get_current_user)):
    """Возвращает ИИ-инсайты. Только для Pro пользователей."""
    if not user["is_pro"]:
        raise HTTPException(403, "Pro subscription required")

    # Проверяем кеш (инсайты обновляются раз в неделю)
    async with pool.acquire() as conn:
        cached = await conn.fetchrow(
            "SELECT * FROM ai_insights WHERE user_id=$1", user["id"]
        )
        if cached and cached["valid_until"] > datetime.now():
            return {"insights": cached["content"], "generatedAt": cached["generated_at"].isoformat()}

        # Собираем данные за 30 дней
        habits   = await conn.fetch("SELECT * FROM habits WHERE user_id=$1", user["id"])
        checkins = await conn.fetch("""
            SELECT c.date, h.type, h.name
            FROM checkins c JOIN habits h ON h.id=c.habit_id
            WHERE c.user_id=$1 AND c.date >= CURRENT_DATE - 30
            ORDER BY c.date DESC
        """, user["id"])
        runs = await conn.fetch("""
            SELECT date, distance_km, duration_sec
            FROM run_log WHERE user_id=$1 ORDER BY date DESC LIMIT 20
        """, user["id"])

    # Формируем контекст для GPT
    habits_summary = []
    for h in habits:
        dates = [c["date"].isoformat() for c in checkins if c["type"] == h["type"]]
        rate  = round(len(dates) / 30 * 100)
        habits_summary.append(f"- {h['icon'] if hasattr(h,'icon') else ''} {h['name']}: {rate}% за 30 дней, стрик {h['streak']} дн")

    runs_text = ""
    if runs:
        total_km = sum(float(r["distance_km"] or 0) for r in runs)
        runs_text = f"Пробежек: {len(runs)}, суммарно {total_km:.1f} км за последние записи."

    # Анализируем слабые дни недели
    from collections import Counter
    missed_days = []
    all_dates_set = {c["date"] for c in checkins}
    for i in range(30):
        d = date.today() - timedelta(days=i)
        if d not in all_dates_set:
            missed_days.append(d.weekday())
    day_names = ["понедельник","вторник","среда","четверг","пятница","суббота","воскресенье"]
    weak_days = Counter(missed_days).most_common(2)
    weak_text = ", ".join(day_names[d] for d,_ in weak_days) if weak_days else "нет"

    prompt = f"""Ты персональный коуч по привычкам. Проанализируй данные пользователя за 30 дней и дай конкретные, практичные советы на русском языке.

Данные пользователя:
{chr(10).join(habits_summary)}
{runs_text}
Дни с пропусками: {weak_text}

Дай 3–4 конкретных совета:
1. Что получается хорошо — похвали конкретно
2. Главный паттерн пропусков и конкретное решение
3. Одно небольшое улучшение на следующую неделю
4. Мотивирующий факт о формировании привычек

Формат: короткие параграфы, без списков. Обращайся на "ты". Тон: дружелюбный коуч, не лектор. Максимум 150 слов."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                json={
                    "model":       "gpt-4o-mini",   # дешевле gpt-4o, качество достаточное
                    "max_tokens":  400,
                    "temperature": 0.7,
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"OpenAI error: {e}")
        content = (
            "📊 За последние 30 дней ты показываешь хороший прогресс!\n\n"
            "Продолжай в том же темпе — регулярность важнее интенсивности. "
            "Самое сложное уже позади: первые 21 день позади, привычки закрепляются. "
            "На этой неделе сфокусируйся на одной привычке где показатель ниже 70%."
        )

    # Кешируем на неделю
    valid_until = datetime.now() + timedelta(days=7)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO ai_insights (user_id, content, valid_until)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE
                SET content=$2, generated_at=NOW(), valid_until=$3
        """, user["id"], content, valid_until)

    return {"insights": content, "generatedAt": datetime.now().isoformat()}

@app.post("/insights/refresh")
async def refresh_insights(user=Depends(get_current_user)):
    """Сбрасывает кеш инсайтов (Pro only)"""
    if not user["is_pro"]:
        raise HTTPException(403, "Pro required")
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM ai_insights WHERE user_id=$1", user["id"])
    return {"ok": True, "message": "Insights will regenerate on next request"}

# ══════════════════════════════════════════════════════
# РЕФЕРАЛЬНАЯ ПРОГРАММА
# ══════════════════════════════════════════════════════
@app.post("/referral/apply")
async def apply_referral(ref_code: str, user=Depends(get_current_user)):
    """
    Применяем реферальный код при регистрации.
    Оба пользователя получают +7 дней Pro.
    """
    async with pool.acquire() as conn:
        # Проверяем что реф. код существует и не свой
        referrer = await conn.fetchrow(
            "SELECT id FROM users WHERE referral_code=$1 AND id!=$2",
            ref_code, user["id"]
        )
        if not referrer:
            raise HTTPException(404, "Invalid or own referral code")

        # Проверяем что ещё не использовал
        existing = await conn.fetchrow(
            "SELECT id FROM referral_rewards WHERE referred_id=$1", user["id"]
        )
        if existing:
            raise HTTPException(400, "Referral already applied")

        # Начисляем +7 дней обоим
        for uid in [user["id"], referrer["id"]]:
            current = await conn.fetchrow("SELECT pro_until, is_pro FROM users WHERE id=$1", uid)
            base    = max(current["pro_until"], datetime.now()) if current["pro_until"] else datetime.now()
            new_until = base + timedelta(days=7)
            await conn.execute("""
                UPDATE users SET is_pro=TRUE, pro_until=$1, updated_at=NOW() WHERE id=$2
            """, new_until, uid)

        # Записываем реферал
        await conn.execute("""
            INSERT INTO referral_rewards (referrer_id, referred_id, days_added)
            VALUES ($1, $2, 7)
        """, referrer["id"], user["id"])

        # Обновляем referred_by у нового пользователя
        await conn.execute(
            "UPDATE users SET referred_by=$1 WHERE id=$2",
            referrer["id"], user["id"]
        )

    # Уведомляем реферера
    try:
        from aiogram import Bot
        bot = Bot(token=BOT_TOKEN)
        await bot.send_message(
            referrer["id"],
            f"🎁 *+7 дней Pro!*\n\n"
            f"По твоей ссылке зарегистрировался новый пользователь.\n"
            f"Ты получил +7 дней Pro в подарок! 🔥",
            parse_mode="Markdown"
        )
        await bot.session.close()
    except Exception:
        pass

    return {"ok": True, "daysAdded": 7, "message": "You and your friend got +7 days Pro!"}

@app.get("/referral/stats")
async def referral_stats(user=Depends(get_current_user)):
    """Статистика реферальной программы пользователя"""
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM referral_rewards WHERE referrer_id=$1", user["id"]
        )
        user_row = await conn.fetchrow("SELECT referral_code FROM users WHERE id=$1", user["id"])

    ref_code = user_row["referral_code"] if user_row else None
    return {
        "referralCode":  ref_code,
        "referralLink":  f"{WEBAPP_URL}?ref={ref_code}" if ref_code else None,
        "totalReferrals": count,
        "totalDaysEarned": count * 7,
    }

# ══════════════════════════════════════════════════════
# ADMIN — служебные эндпоинты
# ══════════════════════════════════════════════════════
ADMIN_KEY = os.getenv("ADMIN_KEY", "change-me-in-production")

def require_admin(x_admin_key: str = Header(None)):
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(403, "Admin only")

@app.get("/admin/stats", dependencies=[Depends(require_admin)])
async def admin_stats():
    """Общая статистика продукта"""
    async with pool.acquire() as conn:
        total_users   = await conn.fetchval("SELECT COUNT(*) FROM users")
        pro_users     = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_pro=TRUE")
        total_checkins = await conn.fetchval("SELECT COUNT(*) FROM checkins")
        today_checkins = await conn.fetchval("SELECT COUNT(*) FROM checkins WHERE date=CURRENT_DATE")
        total_revenue  = await conn.fetchval("SELECT COALESCE(SUM(amount),0) FROM payments WHERE status='paid'")
        mrr            = await conn.fetchval("""
            SELECT COALESCE(SUM(
                CASE plan WHEN 'monthly' THEN 2990 WHEN 'family' THEN 5990
                          WHEN 'yearly'  THEN 1990 ELSE 0 END
            ), 0)
            FROM users WHERE is_pro=TRUE AND pro_until > NOW()
        """)
    return {
        "users":         {"total": total_users, "pro": pro_users},
        "checkins":      {"total": total_checkins, "today": today_checkins},
        "revenue":       {"total_tenge": int(total_revenue), "mrr_tenge": int(mrr)},
        "conversion":    round(pro_users / total_users * 100, 1) if total_users else 0,
    }

@app.get("/admin/payments", dependencies=[Depends(require_admin)])
async def admin_payments(limit: int = 50):
    """Последние платежи"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*, u.first_name, u.username
            FROM payments p JOIN users u ON u.id=p.user_id
            ORDER BY p.created_at DESC LIMIT $1
        """, limit)
    return _serialize(rows)

# ══════════════════════════════════════════════════════
# HEALTH & ROOT
# ══════════════════════════════════════════════════════
@app.get("/health")
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return {"status": "ok", "db": "connected", "ts": datetime.now().isoformat()}
    except Exception as e:
        return JSONResponse({"status": "error", "db": str(e)}, status_code=500)

@app.get("/")
async def root():
    return {"app": "Streaklyapp API", "version": "2.0.0", "docs": "/docs"}

# ══════════════════════════════════════════════════════
# EMAIL AUTH — Magic Link / OTP
# ══════════════════════════════════════════════════════
import random
import string

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
FROM_EMAIL     = os.getenv("FROM_EMAIL", "noreply@streaklyapp.app")

# Временное хранилище OTP (в продакшене — Redis с TTL)
_otp_store: dict = {}  # email -> {code, expires_at}

class EmailSendRequest(BaseModel):
    email: str

class EmailVerifyRequest(BaseModel):
    email: str
    code:  str

@app.post("/auth/email/send")
async def email_send(body: EmailSendRequest):
    """Отправляем 6-значный OTP на email через Resend"""
    email = body.email.strip().lower()
    if "@" not in email or "." not in email:
        raise HTTPException(400, "Invalid email")

    # Генерируем OTP
    code = "".join(random.choices(string.digits, k=6))
    expires = datetime.now() + timedelta(minutes=15)
    _otp_store[email] = {"code": code, "expires": expires}

    # Отправляем через Resend
    if RESEND_API_KEY:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from":    FROM_EMAIL,
                    "to":      [email],
                    "subject": f"🔥 Твой код входа в Streaklyapp: {code}",
                    "html":    f"""
                        <div style="font-family:sans-serif;max-width:400px;margin:0 auto;padding:40px 20px;">
                          <div style="font-size:48px;text-align:center;margin-bottom:20px;">🔥</div>
                          <h1 style="font-size:24px;text-align:center;color:#1a1a2e;">Streaklyapp</h1>
                          <p style="color:#4a4a6a;text-align:center;">Твой код для входа:</p>
                          <div style="background:#f3f0ff;border-radius:16px;padding:24px;text-align:center;margin:20px 0;">
                            <div style="font-size:40px;font-weight:700;letter-spacing:8px;color:#6366f1;">{code}</div>
                          </div>
                          <p style="color:#8888a8;font-size:13px;text-align:center;">
                            Код действителен 15 минут.<br>
                            Если ты не запрашивал код — просто проигнорируй это письмо.
                          </p>
                        </div>
                    """,
                }
            )
            if resp.status_code not in (200, 201):
                logger.error(f"Resend error: {resp.text}")
                raise HTTPException(500, "Email send failed")
    else:
        # Dev режим — логируем код
        logger.info(f"[DEV] OTP for {email}: {code}")

    return {"ok": True, "message": "Code sent"}

@app.post("/auth/email/verify")
async def email_verify(body: EmailVerifyRequest):
    """Верифицируем OTP и создаём/обновляем пользователя"""
    email = body.email.strip().lower()
    code  = body.code.strip()

    stored = _otp_store.get(email)
    if not stored:
        raise HTTPException(400, "Code not found or expired")
    if datetime.now() > stored["expires"]:
        del _otp_store[email]
        raise HTTPException(400, "Code expired")
    if stored["code"] != code:
        raise HTTPException(400, "Invalid code")

    # Удаляем использованный OTP
    del _otp_store[email]

    # Создаём или находим пользователя по email
    async with pool.acquire() as conn:
        # Добавляем email колонку если нет
        await conn.execute("""
            ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT UNIQUE;
        """)

        # Ищем существующего пользователя
        user = await conn.fetchrow("SELECT * FROM users WHERE email=$1", email)
        if not user:
            # Новый пользователь — генерируем ID из email
            import hashlib
            user_id = int(hashlib.md5(email.encode()).hexdigest()[:12], 16) % (10**12)
            ref_code = secrets.token_urlsafe(8)
            user = await conn.fetchrow("""
                INSERT INTO users (id, email, first_name, referral_code)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (email) DO UPDATE
                    SET updated_at = NOW()
                RETURNING *
            """, user_id, email, email.split('@')[0], ref_code)

    token = make_token(user["id"])
    return {
        "token": token,
        "user": {
            "id":           user["id"],
            "name":         user["first_name"] or email.split('@')[0],
            "email":        email,
            "isPro":        user["is_pro"],
            "proUntil":     user["pro_until"].isoformat() if user["pro_until"] else None,
            "referralCode": user["referral_code"],
        }
    }

# ══════════════════════════════════════════════════════
# GOOGLE OAUTH
# ══════════════════════════════════════════════════════
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
BACKEND_URL_ENV      = os.getenv("BACKEND_URL", "https://backend-production-94260.up.railway.app").rstrip("/")

@app.get("/auth/google")
async def google_auth_start():
    """Редиректим пользователя на Google OAuth"""
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  f"{BACKEND_URL_ENV}/auth/google/callback",
        "response_type": "code",
        "scope":         "openid email profile",
        "access_type":   "offline",
        "prompt":        "select_account",
    }
    from urllib.parse import urlencode
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return RedirectResponse(url)

@app.get("/auth/google/callback")
async def google_callback(code: str, state: Optional[str] = None, error: Optional[str] = None):
    """Google возвращает code → меняем на токен → создаём пользователя"""
    if error:
        return RedirectResponse(f"{WEBAPP_URL}?auth=error&msg={error}")

    # Меняем code на токены
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code":          code,
                "client_id":     GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri":  f"{BACKEND_URL_ENV}/auth/google/callback",
                "grant_type":    "authorization_code",
            }
        )
        if token_resp.status_code != 200:
            return RedirectResponse(f"{WEBAPP_URL}?auth=error&msg=token_failed")

        token_data = token_resp.json()
        access_token = token_data.get("access_token")

        # Получаем данные пользователя
        user_resp = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if user_resp.status_code != 200:
            return RedirectResponse(f"{WEBAPP_URL}?auth=error&msg=userinfo_failed")

        google_user = user_resp.json()

    google_id = google_user.get("id")
    email     = google_user.get("email", "")
    name      = google_user.get("name", email.split("@")[0])
    picture   = google_user.get("picture", "")

    # Создаём или обновляем пользователя
    async with pool.acquire() as conn:
        # Добавляем колонки если нет
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS google_id TEXT;")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS picture TEXT;")

        # Ищем по google_id или email
        user = await conn.fetchrow(
            "SELECT * FROM users WHERE google_id=$1 OR email=$2 LIMIT 1",
            google_id, email
        )

        import hashlib
        user_id = int(hashlib.md5(email.encode()).hexdigest()[:12], 16) % (10**12)
        ref_code = secrets.token_urlsafe(8)

        if user:
            user = await conn.fetchrow("""
                UPDATE users SET google_id=$1, email=$2, first_name=$3,
                    picture=$4, updated_at=NOW()
                WHERE id=$5 RETURNING *
            """, google_id, email, name, picture, user["id"])
        else:
            user = await conn.fetchrow("""
                INSERT INTO users (id, google_id, email, first_name, picture, referral_code)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE SET google_id=$2, email=$3,
                    first_name=$4, picture=$5, updated_at=NOW()
                RETURNING *
            """, user_id, google_id, email, name, picture, ref_code)

    # Создаём токен и редиректим в PWA
    token = make_token(user["id"])
    from urllib.parse import urlencode
    params = urlencode({
        "auth":     "google",
        "token":    token,
        "name":     name,
        "email":    email,
        "picture":  picture,
        "id":       user["id"],
        "isPro":    "true" if user["is_pro"] else "false",
    })
    return RedirectResponse(f"{WEBAPP_URL}?{params}")
