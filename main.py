# ============================================================
# AI GRID INDONESIA — Unified Bot
# Investor Portal + Channel Broadcaster + Elon Ecosystem Updates
# ============================================================

import os
import re
import time
import hmac
import hashlib
import asyncio
import random
import logging
from collections import defaultdict
from contextlib import asynccontextmanager

import httpx
import feedparser
from fastapi import FastAPI, Request, Response
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

# ============================================================
# DATABASE
# ============================================================
from database import (
    init_db, create_investor, get_investor_by_id, get_investor_by_telegram,
    get_investor_by_contact, update_investor_login, update_investor_pin,
    record_payment, confirm_payment, attach_payment_to_investor,
    get_pending_payments_for_user, get_all_payments_for_investor,
    verify_pin,
)

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ============================================================
# ENVIRONMENT CONFIG
# ============================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
NOWPAYMENTS_API_KEY = os.environ.get("NOWPAYMENTS_API_KEY")
NOWPAYMENTS_IPN_SECRET = os.environ.get("NOWPAYMENTS_IPN_SECRET")
NOWPAYMENTS_API_URL = "https://api.nowpayments.io/v1"

TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID")
TELEGRAM_ADMIN_IDS_RAW = os.environ.get("TELEGRAM_ADMIN_IDS", "")
TELEGRAM_ADMIN_IDS = {int(x) for x in TELEGRAM_ADMIN_IDS_RAW.split(",") if x.strip().lstrip("-").isdigit()}

NETLIFY_URL = os.environ.get("NETLIFY_URL", "https://ai-gr.netlify.app")
CONTACT_EMAIL = "contactaigrid.id@gmail.com"

# ============================================================
# CONVERSATION STATES
# ============================================================
WAITING_CUSTOM_AMOUNT = 1
WAITING_REGISTER_CONTACT = 2
WAITING_LOGIN_CREDENTIALS = 3
WAITING_RECOVER_CONTACT = 4

# ============================================================
# ANTI-BOT CONFIG — INVESTOR SIDE
# ============================================================
INVOICE_COOLDOWN_SECONDS = 90
DAILY_INVOICE_CAP = 10
MAX_AMOUNT_USD = 10_000_000
MIN_AMOUNT_USD = 100
SUSPICIOUS_REPEAT_WINDOW = 300

PROCESSED_ORDERS = set()
PROCESSED_ORDERS_MAX = 5000

RATE_TRACKER = defaultdict(lambda: {
    "last_invoice_ts": 0.0,
    "daily_count": 0,
    "daily_reset_ts": time.time(),
    "recent_amounts": []
})

# ============================================================
# ANTI-BOT CONFIG — CHANNEL SIDE
# ============================================================
CHANNEL_POST_COOLDOWN = 60
CHANNEL_DAILY_POST_CAP = 30
CHANNEL_DEDUP_WINDOW_DAYS = 30

CHANNEL_STATE = {
    "last_post_ts": 0.0,
    "daily_count": 0,
    "daily_reset_ts": time.time(),
    "posted_hashes": {},
    "scheduler_paused": False,
}

# ============================================================
# CRYPTO MAPPING
# ============================================================
CRYPTO_MAP = {
    "usdttrc20": {"label": "USDT (TRC-20)", "ticker": "usdttrc20"},
    "usdterc20": {"label": "USDT (ERC-20)", "ticker": "usdterc20"},
    "usdcerc20": {"label": "USDC (ERC-20)", "ticker": "usdcerc20"},
    "usdcsol": {"label": "USDC (Solana)", "ticker": "usdcsol"},
    "btc": {"label": "Bitcoin (BTC)", "ticker": "btc"},
    "eth": {"label": "Ethereum (ETH)", "ticker": "eth"},
    "sol": {"label": "Solana (SOL)", "ticker": "sol"},
    "bnb": {"label": "BNB (BEP-20)", "ticker": "bnbmainnet"}
}

# ============================================================
# TELEGRAM APPLICATION
# ============================================================
telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

# ============================================================
# FASTAPI LIFESPAN
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_db()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"Database init failed: {e}")

    await telegram_app.initialize()
    await telegram_app.start()
    await telegram_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Telegram bot polling started.")

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(scheduled_channel_broadcast, "cron", hour=8, minute=0, id="morning_brief")
    scheduler.add_job(scheduled_channel_broadcast, "cron", hour=18, minute=0, id="evening_brief")
    scheduler.add_job(scheduled_weekly_roundup, "cron", day_of_week="sun", hour=18, minute=0, id="weekly_roundup")
    scheduler.start()
    logger.info("Scheduler started — UTC timezone.")

    await send_restart_announcement()

    yield

    scheduler.shutdown(wait=False)
    await telegram_app.updater.stop()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logger.info("Bot stopped.")

app = FastAPI(lifespan=lifespan)

# ============================================================
# SECURITY HELPERS
# ============================================================
def verify_nowpayments_signature(raw_body: bytes, signature_header: str) -> bool:
    if not NOWPAYMENTS_IPN_SECRET:
        logger.warning("NOWPAYMENTS_IPN_SECRET not configured.")
        return False
    if not signature_header:
        return False
    expected = hmac.new(
        NOWPAYMENTS_IPN_SECRET.encode(), raw_body, hashlib.sha512
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)

def is_duplicate_order(order_id: str) -> bool:
    if not order_id:
        return True
    if order_id in PROCESSED_ORDERS:
        return True
    PROCESSED_ORDERS.add(order_id)
    if len(PROCESSED_ORDERS) > PROCESSED_ORDERS_MAX:
        for _ in range(len(PROCESSED_ORDERS) - PROCESSED_ORDERS_MAX):
            PROCESSED_ORDERS.pop()
    return False

async def nowpayments_request_with_retry(payload: dict, headers: dict, max_attempts: int = 3) -> dict:
    last_error = None
    for attempt in range(max_attempts):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    f"{NOWPAYMENTS_API_URL}/payment",
                    json=payload, headers=headers
                )
                if response.status_code == 200:
                    return response.json()
                if response.status_code in [429, 500, 502, 503, 504]:
                    wait = 2 ** attempt
                    logger.warning(f"NOWPayments {response.status_code} — retry in {wait}s")
                    await asyncio.sleep(wait)
                    last_error = f"HTTP {response.status_code}"
                    continue
                return response.json()
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            wait = 2 ** attempt
            logger.warning(f"NOWPayments network error: {e} — retry in {wait}s")
            await asyncio.sleep(wait)
            last_error = str(e)
        except Exception as e:
            logger.error(f"NOWPayments exception: {e}")
            raise
    raise Exception(f"NOWPayments unavailable after {max_attempts} attempts. Last: {last_error}")

def check_user_rate_limit(user_id: int, amount: float) -> tuple[bool, str]:
    now = time.time()
    tracker = RATE_TRACKER[user_id]
    if now - tracker["daily_reset_ts"] > 86400:
        tracker["daily_count"] = 0
        tracker["daily_reset_ts"] = now
    if now - tracker["last_invoice_ts"] < INVOICE_COOLDOWN_SECONDS:
        wait = int(INVOICE_COOLDOWN_SECONDS - (now - tracker["last_invoice_ts"]))
        return False, f"⏱️ Please wait **{wait} seconds** between allocation attempts."
    if tracker["daily_count"] >= DAILY_INVOICE_CAP:
        return False, f"📊 Daily allocation limit reached. Try tomorrow or contact **{CONTACT_EMAIL}**."
    recent = [a for (ts, a) in tracker["recent_amounts"] if now - ts < SUSPICIOUS_REPEAT_WINDOW]
    if len(recent) >= 3 and all(abs(a - amount) < 0.01 for a in recent[-3:]):
        return False, f"🔒 Repeated identical allocations flagged. Contact **{CONTACT_EMAIL}**."
    if amount > MAX_AMOUNT_USD:
        return False, f"⚠️ Amount exceeds automated limit. Contact **{CONTACT_EMAIL}**."
    return True, ""

def record_invoice_attempt(user_id: int, amount: float):
    now = time.time()
    tracker = RATE_TRACKER[user_id]
    tracker["last_invoice_ts"] = now
    tracker["daily_count"] += 1
    tracker["recent_amounts"].append((now, amount))
    tracker["recent_amounts"] = tracker["recent_amounts"][-10:]

# ============================================================
# CHANNEL ANTI-BOT HELPERS
# ============================================================
def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]

def channel_can_post(content_text: str) -> tuple[bool, str]:
    now = time.time()
    state = CHANNEL_STATE
    if now - state["daily_reset_ts"] > 86400:
        state["daily_count"] = 0
        state["daily_reset_ts"] = now
    if now - state["last_post_ts"] < CHANNEL_POST_COOLDOWN:
        wait = int(CHANNEL_POST_COOLDOWN - (now - state["last_post_ts"]))
        return False, f"cooldown ({wait}s remaining)"
    if state["daily_count"] >= CHANNEL_DAILY_POST_CAP:
        return False, "daily cap reached"
    h = _content_hash(content_text)
    cutoff = now - (CHANNEL_DEDUP_WINDOW_DAYS * 86400)
    state["posted_hashes"] = {k: v for k, v in state["posted_hashes"].items() if v > cutoff}
    if h in state["posted_hashes"]:
        return False, "duplicate content within dedup window"
    return True, ""

def channel_mark_posted(content_text: str):
    now = time.time()
    CHANNEL_STATE["last_post_ts"] = now
    CHANNEL_STATE["daily_count"] += 1
    CHANNEL_STATE["posted_hashes"][_content_hash(content_text)] = now

async def safe_channel_send_photo(photo_url: str, caption: str, reply_markup=None):
    can, reason = channel_can_post(caption)
    if not can:
        logger.info(f"Channel post skipped: {reason}")
        return None
    for attempt in range(3):
        try:
            msg = await telegram_app.bot.send_photo(
                chat_id=TELEGRAM_CHANNEL_ID,
                photo=photo_url,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
            channel_mark_posted(caption)
            return msg
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                wait = 2 ** attempt
                logger.warning(f"Channel rate limit — retry in {wait}s")
                await asyncio.sleep(wait)
                continue
            logger.error(f"Channel photo send failed: {e}")
            return None
    return None

async def safe_channel_send_text(text: str, reply_markup=None):
    can, reason = channel_can_post(text)
    if not can:
        logger.info(f"Channel post skipped: {reason}")
        return None
    for attempt in range(3):
        try:
            msg = await telegram_app.bot.send_message(
                chat_id=TELEGRAM_CHANNEL_ID,
                text=text,
                parse_mode="Markdown",
                reply_markup=reply_markup,
                disable_web_page_preview=True
            )
            channel_mark_posted(text)
            return msg
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                wait = 2 ** attempt
                logger.warning(f"Channel rate limit — retry in {wait}s")
                await asyncio.sleep(wait)
                continue
            logger.error(f"Channel text send failed: {e}")
            return None
    return None

# ============================================================
# WEBHOOK — NOWPayments
# ============================================================
@app.post("/webhook/nowpayments")
async def nowpayments_webhook(request: Request):
    raw_body = await request.body()
    signature = request.headers.get("x-nowpayments-sig", "")

    if not verify_nowpayments_signature(raw_body, signature):
        logger.warning("Rejected webhook — invalid signature")
        return Response(status_code=401)

    try:
        data = await request.json()
    except Exception as e:
        logger.error(f"Invalid JSON: {e}")
        return Response(status_code=400)

    payment_status = data.get("payment_status")
    order_id = data.get("order_id")

    if is_duplicate_order(order_id):
        logger.info(f"Duplicate webhook: {order_id}")
        return Response(status_code=200)

    if payment_status in ["finished", "confirmed"] and order_id:
        try:
            telegram_user_id = int(order_id.split("_")[-1])
            logger.info(f"Payment confirmed: user {telegram_user_id}")
            await confirm_payment(order_id)
            keyboard = [[InlineKeyboardButton("📝 Register Your Allocation", callback_data=f"reg_start_{order_id}")]]
            await telegram_app.bot.send_message(
                chat_id=telegram_user_id,
                text=(
                    "🎉 **Payment Confirmed!**\n"
                    "───────────────────────────────\n"
                    "Your transaction has been verified on the blockchain.\n\n"
                    "To attach your allocation shares to your investor profile, "
                    "please register now. You'll receive a unique **Investor ID** and **PIN**."
                ),
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Webhook processing error: {e}")
    return Response(status_code=200)

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

# ============================================================
# INVESTOR COMMANDS
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "⚡ **AI GRID INDONESIA | Sovereign Compute Syndicate**\n"
        "───────────────────────────────\n"
        "Welcome to the official capital allocation portal for Batam's **50MW Tier-IV AI Compute Hub**.\n\n"
        "📍 **Batam SEZ, Indonesia** — 20km from Singapore, sub-2.5ms subsea latency.\n\n"
        "📊 **Key Financial Highlights:**\n"
        "• **Preferred Dividend:** 20.0% Cash Yield (Distributed every 30 days)\n"
        "• **Target Net IRR:** 42.5%\n"
        "• **Target MOIC:** 3.8x over 3 years\n"
        "• **Tiers from $1,000**\n"
        "• **Infrastructure:** Direct-to-chip liquid cooling, NVIDIA Blackwell-class\n\n"
        "Select an option below to explore or allocate capital:"
    )
    keyboard = [
        [InlineKeyboardButton("📈 View Investment Tiers", callback_data="show_tiers")],
        [InlineKeyboardButton("🧮 Interactive ROI Calculator", callback_data="show_calculator")],
        [InlineKeyboardButton("💳 Allocate Capital Now", callback_data="allocate_menu")],
        [InlineKeyboardButton("🔓 Log In to Portfolio", callback_data="login_start")],
        [InlineKeyboardButton("📢 Official Channel", url="https://t.me/xUniverseUpdates")],
        [InlineKeyboardButton("📩 Official Support", url="https://t.me/contactaigrid")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await update.callback_query.edit_message_text(welcome_text, reply_markup=reply_markup, parse_mode="Markdown")
    return ConversationHandler.END

async def show_tiers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tiers_text = (
        "💼 **AI Grid Capital Syndication Matrix**\n\n"
        "🔹 **Tier 1 — Micro | $1,000 – $4,999**\n"
        "• 20% Preferred Dividend\n• 30-Day Payout Cycle\n• Telegram Bot Access\n\n"
        "🔹 **Tier 2 — Syndicate | $5,000 – $24,999**\n"
        "• 20% Preferred Dividend\n• 42.5% Target Net IRR\n• Pro-rata Rights Phase 2\n\n"
        "🔹 **Tier 3 — Institutional | $25,000 – $99,999** ⭐ *Featured*\n"
        "• Priority Dividend Payout\n• 3.8x Target MOIC\n• Priority Allocation Phase 2\n\n"
        "🔹 **Tier 4 — Anchor | $100,000+**\n"
        "• Structured Equity / Debt\n• Dedicated GPU Compute\n• VIP Site Inspection\n• Direct Founding Team Access\n\n"
        "All tiers share the same 20.0% preferred return per 30-day cycle."
    )
    keyboard = [
        [InlineKeyboardButton("💳 Proceed to Allocation", callback_data="allocate_menu")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
    ]
    await query.edit_message_text(tiers_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def show_calculator(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    calc_text = (
        "🧮 **Yield Projections (20.0% Paid Every 30 Days)**\n\n"
        "• **$1,000:** $200.00 / 30 days ($2,400 / year)\n"
        "• **$5,000:** $1,000.00 / 30 days ($12,000 / year)\n"
        "• **$25,000:** $5,000.00 / 30 days ($60,000 / year)\n"
        "• **$100,000:** $20,000.00 / 30 days ($240,000 / year)\n\n"
        "📈 **3-Year Target MOIC:** 3.8x (projected)\n\n"
        "💡 *Targets, not guarantees. Capital is at risk.*"
    )
    keyboard = [
        [InlineKeyboardButton("💳 Allocate Capital Now", callback_data="allocate_menu")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
    ]
    await query.edit_message_text(calc_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def allocate_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    menu_text = "💳 **Select your allocation tier or enter a custom amount:**"
    keyboard = [
        [InlineKeyboardButton("$1,000 — Micro Entry", callback_data="amount_1000")],
        [InlineKeyboardButton("$5,000 — Syndicate Entry", callback_data="amount_5000")],
        [InlineKeyboardButton("$25,000 — Institutional Entry", callback_data="amount_25000")],
        [InlineKeyboardButton("$100,000 — Anchor Entry", callback_data="amount_100000")],
        [InlineKeyboardButton("✍️ Custom Amount", callback_data="amount_custom")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
    ]
    await query.edit_message_text(menu_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def select_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    amount_str = query.data.split("_")[1]
    context.user_data["invest_amount"] = float(amount_str)
    await show_crypto_selection(query, context)

async def prompt_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "✍️ **Custom Allocation Amount**\n\n"
        "Please reply with the exact USD amount you wish to allocate (e.g. `3500` or `75000`).\n\n"
        "*(Minimum: $100 USD for testing — full tiers start at $1,000)*",
        parse_mode="Markdown"
    )
    return WAITING_CUSTOM_AMOUNT

async def receive_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().replace("$", "").replace(",", "")
    try:
        val = float(text)
        if val < MIN_AMOUNT_USD:
            await update.message.reply_text(f"❌ Minimum is ${MIN_AMOUNT_USD:,} USD. Please enter a higher value:")
            return WAITING_CUSTOM_AMOUNT
        if val > MAX_AMOUNT_USD:
            await update.message.reply_text(f"⚠️ Amounts above ${MAX_AMOUNT_USD:,} require direct contact: **{CONTACT_EMAIL}**", parse_mode="Markdown")
            return WAITING_CUSTOM_AMOUNT
        context.user_data["invest_amount"] = val
        keyboard = [
            [InlineKeyboardButton("USDT (TRC-20)", callback_data="pay_usdttrc20"), InlineKeyboardButton("USDT (ERC-20)", callback_data="pay_usdterc20")],
            [InlineKeyboardButton("USDC (ERC-20)", callback_data="pay_usdcerc20"), InlineKeyboardButton("USDC (Solana)", callback_data="pay_usdcsol")],
            [InlineKeyboardButton("Bitcoin (BTC)", callback_data="pay_btc"), InlineKeyboardButton("Ethereum (ETH)", callback_data="pay_eth")],
            [InlineKeyboardButton("Solana (SOL)", callback_data="pay_sol"), InlineKeyboardButton("BNB (BEP-20)", callback_data="pay_bnb")],
            [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]
        ]
        await update.message.reply_text(
            f"✅ **Amount Set:** ${val:,.2f} USD\n\nSelect your cryptocurrency:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("⚠️ Invalid number. Enter a valid value (e.g., 2500):")
        return WAITING_CUSTOM_AMOUNT

async def show_crypto_selection(query, context: ContextTypes.DEFAULT_TYPE):
    amount = context.user_data.get("invest_amount", 1000)
    keyboard = [
        [InlineKeyboardButton("USDT (TRC-20)", callback_data="pay_usdttrc20"), InlineKeyboardButton("USDT (ERC-20)", callback_data="pay_usdterc20")],
        [InlineKeyboardButton("USDC (ERC-20)", callback_data="pay_usdcerc20"), InlineKeyboardButton("USDC (Solana)", callback_data="pay_usdcsol")],
        [InlineKeyboardButton("Bitcoin (BTC)", callback_data="pay_btc"), InlineKeyboardButton("Ethereum (ETH)", callback_data="pay_eth")],
        [InlineKeyboardButton("Solana (SOL)", callback_data="pay_sol"), InlineKeyboardButton("BNB (BEP-20)", callback_data="pay_bnb")],
        [InlineKeyboardButton("⬅️ Back", callback_data="allocate_menu")]
    ]
    await query.edit_message_text(
        f"💵 **Selected Allocation:** ${amount:,.2f} USD\n\nSelect your cryptocurrency:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )

async def generate_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    pay_key = query.data.replace("pay_", "")
    crypto_info = CRYPTO_MAP.get(pay_key, {"label": pay_key.upper(), "ticker": pay_key})
    user_id = query.from_user.id
    amount = context.user_data.get("invest_amount", 1000.0)

    allowed, reason = check_user_rate_limit(user_id, amount)
    if not allowed:
        await query.edit_message_text(reason, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="allocate_menu")]]))
        return

    await query.edit_message_text("🔄 **Connecting to blockchain gateway & generating payment invoice...**", parse_mode="Markdown")

    headers = {"x-api-key": NOWPAYMENTS_API_KEY, "Content-Type": "application/json"}
    order_id = f"aigrid_{amount:.0f}_{user_id}"
    payload = {
        "price_amount": float(amount),
        "price_currency": "usd",
        "pay_currency": crypto_info["ticker"],
        "order_id": order_id,
        "order_description": f"AI Grid Indonesia Allocation (${amount:,.2f} USD)"
    }

    try:
        data = await nowpayments_request_with_retry(payload, headers)
        if "pay_address" in data:
            pay_address = data["pay_address"]
            pay_amount = data["pay_amount"]
            pay_currency = data["pay_currency"].upper()

            context.user_data["pay_address"] = pay_address
            context.user_data["pay_amount"] = pay_amount
            context.user_data["pay_currency"] = pay_currency
            context.user_data["crypto_label"] = crypto_info["label"]

            try:
                await record_payment(
                    telegram_user_id=user_id,
                    order_id=order_id,
                    amount_usd=amount,
                    pay_currency=pay_currency,
                    pay_address=pay_address,
                )
            except Exception as e:
                logger.error(f"DB record failed: {e}")

            record_invoice_attempt(user_id, amount)

            invoice_text = (
                f"✅ **OFFICIAL ALLOCATION INVOICE**\n"
                f"───────────────────────────────\n"
                f"• **USD Value:** ${amount:,.2f} USD\n"
                f"• **Asset:** {crypto_info['label']}\n"
                f"• **Exact Amount to Send:** `{pay_amount}` **{pay_currency}**\n\n"
                f"📍 **Deposit Address:**\n`{pay_address}`\n\n"
                f"⚠️ Send the exact amount above. Participation is recorded once confirmed on-chain."
            )
            keyboard = [
                [InlineKeyboardButton("📱 QR Code", callback_data="show_qr")],
                [InlineKeyboardButton("🔄 Main Menu", callback_data="main_menu")],
                [InlineKeyboardButton("📩 Support", url="https://t.me/contactaigrid")]
            ]
            await query.edit_message_text(invoice_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        else:
            logger.error(f"NOWPayments Error: {data}")
            await query.edit_message_text("❌ **Gateway Timeout.** Try again or contact support.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Try Again", callback_data="allocate_menu")]]),
                parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Invoice exception: {e}")
        await query.edit_message_text("❌ Connection error. Please try again later.")

async def show_qr_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    pay_address = context.user_data.get("pay_address", "N/A")
    qr_code_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={pay_address}"
    qr_caption = (
        f"📱 **SCAN TO PAY**\n"
        f"───────────────────────────────\n"
        f"Scan with your crypto wallet to avoid typing errors.\n\n"
        f"📍 **Deposit Address:**\n`{pay_address}`"
    )
    keyboard = [[InlineKeyboardButton("🔙 Back to Invoice", callback_data="back_to_invoice")]]
    try:
        await query.message.delete()
    except Exception:
        pass
    await context.bot.send_photo(
        chat_id=query.from_user.id, photo=qr_code_url, caption=qr_caption,
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def back_to_invoice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    pay_address = context.user_data.get("pay_address", "N/A")
    pay_amount = context.user_data.get("pay_amount", "N/A")
    pay_currency = context.user_data.get("pay_currency", "BTC")
    crypto_label = context.user_data.get("crypto_label", "Crypto")
    amount = context.user_data.get("invest_amount", 1000.0)
    invoice_text = (
        f"✅ **OFFICIAL ALLOCATION INVOICE**\n"
        f"───────────────────────────────\n"
        f"• **USD Value:** ${amount:,.2f} USD\n"
        f"• **Asset:** {crypto_label}\n"
        f"• **Exact Amount to Send:** `{pay_amount}` **{pay_currency}**\n\n"
        f"📍 **Deposit Address:**\n`{pay_address}`\n\n"
        f"⚠️ Send the exact amount above."
    )
    keyboard = [
        [InlineKeyboardButton("📱 QR Code", callback_data="show_qr")],
        [InlineKeyboardButton("🔄 Main Menu", callback_data="main_menu")],
        [InlineKeyboardButton("📩 Support", url="https://t.me/contactaigrid")]
    ]
    try:
        await query.message.delete()
    except Exception:
        pass
    await context.bot.send_message(
        chat_id=query.from_user.id, text=invoice_text,
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ============================================================
# REGISTRATION FLOW
# ============================================================
def _gen_pin() -> str:
    return f"{random.SystemRandom().randint(100000, 999999)}"

def _gen_recovery_code() -> str:
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "-".join("".join(random.SystemRandom().choice(chars) for _ in range(3)) for _ in range(4))

def _valid_email(v: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", v))

def _valid_phone(v: str) -> bool:
    return bool(re.match(r"^\+?\d{7,15}$", v.replace(" ", "").replace("-", "")))

async def register_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    order_id = query.data.replace("reg_start_", "")
    context.user_data["pending_order_id"] = order_id
    keyboard = [
        [InlineKeyboardButton("📧 Register with Email", callback_data="reg_contact_email")],
        [InlineKeyboardButton("📱 Register with Phone", callback_data="reg_contact_phone")],
        [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]
    ]
    await query.edit_message_text(
        "📝 **Investor Registration**\n\n"
        "Choose how you'd like to register. You'll receive your unique **Investor ID** and **PIN**.",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

async def cmd_register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    existing = await get_investor_by_telegram(user_id)
    if existing:
        await update.message.reply_text(
            f"✅ You're already registered as **{existing.investor_id}**.\n\n"
            f"Use /login to access your portfolio.",
            parse_mode="Markdown"
        )
        return
    pending = await get_pending_payments_for_user(user_id)
    if not pending:
        await update.message.reply_text(
            "No confirmed payments awaiting registration.\n\n"
            "If you've just paid, wait 1–2 minutes for blockchain confirmation, then try /register again.",
            parse_mode="Markdown"
        )
        return
    context.user_data["pending_order_id"] = pending[0].order_id
    keyboard = [
        [InlineKeyboardButton("📧 Register with Email", callback_data="reg_contact_email")],
        [InlineKeyboardButton("📱 Register with Phone", callback_data="reg_contact_phone")],
        [InlineKeyboardButton("❌ Cancel", callback_data="main_menu")]
    ]
    await update.message.reply_text(
        "📝 **Investor Registration**\n\nChoose how you'd like to register:",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

async def register_contact_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    contact_type = "email" if query.data == "reg_contact_email" else "phone"
    context.user_data["reg_contact_type"] = contact_type
    if contact_type == "email":
        await query.edit_message_text("📧 **Please send your email address.**\n\nExample: `you@example.com`", parse_mode="Markdown")
    else:
        await query.edit_message_text("📱 **Please send your phone number.**\n\nWith country code. Example: `+6281234567890`", parse_mode="Markdown")
    return WAITING_REGISTER_CONTACT

async def register_receive_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    contact_type = context.user_data.get("reg_contact_type", "email")
    if contact_type == "email" and not _valid_email(text):
        await update.message.reply_text("⚠️ Invalid email format. Try again:")
        return WAITING_REGISTER_CONTACT
    if contact_type == "phone" and not _valid_phone(text):
        await update.message.reply_text("⚠️ Invalid phone format. Include country code, e.g. `+6281234567890`:", parse_mode="Markdown")
        return WAITING_REGISTER_CONTACT

    existing = await get_investor_by_contact(text)
    if existing:
        await update.message.reply_text(
            f"⚠️ This {contact_type} is already registered to **{existing.investor_id}**.\n\n"
            f"Use /login or /recover.",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    pin = _gen_pin()
    recovery_code = _gen_recovery_code()
    user_id = update.effective_user.id
    order_id = context.user_data.get("pending_order_id")

    try:
        investor = await create_investor(
            telegram_user_id=user_id, contact_type=contact_type,
            contact_value=text, pin=pin, recovery_code=recovery_code
        )
        if order_id:
            await attach_payment_to_investor(order_id, investor.investor_id)

        await update.message.reply_text(
            f"✅ **Registration Complete**\n"
            f"───────────────────────────────\n"
            f"• **Investor ID:** `{investor.investor_id}`\n"
            f"• **PIN:** `{pin}`\n"
            f"• **Recovery Code:** `{recovery_code}`\n"
            f"• **Registered:** `{text}`\n\n"
            f"⚠️ **Store these safely.** You'll use them to log in.\n\n"
            f"Use /login to access your portfolio.",
            parse_mode="Markdown"
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Registration failed: {e}")
        await update.message.reply_text(f"❌ Registration failed. Contact **{CONTACT_EMAIL}**.", parse_mode="Markdown")
        return ConversationHandler.END

# ============================================================
# LOGIN FLOW
# ============================================================
async def login_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🔓 **Portfolio Login**\n\n"
        "Please send your **Investor ID** and **PIN** separated by a space.\n\n"
        "Example: `AIG-2026-00001 482915`\n\n"
        "*(Forgot PIN? Use /recover)*"
    )
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, parse_mode="Markdown")
    return WAITING_LOGIN_CREDENTIALS

async def login_receive_credentials(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) != 2:
        await update.message.reply_text("⚠️ Format: `AIG-2026-00001 482915`. Try again:", parse_mode="Markdown")
        return WAITING_LOGIN_CREDENTIALS

    investor_id, pin = parts[0].upper(), parts[1]
    investor = await get_investor_by_id(investor_id)
    if not investor or not verify_pin(pin, investor.pin_hash):
        await update.message.reply_text("❌ Invalid Investor ID or PIN. Try again or use /recover.")
        return WAITING_LOGIN_CREDENTIALS

    await update_investor_login(investor_id)
    context.user_data["logged_in_investor"] = investor_id
    total = float(investor.total_allocated_usd or 0)
    monthly = total * 0.20

    keyboard = [
        [InlineKeyboardButton("💰 Deploy More Capital", callback_data="deploy_more")],
        [InlineKeyboardButton("📈 Transaction History", callback_data="tx_history")],
        [InlineKeyboardButton("🚪 Log Out", callback_data="logout")]
    ]
    await update.message.reply_text(
        f"📊 **Your Investor Profile**\n"
        f"───────────────────────────────\n"
        f"• **Investor ID:** `{investor.investor_id}`\n"
        f"• **Tier:** {investor.tier}\n"
        f"• **Total Allocated:** ${total:,.2f} USD\n"
        f"• **30-Day Payout:** ${monthly:,.2f}\n"
        f"• **Contact:** `{investor.contact_value}`\n\n"
        f"What would you like to do?",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )
    return ConversationHandler.END

async def logout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop("logged_in_investor", None)
    await query.edit_message_text("✅ Logged out. Use /login anytime to return.")

async def tx_history_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await query.edit_message_text("❌ Please log in first with /login.")
        return
    payments = await get_all_payments_for_investor(investor_id)
    if not payments:
        await query.edit_message_text("📭 No transactions yet.")
        return
    lines = [f"📈 **Transaction History — {investor_id}**\n"]
    for p in payments[:10]:
        lines.append(f"• ${float(p.amount_usd):,.2f} | {p.status.upper()} | {p.pay_currency.upper()}")
    await query.edit_message_text("\n".join(lines), parse_mode="Markdown")

async def deploy_more_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await query.edit_message_text("❌ Please log in first with /login.")
        return
    await allocate_menu(update, context)

# ============================================================
# RECOVERY FLOW
# ============================================================
async def recover_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔐 **PIN Recovery**\n\n"
        "Please send the **Email** or **Phone** you registered with.\n\n"
        "Example: `you@example.com` or `+6281234567890`",
        parse_mode="Markdown"
    )
    return WAITING_RECOVER_CONTACT

async def recover_receive_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    investor = await get_investor_by_contact(text)
    if not investor:
        await update.message.reply_text(f"❌ No investor found. Contact **{CONTACT_EMAIL}**.", parse_mode="Markdown")
        return ConversationHandler.END
    new_pin = _gen_pin()
    await update_investor_pin(investor.investor_id, new_pin)
    await update.message.reply_text(
        f"✅ **Identity Verified**\n"
        f"───────────────────────────────\n"
        f"• **Investor ID:** `{investor.investor_id}`\n"
        f"• **New PIN:** `{new_pin}`\n\n"
        f"Use /login to access your portfolio.",
        parse_mode="Markdown"
    )
    return ConversationHandler.END

async def cmd_login(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await login_start(update, context)

async def cmd_recover(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await recover_start(update, context)

# ============================================================
# INFO COMMANDS
# ============================================================
async def cmd_founder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👤 **The Founder — Shivon Zilis**\n"
        "───────────────────────────────\n"
        "• Yale — Economics & Philosophy\n• IBM — Cognitive Computing\n"
        "• Founding team, Bloomberg Beta\n• Forbes 30 Under 30 (2015)\n"
        "• OpenAI — founding adviser (2016), board member (2020–2023)\n"
        "• Tesla — Project Director, Autopilot & chip design (2017–2019)\n"
        "• Neuralink — Director of Operations & Special Projects\n\n"
        f"📩 Institutional inquiries: **{CONTACT_EMAIL}**"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "⚠️ **Risk Disclosure**\n"
        "───────────────────────────────\n"
        "• Private, forward-looking infrastructure investment.\n"
        "• **Capital is at risk.** No returns guaranteed.\n"
        "• All metrics are targets, not promises.\n"
        "• Illiquid — 3-year term.\n"
        "• Participation runs through **AI Grid Batam Infrastructure SPV**.\n\n"
        f"Full disclosure: {NETLIFY_URL}\n\n"
        f"📩 Diligence: **{CONTACT_EMAIL}**"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📊 **Project Status**\n"
        "───────────────────────────────\n"
        "• **Land** — Allocation in progress within Batam SEZ\n"
        "• **Power** — 150kV dual-feed framework with PLN Batam\n"
        "• **Cooling** — Direct-to-chip finalized, PUE < 1.15\n"
        "• **Syndication** — Phase 1 open\n"
        "• **Founding Board** — 10 seats being formalized\n\n"
        f"📩 Diligence: **{CONTACT_EMAIL}**"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# ============================================================
# ADMIN COMMANDS
# ============================================================
def is_admin(user_id: int) -> bool:
    return user_id in TELEGRAM_ADMIN_IDS

async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not TELEGRAM_CHANNEL_ID:
        await update.message.reply_text("❌ Channel ID not configured.")
        return

    text_content = " ".join(context.args).strip() if context.args else ""

    if not text_content:
        await update.message.reply_text(
            "📢 **Broadcast Usage**\n\n"
            "Send updates to the channel:\n\n"
            "`/post Your message here`\n\n"
            "Example:\n"
            "`/post Phase 1 land allocation now 60% complete.`",
            parse_mode="Markdown"
        )
        return

    if update.message.reply_to_message and update.message.reply_to_message.photo:
        photo_id = update.message.reply_to_message.photo[-1].file_id
        msg = await safe_channel_send_photo(
            photo_url=photo_id,
            caption=f"📢 **AI Grid Indonesia Update**\n\n{text_content}"
        )
    else:
        msg = await safe_channel_send_text(
            text=f"📢 **AI Grid Indonesia Update**\n\n{text_content}"
        )

    if msg:
        await update.message.reply_text("✅ Broadcast sent to channel.")
    else:
        await update.message.reply_text("⚠️ Broadcast skipped — cooldown, duplicate, or daily cap reached.")

async def cmd_postmedia(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "📢 **Media Broadcast**\n\n"
            "Reply to any photo/video with `/postmedia <caption>` to forward it to the channel.",
            parse_mode="Markdown"
        )
        return

    reply = update.message.reply_to_message
    caption = " ".join(context.args).strip() if context.args else ""
    full_caption = f"📢 **AI Grid Indonesia Update**\n\n{caption}" if caption else "📢 **AI Grid Indonesia Update**"

    try:
        if reply.photo:
            msg = await safe_channel_send_photo(photo_url=reply.photo[-1].file_id, caption=full_caption)
        elif reply.video:
            can, reason = channel_can_post(full_caption)
            if not can:
                await update.message.reply_text(f"⚠️ Skipped — {reason}.")
                return
            msg = await telegram_app.bot.send_video(
                chat_id=TELEGRAM_CHANNEL_ID,
                video=reply.video.file_id,
                caption=full_caption,
                parse_mode="Markdown"
            )
            channel_mark_posted(full_caption)
        else:
            await update.message.reply_text("⚠️ Reply to a photo or video.")
            return
        if msg:
            await update.message.reply_text("✅ Media posted to channel.")
        else:
            await update.message.reply_text("⚠️ Skipped — cooldown or daily cap reached.")
    except Exception as e:
        logger.error(f"Media post failed: {e}")
        await update.message.reply_text(f"❌ Failed: {e}")

async def cmd_quiet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    CHANNEL_STATE["scheduler_paused"] = True
    await update.message.reply_text("🔇 Auto-scheduler paused. Manual /post still works.")

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    CHANNEL_STATE["scheduler_paused"] = False
    await update.message.reply_text("🔊 Auto-scheduler resumed.")

async def cmd_chanstat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    now = time.time()
    since = int(now - CHANNEL_STATE["last_post_ts"]) if CHANNEL_STATE["last_post_ts"] else None
    text = (
        "📊 **Channel Scheduler Status**\n"
        "───────────────────────────────\n"
        f"• Last post: {'never' if since is None else f'{since}s ago'}\n"
        f"• Posts today: {CHANNEL_STATE['daily_count']} / {CHANNEL_DAILY_POST_CAP}\n"
        f"• Scheduler: {'⏸️ PAUSED' if CHANNEL_STATE['scheduler_paused'] else '▶️ ACTIVE'}\n"
        f"• Dedup window: {CHANNEL_DEDUP_WINDOW_DAYS} days\n"
        f"• Cooldown: {CHANNEL_POST_COOLDOWN}s between posts\n\n"
        f"Commands: /post /postmedia /quiet /resume /chanstat"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# ============================================================
# CHANNEL CONTENT POOLS
# ============================================================
AI_GRID_NEWS_DATASET = [
    {
        "title": "AI Grid Indonesia Deploys Direct-to-Chip Liquid Cooling in Batam Phase-1",
        "description": "Achieving a PUE under 1.15, the Batam SEZ 50MW facility sets a new benchmark for energy efficiency in Southeast Asian hyperscale AI compute.",
        "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg",
        "category": "Infrastructure"
    },
    {
        "title": "Batam SEZ Secures Subsea Fiber Interconnectivity Under 2.5ms to Singapore",
        "description": "Ultra-low latency routing connects AI Grid Indonesia directly to major regional financial hubs, ensuring high-speed throughput for LLM training.",
        "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg",
        "category": "Connectivity"
    },
    {
        "title": "Understanding the 20.0% Preferred Dividend Structure on 30-Day Cycles",
        "description": "How AI Grid Indonesia provides predictable cashflow distributions backed by enterprise lease agreements and high-density rack syndication.",
        "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg",
        "category": "Financials"
    },
    {
        "title": "NVIDIA Blackwell B200 Architecture and High-Density Rack Readiness",
        "description": "Preparing hyperscale halls for 120kW per rack power densities required by next-generation cluster deployments.",
        "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg",
        "category": "Hardware"
    },
    {
        "title": "PLN Batam Confirms N+2 Dual 150kV Substation Feed Reliability",
        "description": "Uninterrupted power assurance for mission-critical AI compute workloads through robust high-voltage grid integration.",
        "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg",
        "category": "Power"
    },
    {
        "title": "Tax Exemptions in Batam SEZ: 15-Year 0% Corporate Income Tax Advantages",
        "description": "Exploring the regulatory framework driving institutional capital into Indonesia's premier special economic zone.",
        "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg",
        "category": "Regulation"
    },
    {
        "title": "Sub-2.5ms Latency: The Strategic Case for Batam-Singapore Compute Corridor",
        "description": "Why subsea fiber topology between Batam and Singapore is the moat for APAC AI infrastructure.",
        "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg",
        "category": "Connectivity"
    },
    {
        "title": "Institutional Syndicate Allocation Opens for Batam 50MW Phase-1",
        "description": "Qualified participants can now secure tier positions from $1,000 micro allocations up to $100,000+ anchor syndicates.",
        "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg",
        "category": "Syndication"
    },
    {
        "title": "The Rise of Southeast Asia as a Global AI Compute Supercluster",
        "description": "Why foreign direct investment is rapidly pivoting toward Indonesian renewable and hybrid energy-powered data centers.",
        "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg",
        "category": "Market Trends"
    },
    {
        "title": "Automated Telegram Bot Payouts and Investor Transparency",
        "description": "Leveraging smart tooling to streamline communication, yield tracking, and instant platform updates for our global community.",
        "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg",
        "category": "Platform"
    },
    {
        "title": "Optimizing PUE: Why Liquid Cooling Outperforms Traditional Air Systems",
        "description": "Deep dive into thermal dynamics and coolant distribution units maintaining peak operating thresholds for high-density GPUs.",
        "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg",
        "category": "Infrastructure"
    },
    {
        "title": "Cross-Border Data Flows and Singapore-Batam Synergy",
        "description": "How regulatory frameworks within the SEZ facilitate seamless enterprise data transfers.",
        "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg",
        "category": "Connectivity"
    },
    {
        "title": "Target IRR Realities: Assessing 42.5% Net Returns in Infrastructure",
        "description": "Breaking down the financial models and long-term lease economics behind our projected target net IRR.",
        "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg",
        "category": "Financials"
    },
    {
        "title": "Scaling AI Workloads: Cluster Architecture for Large Language Models",
        "description": "Analyzing network fabrics, InfiniBand switching, and low-latency interconnects deployed across Batam halls.",
        "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg",
        "category": "Hardware"
    },
    {
        "title": "Hybrid Solar Integration and Clean Energy PPA Milestones",
        "description": "Progress update on integrating renewable energy microgrids to support sustainable hyperscale operations.",
        "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg",
        "category": "Power"
    },
    {
        "title": "Regulatory Safeguards and Asset Protection in SEZ Frameworks",
        "description": "How Indonesian investment laws protect foreign capital and ensure clear legal title for infrastructure syndications.",
        "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg",
        "category": "Regulation"
    },
    {
        "title": "NVIDIA Blackwell B200 / Direct Liquid Architecture Deep Dive",
        "description": "Custom liquid cooling manifold plates maintain thermal equilibrium for 1,000+ GPU clusters at 100% continuous load.",
        "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg",
        "category": "Hardware"
    },
    {
        "title": "Syndicate Growth: Expanding Phase-2 Capacity Planning in Batam",
        "description": "Looking ahead at upcoming land acquisition and electrical grid capacity expansions for our 2027 roadmap.",
        "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg",
        "category": "Syndication"
    },
    {
        "title": "Global Semiconductor Supply Chains and Data Center Readiness",
        "description": "Navigating logistics and hardware procurement timelines to ensure on-schedule rack deployment.",
        "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg",
        "category": "Market Trends"
    },
    {
        "title": "Enhanced Security Protocols Across AI Grid Portal Infrastructure",
        "description": "Implementing multi-factor authentication, hardware-secured session tokens, and encrypted bot ledger verification.",
        "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg",
        "category": "Platform"
    },
]

# ============================================================
# ALLOCATOR FEEDBACK POOL — 100+ entries
# ============================================================
TESTIMONIES_POOL = [
    # ----- Institutional / Fund (1–20) -----
    ("Marcus Sterling", "Managing Partner, Apex Digital Capital, Singapore", "The 20.0% preferred dividend structure combined with Batam's 15-year 0% corporate tax framework provides unmatched yield visibility for our infrastructure portfolio."),
    ("Elena Rostova", "Venture Partner, Hyperion Compute Fund, Zurich", "As global power constraints limit Western data center scaling, projects like AI Grid Indonesia with dual 150kV backups represent the logical future of AI deployment."),
    ("Sarah Jenkins", "Infrastructure Investment Director, London", "The 3.8x target MOIC over 3 years is exceptionally well-modeled given the massive underlying demand from enterprise LLM trainers."),
    ("Liam O'Connor", "Fund Manager, Pacific Rim Assets, Sydney", "Direct-to-chip cooling and PUE under 1.15 prove this facility is built to handle next-generation NVIDIA Blackwell clusters without breaking a sweat."),
    ("Fatima Al-Mansoor", "Portfolio Director, Gulf Tech Ventures, Dubai", "Energy security is everything in AI right now. The dual 150kV feeds from PLN Batam give us total peace of mind."),
    ("Hiroshi Nakamura", "General Partner, Meridian Infrastructure, Tokyo", "Sub-2.5ms to Singapore is the moat. No other Southeast Asian project combines this latency with this tax profile."),
    ("Amelia Hartwell", "Managing Director, Kensington Capital Partners, London", "We've reviewed dozens of APAC data center deals. Few offer this combination of Tier-IV design rigor and SEZ tax efficiency."),
    ("Rajesh Menon", "Head of Real Assets, Indus Sovereign Fund, Mumbai", "The 50MW Phase-1 scope is right-sized for institutional capital. Large enough to matter, focused enough to deliver."),
    ("Clara Bergstrom", "CIO, Nordic Compute Partners, Stockholm", "We look for assets with hard backing and contractual cashflow. AI Grid Indonesia checks both boxes cleanly."),
    ("Daniel Oyelaran", "Managing Partner, Pan-African Data Fund, Lagos", "Emerging market infrastructure requires government alignment. Indonesia's public commitment to AI data centers is exactly what we want to see."),
    ("Yuki Tanaka", "Director of Infrastructure, Sakura Holdings, Osaka", "Liquid cooling at 120kW per rack is table stakes for Blackwell-class compute. This facility is spec'd correctly from day one."),
    ("Antonio Silva", "Head of Real Estate, Iberian Capital Partners, Lisbon", "Batam SEZ offers the cleanest land title framework we've seen in Southeast Asia. The sovereign lease structure is institutional-grade."),
    ("Priya Kapoor", "Managing Director, Vista Infrastructure, Singapore", "The AI compute thesis is real. This project delivers exposure without single-stock risk or hyperscaler concentration."),
    ("Gregor Hoffmann", "Partner, Austrian Industrial Capital, Vienna", "PUE under 1.15 is not marketing. That's a real operational cost advantage that flows to investor distributions."),
    ("Mei-Ling Chen", "Head of Alternatives, Taipei Family Office, Taipei", "We allocate to infrastructure because it protects capital. AI Grid Indonesia does that with an attractive yield on top."),
    ("Jonathan Reeves", "Senior Partner, Camden Capital, New York", "The 30-day distribution cycle is unusual for infrastructure. It reflects confidence in enterprise lease cashflow."),
    ("Isabella Rossi", "Managing Director, Mediterranean Growth Fund, Milan", "We backed this because the founder has actually operated at OpenAI, Tesla, and Neuralink. That operational depth matters."),
    ("Kwame Asante", "Head of Tech Investing, Westbridge Capital, Accra", "Southeast Asia is the next compute frontier. This project positions capital at the front of that wave."),
    ("Anna Kowalski", "Principal, Baltic Infrastructure Fund, Warsaw", "The 20% preferred return is aggressive for infrastructure, but the underlying economics support it given the compute demand curve."),
    ("Pedro Alvarez", "Managing Partner, Andes Capital, Bogotá", "We compare every data center deal to grid reliability. AI Grid Indonesia's N+2 power architecture is best-in-class for the region."),

    # ----- Syndicate / Mid-tier (21–45) -----
    ("Kenji Takahashi", "Private Syndicate Allocator, Tokyo", "The transparency of the automated 30-day payout cycles and the seamless Telegram bot integration make tracking yields effortless."),
    ("Chloe Van Der Berg", "Private Equity Analyst, Amsterdam", "The micro-tier entry starting at $1,000 allows individual investors to access asset classes previously locked behind institutional doors."),
    ("David Chen", "Quant Strategist, Hong Kong", "Predictable monthly cashflow paired with hard-asset backing creates a stellar risk-adjusted profile."),
    ("Rebecca Tomlinson", "Angel Investor, London", "I've tracked tech infrastructure deals for a decade. This is the first time I could participate meaningfully from a $5,000 ticket."),
    ("Ahmed Al-Rashid", "Private Investor, Riyadh", "Physical assets in politically stable jurisdictions with tax incentives are exactly what my portfolio needs."),
    ("Sophia Bennett", "Real Estate Developer, Miami", "I understand land and power. This project gets both right — and adds a 20% preferred return on top."),
    ("Carlos Mendoza", "Founder, Tijuana Tech Ventures, Mexico", "Compute is the commodity of the 2020s. Owning the infrastructure that produces it is smart capital allocation."),
    ("Emma Lindqvist", "Family Office Director, Stockholm", "We've been looking for APAC tech infrastructure exposure without direct operating risk. This structure fits perfectly."),
    ("Ravi Subramaniam", "Investment Banker, Chennai", "The AI Grid Batam Infrastructure SPV is a clean legal wrapper. Everything routes through one entity with clear documentation."),
    ("Yasmin Hassan", "Pharmaceutical Executive & Investor, Dubai", "Diversification into AI compute balances my healthcare holdings. The 30-day payout cycle is also useful for cashflow planning."),
    ("Mark Fitzgerald", "Retired Portfolio Manager, Boston", "I've watched AI infrastructure deals flow past for years. This is the first one where the economics actually made sense to me."),
    ("Nina Petrova", "Wealth Manager, Zurich", "Our clients want yield with asset backing. AI Grid Indonesia provides both without excessive complexity."),
    ("Tomás Vidal", "Real Estate Investor, Barcelona", "Land, power, cooling — this is real estate plus tech. That's a compelling combination for long-term capital."),
    ("Grace Okafor", "Tech Founder, Lagos", "Building compute infrastructure is what I'd do if I had $50 million and unlimited time. Backing this project is the next best thing."),
    ("Sebastián Ruiz", "Private Investor, Buenos Aires", "The calculator on the site showed me exactly what $10,000 would yield. Full clarity before commitment is rare in this space."),
    ("Maya Rosenberg", "Venture Capital Partner, Tel Aviv", "Neuralink, OpenAI, Tesla, and now this. The founder's track record across AI and infrastructure is what convinced me."),
    ("Robert Klinsky", "Retired Engineer, Seattle", "The engineering specs on this facility are credible. Direct-to-chip liquid cooling is the right answer at 120kW/rack."),
    ("Aisha Bello", "Investment Consultant, Cape Town", "Emerging market infrastructure with sovereign SEZ backing is compelling. This is the type of deal I recommend to clients."),
    ("Henrik Sørensen", "Shipping Executive & Investor, Copenhagen", "Sub-2.5ms to Singapore is a real moat. Subsea fiber topology matters more than people realize."),
    ("Valentina Moretti", "Fashion Entrepreneur, Milan", "I don't understand all the technical specs, but I understand cashflow. The 30-day cycle works for my portfolio."),
    ("Kwesi Mensah", "Family Office Principal, Accra", "The SEZ framework and 100% foreign ownership clarity sold me. Legal structure is everything in cross-border deals."),
    ("Astrid Lindgren", "Pension Fund Advisor, Oslo", "Infrastructure with inflation-linked cashflow is what our retirees need. This project delivers."),
    ("Michael Oduya", "Tech Entrepreneur, Nairobi", "Being able to participate from $5,000 with the same terms as institutional allocators is genuinely new."),
    ("Rina Wijaya", "Property Developer, Jakarta", "I understand land in Indonesia. The Batam SEZ land title framework is legitimately strong."),

    # ----- Micro / Retail (46–65) -----
    ("Hendra Wijaya", "Tech Sector Venture Capitalist, Surabaya", "Placing a world-class 50MW compute hub in the Batam SEZ takes full advantage of Indonesia's digital economy momentum."),
    ("Budi Santoso", "Software Engineer, Jakarta", "I started with $1,000 because I wanted to test. The 30-day payout arrived exactly on time. I've added more since."),
    ("Siti Rahma", "Digital Marketing Manager, Bandung", "I've invested in crypto and stocks. This is the first asset where I feel I actually own something real."),
    ("Kevin Tan", "Retail Investor, Singapore", "The Telegram bot made this easy. I'm not a sophisticated investor, but I understood exactly what I was getting."),
    ("Dewi Lestari", "Small Business Owner, Bali", "My allocation pays out every 30 days. That's become part of my monthly cashflow planning now."),
    ("Ahmad Fauzi", "University Lecturer, Yogyakarta", "The infrastructure thesis is intellectually honest. I allocated $2,500 after reading the risk disclosure."),
    ("Linda Wijaya", "Retired Teacher, Surabaya", "I don't understand blockchain, but I understand the Batam SEZ concept. The land and power are real."),
    ("Reza Pratama", "Restaurant Owner, Medan", "Started small. Now I check the payout regularly. It's become a reliable part of my monthly income."),
    ("Ratna Sari", "Freelance Designer, Jakarta", "The transparency of the whole process surprised me. I know exactly where my money is going."),
    ("Hendra Gunawan", "Factory Supervisor, Batam", "I live near the SEZ. I've watched the construction. This is a real project, not a fantasy."),
    ("Yuni Astuti", "Nurse, Semarang", "The 30-day payout fits my monthly budgeting. I save what comes in from this allocation."),
    ("Agus Setiawan", "Logistics Manager, Surabaya", "I've researched this type of investment for months. This is the cleanest structure I found."),
    ("Dian Sastrowardoyo", "Marketing Executive, Jakarta", "Being able to start with $1,000 changed everything for me. I finally have exposure to real infrastructure."),
    ("Ferry Salim", "Taxi Driver, Jakarta", "I saved for months to allocate $1,500. The payout has been consistent since day one."),
    ("Wulan Guritno", "Model & Investor, Jakarta", "The Telegram bot made everything easy. I don't have to deal with complicated platforms."),
    ("Prita Kemal Gani", "PR Agency Founder, Jakarta", "Diversification into AI infrastructure balances my business holdings. This is smart hedging."),
    ("Glenn Fredly", "Musician, Ambon", "I'm new to investing. The educational content on the site helped me understand before committing."),
    ("Rossa Roslaina", "Singer, Jakarta", "I allocated after seeing the founder's actual background. Experience matters in this space."),
    ("Raffi Ahmad", "TV Host, Jakarta", "The 20% preferred return got my attention. The Batam SEZ tax structure kept it."),
    ("Nagita Slavina", "Entrepreneur, Jakarta", "I invest in real estate normally. This is real estate plus technology — a natural extension for me."),

    # ----- Founder / Tech / Ecosystem (66–85) -----
    ("Vikram Singh", "AI Startup Founder, Bangalore", "I build LLM applications. I know the compute bottleneck firsthand. Backing this infrastructure is backing my own industry."),
    ("Naomi Klein", "Biotech Founder, Tel Aviv", "Our genomics workloads need serious compute. This facility is positioned for exactly this class of problem."),
    ("Marcus Webb", "Cybersecurity Executive, Austin", "The security architecture on this project is stronger than most. Direct power redundancy is critical for anything serious."),
    ("Chen Wei", "Semiconductor Executive, Shenzhen", "I know what 120kW per rack really means. This facility is engineered correctly for the current chip generation."),
    ("Lauren Blake", "Climate Tech Founder, Vancouver", "Hybrid solar integration plus clean PLN tariffs makes this a defensible sustainability story. It matters."),
    ("Diego Fernández", "Robotics Founder, Mexico City", "Autonomous systems need low-latency compute. Sub-2.5ms to Singapore is strategically positioned for APAC robotics."),
    ("Aisha Khan", "Healthcare AI Founder, Karachi", "Medical imaging AI requires massive compute. Infrastructure like this unlocks what we can build in the region."),
    ("Tom Wheeler", "Former FCC Chairman & Investor, Washington DC", "Infrastructure policy and capital formation need to move together. This project reflects that discipline."),
    ("Sarah Kwon", "Deep Learning Researcher, Seoul", "I've run models on rented compute for years. Owning a piece of the infrastructure is the smarter long-term position."),
    ("Ahmed Mansour", "Autonomous Vehicle Executive, Dubai", "Robotics and AVs are the next compute wave. Infrastructure projects positioned for this wave deserve attention."),
    ("Elena Petrova", "Quantum Computing Researcher, Moscow", "Quantum workloads need different thermal profiles. The cooling architecture here can adapt to that future."),
    ("Daniel Kim", "Crypto Exchange Founder, Singapore", "I've built infrastructure. I understand the components. This project is engineered correctly."),
    ("Priyanka Reddy", "SaaS Founder, Hyderabad", "Every successful SaaS company I know is eventually compute-constrained. Backing the infrastructure solves a real bottleneck."),
    ("Nathan Brooks", "Space Tech Executive, Los Angeles", "Orbital compute will eventually need ground-side processing. This facility could be part of that pipeline."),
    ("Yuki Matsumoto", "Gaming Studio CEO, Tokyo", "Game server infrastructure is one of the largest compute categories. This facility is positioned to serve it."),
    ("Grace Chen", "Fintech Founder, Taipei", "HFT, risk modeling, and AI trading all need low-latency compute. Sub-2.5ms to Singapore is a competitive edge."),
    ("Marcus Osei", "Renewable Energy Developer, Accra", "The hybrid solar integration is the part I understand best. This facility is genuinely sustainable."),
    ("Jennifer Walsh", "Legal Tech Founder, Toronto", "Regulatory compliance workloads need jurisdiction-specific infrastructure. Indonesia's SEZ provides exactly that."),
    ("Anton Volkov", "Data Center Consultant, Amsterdam", "I consult on facilities like this. The Tier-IV design and dual 150kV feeds meet international standards."),
    ("Samir Patel", "Venture Partner, Mumbai", "The Indian market has been watching Southeast Asia infrastructure closely. This is the template everyone's looking at."),

    # ----- Family Office / Sovereign (86–100) -----
    ("Abdullah Al-Rashid", "Principal, Al-Rashid Family Office, Riyadh", "We diversify across tech, energy, and real estate. AI infrastructure is the intersection of all three."),
    ("Christina Wong", "Director, Wong Family Holdings, Hong Kong", "Our mandate is capital preservation first. This project offers that with an aggressive preferred return."),
    ("Richard Ashworth", "Trustee, Ashworth Trust, London", "Generational wealth needs assets with real backing. Physical data center hardware is exactly that."),
    ("Beatriz Silva", "Partner, Silva Family Office, São Paulo", "We've watched the AI compute wave from Latin America. Southeast Asia infrastructure is the natural next position."),
    ("Kwame Addo", "Director, Addo Holdings, Accra", "The transparency of the SPV structure and 30-day payout cycle is exceptional for emerging market exposure."),
    ("Helena Novak", "Principal, Novak Trust, Prague", "We backed this after visiting the Batam SEZ virtually. The land title framework is the cleanest we've seen."),
    ("Sultan Al-Qasimi", "Director, Al-Qasimi Holdings, Abu Dhabi", "Sovereign-adjacent capital wants yield with sovereign-grade backing. This project delivers."),
    ("Isabella Fernández", "Managing Partner, Fernández Family Office, Mexico City", "Infrastructure assets are our core. This project adds the AI thesis layer to a physical foundation."),
    ("John Whitmore", "Trustee, Whitmore Family Trust, Boston", "My grandfather's trust invested in railroads. I'm doing the same with compute infrastructure. History repeats with better yields."),
    ("Leila Haddad", "Director, Haddad Holdings, Beirut", "Real assets in stable jurisdictions are the answer to currency risk. This is exactly what we needed."),
    ("Cristina Rossi", "Principal, Rossi Family Office, Rome", "We've reviewed this deal three times. Each time we found more reasons to allocate. That's rare."),
    ("Peter Andersen", "Director, Andersen Family Trust, Oslo", "Northern European capital sees Southeast Asia as the growth story of the next decade. We're positioned early."),
    ("Amina Diallo", "Managing Director, Diallo Holdings, Dakar", "Sovereign wealth practices are changing. Exposure to AI infrastructure is now a strategic necessity."),
    ("Mateo Cortés", "Partner, Cortés Family Office, Santiago", "Latin American infrastructure taught me to value reliability. This project has it."),
    ("Sarah Cohen", "Director, Cohen Family Trust, Tel Aviv", "The founder's actual track record is what convinced the trust. We don't back anonymous operators."),

    # ----- Extra institutional-style entries (101–120) -----
    ("Hiroshi Sato", "Senior Analyst, Nomura Infrastructure Research, Tokyo", "Grid reliability is the underlying thesis of AI infrastructure. AI Grid Indonesia gets the fundamentals right."),
    ("Amara Okonkwo", "Portfolio Manager, Lagos Infrastructure Fund, Lagos", "The African data center story is 5 years behind Southeast Asia. I invest where the maturity already exists."),
    ("Luca Bianchi", "Partner, Milan Infrastructure Partners, Milan", "European capital is waking up to Southeast Asian compute infrastructure. This project is early enough."),
    ("Sofia Vasquez", "Fund Manager, Madrid Tech Fund, Madrid", "I compare every AI deal against the underlying compute demand curve. This one is well-timed."),
    ("Nils Berg", "Principal, Stockholm Growth Partners, Stockholm", "Clean energy integration plus AI compute is the intersection I want exposure to."),
    ("Charlotte Dupont", "Managing Director, Paris Infrastructure, Paris", "Southeast Asian data centers are structurally underserved. This project addresses a real supply gap."),
    ("Erik Johansson", "Director, Copenhagen Tech Capital, Copenhagen", "I look for assets with clean legal structures. The Batam SEZ framework is clean."),
    ("Mira Patel", "Fund Manager, London India Capital, London", "The Indian diaspora is watching Southeast Asian infrastructure closely. This project has strong interest."),
    ("Adam Rosenberg", "Managing Partner, New York Tech Fund, New York", "I've been through three compute cycles. Infrastructure is where the durability is."),
    ("Samantha Foster", "Director, Chicago Real Assets, Chicago", "Data centers are the new commercial real estate. This project is the template."),
    ("Hana Suzuki", "Analyst, Mizuho Infrastructure, Tokyo", "The 120kW per rack spec is important. Older facilities can't be upgraded to this density."),
    ("Michael Wu", "Managing Director, Taipei Infrastructure, Taipei", "The Taiwan compute ecosystem is watching Batam. This could be a template for regional expansion."),
    ("Daniela Moreno", "Fund Manager, Bogotá Infrastructure, Bogotá", "Latin American data centers face power constraints. Indonesia's PLN framework avoids this bottleneck."),
    ("Alexei Petrov", "Principal, Moscow Tech Fund, Moscow", "Emerging markets with strong government alignment are where the best risk-adjusted returns are. This fits."),
    ("Ines Ferreira", "Partner, Lisbon Real Assets, Lisbon", "Portugal and Indonesia share a similar land development approach. The SEZ framework is familiar to us."),
    ("Yusuf Ibrahim", "Director, Cairo Infrastructure Fund, Cairo", "AI compute is the new oil. Owning the refinery matters more than owning the crude."),
    ("Ravi Shankar", "Fund Manager, Chennai Infrastructure, Chennai", "Indian family offices are allocating to Southeast Asian data centers for the first time. This project leads the wave."),
    ("Mei Wong", "Director, Kuala Lumpur Growth Fund, Kuala Lumpur", "Malaysia and Indonesia share the SIJORI corridor. This project strengthens the whole region."),
    ("Ji-Ho Park", "Managing Director, Seoul Tech Partners, Seoul", "Korea's semiconductor ecosystem understands data center fundamentals. This project passes our review."),
    ("Esther Kimani", "Fund Manager, Nairobi Growth Capital, Nairobi", "African infrastructure investors look to Southeast Asia as the model. This project sets the standard."),

    # ----- Additional diverse voices (121–130) -----
    ("Julia Campbell", "Real Estate Attorney, Los Angeles", "I review land titles for a living. The Batam SEZ framework is genuinely among the strongest I've seen."),
    ("Daniel Okafor", "Energy Consultant, Lagos", "Power reliability is the hidden risk in most data center deals. This project solves it with N+2 feeds."),
    ("Sophia Zhang", "AI Researcher, Singapore", "I work in this field. The sub-2.5ms latency to Singapore is exactly what regional AI workloads need."),
    ("Ahmed Al-Farouq", "Petroleum Engineer & Investor, Doha", "I've spent 20 years in energy infrastructure. This project approaches compute the same way serious energy projects are structured."),
    ("Priya Sharma", "Investment Banker, Mumbai", "The 20% preferred return structure is aggressive but well-justified by the compute demand backdrop."),
    ("Jonathan Lee", "Semiconductor Analyst, Taipei", "I cover the chip industry. This infrastructure buildout is what the industry needs to absorb new chip output."),
    ("Emma Thompson", "Environmental Engineer, Vancouver", "The hybrid solar integration is real, not greenwashing. This matters for my allocation."),
    ("Ravi Krishna", "Data Center Operator, Bangalore", "I run facilities like this. The cooling architecture on this project is state-of-the-art."),
    ("Anna Müller", "Quant Researcher, Frankfurt", "I modeled the risk-adjusted return on this deal. It beats most alternatives I've evaluated."),
    ("Marcus Johnson", "Retired Data Center Executive, Dallas", "I've been in this industry for 30 years. The 50MW scale with liquid cooling at this density is a serious build."),
]

TESTIMONY_IMAGES = [
    "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg",
    "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg",
    "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg",
    "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg",
    "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg",
    "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg",
]

ADS_POOL = [
    f"Need predictable cashflow in a volatile market? AI Grid Indonesia's 50MW Batam compute hub offers a 20.0% Preferred Dividend paid on a strict 30-day cycle. Secure your tier: {NETLIFY_URL}",
    f"Singapore-grade subsea latency under 2.5ms, 15-year 0% corporate tax status, and direct-to-chip liquid cooling. That's what makes Batam SEZ a powerhouse. Check the numbers: {NETLIFY_URL}",
    f"Institutional-grade AI infrastructure isn't just for Silicon Valley giants anymore. Syndication tiers start at $1,000. Explore terms: {NETLIFY_URL}",
    f"Looking for real-world asset backing? AI Grid Indonesia combines high-density NVIDIA cluster deployment with structural 20% preferred returns. Review allocations: {NETLIFY_URL}",
    f"Data centers are the new oil refineries. Position your portfolio ahead of the curve with AI Grid Indonesia's Batam infrastructure buildout: {NETLIFY_URL}",
    f"From subsea fiber to robust 150kV power backups, AI Grid Indonesia is building Southeast Asia's premier AI backbone. View participation tiers: {NETLIFY_URL}",
    f"Consistent monthly returns backed by physical server infrastructure. See why savvy investors are allocating into AI Grid Indonesia: {NETLIFY_URL}",
    f"Sub-2.5ms latency to Singapore makes Batam the ultimate strategic position for regional AI processing. Explore syndication details: {NETLIFY_URL}",
]

ENGAGEMENT_POOL = [
    "⚡ **Community Check:** Would you prefer a strict monthly 20% dividend payout or long-term compounding growth on your tech infrastructure?\n\n👍 for Monthly Cashflow Payouts\n👎 for Long-Term Compounding\n\n" + f"*(Explore options: {NETLIFY_URL})*",
    "🧠 **Tech Ecosystem Poll:** Do you believe Southeast Asia will surpass traditional Western markets in high-density AI data center construction over the next 3 years?\n\n👍 for Yes\n👎 for Unlikely",
    "🔋 **Grid Security Question:** When evaluating high-density GPU clusters like NVIDIA Blackwell, is dedicated 150kV substation power backup the #1 priority?\n\n👍 for YES, power is everything\n👎 for NO, cooling matters more",
    "🚀 **Ecosystem Poll:** Are you actively following xAI's Grok updates and scaling milestones across the global tech grid?\n\n👍 for YES, tracking daily\n👎 for NO, just here for the yields",
    "💎 **Asset-Backed Portfolios:** Do you feel safer allocating capital into physical data center hardware rather than speculative digital tokens?\n\n👍 for YES, physical hardware rules\n👎 for NO, prefer digital assets",
]

_used_news_indices = []
_used_testimony_indices = []
_used_ad_indices = []
_used_engagement_indices = []

def _pick_unused(pool_length, used_list):
    if len(used_list) >= pool_length:
        used_list.clear()
    available = [i for i in range(pool_length) if i not in used_list]
    idx = random.choice(available)
    used_list.append(idx)
    return idx

def get_dataset_news_item():
    idx = _pick_unused(len(AI_GRID_NEWS_DATASET), _used_news_indices)
    item = AI_GRID_NEWS_DATASET[idx]
    text = (
        f"🌟 **AI GRID INDONESIA — DAILY DIGEST** 🌟\n\n"
        f"📌 **{item['category']}**\n\n"
        f"🔹 **{item['title']}**\n\n"
        f"💬 *{item['description']}*\n\n"
        f"🚀 [Explore Portal]({NETLIFY_URL})\n"
        f"🤖 [Bot Portal](https://t.me/aigridid_bot)"
    )
    return {"type": "photo", "image": item["image"], "text": text}

def get_testimony():
    idx = _pick_unused(len(TESTIMONIES_POOL), _used_testimony_indices)
    name, role, content = TESTIMONIES_POOL[idx]
    image = random.choice(TESTIMONY_IMAGES)
    text = (
        f"🌟 **ALLOCATOR FEEDBACK** 🌟\n\n"
        f"👤 **{name}**\n"
        f"💼 *{role}*\n\n"
        f"💬 \"{content}\"\n\n"
        f"🚀 [Secure Your Allocation]({NETLIFY_URL})\n"
        f"🤖 [Bot Portal](https://t.me/aigridid_bot)"
    )
    return {"type": "photo", "image": image, "text": text}

def get_ad():
    idx = _pick_unused(len(ADS_POOL), _used_ad_indices)
    return {"type": "photo", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "text": ADS_POOL[idx]}

def get_engagement():
    idx = _pick_unused(len(ENGAGEMENT_POOL), _used_engagement_indices)
    return {"type": "text", "text": ENGAGEMENT_POOL[idx]}

RSS_FEEDS = [
    "https://feeds.feedburner.com/teslarati",
    "https://techcrunch.com/category/artificial-intelligence/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://www.wired.com/feed/rss",
    "https://venturebeat.com/category/ai/feed/",
]

def fetch_rss_item():
    try:
        feed_url = random.choice(RSS_FEEDS)
        parsed = feedparser.parse(feed_url)
        if parsed.entries:
            entry = random.choice(parsed.entries[:5])
            title = entry.get("title", "Tech Update")
            link = entry.get("link", NETLIFY_URL)
            summary = entry.get("summary", "New ecosystem milestone reached.")
            clean = re.sub("<.*?>", "", summary)[:220] + "..."
            text = (
                f"📰 **LIVE INDUSTRY UPDATE** 📰\n\n"
                f"🔹 **{title}**\n\n"
                f"💬 *{clean}*\n\n"
                f"💡 *Context:* Global AI and power grid expansion drives demand for high-density compute at our Batam hub.\n\n"
                f"🔗 [Read Source]({link})\n"
                f"🚀 [Explore Allocations]({NETLIFY_URL})"
            )
            return {"type": "photo", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "text": text}
    except Exception as e:
        logger.warning(f"RSS fetch warning: {e}")
    return None

async def get_channel_content():
    choice = random.choice(["dataset", "dataset", "dataset", "rss", "testimony", "testimony", "ad", "engagement"])
    if choice == "dataset":
        return get_dataset_news_item()
    if choice == "rss":
        rss = fetch_rss_item()
        if rss:
            return rss
        return get_dataset_news_item()
    if choice == "testimony":
        return get_testimony()
    if choice == "ad":
        return get_ad()
    return get_engagement()

async def send_restart_announcement():
    if not TELEGRAM_CHANNEL_ID:
        return
    text = (
        "🚀 AI GRID INDONESIA | Back Online\n\n"
        "Our unified bot is now live — investor portal, channel updates, and Elon ecosystem intelligence in one place.\n\n"
        "• 50MW Tier-IV Batam Compute Hub\n"
        "• 20.0% Preferred Dividend per 30-day cycle\n"
        "• 42.5% Target Net IRR\n"
        "• Tiers from $1,000\n\n"
        f"👉 Portal: {NETLIFY_URL}\n"
        "👉 Bot: @aigridid_bot"
    )
    try:
        await telegram_app.bot.send_photo(
            chat_id=TELEGRAM_CHANNEL_ID,
            photo="https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg",
            caption=text
        )
        logger.info("Startup announcement sent.")
    except Exception as e:
        logger.error(f"Startup announcement failed: {e}")
        
async def scheduled_channel_broadcast():
    if CHANNEL_STATE["scheduler_paused"]:
        logger.info("Scheduler paused — skipping broadcast.")
        return
    if not TELEGRAM_CHANNEL_ID:
        return
    content = await get_channel_content()
    try:
        if content["type"] == "photo":
            msg = await safe_channel_send_photo(
                photo_url=content["image"],
                caption=content["text"]
            )
        else:
            msg = await safe_channel_send_text(text=content["text"])

        if msg and "👍" in content["text"] and "👎" in content["text"]:
            try:
                from telegram import ReactionTypeEmoji
                await telegram_app.bot.set_message_reaction(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    message_id=msg.message_id,
                    reaction=[ReactionTypeEmoji(emoji="👍"), ReactionTypeEmoji(emoji="👎")]
                )
            except Exception as e:
                logger.info(f"Reaction set skipped: {e}")
    except Exception as e:
        logger.error(f"Scheduled broadcast failed: {e}")

async def scheduled_weekly_roundup():
    if CHANNEL_STATE["scheduler_paused"]:
        return
    if not TELEGRAM_CHANNEL_ID:
        return
    text = (
        "📊 **WEEKLY ROUNDUP — AI GRID INDONESIA** 📊\n\n"
        "• Batam Phase-1 development progressing on schedule\n"
        "• Direct-to-chip cooling architecture finalized\n"
        "• 150kV dual-feed framework with PLN Batam active\n"
        "• Syndication Phase 1 open — tiers from $1,000\n"
        "• Founding Board — 10 seats being formalized\n\n"
        f"🚀 [Explore Portal]({NETLIFY_URL})\n"
        f"🤖 [Bot Portal](https://t.me/aigridid_bot)"
    )
    await safe_channel_send_photo(
        photo_url="https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg",
        caption=text
    )

# ============================================================
# HANDLER REGISTRATION
# ============================================================
custom_amount_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(prompt_custom_amount, pattern="^amount_custom$")],
    states={WAITING_CUSTOM_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_custom_amount)]},
    fallbacks=[CommandHandler("start", start)]
)

register_handler = ConversationHandler(
    entry_points=[
        CallbackQueryHandler(register_start, pattern="^reg_start_"),
        CommandHandler("register", cmd_register),
    ],
    states={WAITING_REGISTER_CONTACT: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_receive_contact)]},
    fallbacks=[CommandHandler("start", start)]
)

login_handler = ConversationHandler(
    entry_points=[
        CallbackQueryHandler(login_start, pattern="^login_start$"),
        CommandHandler("login", cmd_login),
    ],
    states={WAITING_LOGIN_CREDENTIALS: [MessageHandler(filters.TEXT & ~filters.COMMAND, login_receive_credentials)]},
    fallbacks=[CommandHandler("start", start)]
)

recover_handler = ConversationHandler(
    entry_points=[CommandHandler("recover", cmd_recover)],
    states={WAITING_RECOVER_CONTACT: [MessageHandler(filters.TEXT & ~filters.COMMAND, recover_receive_contact)]},
    fallbacks=[CommandHandler("start", start)]
)

telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("founder", cmd_founder))
telegram_app.add_handler(CommandHandler("risk", cmd_risk))
telegram_app.add_handler(CommandHandler("status", cmd_status))
telegram_app.add_handler(CommandHandler("post", cmd_post))
telegram_app.add_handler(CommandHandler("postmedia", cmd_postmedia))
telegram_app.add_handler(CommandHandler("quiet", cmd_quiet))
telegram_app.add_handler(CommandHandler("resume", cmd_resume))
telegram_app.add_handler(CommandHandler("chanstat", cmd_chanstat))
telegram_app.add_handler(register_handler)
telegram_app.add_handler(login_handler)
telegram_app.add_handler(recover_handler)
telegram_app.add_handler(custom_amount_handler)
telegram_app.add_handler(CallbackQueryHandler(register_contact_type, pattern="^reg_contact_(email|phone)$"))
telegram_app.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
telegram_app.add_handler(CallbackQueryHandler(show_tiers, pattern="^show_tiers$"))
telegram_app.add_handler(CallbackQueryHandler(show_calculator, pattern="^show_calculator$"))
telegram_app.add_handler(CallbackQueryHandler(allocate_menu, pattern="^allocate_menu$"))
telegram_app.add_handler(CallbackQueryHandler(select_payment_method, pattern="^amount_(1000|5000|25000|100000)$"))
telegram_app.add_handler(CallbackQueryHandler(generate_invoice, pattern="^pay_"))
telegram_app.add_handler(CallbackQueryHandler(show_qr_handler, pattern="^show_qr$"))
telegram_app.add_handler(CallbackQueryHandler(back_to_invoice_handler, pattern="^back_to_invoice$"))
telegram_app.add_handler(CallbackQueryHandler(logout_handler, pattern="^logout$"))
telegram_app.add_handler(CallbackQueryHandler(tx_history_handler, pattern="^tx_history$"))
telegram_app.add_handler(CallbackQueryHandler(deploy_more_handler, pattern="^deploy_more$"))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
