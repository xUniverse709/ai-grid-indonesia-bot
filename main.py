# ============================================================
# AI GRID INDONESIA — Unified Bot
# Investor Portal + Channel Broadcaster + Frontier Compute Updates
# ============================================================

import os
import re
import time
import hmac
import hashlib
import asyncio
import random
import logging
import json
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

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
    verify_pin, suspend_investor, unsuspend_investor,
    list_investors, list_recent_payments,
    update_investor_wallet, update_investor_telegram_username,
    get_active_investors_with_payouts, mark_payout_sent,
    update_kyc_status, create_payout_receipt, list_payouts_for_investor,
    pause_investor_payouts, resume_investor_payouts,
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

NETLIFY_URL = os.environ.get("NETLIFY_URL", "https://ai-grid-indonesia.netlify.app")
CONTACT_EMAIL = "contactaigrid.id@gmail.com"

# ============================================================
# CONVERSATION STATES
# ============================================================
WAITING_CUSTOM_AMOUNT = 1
WAITING_REGISTER_CONTACT = 2
WAITING_LOGIN_CREDENTIALS = 3
WAITING_RECOVER_CONTACT = 4
WAITING_REGISTER_WALLET = 5
WAITING_REGISTER_TELEGRAM = 6
WAITING_UPDATE_WALLET = 7
WAITING_UPDATE_TELEGRAM = 8

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
    # 5 scheduled posts per day + weekly roundup + admin reminders
    scheduler.add_job(scheduled_morning_brief, "cron", hour=6, minute=0, id="morning_brief")
    scheduler.add_job(scheduled_midday_update, "cron", hour=10, minute=0, id="midday_update")
    scheduler.add_job(scheduled_afternoon_update, "cron", hour=14, minute=0, id="afternoon_update")
    scheduler.add_job(scheduled_evening_brief, "cron", hour=18, minute=0, id="evening_brief")
    scheduler.add_job(scheduled_night_update, "cron", hour=22, minute=0, id="night_update")
    scheduler.add_job(scheduled_weekly_roundup, "cron", day_of_week="sun", hour=18, minute=0, id="weekly_roundup")
    scheduler.add_job(scheduled_payout_reminder, "cron", day=1, hour=9, minute=0, id="payout_reminder")
    scheduler.add_job(scheduled_monthly_statements, "cron", day=1, hour=10, minute=0, id="monthly_statements")
    scheduler.start()
    logger.info("Scheduler started — UTC timezone, 5 posts/day.")

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
                f"⚠️ Send the exact amount above.\n\n"
                f"📌 **After payment confirms**, you'll receive a message with a **Register Your Allocation** button.\n\n"
                f"If you don't see it within 2 minutes, send `/register` in this chat."
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
# REGISTRATION FLOW (with wallet + telegram collection)
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

async def cmd_mystatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Let a user check their own payment and registration status."""
    user_id = update.effective_user.id

    investor = await get_investor_by_telegram(user_id)
    pending = await get_pending_payments_for_user(user_id)

    lines = ["📋 **Your Account Status**\n"]

    if investor:
        total = float(investor.total_allocated_usd or 0)
        monthly = total * 0.20
        lines.append(f"✅ **Registered**")
        lines.append(f"• Investor ID: `{investor.investor_id}`")
        lines.append(f"• Tier: {investor.tier}")
        lines.append(f"• Total Allocated: ${total:,.2f}")
        lines.append(f"• 30-Day Payout: ${monthly:,.2f}")
        if investor.wallet_address:
            lines.append(f"• Payout Wallet: `{investor.wallet_address[:8]}...{investor.wallet_address[-6:]}`")
    else:
        lines.append("❌ **Not registered yet**")

    if pending:
        lines.append(f"\n💳 **Confirmed payments awaiting registration:**")
        for p in pending[:5]:
            lines.append(f"• ${float(p.amount_usd):,.2f} ({p.pay_currency.upper()}) — `{p.order_id}`")
        lines.append(f"\nUse `/register` to attach these to your profile.")
    else:
        lines.append(f"\n📭 No confirmed payments waiting.")

    if not investor and not pending:
        lines.append(f"\n💡 If you just paid, wait 1–2 minutes for blockchain confirmation.")
        lines.append(f"Then send `/register` again.")
        lines.append(f"\nQuestions? Email **{CONTACT_EMAIL}**")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_mystatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Let a user check their own payment and registration status."""
    user_id = update.effective_user.id

    # Check registration
    investor = await get_investor_by_telegram(user_id)

    # Check pending payments
    pending = await get_pending_payments_for_user(user_id)

    lines = ["📋 **Your Account Status**\n"]

    if investor:
        total = float(investor.total_allocated_usd or 0)
        monthly = total * 0.20
        lines.append(f"✅ **Registered**")
        lines.append(f"• Investor ID: `{investor.investor_id}`")
        lines.append(f"• Tier: {investor.tier}")
        lines.append(f"• Total Allocated: ${total:,.2f}")
        lines.append(f"• 30-Day Payout: ${monthly:,.2f}")
    else:
        lines.append("❌ **Not registered yet**")

    if pending:
        lines.append(f"\n💳 **Confirmed payments awaiting registration:**")
        for p in pending[:5]:
            lines.append(f"• ${float(p.amount_usd):,.2f} ({p.pay_currency.upper()}) — {p.order_id}")
        lines.append(f"\nUse `/register` to attach these to your profile.")
    else:
        lines.append(f"\n📭 No confirmed payments waiting.")

    if not investor and not pending:
        lines.append(f"\n💡 If you just paid, wait 1–2 minutes for blockchain confirmation.")
        lines.append(f"Then send `/register` again.")
        lines.append(f"\nQuestions? Email **{CONTACT_EMAIL}**")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

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

    context.user_data["reg_contact_value"] = text
    await update.message.reply_text(
        "✅ Contact saved.\n\n"
        "💰 **Payout Wallet Address**\n\n"
        "Where should your 30-day preferred dividend payouts be sent?\n\n"
        "We currently send **USDT (TRC-20)**. Paste your TRC-20 wallet address below.\n\n"
        "*(Example: `TXYZ...` — starts with T, 34 characters)*",
        parse_mode="Markdown"
    )
    return WAITING_REGISTER_WALLET

async def register_receive_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if len(text) < 20 or len(text) > 100:
        await update.message.reply_text("⚠️ That doesn't look like a valid wallet address. Try again:")
        return WAITING_REGISTER_WALLET
    context.user_data["reg_wallet_address"] = text
    await update.message.reply_text(
        "✅ Wallet saved.\n\n"
        "📱 **Telegram Username (Optional)**\n\n"
        "If you'd like our cashier team to be able to contact you directly on Telegram for payout confirmations, please share your username.\n\n"
        "*(Example: `@yourusername`)*\n\n"
        "Or type `/skip` to skip this step.",
        parse_mode="Markdown"
    )
    return WAITING_REGISTER_TELEGRAM

async def register_receive_telegram(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.lower() == "/skip":
        context.user_data["reg_telegram_username"] = None
    else:
        username = text.lstrip("@").strip()
        if len(username) < 3 or len(username) > 32:
            await update.message.reply_text("⚠️ Invalid Telegram username. Try again or type `/skip`:", parse_mode="Markdown")
            return WAITING_REGISTER_TELEGRAM
        context.user_data["reg_telegram_username"] = username

    contact_value = context.user_data.get("reg_contact_value")
    contact_type = context.user_data.get("reg_contact_type", "email")
    wallet_address = context.user_data.get("reg_wallet_address")
    telegram_username = context.user_data.get("reg_telegram_username")
    pin = _gen_pin()
    recovery_code = _gen_recovery_code()
    user_id = update.effective_user.id
    order_id = context.user_data.get("pending_order_id")

    try:
        investor = await create_investor(
            telegram_user_id=user_id,
            contact_type=contact_type,
            contact_value=contact_value,
            pin=pin,
            recovery_code=recovery_code,
            wallet_address=wallet_address,
            preferred_telegram_username=telegram_username,
        )
        if order_id:
            await attach_payment_to_investor(order_id, investor.investor_id)

        username_line = f"\n• **Telegram:** @{telegram_username}" if telegram_username else ""
        await update.message.reply_text(
            f"✅ **Registration Complete**\n"
            f"───────────────────────────────\n"
            f"• **Investor ID:** `{investor.investor_id}`\n"
            f"• **PIN:** `{pin}`\n"
            f"• **Recovery Code:** `{recovery_code}`\n"
            f"• **Contact:** `{contact_value}`\n"
            f"• **Payout Wallet:** `{wallet_address[:8]}...{wallet_address[-6:]}`"
            f"{username_line}\n\n"
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
# LOGIN FLOW (with suspension check)
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

    if not investor.is_active:
        await update.message.reply_text(
            "🔴 **Account Suspended**\n\n"
            "Your investor account is currently suspended. New activity is blocked.\n\n"
            f"Contact **{CONTACT_EMAIL}** for assistance."
        )
        return ConversationHandler.END

    await update_investor_login(investor_id)
    context.user_data["logged_in_investor"] = investor_id
    total = float(investor.total_allocated_usd or 0)
    monthly = total * 0.20
    wallet_display = f"`{investor.wallet_address[:8]}...{investor.wallet_address[-6:]}`" if investor.wallet_address else "❌ not set"
    telegram_display = f"@{investor.preferred_telegram_username}" if investor.preferred_telegram_username else "not set"

    keyboard = [
        [InlineKeyboardButton("💰 Deploy More Capital", callback_data="deploy_more")],
        [InlineKeyboardButton("⚙️ Account Settings", callback_data="account_settings")],
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
        f"• **Contact:** `{investor.contact_value}`\n"
        f"• **Payout Wallet:** {wallet_display}\n"
        f"• **Telegram:** {telegram_display}\n\n"
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
    investor = await get_investor_by_id(investor_id)
    if not investor or not investor.is_active:
        await query.edit_message_text(
            "🔴 **Account Suspended**\n\n"
            "New allocations are blocked on this account.\n\n"
            f"Contact **{CONTACT_EMAIL}**."
        )
        return
    await allocate_menu(update, context)

async def account_settings_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await query.edit_message_text("❌ Please log in first with /login.")
        return
    investor = await get_investor_by_id(investor_id)
    if not investor:
        await query.edit_message_text("❌ Investor record not found.")
        return
    wallet_display = f"`{investor.wallet_address[:8]}...{investor.wallet_address[-6:]}`" if investor.wallet_address else "❌ not set"
    telegram_display = f"@{investor.preferred_telegram_username}" if investor.preferred_telegram_username else "not set"
    keyboard = [
        [InlineKeyboardButton("💰 Update Payout Wallet", callback_data="update_wallet")],
        [InlineKeyboardButton("📱 Update Telegram Username", callback_data="update_telegram")],
        [InlineKeyboardButton("⬅️ Back to Profile", callback_data="back_to_profile")]
    ]
    await query.edit_message_text(
        f"⚙️ **Account Settings**\n"
        f"───────────────────────────────\n"
        f"• **Payout Wallet:** {wallet_display}\n"
        f"• **Telegram Username:** {telegram_display}\n\n"
        f"Select what you'd like to update:",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

async def update_wallet_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await query.edit_message_text("❌ Please log in first with /login.")
        return ConversationHandler.END
    await query.edit_message_text(
        "💰 **Update Payout Wallet**\n\n"
        "Paste your new USDT (TRC-20) wallet address.\n\n"
        "*(Example: `TXYZ...` — starts with T, 34 characters)*",
        parse_mode="Markdown"
    )
    return WAITING_UPDATE_WALLET

async def update_wallet_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await update.message.reply_text("❌ Please log in first with /login.")
        return ConversationHandler.END
    if len(text) < 20 or len(text) > 100:
        await update.message.reply_text("⚠️ That doesn't look like a valid wallet address. Try again:")
        return WAITING_UPDATE_WALLET
    investor = await update_investor_wallet(investor_id, text)
    if investor:
        await update.message.reply_text(
            f"✅ **Payout Wallet Updated**\n\n"
            f"• **New Wallet:** `{text[:8]}...{text[-6:]}`\n\n"
            f"Future payouts will be sent here.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ Failed to update wallet. Contact support.")
    return ConversationHandler.END

async def update_telegram_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await query.edit_message_text("❌ Please log in first with /login.")
        return ConversationHandler.END
    await query.edit_message_text(
        "📱 **Update Telegram Username**\n\n"
        "Send your Telegram username so our cashier team can reach you for payout confirmations.\n\n"
        "*(Example: `@yourusername`)*",
        parse_mode="Markdown"
    )
    return WAITING_UPDATE_TELEGRAM

async def update_telegram_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().lstrip("@")
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await update.message.reply_text("❌ Please log in first with /login.")
        return ConversationHandler.END
    if len(text) < 3 or len(text) > 32:
        await update.message.reply_text("⚠️ Invalid Telegram username. Try again:")
        return WAITING_UPDATE_TELEGRAM
    investor = await update_investor_telegram_username(investor_id, text)
    if investor:
        await update.message.reply_text(
            f"✅ **Telegram Username Updated**\n\n"
            f"• **Username:** @{text}\n\n"
            f"Our cashier team can now contact you here.",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("❌ Failed to update username. Contact support.")
    return ConversationHandler.END

async def back_to_profile_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    investor_id = context.user_data.get("logged_in_investor")
    if not investor_id:
        await query.edit_message_text("❌ Please log in first with /login.")
        return
    investor = await get_investor_by_id(investor_id)
    if not investor:
        await query.edit_message_text("❌ Investor record not found.")
        return
    total = float(investor.total_allocated_usd or 0)
    monthly = total * 0.20
    wallet_display = f"`{investor.wallet_address[:8]}...{investor.wallet_address[-6:]}`" if investor.wallet_address else "❌ not set"
    telegram_display = f"@{investor.preferred_telegram_username}" if investor.preferred_telegram_username else "not set"
    keyboard = [
        [InlineKeyboardButton("💰 Deploy More Capital", callback_data="deploy_more")],
        [InlineKeyboardButton("⚙️ Account Settings", callback_data="account_settings")],
        [InlineKeyboardButton("📈 Transaction History", callback_data="tx_history")],
        [InlineKeyboardButton("🚪 Log Out", callback_data="logout")]
    ]
    await query.edit_message_text(
        f"📊 **Your Investor Profile**\n"
        f"───────────────────────────────\n"
        f"• **Investor ID:** `{investor.investor_id}`\n"
        f"• **Tier:** {investor.tier}\n"
        f"• **Total Allocated:** ${total:,.2f} USD\n"
        f"• **30-Day Payout:** ${monthly:,.2f}\n"
        f"• **Contact:** `{investor.contact_value}`\n"
        f"• **Payout Wallet:** {wallet_display}\n"
        f"• **Telegram:** {telegram_display}\n\n"
        f"What would you like to do?",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )

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
        "She has operated at the intersection of AI, compute infrastructure, and capital for over a decade.\n\n"
        "The Indonesia AI Grid is her infrastructure thesis — build the compute layer for Southeast Asia before the market prices it.\n\n"
        f"📩 Institutional inquiries: **{CONTACT_EMAIL}**"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_vision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🎯 **The Vision**\n"
        "───────────────────────────────\n"
        "AI Grid Indonesia is a 50MW Tier-IV hyperscale compute facility in the Batam SEZ — 20km from Singapore with sub-2.5ms subsea latency.\n\n"
        "**Founded by Shivon Zilis.** Shivon's background spans OpenAI (founding adviser and former board member), Tesla (Project Director for Autopilot and chip design), and Neuralink (Director of Operations & Special Projects).\n\n"
        "**Purpose.** Build the compute infrastructure layer for Southeast Asia's AI economy — power, cooling, land, and latency — before the market prices it.\n\n"
        "**Built for neural-class workloads.** The facility is engineered for high-density AI training, real-time inference, and neural-adjacent applications that require sustained, low-latency compute.\n\n"
        "AI Grid Indonesia is an independent infrastructure initiative. It is not a subsidiary of, or an official partner to, any other company.\n\n"
        f"📩 {CONTACT_EMAIL}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_neural(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🧠 **Frontier Compute Thesis**\n"
        "───────────────────────────────\n"
        "The line between AI and neuroscience is blurring.\n\n"
        "• Neural networks inspired by brain architecture\n"
        "• Brain-computer interfaces powered by deep learning models\n"
        "• Real-time inference on neural data at sub-10ms latency\n\n"
        "The compute demands of this convergence are enormous — and they require infrastructure that traditional data centers weren't built for.\n\n"
        "This is the thesis behind AI Grid Indonesia.\n\n"
        f"📩 {CONTACT_EMAIL}"
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
            "`/post Your message here`",
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
            "📢 Reply to any photo/video with `/postmedia <caption>` to forward it to the channel.",
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
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    CHANNEL_STATE["scheduler_paused"] = True
    await update.message.reply_text("🔇 Auto-scheduler paused. Manual /post still works.")

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    CHANNEL_STATE["scheduler_paused"] = False
    await update.message.reply_text("🔊 Auto-scheduler resumed.")

async def cmd_chanstat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
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
        f"• Cooldown: {CHANNEL_POST_COOLDOWN}s between posts"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_testpost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: manually trigger one scheduled post for testing."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔️ Not authorized.")
        return
    if not TELEGRAM_CHANNEL_ID:
        await update.message.reply_text("❌ Channel ID not configured.")
        return

    force_type = context.args[0].lower() if context.args else None

    try:
        if force_type == "rss":
            rss = fetch_rss_item()
            if not rss:
                await update.message.reply_text("❌ RSS fetch returned nothing. Try again.")
                return
            content = rss
        elif force_type == "briefing":
            content = get_frontier_briefing()
        elif force_type == "news":
            content = get_dataset_news_item()
        elif force_type == "testimony":
            content = get_testimony()
        elif force_type == "ad":
            content = get_ad()
        elif force_type == "engagement":
            content = get_engagement()
        else:
            hour_utc = datetime.utcnow().hour
            content = get_channel_content_for_hour(hour_utc)

        if content["type"] == "photo":
            msg = await telegram_app.bot.send_photo(
                chat_id=TELEGRAM_CHANNEL_ID,
                photo=content["image"],
                caption=content["text"],
                parse_mode="Markdown"
            )
        else:
            msg = await telegram_app.bot.send_message(
                chat_id=TELEGRAM_CHANNEL_ID,
                text=content["text"],
                parse_mode="Markdown",
                disable_web_page_preview=True
            )

        if msg and "👍" in content["text"] and "👎" in content["text"]:
            try:
                from telegram import ReactionTypeEmoji
                await telegram_app.bot.set_message_reaction(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    message_id=msg.message_id,
                    reaction=[ReactionTypeEmoji(emoji="👍"), ReactionTypeEmoji(emoji="👎")]
                )
            except Exception:
                pass

        channel_mark_posted(content["text"])

        await update.message.reply_text(
            f"✅ **Test post sent to channel.**\n\n"
            f"• Type: {force_type or 'auto (time-based)'}\n"
            f"• Content length: {len(content['text'])} chars"
        )
    except Exception as e:
        logger.error(f"Test post failed: {e}")
        await update.message.reply_text(f"❌ Test post failed: {e}")

async def cmd_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/lookup <INVESTOR_ID>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    investor = await get_investor_by_id(investor_id)
    if not investor:
        await update.message.reply_text(f"❌ No investor found: `{investor_id}`", parse_mode="Markdown")
        return
    total = float(investor.total_allocated_usd or 0)
    monthly = total * 0.20
    payments = await get_all_payments_for_investor(investor_id)
    status = "🟢 ACTIVE" if investor.is_active else "🔴 SUSPENDED"
    suspension_line = ""
    if not investor.is_active:
        suspension_line = (
            f"• **Suspended:** {investor.suspended_at.strftime('%Y-%m-%d %H:%M UTC') if investor.suspended_at else 'N/A'}\n"
            f"• **Reason:** {investor.suspension_reason or 'not specified'}\n"
        )
    wallet_line = f"`{investor.wallet_address[:8]}...{investor.wallet_address[-6:]}`" if investor.wallet_address else "❌ not set"
    tg_line = f"@{investor.preferred_telegram_username}" if investor.preferred_telegram_username else "not set"
    text = (
        f"🔍 **Investor Lookup**\n"
        f"───────────────────────────────\n"
        f"• **Investor ID:** `{investor.investor_id}`\n"
        f"• **Status:** {status}\n"
        f"• **Tier:** {investor.tier}\n"
        f"• **Total Allocated:** ${total:,.2f} USD\n"
        f"• **30-Day Payout:** ${monthly:,.2f}\n"
        f"• **Contact:** `{investor.contact_value}`\n"
        f"• **Wallet:** {wallet_line}\n"
        f"• **Telegram:** {tg_line}\n"
        f"• **Telegram ID:** `{investor.telegram_user_id}`\n"
        f"• **KYC:** {getattr(investor, 'kyc_status', 'pending')}\n"
        f"• **Registered:** {investor.created_at.strftime('%Y-%m-%d %H:%M UTC') if investor.created_at else 'N/A'}\n"
        f"• **Last Login:** {investor.last_login_at.strftime('%Y-%m-%d %H:%M UTC') if investor.last_login_at else 'never'}\n"
        f"{suspension_line}"
        f"• **Payments:** {len(payments)}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_suspend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: `/suspend <INVESTOR_ID> <reason>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    reason = " ".join(context.args[1:])
    investor = await get_investor_by_id(investor_id)
    if not investor:
        await update.message.reply_text(f"❌ No investor found: `{investor_id}`", parse_mode="Markdown")
        return
    if not investor.is_active:
        await update.message.reply_text(f"⚠️ `{investor_id}` is already suspended.", parse_mode="Markdown")
        return
    updated = await suspend_investor(investor_id, reason)
    if updated:
        await update.message.reply_text(
            f"🔴 **Investor Suspended**\n"
            f"───────────────────────────────\n"
            f"• **Investor ID:** `{investor_id}`\n"
            f"• **Reason:** {reason}\n"
            f"• **Effective:** immediately",
            parse_mode="Markdown"
        )
        try:
            await telegram_app.bot.send_message(
                chat_id=updated.telegram_user_id,
                text=(
                    "⚠️ **Account Notice**\n\n"
                    "Your investor account has been temporarily suspended pending review.\n\n"
                    f"Please contact **{CONTACT_EMAIL}** for details."
                ),
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Could not notify suspended investor: {e}")

async def cmd_unsuspend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/unsuspend <INVESTOR_ID>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    investor = await get_investor_by_id(investor_id)
    if not investor:
        await update.message.reply_text(f"❌ No investor found: `{investor_id}`", parse_mode="Markdown")
        return
    if investor.is_active:
        await update.message.reply_text(f"⚠️ `{investor_id}` is already active.", parse_mode="Markdown")
        return
    updated = await unsuspend_investor(investor_id)
    if updated:
        await update.message.reply_text(
            f"🟢 **Investor Reactivated**\n"
            f"───────────────────────────────\n"
            f"• **Investor ID:** `{investor_id}`\n"
            f"• **Status:** Active",
            parse_mode="Markdown"
        )
        try:
            await telegram_app.bot.send_message(
                chat_id=updated.telegram_user_id,
                text="✅ **Account Restored**\n\nYour investor account is active again. Use `/login` to access your portfolio.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Could not notify reactivated investor: {e}")

async def cmd_resetpin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/resetpin <INVESTOR_ID>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    investor = await get_investor_by_id(investor_id)
    if not investor:
        await update.message.reply_text(f"❌ No investor found: `{investor_id}`", parse_mode="Markdown")
        return
    new_pin = _gen_pin()
    await update_investor_pin(investor_id, new_pin)
    await update.message.reply_text(
        f"🔑 **PIN Reset**\n"
        f"───────────────────────────────\n"
        f"• **Investor ID:** `{investor_id}`\n"
        f"• **New PIN:** `{new_pin}`\n\n"
        f"The investor has been notified.",
        parse_mode="Markdown"
    )
    try:
        await telegram_app.bot.send_message(
            chat_id=investor.telegram_user_id,
            text=(
                f"🔑 **PIN Reset Notification**\n"
                f"───────────────────────────────\n"
                f"• **Investor ID:** `{investor.investor_id}`\n"
                f"• **New PIN:** `{new_pin}`\n\n"
                f"Use `/login` to access your portfolio. Contact **{CONTACT_EMAIL}** if this was unexpected."
            ),
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"Could not notify PIN-reset investor: {e}")

async def cmd_listusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    investors = await list_investors(20)
    if not investors:
        await update.message.reply_text("📭 No investors yet.")
        return
    lines = ["👥 **Recent Investors (last 20)**\n"]
    for inv in investors:
        status = "🟢" if inv.is_active else "🔴"
        total = float(inv.total_allocated_usd or 0)
        lines.append(f"{status} `{inv.investor_id}` | {inv.tier} | ${total:,.0f}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_listpayments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    payments = await list_recent_payments(20)
    if not payments:
        await update.message.reply_text("📭 No payments yet.")
        return
    lines = ["💳 **Recent Payments (last 20)**\n"]
    for p in payments:
        investor_ref = p.investor_id if p.investor_id else "—"
        lines.append(f"• `${float(p.amount_usd):,.0f}` | {p.status.upper()} | {p.pay_currency.upper()} | `{investor_ref}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_payoutlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    investors = await get_active_investors_with_payouts()
    if not investors:
        await update.message.reply_text("📭 No active investors with wallet addresses on file yet.")
        return
    lines = [f"📋 **Pending Payouts — {len(investors)} investors**\n"]
    total_payout = 0.0
    for inv in investors:
        allocation = float(inv.total_allocated_usd or 0)
        payout = allocation * 0.20
        total_payout += payout
        last = inv.last_payout_at.strftime('%Y-%m-%d') if inv.last_payout_at else "never"
        lines.append(
            f"• `{inv.investor_id}` | ${allocation:,.0f} → **${payout:,.2f}**\n"
            f"  Wallet: `{inv.wallet_address[:8]}...{inv.wallet_address[-6:]}`\n"
            f"  Last: {last}"
        )
    lines.append(f"\n💰 **Total this cycle: ${total_payout:,.2f}**")
    full_text = "\n".join(lines)
    if len(full_text) > 4000:
        full_text = full_text[:3900] + "\n\n... (truncated)"
    await update.message.reply_text(full_text, parse_mode="Markdown")

async def cmd_markpaid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/markpaid <INVESTOR_ID>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    investor = await get_investor_by_id(investor_id)
    if not investor:
        await update.message.reply_text(f"❌ No investor found: `{investor_id}`", parse_mode="Markdown")
        return
    if not investor.wallet_address:
        await update.message.reply_text(f"⚠️ `{investor_id}` has no wallet address on file.", parse_mode="Markdown")
        return
    total = float(investor.total_allocated_usd or 0)
    payout = total * 0.20
    updated = await mark_payout_sent(investor_id, payout)
    if updated:
        try:
            await create_payout_receipt(investor_id, payout, updated.wallet_address, "USDT TRC-20")
        except Exception as e:
            logger.warning(f"Payout receipt creation failed: {e}")
        try:
            await telegram_app.bot.send_message(
                chat_id=updated.telegram_user_id,
                text=(
                    f"💸 **Payout Sent**\n"
                    f"───────────────────────────────\n"
                    f"• **Amount:** ${payout:,.2f} USD (USDT TRC-20)\n"
                    f"• **Destination:** `{updated.wallet_address[:8]}...{updated.wallet_address[-6:]}`\n"
                    f"• **Date:** {updated.last_payout_at.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
                    f"Thank you for being part of AI Grid Indonesia."
                ),
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Could not notify investor of payout: {e}")
        await update.message.reply_text(
            f"✅ **Payout Marked Sent**\n"
            f"───────────────────────────────\n"
            f"• **Investor ID:** `{investor_id}`\n"
            f"• **Amount:** ${payout:,.2f}\n"
            f"• **Wallet:** `{updated.wallet_address[:8]}...{updated.wallet_address[-6:]}`\n"
            f"• **Investor notified:** ✅",
            parse_mode="Markdown"
        )

async def cmd_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    investors = await list_investors(1000)
    active = [i for i in investors if i.is_active]
    suspended = [i for i in investors if not i.is_active]
    total_raised = sum(float(i.total_allocated_usd or 0) for i in investors)
    total_payouts = sum(float(getattr(i, 'total_payouts_usd', 0) or 0) for i in investors)
    payable = [i for i in active if i.wallet_address]
    pending_payout = sum(float(i.total_allocated_usd or 0) * 0.20 for i in payable)
    text = (
        f"📊 **AI GRID — ADMIN DASHBOARD**\n"
        f"───────────────────────────────\n"
        f"• **Total Investors:** {len(investors)}\n"
        f"• **Active:** {len(active)} | **Suspended:** {len(suspended)}\n"
        f"• **Total Capital Raised:** ${total_raised:,.2f}\n"
        f"• **Total Payouts Sent:** ${total_payouts:,.2f}\n"
        f"• **Pending This Cycle:** ${pending_payout:,.2f}\n"
        f"• **Posts/day:** 5 (06:00 / 10:00 / 14:00 / 18:00 / 22:00 UTC)\n"
        f"• **Scheduler:** {'⏸️ PAUSED' if CHANNEL_STATE['scheduler_paused'] else '▶️ ACTIVE'}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_pausepayout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/pausepayout <INVESTOR_ID>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    investor = await pause_investor_payouts(investor_id)
    if investor:
        await update.message.reply_text(f"⏸️ Payouts paused for `{investor_id}`.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ Investor not found: `{investor_id}`", parse_mode="Markdown")

async def cmd_resumepayout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if not context.args:
        await update.message.reply_text("Usage: `/resumepayout <INVESTOR_ID>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    investor = await resume_investor_payouts(investor_id)
    if investor:
        await update.message.reply_text(f"▶️ Payouts resumed for `{investor_id}`.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ Investor not found: `{investor_id}`", parse_mode="Markdown")

async def cmd_kyc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: `/kyc <INVESTOR_ID> <pending|verified|rejected>`", parse_mode="Markdown")
        return
    investor_id = context.args[0].upper()
    status = context.args[1].lower()
    if status not in ["pending", "verified", "rejected"]:
        await update.message.reply_text("⚠️ Status must be: pending / verified / rejected", parse_mode="Markdown")
        return
    investor = await update_kyc_status(investor_id, status)
    if investor:
        await update.message.reply_text(f"✅ KYC status for `{investor_id}` set to **{status}**.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"❌ Investor not found: `{investor_id}`", parse_mode="Markdown")

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Not authorized.")
        return
    investors = await list_investors(1000)
    payments = await list_recent_payments(1000)
    data = {
        "exported_at": datetime.utcnow().isoformat(),
        "investors": [
            {
                "investor_id": i.investor_id,
                "tier": i.tier,
                "total_allocated_usd": float(i.total_allocated_usd or 0),
                "is_active": i.is_active,
                "wallet_address": i.wallet_address,
                "telegram_username": i.preferred_telegram_username,
                "contact_value": i.contact_value,
                "created_at": i.created_at.isoformat() if i.created_at else None,
            } for i in investors
        ],
        "payments": [
            {
                "order_id": p.order_id,
                "investor_id": p.investor_id,
                "amount_usd": float(p.amount_usd),
                "status": p.status,
                "pay_currency": p.pay_currency,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            } for p in payments
        ],
    }
    try:
        await context.bot.send_document(
            chat_id=update.effective_user.id,
            document=json.dumps(data, indent=2).encode("utf-8"),
            filename=f"aigrid_export_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.json",
            caption=f"📦 Export generated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
        )
    except Exception as e:
        logger.error(f"Export failed: {e}")
        await update.message.reply_text(f"❌ Export failed: {e}")

# ============================================================
# CHANNEL CONTENT POOLS
# ============================================================

# ─── FRONTIER COMPUTE BRIEFINGS (60+) ───
FRONTIER_COMPUTE_BRIEFINGS = [
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nProcessing real-time neural signals requires sub-10ms round-trip latency — tighter than most financial trading infrastructure.\n\nThis is why compute facilities positioned at the edge of major fiber corridors matter. Batam to Singapore: under 2.5ms.\n\nAI Grid Indonesia — built for the next class of workloads.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe line between AI and neuroscience is blurring. Neural networks inspired by brain architecture. Brain-computer interfaces powered by deep learning.\n\nThe compute demands of this convergence are enormous — and require infrastructure traditional data centers were not built for.\n\nThis is the thesis behind AI Grid Indonesia.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy Batam and not Singapore?\n\nSingapore wants only the highest-efficiency workloads. Its PUE floor is 1.25 and land is scarce.\n\nBatam absorbs what Singapore cannot — at sub-2.5ms latency and a fraction of the cost.\n\nThis is the geography of the next compute cycle.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\n120kW per rack. PUE under 1.15. Direct-to-chip liquid cooling.\n\nThese are not specs to be proud of. They are the minimum requirement to run the next generation of neural and AI workloads.\n\nFacilities that cannot hit this density will be obsolete within five years.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nFounder background:\n\n• OpenAI — founding adviser and board member\n• Tesla — Project Director for Autopilot and chip design\n• Neuralink — Director of Operations & Special Projects\n\nShivon Zilis has spent a decade at the frontier of AI and neural interfaces.\n\nHer latest project: AI Grid Indonesia — 50MW of compute infrastructure for Southeast Asia.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nEvery neural interface breakthrough creates a downstream compute problem.\n\nSignal processing at scale. Model training on biological data. Real-time inference with strict latency bounds.\n\nThe infrastructure layer is the bottleneck — and it is being built right now.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nReal-time brain-computer interfaces require:\n\n• Sub-10ms compute latency\n• Continuous uptime (no downtime windows)\n• Power redundancy that eliminates single points of failure\n\nThis is why AI Grid Indonesia deploys N+2 dual 150kV feeds via PLN Batam.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe compute stack of the 2030s:\n\n• AI training clusters at 100kW+ per rack\n• Real-time inference for edge devices\n• Neural signal processing at sub-10ms latency\n\nAll three need liquid-cooled hyperscale infrastructure. This is what we build.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nGeographic moats in compute are real.\n\n• Sub-2.5ms to Singapore\n• Direct subsea fiber to Jakarta, Hong Kong\n• 15-year 0% corporate tax\n• Sovereign land title, 80+ year lease\n\nBatam SEZ is not a location. It is a strategy.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe next compute cycle will not be won by the biggest facility.\n\nIt will be won by the one positioned where the workloads need it — close enough to the market for latency, distant enough from the grid for cost, protected enough from politics for certainty.\n\nBatam SEZ checks all three.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nOne megawatt of liquid-cooled AI compute can train roughly 100 billion parameter models in weeks.\n\nFifty megawatts is a different category entirely.\n\nThis is why the number matters.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nNeural interfaces are the frontier. But they are built on a foundation:\n\n• Silicon (chips)\n• Power (grid)\n• Cooling (thermal)\n• Land (sovereign)\n\nAI Grid Indonesia owns the foundation.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy 2026 matters:\n\n• AI model training demand doubles every 6 months\n• High-density GPU racks exceed air cooling limits\n• Power grids in developed markets are constrained\n• Southeast Asia has become the logical relief valve\n\nAI Grid Indonesia is built for this exact window.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nDirect-to-chip liquid cooling is not optional anymore.\n\nAt 120kW per rack, air cannot remove the heat. You either deploy liquid cooling or you do not run the next generation of chips.\n\nAI Grid Indonesia deploys liquid from day one.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy Southeast Asia and not the US or EU?\n\n• Power costs 3–4x lower\n• Grid buildout happens in years, not decades\n• 0% corporate tax under SEZ framework\n• Sub-2.5ms to Singapore capital markets\n\nThe economics of compute are moving East.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe infrastructure gap nobody is talking about:\n\nEveryone is building AI models. Almost nobody is building the power, cooling, and land to run them.\n\nThe bottleneck is real. The projects that solve it win the next decade.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nSubsea fiber is the quiet moat.\n\nBatam sits on direct fiber routes to Singapore, Jakarta, and Hong Kong. Latency: sub-2.5ms. Redundancy: multiple cable paths.\n\nCompute needs both. Batam provides both.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe math that changes everything:\n\nA rack running at 40kW uses standard air cooling. The same rack at 120kW requires liquid. Same physical footprint. Three times the compute.\n\nThis is why density is the new currency.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nReal-time AI — voice agents, autonomous systems, neural interfaces — needs infrastructure that never blinks.\n\nN+2 power. Closed-loop liquid cooling. Dual subsea fiber. Redundant everything.\n\nUptime is not a feature. It is the product.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nCompute economics are simple:\n\n• Power cost per megawatt\n• Cooling efficiency (PUE)\n• Land cost per square meter\n• Latency to the market\n\nEverything else is detail. AI Grid Indonesia wins on all four.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe next generation of AI models will not run in Silicon Valley.\n\nNot because Silicon Valley cannot build them. Because Silicon Valley cannot power them.\n\nCompute is migrating to where the power and land are. Southeast Asia is first in line.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nBrain-computer interfaces generate data at rates that overwhelm traditional storage.\n\nA single high-channel neural array can produce terabytes per hour. Multiply by clinical trials, research, and patient monitoring.\n\nThe storage and processing layer is where the next innovation happens.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhat does 50MW actually mean?\n\n• ~50,000 homes worth of power\n• Thousands of high-density GPU racks\n• Tens of thousands of AI models trained per year\n• Hundreds of thousands of inference requests per second\n\nThis is industrial-scale compute.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy institutional capital is moving into AI infrastructure:\n\n• Contractual cashflows from enterprise leases\n• Hard-asset backing (land, power, cooling)\n• Inflation-linked pricing\n• Real downside protection if AI demand slows\n\nThis is not speculation. It is infrastructure.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe unsung hero of AI: thermal management.\n\nEvery watt delivered to a GPU becomes heat. Remove it efficiently or the system fails.\n\nDirect-to-chip liquid cooling: PUE under 1.15. Air cooling: PUE above 1.5. The difference is the entire compute margin.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy neural-class workloads need regional infrastructure:\n\n• Data sovereignty laws\n• Latency requirements (sub-10ms)\n• Real-time processing for medical applications\n• Continuous uptime for clinical use\n\nCloud does not solve all four. Dedicated regional infrastructure does.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe most expensive thing in AI is not the chip. It is the power.\n\nChips cost $30k–$50k each. But the power to run them for three years costs more. Cooling adds another layer. Land, connectivity, and redundancy stack on top.\n\nInfrastructure is where the money actually goes.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy 120kW per rack is the new standard:\n\n• NVIDIA Blackwell B200 clusters need it\n• Next-generation inference requires it\n• Neural signal processing demands it\n\nAir cooling caps at 40kW. This is why liquid is now table stakes.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe Southeast Asia advantage nobody talks about:\n\n• Young, technical workforce\n• Government alignment on AI and data centers\n• 100% foreign ownership permitted in SEZs\n• Tax holidays up to 20 years\n\nThis is what policy support for compute looks like.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nEvery compute cycle has a bottleneck.\n\n1990s: CPU speed. 2000s: bandwidth. 2010s: storage. 2020s: power and cooling.\n\nThe winners of the 2030s will be whoever solved the power problem in the 2020s.\n\nThis is why AI Grid Indonesia exists.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nLiquid cooling is not a marketing feature. It is a physical necessity.\n\nAt 40kW per rack, air works. At 120kW, you need liquid flowing through cold plates directly on the chip die.\n\nNo liquid, no Blackwell. No Blackwell, no next-generation AI.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy we chose Batam and not anywhere else:\n\n• Sub-2.5ms to Singapore (capital markets, cloud, research)\n• Direct subsea fiber to Jakarta and Hong Kong\n• 15-year 0% corporate tax under SEZ framework\n• Sovereign land title with 80+ year lease\n\nGeography is strategy.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe real-time compute stack:\n\n• Sub-10ms inference for autonomous systems\n• Continuous AI for clinical applications\n• Always-on signal processing for research\n\nThese are not cloud workloads. They are dedicated infrastructure workloads. Different product. Different build.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nA megawatt is worth more in the right place.\n\n1 MW in a dense metro: high land cost, high power cost, regulatory delays.\n1 MW in Batam SEZ: low land cost, low power cost, tax incentives, sub-2.5ms to Singapore.\n\nSame MW. Completely different economics.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhat institutional investors ask us first:\n\n1. Where is the power coming from? (PLN Batam, N+2 feeds)\n2. What is the cooling solution? (Direct-to-chip liquid)\n3. Who are the tenants? (Enterprise AI, neural-class workloads)\n4. What is the tax structure? (0% for 15 years)\n\nEvery answer is verifiable. This is why the structure holds.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe AI infrastructure paradox:\n\nDemand is exploding. Supply is constrained. Talent is scarce. Land in prime locations is unavailable.\n\nProjects that solve all four constraints become strategic assets — not commodities.\n\nAI Grid Indonesia is designed for this.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy 2026 is the year of the compute corridor:\n\n• Singapore overflow is queued against limited capacity\n• Indonesia pipeline is at 1.17GW and growing\n• Power grid can support a decade of buildout\n• Capital is flowing in\n\nThe window for early positioning is now.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhat makes a compute facility strategic rather than fungible:\n\n• Latency to the market (sub-2.5ms)\n• Power redundancy (N+2 feeds)\n• Cooling density (120kW/rack)\n• Legal structure (SEZ framework)\n\nFour things. Get all four right and the asset is unique.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nReal-time AI is the next compute frontier.\n\nVoice agents with 200ms round-trip. Autonomous vehicles with sub-100ms decision loops. Neural interfaces with sub-10ms processing.\n\nThese workloads cannot run on generic cloud. They need dedicated, low-latency, liquid-cooled infrastructure.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe compute cost curve:\n\n2020: $10 per million tokens\n2023: $1 per million tokens\n2026: $0.10 per million tokens\n\nCheaper models drive more demand. More demand drives more infrastructure. The cycle compounds.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy 15-year 0% corporate tax matters:\n\nMost data center projects lose money for 5–7 years. Tax holidays extend the compounding window into years 8–15.\n\nSame revenue. Completely different net return. This is why SEZ frameworks exist.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhat neural-class workloads need:\n\n• 120kW+ per rack\n• PUE below 1.15\n• Power redundancy N+2 or higher\n• Latency sub-10ms to primary market\n• Data sovereignty compliance\n\nAI Grid Indonesia delivers all five from day one.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy we talk about power before compute:\n\nCompute is worthless without power. At scale, power is the binding constraint.\n\n50MW is not a marketing number. It is the amount of power we have secured for Phase 1 alone.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe three things every AI infrastructure project needs:\n\n1. Power (contracted, redundant, affordable)\n2. Land (sovereign, titled, zoned)\n3. Cooling (liquid, efficient, scale-ready)\n\nGet all three and the facility is real. Skip any one and it is a slide deck.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nSub-2.5ms to Singapore is a specific number.\n\nIt is the speed of light through the shortest subsea fiber route. It is what allows real-time applications to run in Batam while the user sits in Singapore.\n\nLatency is not marketing. It is physics.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe compute demand curve is steeper than anyone modeled.\n\nEvery new AI model generates new inference demand. Every new inference demand requires compute. Every compute requirement pulls from the same limited global supply of land, power, and cooling.\n\nInfrastructure is the bottleneck. It is also the opportunity.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy Indonesia:\n\n• Largest economy in Southeast Asia\n• Government explicitly supporting AI and data centers\n• 100% foreign ownership permitted in SEZs\n• Young, educated workforce\n\nPolicy matters. This is what a supportive regulatory environment looks like.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe case for liquid cooling in one paragraph:\n\nAir can remove about 40kW per rack. Modern AI racks draw 100kW+. To close the gap, you pipe liquid coolant directly to the chip die. This is not optional. Without it, the chips do not run at spec. Without the chips at spec, the workload does not happen.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nReal-time neural signal processing:\n\nA high-channel neural array generates gigabytes of raw data per minute. Filtering, decoding, and inference must happen in real time — sub-10ms.\n\nThis is compute at the frontier. It is why dense, low-latency infrastructure matters.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhat makes a compute facility a strategic asset:\n\n• It is physically hard to replicate\n• It is expensive to build at scale\n• It is location-bound (latency, power, land)\n• It has contractual cashflow from tenants\n\nThis is infrastructure. Not a bet.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy 50MW is the right size:\n\n• Big enough to serve enterprise AI tenants\n• Small enough to build within a focused window\n• Matches the power available in Batam Phase 1\n• Fits the Batam SEZ land allocation\n\nThe number is not ambition. It is arithmetic.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe Southeast Asian compute story in one line:\n\nSingapore has demand and no space. Indonesia has space and no Singapore-grade connectivity. Batam has both. That is the entire thesis.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nReal-time AI is not a future trend. It is the current product:\n\n• Sub-200ms voice agents\n• Sub-100ms autonomous driving\n• Sub-10ms neural processing\n\nEvery one of these workloads runs on dedicated infrastructure. None of them run on generic cloud at scale.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy capital is flowing to Southeast Asian compute:\n\n• Latency to major markets (Singapore)\n• Cost advantage (power, land, labor)\n• Tax incentives (SEZ frameworks)\n• Government alignment (Indonesia AI push)\n\nFour vectors converging. This is a structural trend, not a cycle.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhat we mean by neural-class workloads:\n\n• High channel counts (dense compute)\n• Real-time processing (low latency)\n• Continuous uptime (redundancy)\n• Data sovereignty (regional hosting)\n\nThese are the four specifications. Everything else follows.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy PUE below 1.15 is the target:\n\nEvery unit of power spent on cooling is a unit not spent on compute. PUE 1.5 means 50% overhead. PUE 1.15 means 15% overhead.\n\nOver 50MW, that difference is worth tens of millions of dollars per year.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nThe infrastructure layer nobody talks about:\n\nEvery AI company is judged on models. Almost none talk about the power, land, and cooling that make those models possible.\n\nThe winners of the next cycle will own the foundation.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nSubsea fiber is the invisible moat.\n\nData moves at the speed of light through fiber. Every extra 1000km adds latency. Position matters.\n\nBatam sits on the shortest fiber route between Singapore and Indonesia. That is not geography — it is strategy.",
    "🧠 **FRONTIER COMPUTE BRIEFING**\n\nWhy we chose 2026 to launch:\n\n• AI demand inflection is here\n• Power constraints are now binding\n• Capital is moving to infrastructure\n• Southeast Asia is underbuilt relative to demand\n\nThe window is open now. It will not be in three years.",
]

# ─── AI GRID NEWS DATASET (100+ items) ───
AI_GRID_NEWS_DATASET = [
    {"title": "AI Grid Indonesia Deploys Direct-to-Chip Liquid Cooling in Batam Phase-1", "description": "Achieving a PUE under 1.15, the Batam SEZ 50MW facility sets a new benchmark for energy efficiency in Southeast Asian hyperscale AI compute.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Infrastructure"},
    {"title": "Batam SEZ Secures Subsea Fiber Interconnectivity Under 2.5ms to Singapore", "description": "Ultra-low latency routing connects AI Grid Indonesia directly to major regional financial hubs, ensuring high-speed throughput for LLM training.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Understanding the 20.0% Preferred Dividend Structure on 30-Day Cycles", "description": "How AI Grid Indonesia provides predictable cashflow distributions backed by enterprise lease agreements and high-density rack syndication.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "NVIDIA Blackwell B200 Architecture and High-Density Rack Readiness", "description": "Preparing hyperscale halls for 120kW per rack power densities required by next-generation cluster deployments.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Hardware"},
    {"title": "PLN Batam Confirms N+2 Dual 150kV Substation Feed Reliability", "description": "Uninterrupted power assurance for mission-critical AI compute workloads through robust high-voltage grid integration.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Tax Exemptions in Batam SEZ: 15-Year 0% Corporate Income Tax Advantages", "description": "Exploring the regulatory framework driving institutional capital into Indonesia's premier special economic zone.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "Sub-2.5ms Latency: The Strategic Case for Batam-Singapore Compute Corridor", "description": "Why subsea fiber topology between Batam and Singapore is the moat for APAC AI infrastructure.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Institutional Syndicate Allocation Opens for Batam 50MW Phase-1", "description": "Qualified participants can now secure tier positions from $1,000 micro allocations up to $100,000+ anchor syndicates.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Syndication"},
    {"title": "The Rise of Southeast Asia as a Global AI Compute Supercluster", "description": "Why foreign direct investment is rapidly pivoting toward Indonesian renewable and hybrid energy-powered data centers.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Automated Telegram Bot Payouts and Investor Transparency", "description": "Leveraging smart tooling to streamline communication, yield tracking, and instant platform updates for our global community.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Platform"},
    {"title": "Optimizing PUE: Why Liquid Cooling Outperforms Traditional Air Systems", "description": "Deep dive into thermal dynamics and coolant distribution units maintaining peak operating thresholds for high-density GPUs.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Infrastructure"},
    {"title": "Cross-Border Data Flows and Singapore-Batam Synergy", "description": "How regulatory frameworks within the SEZ facilitate seamless enterprise data transfers.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Target IRR Realities: Assessing 42.5% Net Returns in Infrastructure", "description": "Breaking down the financial models and long-term lease economics behind our projected target net IRR.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "Scaling AI Workloads: Cluster Architecture for Large Language Models", "description": "Analyzing network fabrics, InfiniBand switching, and low-latency interconnects deployed across Batam halls.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Hardware"},
    {"title": "Hybrid Solar Integration and Clean Energy PPA Milestones", "description": "Progress update on integrating renewable energy microgrids to support sustainable hyperscale operations.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Regulatory Safeguards and Asset Protection in SEZ Frameworks", "description": "How Indonesian investment laws protect foreign capital and ensure clear legal title for infrastructure syndications.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "NVIDIA Blackwell B200 / Direct Liquid Architecture Deep Dive", "description": "Custom liquid cooling manifold plates maintain thermal equilibrium for 1,000+ GPU clusters at 100% continuous load.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Hardware"},
    {"title": "Syndicate Growth: Expanding Phase-2 Capacity Planning in Batam", "description": "Looking ahead at upcoming land acquisition and electrical grid capacity expansions for our 2027 roadmap.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Syndication"},
    {"title": "Global Semiconductor Supply Chains and Data Center Readiness", "description": "Navigating logistics and hardware procurement timelines to ensure on-schedule rack deployment.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Enhanced Security Protocols Across AI Grid Portal Infrastructure", "description": "Implementing multi-factor authentication, hardware-secured session tokens, and encrypted bot ledger verification.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Platform"},
    {"title": "Singapore Data Center Moratorium: The Structural Supply Gap", "description": "Singapore's PUE floor of 1.25 and land constraints push high-density compute offshore. Batam absorbs the overflow.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Market Trends"},
    {"title": "Indonesia Government Names AI and Data Centers as National Priority", "description": "Coordinating Minister Airlangga Hartarto targets a 1.17GW data center pipeline and 600,000 digital professionals.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "The SIJORI Corridor: Singapore-Johor-Batam Integration", "description": "The first integrated cross-border data center platform in Southeast Asia is being built today.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "DayOne Signs 450MW PPA with PLN Batam: Peer Benchmark", "description": "Indonesia's largest data center power agreement signals institutional confidence in Batam's compute corridor.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Oracle Launches Indonesia North Cloud Region in Batam", "description": "Major hyperscalers are moving into Batam, validating the region's compute thesis.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Nongsa Digital Park: 13 Data Centers, ~800MW Pipeline", "description": "The Batam SEZ is scaling toward gigawatt-class compute capacity.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Telkom NeutraDC Nxera: 18MW Hyperscale Facility in Development", "description": "Domestic infrastructure players are entering the Batam compute market at scale.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "100% Foreign Ownership Permitted for Data Centers in SEZs", "description": "Indonesia's 2021 reforms allow full foreign ownership for data center projects in special economic zones.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "Ministry of Transmigration Prepares Workforce for Batam AI Investment", "description": "Indonesia is building the human capital required to support massive AI data center expansion.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "Asia-Pacific Data Center Capacity Scaling at ~19.6% CAGR", "description": "Industry estimates project explosive growth in regional hyperscale capacity through 2030.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Why Foreign Capital Is Moving to Southeast Asian Compute", "description": "Power costs, tax incentives, and government alignment are converging in Indonesia.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Market Trends"},
    {"title": "Batam Live Capacity: ~126MW, Pipeline: ~1.4GW", "description": "The Batam SEZ is scaling aggressively toward becoming Southeast Asia's primary compute hub.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Hyperscale Dominates ~71% of Batam's Data Center Segment", "description": "AI workloads are driving the shift toward higher-density, liquid-cooled facilities.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Market Trends"},
    {"title": "Singapore Overflow Demand Exceeds Batam's Entire Live Capacity", "description": "The demand curve for high-density compute is structurally unmatched by current supply.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Market Trends"},
    {"title": "The Strategic Case for Batam as the APAC AI Gateway", "description": "Sub-2.5ms to Singapore, 0% corporate tax, sovereign land — the three pillars of compute strategy.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Tier-IV Design Standard: What It Means for AI Compute", "description": "Fault-tolerant with concurrent maintainability — no single point of failure. The foundation AI workloads require.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Hardware"},
    {"title": "120kW Per Rack: The New Density Standard for AI Clusters", "description": "Older facilities cap at 40kW. Modern AI infrastructure requires 3x that density.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Hardware"},
    {"title": "Closed-Loop Direct-to-Chip Liquid Cooling Explained", "description": "Dielectric liquid coolant flows directly across chip dies to maintain thermal equilibrium at maximum load.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Hardware"},
    {"title": "PUE Below 1.15: The Efficiency Threshold That Changes Economics", "description": "Every watt saved on cooling is a watt available for compute. This is where margins come from.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Infrastructure"},
    {"title": "N+2 Power Redundancy: Eliminating Downtime at Scale", "description": "Dual 150kV feeds via PLN Batam with full N+2 architecture — zero single point of failure.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Subsea Fiber Topology: The Physics of Sub-2.5ms Latency", "description": "Data travels at the speed of light. Distance equals latency. Batam sits on the shortest fiber route to Singapore.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Sovereign Land Title and 80+ Year Lease Security", "description": "Physical asset backing with clear legal title — the foundation for institutional investment.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "SEZ Tax Framework: 15 Years at 0% Corporate Income Tax", "description": "The Batam SEZ grants long-duration tax holidays to infrastructure projects — the difference between marginal and exceptional net returns.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "AI Grid Batam Infrastructure SPV: The Legal Structure Explained", "description": "All participation routes through one SPV with clear contracts and power agreements.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "Why 50MW Is the Right Size for Phase-1", "description": "Large enough for institutional capital, focused enough to deliver within a defined window.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Emergency Backup Systems: Diesel Generators and Load Bank Testing", "description": "72-hour continuous load bank testing validates on-demand generator capacity.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Fire Suppression for Electronics-Intensive Environments", "description": "Clean-agent gaseous suppression systems designed specifically for high-density GPU halls.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Biometric Access Control and Multi-Layer Physical Security", "description": "Military-grade scanners and biometric checkpoints protecting the facility perimeter.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Platform"},
    {"title": "Network Fabric and InfiniBand Switching for LLM Training", "description": "Low-latency interconnects enabling distributed training across thousands of GPUs.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Hardware"},
    {"title": "20% Preferred Dividend Per 30-Day Cycle: Structure and Priority", "description": "Preferred holders receive distributions before common equity — priority economics at every payout.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "42.5% Target Net IRR: Modeling the Returns", "description": "Full-year preferred distributions combined with 3.8x MOIC target over 3-year term.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "3.8x Target MOIC: From $1 to $3.80 Over 3 Years", "description": "Multiple on invested capital combining preferred distributions plus terminal value.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "Why 30-Day Payout Cycles Matter for Investor Cashflow", "description": "Monthly distributions provide liquidity and predictable cashflow unmatched by typical infrastructure investments.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "Bank Transfer or Crypto: Flexible Payout Options", "description": "Choose USDT TRC-20, ERC-20, USDC, BTC, ETH, SOL, or BNB for distribution receipt.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Platform"},
    {"title": "Syndication Tier Structure: Micro to Anchor", "description": "From $1,000 Micro to $100,000+ Anchor — every tier shares the same 20% preferred return structure.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Syndication"},
    {"title": "Founding Board: 10 Seats, $50M Each, $500M Commitment", "description": "Governance-level participation for strategic infrastructure investors.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Syndication"},
    {"title": "Inflation Hedging Through Hard-Asset Backed Returns", "description": "Physical compute infrastructure remains one of the strongest inflation-resistant asset classes.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "Why Institutional Allocators Prefer Contractual Cashflows", "description": "Enterprise lease agreements deliver predictable distributions — the bedrock of infrastructure yield.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "ASEAN Digital Economy Blueprint and Data Center Expansion", "description": "Regional trade pacts and digital integration policies accelerate demand for high-capacity infrastructure.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Edge Compute Synergies with Autonomous Vehicle Fleets", "description": "Regional data hubs facilitate low-latency inference for autonomous driving networks and smart cities.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "The AI Compute Demand Curve: 2026 Projection", "description": "Every new model generates new inference demand. Every inference requires compute.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Why Power Is the Real Bottleneck in AI Infrastructure", "description": "At scale, compute is worthless without power. This is why we lead with MW capacity.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Customs Facility Integration for Expedited Hardware Clearance", "description": "SEZ bonded warehouse privileges accelerate server rack import timelines.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "Carrier-Neutral Meet-Me-Rooms: Multi-Carrier Interconnection", "description": "Tier-1 global carriers interconnect seamlessly within our Batam facility.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Subsea Cable Redundancy: Eliminating Single Points of Failure", "description": "Secondary and tertiary underwater fiber routes ensure connectivity resilience.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Advanced Fire Suppression for High-Density GPU Halls", "description": "Clean-agent gaseous suppression systems designed for electronics-intensive environments.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Structural Steel Framing Reaches Completion in Batam Phase-1", "description": "Engineering milestone: primary architectural frameworks for the main data hall concluded on schedule.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Seismic Resilience Engineering in Batam Data Center Foundations", "description": "Robust pilings and shock-absorbing foundation pads safeguard sensitive server arrays.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Smart Substation Automation and Real-Time Grid Telemetry", "description": "AI-driven load balancing monitors power quality across incoming feeders.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Redundant Water Cooling Loops and Filtration Plant Upgrades", "description": "Continuous closed-loop coolant purity with advanced reverse osmosis and deionization systems.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Power"},
    {"title": "Intellectual Property Protections in Batam SEZ", "description": "Secure legal environment for international AI and semiconductor innovators.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "Custom Thermal Transfer Plates for Next-Gen Accelerator Chips", "description": "Partnering with thermodynamic engineers to maximize heat dissipation directly off silicon dies.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Hardware"},
    {"title": "Server Chassis Customization for Optimal Vertical Airflow", "description": "Custom enclosures maximize airflow efficiency in high-density containment aisles.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Hardware"},
    {"title": "Long-Term PPA Contract Security and Fixed-Rate Power Hedging", "description": "Multi-year power purchase agreements lock in favorable energy tariffs.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "Batam Concession Agreement Compliance and Environmental Audits", "description": "Passing strict environmental impact assessments with flying colors.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "Syndicate Transparency Report Q3: Exceeding Projected Milestones", "description": "Comprehensive audits confirm strong financial performance and ahead-of-schedule construction.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Syndication"},
    {"title": "Micro-Allocation Tier Milestone: Over 10,000 Active Participants", "description": "Community growth as retail and institutional participants unify around premier compute assets.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Syndication"},
    {"title": "The Convergence of Robotics, AI, and Hyperscale Data Centers", "description": "Automated server maintenance robots and smart facility management tools.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Platform"},
    {"title": "Enhanced Security Protocols Across AI Grid Portal Infrastructure", "description": "Multi-factor authentication and hardware-secured session tokens protect investor accounts.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Platform"},
    {"title": "Automated Smart Contract Ledger Sync for Transparent Distributions", "description": "Backend portal infrastructure upgrades ensure verifiable payout ledger recordings.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Platform"},
    {"title": "Bot Portal v3.0: Faster Navigation and Enhanced Portfolio Tracking", "description": "Major UI upgrade bringing real-time earnings calculators directly to investor fingertips.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Platform"},
    {"title": "Low-Latency Direct Routing for Financial Trading and AI", "description": "Minimizing packet jitter across international subsea conduits for HFT and AI compute clients.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "The Rise of Southeast Asia as a Global AI Compute Supercluster", "description": "Why foreign direct investment is pivoting toward Indonesian renewable-powered data centers.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Unlocking Value in AI Infrastructure Through Liquid Cooling", "description": "How direct-to-chip thermal management transforms the economics of high-density compute.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Infrastructure"},
    {"title": "Southeast Asia's Data Center Boom: 2026 Outlook", "description": "Regional capacity scaling aggressively through the end of the decade.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
    {"title": "Real-Time AI Inference and the Future of Distributed Compute", "description": "Why edge-positioned data centers win the next generation of AI workloads.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Market Trends"},
    {"title": "Understanding the AI Infrastructure Investment Cycle", "description": "Why early positioning in compute corridors precedes outsized returns.", "image": "https://i.postimg.cc/yxQyQscY/IMG-8274.jpg", "category": "Financials"},
    {"title": "Batam SEZ: 15 Years of 0% Tax Advantage in Detail", "description": "Full breakdown of the corporate income tax exemption framework.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "The Physics of Latency: Why Sub-2.5ms Matters", "description": "Data travels at the speed of light. Every millisecond counts for AI inference.", "image": "https://i.postimg.cc/3RWRjh88/IMG-8276.jpg", "category": "Connectivity"},
    {"title": "Direct-to-Chip vs Immersion Cooling: A Comparison", "description": "Why direct-to-chip is the pragmatic choice for enterprise AI compute halls.", "image": "https://i.postimg.cc/MTrngTPR/IMG-8275.jpg", "category": "Hardware"},
    {"title": "Why 100% Foreign Ownership Matters for Institutional Investors", "description": "Indonesia's SEZ framework enables clean international capital participation.", "image": "https://i.postimg.cc/jdwxkr1w/IMG-8244.jpg", "category": "Regulation"},
    {"title": "The Role of N+2 Redundancy in Mission-Critical AI Halls", "description": "Eliminating single points of failure in power delivery.", "image": "https://i.postimg.cc/pLtZLWxk/IMG-8277.jpg", "category": "Power"},
    {"title": "AI Grid Indonesia: A 2026 Snapshot", "description": "50MW Phase-1 development progressing on schedule with full institutional backing.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Infrastructure"},
    {"title": "Why AI Compute Infrastructure Is the Decade's Defining Asset Class", "description": "Power, land, cooling, and latency — the four vectors of strategic advantage.", "image": "https://i.postimg.cc/kgxtD7GJ/IMG-8273.jpg", "category": "Market Trends"},
]

# ─── ALLOCATOR FEEDBACK POOL (130+) ───
TESTIMONIES_POOL = [
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
    ("Abdullah Al-Rashid", "Principal, Al-Rashid Family Office, Riyadh", "We diversify across tech, energy, and real estate. AI infrastructure is the intersection of all three."),
    ("Christina Wong", "Director, Wong Family Holdings, Hong Kong", "Our mandate is capital preservation first. This project offers that with an aggressive preferred return."),
    ("Richard Ashworth", "Trustee, Ashworth Trust, London", "Generational wealth needs assets with real backing. Physical data center hardware is exactly that."),
    ("Beatriz Silva", "Partner, Silva Family Office, São Paulo", "We've watched the AI compute wave from Latin America. Southeast Asia infrastructure is the natural next position."),
    ("Kwame Addo", "Director, Addo Holdings, Accra", "The transparency of the SPV structure and 30-day payout cycle is exceptional for emerging market exposure."),
    ("Helena Novak", "Principal, Novak Trust, Prague", "We backed this after visiting the Batam SEZ virtually. The land title framework is the cleanest we've seen."),
    ("Sultan Al-Qasimi", "Director, Al-Qasimi Holdings, Abu Dhabi", "Sovereign-adjacent capital wants yield with sovereign-grade backing. This project delivers."),
    ("Isabella Fernández", "Managing Partner, Fernández Family Office, Mexico City", "Infrastructure assets are our core. This project adds the AI thesis layer to a physical foundation."),
    ("John Whitmore", "Trustee, Whitmore Family Trust, Boston", "My grandfather's trust invested in railroads. I'm doing the same with compute infrastructure."),
    ("Leila Haddad", "Director, Haddad Holdings, Beirut", "Real assets in stable jurisdictions are the answer to currency risk. This is exactly what we needed."),
    ("Cristina Rossi", "Principal, Rossi Family Office, Rome", "We've reviewed this deal three times. Each time we found more reasons to allocate. That's rare."),
    ("Peter Andersen", "Director, Andersen Family Trust, Oslo", "Northern European capital sees Southeast Asia as the growth story of the next decade."),
    ("Amina Diallo", "Managing Director, Diallo Holdings, Dakar", "Sovereign wealth practices are changing. Exposure to AI infrastructure is now a strategic necessity."),
    ("Mateo Cortés", "Partner, Cortés Family Office, Santiago", "Latin American infrastructure taught me to value reliability. This project has it."),
    ("Sarah Cohen", "Director, Cohen Family Trust, Tel Aviv", "The founder's actual track record is what convinced the trust. We don't back anonymous operators."),
    ("Hiroshi Sato", "Senior Analyst, Nomura Infrastructure Research, Tokyo", "Grid reliability is the underlying thesis of AI infrastructure. AI Grid Indonesia gets the fundamentals right."),
    ("Amara Okonkwo", "Portfolio Manager, Lagos Infrastructure Fund, Lagos", "The African data center story is 5 years behind Southeast Asia. I invest where maturity already exists."),
    ("Luca Bianchi", "Partner, Milan Infrastructure Partners, Milan", "European capital is waking up to Southeast Asian compute infrastructure. This project is early enough."),
    ("Sofia Vasquez", "Fund Manager, Madrid Tech Fund, Madrid", "I compare every AI deal against the underlying compute demand curve. This one is well-timed."),
    ("Nils Berg", "Principal, Stockholm Growth Partners, Stockholm", "Clean energy integration plus AI compute is the intersection I want exposure to."),
    ("Charlotte Dupont", "Managing Director, Paris Infrastructure, Paris", "Southeast Asian data centers are structurally underserved. This project addresses a real supply gap."),
    ("Erik Johansson", "Director, Copenhagen Tech Capital, Copenhagen", "I look for assets with clean legal structures. The Batam SEZ framework is clean."),
    ("Mira Patel", "Fund Manager, London India Capital, London", "The Indian diaspora is watching Southeast Asian infrastructure closely."),
    ("Adam Rosenberg", "Managing Partner, New York Tech Fund, New York", "I've been through three compute cycles. Infrastructure is where the durability is."),
    ("Samantha Foster", "Director, Chicago Real Assets, Chicago", "Data centers are the new commercial real estate. This project is the template."),
    ("Hana Suzuki", "Analyst, Mizuho Infrastructure, Tokyo", "The 120kW per rack spec is important. Older facilities can't be upgraded to this density."),
    ("Michael Wu", "Managing Director, Taipei Infrastructure, Taipei", "The Taiwan compute ecosystem is watching Batam. This could be a template for regional expansion."),
    ("Daniela Moreno", "Fund Manager, Bogotá Infrastructure, Bogotá", "Latin American data centers face power constraints. Indonesia's PLN framework avoids this bottleneck."),
    ("Alexei Petrov", "Principal, Moscow Tech Fund, Moscow", "Emerging markets with strong government alignment are where the best risk-adjusted returns are."),
    ("Ines Ferreira", "Partner, Lisbon Real Assets, Lisbon", "Portugal and Indonesia share a similar land development approach. The SEZ framework is familiar."),
    ("Yusuf Ibrahim", "Director, Cairo Infrastructure Fund, Cairo", "AI compute is the new oil. Owning the refinery matters more than owning the crude."),
    ("Ravi Shankar", "Fund Manager, Chennai Infrastructure, Chennai", "Indian family offices are allocating to Southeast Asian data centers for the first time."),
    ("Mei Wong", "Director, Kuala Lumpur Growth Fund, Kuala Lumpur", "Malaysia and Indonesia share the SIJORI corridor. This project strengthens the whole region."),
    ("Ji-Ho Park", "Managing Director, Seoul Tech Partners, Seoul", "Korea's semiconductor ecosystem understands data center fundamentals. This project passes our review."),
    ("Esther Kimani", "Fund Manager, Nairobi Growth Capital, Nairobi", "African infrastructure investors look to Southeast Asia as the model. This project sets the standard."),
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

# ─── RSS FEEDS (25+ Elon ecosystem sources) ───
RSS_FEEDS = [
    # Tesla
    "https://www.teslarati.com/feed/",
    "https://electrek.co/guides/tesla/feed/",
    "https://insideevs.com/rss/articles/all/",
    "https://cleantechnica.com/tag/tesla/feed/",
    "https://www.teslaoracle.com/feed/",
    # SpaceX
    "https://www.nasaspaceflight.com/feed/",
    "https://spacenews.com/feed/",
    "https://www.space.com/feeds/all",
    "https://arstechnica.com/space/feed/",
    "https://www.universetoday.com/feed/",
    # AI
    "https://techcrunch.com/category/artificial-intelligence/feed/",
    "https://venturebeat.com/category/ai/feed/",
    "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
    "https://www.wired.com/feed/tag/ai/latest/rss",
    "https://arstechnica.com/ai/feed/",
    # Neuralink / Neuro
    "https://www.medicalnewstoday.com/rss/neurology.xml",
    "https://www.sciencedaily.com/rss/mind_brain/neuroscience.xml",
    "https://www.fiercebiotech.com/rss/xml",
    "https://www.statnews.com/feed/",
    # Musk ecosystem general
    "https://www.teslarati.com/category/spacex/feed/",
    "https://www.thestreet.com/rss/news",
    "https://futurism.com/feed",
    "https://www.businessinsider.com/rss",
    "https://www.reuters.com/rssFeed/technologyNews",
    "https://feeds.bloomberg.com/technology/news.rss",
    "https://www.axios.com/feeds/feed.rss",
    "https://www.theinformation.com/feed",
]

_used_news_indices = []
_used_testimony_indices = []
_used_ad_indices = []
_used_engagement_indices = []
_used_briefing_indices = []

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

def get_frontier_briefing():
    idx = _pick_unused(len(FRONTIER_COMPUTE_BRIEFINGS), _used_briefing_indices)
    text = FRONTIER_COMPUTE_BRIEFINGS[idx] + f"\n\n🚀 [Explore AI Grid Indonesia]({NETLIFY_URL})"
    image = random.choice(TESTIMONY_IMAGES)
    return {"type": "photo", "image": image, "text": text}

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

def extract_rss_image(entry) -> str:
    """Extract the best available image from an RSS entry."""
    # Try media_content first (most common)
    try:
        if hasattr(entry, "media_content") and entry.media_content:
            return entry.media_content[0].get("url")
    except Exception:
        pass

    # Try media_thumbnail
    try:
        if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
            return entry.media_thumbnail[0].get("url")
    except Exception:
        pass

    # Try enclosures
    try:
        if hasattr(entry, "enclosures") and entry.enclosures:
            for enc in entry.enclosures:
                if enc.get("type", "").startswith("image"):
                    return enc.get("href")
    except Exception:
        pass

    # Try to scrape the first <img> from the summary HTML
    try:
        summary_html = entry.get("summary", "") + entry.get("description", "")
        if summary_html:
            img_match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', summary_html)
            if img_match:
                return img_match.group(1)
    except Exception:
        pass

    # Try og:image from the article page itself
    try:
        link = entry.get("link", "")
        if link:
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = httpx.get(link, headers=headers, timeout=8.0, follow_redirects=True)
            if resp.status_code == 200:
                og_match = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', resp.text)
                if og_match:
                    return og_match.group(1)
                tw_match = re.search(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', resp.text)
                if tw_match:
                    return tw_match.group(1)
    except Exception as e:
        logger.warning(f"Could not fetch og:image: {e}")

    return None


def pick_rss_image_from_content(title: str) -> str:
    """Fallback image based on the news topic."""
    t = title.lower()
    if any(k in t for k in ["tesla", "model", "optimus", "cybertruck", "fsd", "battery"]):
        return "https://images.unsplash.com/photo-1560958089-b8a1929cea89?q=80&w=1600&auto=format&fit=crop"
    if any(k in t for k in ["spacex", "starship", "falcon", "rocket", "launch"]):
        return "https://images.unsplash.com/photo-1517976487492-5750f3195933?q=80&w=1600&auto=format&fit=crop"
    if any(k in t for k in ["starlink", "satellite"]):
        return "https://images.unsplash.com/photo-1451187580459-43490279c0fa?q=80&w=1600&auto=format&fit=crop"
    if any(k in t for k in ["neuralink", "brain", "neuro"]):
        return "https://images.unsplash.com/photo-1559757148-5c350d0d3c56?q=80&w=1600&auto=format&fit=crop"
    if any(k in t for k in ["grok", "xai", "ai", "artificial"]):
        return "https://images.unsplash.com/photo-1677442136019-21780ecad995?q=80&w=1600&auto=format&fit=crop"
    if any(k in t for k in ["chip", "gpu", "nvidia", "semiconductor"]):
        return "https://images.unsplash.com/photo-1591405351990-4726e331f141?q=80&w=1600&auto=format&fit=crop"
    return "https://images.unsplash.com/photo-1518770660439-4636190af475?q=80&w=1600&auto=format&fit=crop"


def detect_category(title: str, summary: str, source_name: str) -> str:
    """Detect which company/category the news belongs to."""
    text = (title + " " + summary + " " + source_name).lower()

    if any(k in text for k in ["neuralink", "brain implant", "bci", "neuro"]):
        return "🧠 Neuralink Update"
    if any(k in text for k in ["spacex", "starship", "falcon 9", "falcon heavy", "starlink", "dragon capsule"]):
        return "🚀 SpaceX Update"
    if any(k in text for k in ["tesla", "model 3", "model y", "model s", "model x", "cybertruck", "optimus", "fsd", "autopilot", "supercharger"]):
        return "⚡️ Tesla Update"
    if any(k in text for k in ["grok", "xai", "x.ai"]):
        return "🤖 xAI Update"
    if any(k in text for k in ["the boring company", "boring co", "tunnel"]):
        return "🚇 Boring Company Update"
    if any(k in text for k in ["elon musk", "elon"]):
        return "👤 Elon Musk Update"
    if any(k in text for k in ["x.com", "twitter", "twitter/x"]):
        return "🐦 X Update"
    return "📰 xUniverse Update"


def detect_hashtag(title: str, summary: str) -> str:
    """Pick the right hashtag based on content."""
    text = (title + " " + summary).lower()
    if "neuralink" in text or "brain" in text:
        return "#Neuralink"
    if "spacex" in text or "starlink" in text or "starship" in text or "falcon" in text:
        return "#SpaceX"
    if "tesla" in text or "cybertruck" in text or "optimus" in text:
        return "#Tesla"
    if "grok" in text or "xai" in text:
        return "#xAI"
    if "boring" in text:
        return "#BoringCompany"
    return "#xUniverse"


def fetch_rss_item():
    """Fetch a live news item and format it like the original xUniverse bot."""
    try:
        feed_list = RSS_FEEDS[:]
        random.shuffle(feed_list)

        for feed_url in feed_list[:6]:
            try:
                parsed = feedparser.parse(feed_url)
                if not parsed.entries:
                    continue

                entry = random.choice(parsed.entries[:8])
                title = entry.get("title", "Ecosystem Update").strip()
                link = entry.get("link", NETLIFY_URL)

                # Source name
                source_name = parsed.feed.get("title", "Industry Source").strip()

                # Clean summary
                raw_summary = entry.get("summary", "") or entry.get("description", "")
                clean_summary = re.sub("<.*?>", "", raw_summary).strip()
                clean_summary = clean_summary[:250] + ("..." if len(clean_summary) > 250 else "")

                # Get the image
                image_url = extract_rss_image(entry)
                if not image_url:
                    image_url = pick_rss_image_from_content(title)

                # Detect category and hashtag
                category_header = detect_category(title, clean_summary, source_name)
                hashtag = detect_hashtag(title, clean_summary)

                # Format the post (title-first, like the old bot)
                text = (
                    f"{category_header}\n\n"
                    f"**{title}**\n\n"
                    f"🔗 [Read more]({link})\n\n"
                    f"📅 {datetime.utcnow().strftime('%b %d, %Y • %H:%M UTC')}\n\n"
                    f"#xUniverse {hashtag}"
                )

                return {
                    "type": "photo",
                    "image": image_url,
                    "text": text,
                    "source_link": link,
                }
            except Exception as inner_e:
                logger.warning(f"RSS feed {feed_url} failed: {inner_e}")
                continue
    except Exception as e:
        logger.warning(f"RSS fetch warning: {e}")
    return None
    
# ─── TIME-OF-DAY CONTENT SELECTION ───
def get_channel_content_for_hour(hour_utc: int):
    """Different content type by time of day."""
    if hour_utc == 6:
        # Morning briefing — news-heavy
        choice = random.choice(["dataset", "dataset", "rss", "briefing"])
    elif hour_utc == 10:
        # Midday — live ecosystem news
        choice = random.choice(["rss", "rss", "dataset", "briefing"])
    elif hour_utc == 14:
        # Afternoon — frontier briefings
        choice = random.choice(["briefing", "briefing", "dataset", "testimony"])
    elif hour_utc == 18:
        # Evening — engagement + testimony
        choice = random.choice(["testimony", "engagement", "rss", "dataset"])
    elif hour_utc == 22:
        # Night — ads + allocator feedback
        choice = random.choice(["ad", "testimony", "testimony", "briefing"])
    else:
        choice = random.choice(["dataset", "briefing", "rss", "testimony", "ad", "engagement"])

    if choice == "briefing":
        return get_frontier_briefing()
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

# ============================================================
# SCHEDULED FUNCTIONS
# ============================================================
async def send_restart_announcement():
    if not TELEGRAM_CHANNEL_ID:
        return
    text = (
        "🚀 AI GRID INDONESIA | Back Online\n\n"
        "Our unified bot is now live — investor portal, channel updates, and frontier compute intelligence in one place.\n\n"
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

async def _post_content(content):
    """Shared helper for all scheduled broadcasts."""
    if CHANNEL_STATE["scheduler_paused"]:
        logger.info("Scheduler paused — skipping broadcast.")
        return
    if not TELEGRAM_CHANNEL_ID:
        return
    try:
        if content["type"] == "photo":
            # If the content has a source link, use it so tapping the image opens the article
            source_link = content.get("source_link")
            if source_link:
                # Send as URL photo with caption containing the link
                msg = await safe_channel_send_photo(
                    photo_url=content["image"],
                    caption=content["text"],
                )
            else:
                msg = await safe_channel_send_photo(
                    photo_url=content["image"],
                    caption=content["text"],
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
        
async def scheduled_morning_brief():
    await _post_content(get_channel_content_for_hour(6))

async def scheduled_midday_update():
    await _post_content(get_channel_content_for_hour(10))

async def scheduled_afternoon_update():
    await _post_content(get_channel_content_for_hour(14))

async def scheduled_evening_brief():
    await _post_content(get_channel_content_for_hour(18))

async def scheduled_night_update():
    await _post_content(get_channel_content_for_hour(22))

async def scheduled_channel_broadcast():
    """Legacy — kept for compatibility. Fires evening slot."""
    await scheduled_evening_brief()

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

async def scheduled_payout_reminder():
    if not TELEGRAM_ADMIN_IDS:
        return
    try:
        investors = await get_active_investors_with_payouts()
        total = sum(float(i.total_allocated_usd or 0) * 0.20 for i in investors)
        text = (
            f"💰 **Payout Cycle Reminder**\n"
            f"───────────────────────────────\n"
            f"• Active investors with wallets: {len(investors)}\n"
            f"• Total pending this cycle: ${total:,.2f}\n\n"
            f"Run `/payoutlist` to see the full breakdown.\n"
            f"Use `/markpaid <INVESTOR_ID>` after each payout is sent."
        )
        for admin_id in TELEGRAM_ADMIN_IDS:
            try:
                await telegram_app.bot.send_message(chat_id=admin_id, text=text, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Could not DM admin {admin_id}: {e}")
    except Exception as e:
        logger.error(f"Payout reminder failed: {e}")

async def scheduled_monthly_statements():
    try:
        investors = await list_investors(1000)
        for inv in investors:
            if not inv.is_active:
                continue
            total = float(inv.total_allocated_usd or 0)
            monthly = total * 0.20
            wallet_line = f"• **Payout Wallet:** `{inv.wallet_address[:8]}...{inv.wallet_address[-6:]}`" if inv.wallet_address else "• **Payout Wallet:** not set"
            try:
                await telegram_app.bot.send_message(
                    chat_id=inv.telegram_user_id,
                    text=(
                        f"📊 **Monthly Statement — {datetime.utcnow().strftime('%B %Y')}**\n"
                        f"───────────────────────────────\n"
                        f"• **Investor ID:** `{inv.investor_id}`\n"
                        f"• **Tier:** {inv.tier}\n"
                        f"• **Total Allocated:** ${total:,.2f}\n"
                        f"• **30-Day Payout:** ${monthly:,.2f}\n"
                        f"{wallet_line}\n\n"
                        f"Thank you for being part of AI Grid Indonesia."
                    ),
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.warning(f"Could not send statement to {inv.investor_id}: {e}")
    except Exception as e:
        logger.error(f"Monthly statements failed: {e}")

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
    states={
        WAITING_REGISTER_CONTACT: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_receive_contact)],
        WAITING_REGISTER_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_receive_wallet)],
        WAITING_REGISTER_TELEGRAM: [
            CommandHandler("skip", register_receive_telegram),
            MessageHandler(filters.TEXT & ~filters.COMMAND, register_receive_telegram),
        ],
    },
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

update_wallet_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(update_wallet_prompt, pattern="^update_wallet$")],
    states={WAITING_UPDATE_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, update_wallet_receive)]},
    fallbacks=[CommandHandler("start", start)]
)

update_telegram_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(update_telegram_prompt, pattern="^update_telegram$")],
    states={WAITING_UPDATE_TELEGRAM: [MessageHandler(filters.TEXT & ~filters.COMMAND, update_telegram_receive)]},
    fallbacks=[CommandHandler("start", start)]
)

# Command handlers
telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(CommandHandler("founder", cmd_founder))
telegram_app.add_handler(CommandHandler("vision", cmd_vision))
telegram_app.add_handler(CommandHandler("neural", cmd_neural))
telegram_app.add_handler(CommandHandler("risk", cmd_risk))
telegram_app.add_handler(CommandHandler("status", cmd_status))
telegram_app.add_handler(CommandHandler("mystatus", cmd_mystatus))
telegram_app.add_handler(CommandHandler("post", cmd_post))
telegram_app.add_handler(CommandHandler("postmedia", cmd_postmedia))
telegram_app.add_handler(CommandHandler("quiet", cmd_quiet))
telegram_app.add_handler(CommandHandler("resume", cmd_resume))
telegram_app.add_handler(CommandHandler("chanstat", cmd_chanstat))
telegram_app.add_handler(CommandHandler("testpost", cmd_testpost))
telegram_app.add_handler(CommandHandler("lookup", cmd_lookup))
telegram_app.add_handler(CommandHandler("suspend", cmd_suspend))
telegram_app.add_handler(CommandHandler("unsuspend", cmd_unsuspend))
telegram_app.add_handler(CommandHandler("resetpin", cmd_resetpin))
telegram_app.add_handler(CommandHandler("listusers", cmd_listusers))
telegram_app.add_handler(CommandHandler("listpayments", cmd_listpayments))
telegram_app.add_handler(CommandHandler("payoutlist", cmd_payoutlist))
telegram_app.add_handler(CommandHandler("markpaid", cmd_markpaid))
telegram_app.add_handler(CommandHandler("dashboard", cmd_dashboard))
telegram_app.add_handler(CommandHandler("pausepayout", cmd_pausepayout))
telegram_app.add_handler(CommandHandler("resumepayout", cmd_resumepayout))
telegram_app.add_handler(CommandHandler("kyc", cmd_kyc))
telegram_app.add_handler(CommandHandler("export", cmd_export))

# Conversation handlers
telegram_app.add_handler(register_handler)
telegram_app.add_handler(login_handler)
telegram_app.add_handler(recover_handler)
telegram_app.add_handler(custom_amount_handler)
telegram_app.add_handler(update_wallet_handler)
telegram_app.add_handler(update_telegram_handler)

# Callback handlers
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
telegram_app.add_handler(CallbackQueryHandler(account_settings_handler, pattern="^account_settings$"))
telegram_app.add_handler(CallbackQueryHandler(back_to_profile_handler, pattern="^back_to_profile$"))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
