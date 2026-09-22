import os
import logging
import time
import httpx
from collections import defaultdict
from fastapi import FastAPI, Request, Response
import uvicorn
from contextlib import asynccontextmanager

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

# Setup logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

NOWPAYMENTS_API_KEY = os.environ.get("NOWPAYMENTS_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
NOWPAYMENTS_API_URL = "https://api.nowpayments.io/v1"

# Conversation State
WAITING_CUSTOM_AMOUNT = 1

# ============================================================
# [ADDED — ANTI-BOT CONFIG]
# Rate limits, caps, and sanity thresholds to protect NOWPayments
# ============================================================
INVOICE_COOLDOWN_SECONDS = 90          # 1 invoice per user per 90 seconds
DAILY_INVOICE_CAP = 10                  # max 10 invoices per user per day
MAX_AMOUNT_USD = 10_000_000             # hard cap on single allocation
MIN_AMOUNT_USD = 1_000                  # raised from $100 to $1,000
SUSPICIOUS_REPEAT_WINDOW = 300          # 5 minutes — same amount = flag

# In-memory rate tracker — survives across conversations
# Structure: { user_id: { "last_invoice_ts": float, "daily_count": int, "daily_reset_ts": float, "recent_amounts": [(ts, amount), ...] } }
RATE_TRACKER = defaultdict(lambda: {
    "last_invoice_ts": 0.0,
    "daily_count": 0,
    "daily_reset_ts": time.time(),
    "recent_amounts": []
})

# Crypto ticker mapping for NOWPayments API
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

# Initialize Telegram application globally
telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await telegram_app.initialize()
    await telegram_app.start()
    await telegram_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    logging.info("Telegram bot polling started successfully via FastAPI lifespan!")
    yield
    # Shutdown
    await telegram_app.updater.stop()
    await telegram_app.stop()
    await telegram_app.shutdown()
    logging.info("Telegram bot stopped.")

app = FastAPI(lifespan=lifespan)

@app.post("/webhook/nowpayments")
async def nowpayments_webhook(request: Request):
    data = await request.json()
    payment_status = data.get("payment_status")
    order_id = data.get("order_id")
    
    if payment_status in ["finished", "confirmed"] and order_id:
        try:
            telegram_user_id = int(order_id.split("_")[-1])
            logging.info(f"Payment confirmed successfully for user {telegram_user_id}!")
            await telegram_app.bot.send_message(
                chat_id=telegram_user_id,
                text="🎉 **Capital Allocation Confirmed!**\n\nYour transaction has been verified on the blockchain. Our investor relations desk will follow up shortly with your official participation agreement.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logging.error(f"Error processing webhook user ID: {e}")
            
    return Response(status_code=200)

# ============================================================
# [ADDED — ANTI-BOT HELPERS]
# All rate-limit logic lives here
# ============================================================
def check_user_rate_limit(user_id: int, amount: float) -> tuple[bool, str]:
    """
    Returns (allowed, reason).
    If allowed=False, reason contains the message to show the user.
    """
    now = time.time()
    tracker = RATE_TRACKER[user_id]
    
    # Reset daily counter if 24h passed
    if now - tracker["daily_reset_ts"] > 86400:
        tracker["daily_count"] = 0
        tracker["daily_reset_ts"] = now
    
    # Layer 1 — cooldown between invoices
    if now - tracker["last_invoice_ts"] < INVOICE_COOLDOWN_SECONDS:
        wait = int(INVOICE_COOLDOWN_SECONDS - (now - tracker["last_invoice_ts"]))
        return False, f"⏱️ Please wait **{wait} seconds** between allocation attempts. This protects the payment gateway."
    
    # Layer 2 — daily cap
    if tracker["daily_count"] >= DAILY_INVOICE_CAP:
        return False, "📊 Daily allocation limit reached. Please try again tomorrow or contact support at **contact@aigrid.id**."
    
    # Layer 3 — same amount repeated too often in short window
    recent = [a for (ts, a) in tracker["recent_amounts"] if now - ts < SUSPICIOUS_REPEAT_WINDOW]
    if len(recent) >= 3 and all(abs(a - amount) < 0.01 for a in recent[-3:]):
        return False, "🔒 Repeated identical allocations flagged for review. Please contact **contact@aigrid.id** to proceed."
    
    # Layer 4 — amount sanity checks
    if amount > MAX_AMOUNT_USD:
        return False, f"⚠️ Amount exceeds the automated allocation limit. For allocations above ${MAX_AMOUNT_USD:,.0f}, please contact **contact@aigrid.id** directly."
    
    return True, ""

def record_invoice_attempt(user_id: int, amount: float):
    """Record a successful invoice creation for rate limiting."""
    now = time.time()
    tracker = RATE_TRACKER[user_id]
    tracker["last_invoice_ts"] = now
    tracker["daily_count"] += 1
    tracker["recent_amounts"].append((now, amount))
    # Keep only last 10 entries
    tracker["recent_amounts"] = tracker["recent_amounts"][-10:]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Executive Dynamic Onboarding Flow"""
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
        "• 20% Preferred Dividend\n"
        "• 30-Day Payout Cycle (bank or crypto)\n"
        "• Telegram Bot Access\n\n"
        "🔹 **Tier 2 — Syndicate | $5,000 – $24,999**\n"
        "• 20% Preferred Dividend\n"
        "• 42.5% Target Net IRR\n"
        "• Pro-rata Rights Phase 2\n\n"
        "🔹 **Tier 3 — Institutional | $25,000 – $99,999** ⭐ *Featured*\n"
        "• Priority Dividend Payout\n"
        "• 3.8x Target MOIC\n"
        "• Priority Allocation Phase 2\n\n"
        "🔹 **Tier 4 — Anchor | $100,000+**\n"
        "• Structured Equity / Debt\n"
        "• Dedicated GPU Compute\n"
        "• VIP Site Inspection\n"
        "• Direct Founding Team Access\n\n"
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
        "• **$1,000 Allocation:** **$200.00** / 30 days ($2,400 / year)\n"
        "• **$5,000 Allocation:** **$1,000.00** / 30 days ($12,000 / year)\n"
        "• **$25,000 Allocation:** **$5,000.00** / 30 days ($60,000 / year)\n"
        "• **$100,000 Allocation:** **$20,000.00** / 30 days ($240,000 / year)\n\n"
        "📈 **3-Year Target MOIC:** 3.8x (projected)\n\n"
        "💡 *All figures are forward-looking targets, not guarantees. Capital is at risk. Payouts route via bank transfer or crypto (USDT/BTC/ETH).*"
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
        [InlineKeyboardButton("✍️ Custom Amount ($1,000+)", callback_data="amount_custom")],
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
        "*(Minimum allocation: $1,000 USD)*",
        parse_mode="Markdown"
    )
    return WAITING_CUSTOM_AMOUNT

async def receive_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().replace("$", "").replace(",", "")
    try:
        val = float(text)
        if val < MIN_AMOUNT_USD:
            await update.message.reply_text(f"❌ Minimum allocation is ${MIN_AMOUNT_USD:,} USD. Please enter a higher value:")
            return WAITING_CUSTOM_AMOUNT
        
        if val > MAX_AMOUNT_USD:
            await update.message.reply_text(f"⚠️ Amounts above ${MAX_AMOUNT_USD:,} require direct contact. Please email **contact@aigrid.id**.", parse_mode="Markdown")
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
            f"✅ **Amount Set:** ${val:,.2f} USD\n\nSelect your preferred cryptocurrency payment method:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text("⚠️ Invalid number format. Please enter a valid numerical value (e.g., 2500):")
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
        f"💵 **Selected Allocation:** ${amount:,.2f} USD\n\n"
        f"Select your preferred cryptocurrency for payment:",
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
    
    # ============================================================
    # [ADDED — ANTI-BOT CHECK]
    # Run rate limits before touching NOWPayments
    # ============================================================
    allowed, reason = check_user_rate_limit(user_id, amount)
    if not allowed:
        await query.edit_message_text(
            reason,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Allocation", callback_data="allocate_menu")]])
        )
        return
    
    await query.edit_message_text("🔄 **Connecting to blockchain gateway & generating payment invoice...**", parse_mode="Markdown")
    
    headers = {"x-api-key": NOWPAYMENTS_API_KEY, "Content-Type": "application/json"}
    payload = {
        "price_amount": float(amount),
        "price_currency": "usd",
        "pay_currency": crypto_info["ticker"],
        "order_id": f"aigrid_{amount:.0f}_{user_id}",
        "order_description": f"AI Grid Indonesia Allocation (${amount:,.2f} USD)"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{NOWPAYMENTS_API_URL}/payment", json=payload, headers=headers)
            data = response.json()
            
            if "pay_address" in data:
                pay_address = data["pay_address"]
                pay_amount = data["pay_amount"]
                pay_currency = data["pay_currency"].upper()
                
                # Save session data for toggling QR code view
                context.user_data["pay_address"] = pay_address
                context.user_data["pay_amount"] = pay_amount
                context.user_data["pay_currency"] = pay_currency
                context.user_data["crypto_label"] = crypto_info["label"]
                
                # ============================================================
                # [ADDED — ANTI-BOT RECORD]
                # Record successful invoice for rate tracking
                # ============================================================
                record_invoice_attempt(user_id, amount)
                
                invoice_text = (
                    f"✅ **OFFICIAL ALLOCATION INVOICE**\n"
                    f"───────────────────────────────\n"
                    f"• **USD Value:** ${amount:,.2f} USD\n"
                    f"• **Asset:** {crypto_info['label']}\n"
                    f"• **Exact Amount to Send:** `{pay_amount}` **{pay_currency}**\n\n"
                    f"📍 **Deposit Address:**\n"
                    f"`{pay_address}`\n\n"
                    f"⚠️ *Important:* Send the exact amount above. Your participation will be recorded automatically as soon as the transaction is confirmed on the network."
                )
                
                keyboard = [
                    [InlineKeyboardButton("📱 Do you need a QR code?", callback_data="show_qr")],
                    [InlineKeyboardButton("🔄 Main Menu", callback_data="main_menu")],
                    [InlineKeyboardButton("📩 Contact Support", url="https://t.me/contactaigrid")]
                ]
                await query.edit_message_text(invoice_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            else:
                logging.error(f"NOWPayments Error: {data}")
                await query.edit_message_text(
                    "❌ **Gateway Timeout:** Error creating crypto invoice. Please try again or contact support.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Try Again", callback_data="allocate_menu")]]),
                    parse_mode="Markdown"
                )
    except Exception as e:
        logging.error(f"Exception generating invoice: {e}")
        await query.edit_message_text("❌ Connection error. Please try again later.")

async def show_qr_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    pay_address = context.user_data.get("pay_address", "N/A")
    qr_code_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={pay_address}"
    
    qr_caption = (
        f"📱 **SCAN TO PAY**\n"
        f"───────────────────────────────\n"
        f"Scanning this QR code directly from your crypto wallet app eliminates manual typing mistakes and ensures funds route safely.\n\n"
        f"📍 **Deposit Address:**\n"
        f"`{pay_address}`"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Wallet Address", callback_data="back_to_invoice")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.message.delete()
    except Exception:
        pass
        
    await context.bot.send_photo(
        chat_id=query.from_user.id,
        photo=qr_code_url,
        caption=qr_caption,
        parse_mode="Markdown",
        reply_markup=reply_markup
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
        f"📍 **Deposit Address:**\n"
        f"`{pay_address}`\n\n"
        f"⚠️ *Important:* Send the exact amount above. Your participation will be recorded automatically as soon as the transaction is confirmed on the network."
    )
    
    keyboard = [
        [InlineKeyboardButton("📱 Do you need a QR code?", callback_data="show_qr")],
        [InlineKeyboardButton("🔄 Main Menu", callback_data="main_menu")],
        [InlineKeyboardButton("📩 Contact Support", url="https://t.me/contactaigrid")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.message.delete()
    except Exception:
        pass
        
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text=invoice_text,
        parse_mode="Markdown",
        reply_markup=reply_markup
    )

# ============================================================
# [ADDED — NEW COMMANDS]
# Founder, Risk, Status
# ============================================================
async def cmd_founder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Speaks about the founder — Shivon Zilis"""
    text = (
        "👤 **The Founder — Shivon Zilis**\n"
        "───────────────────────────────\n"
        "• Yale — Economics & Philosophy\n"
        "• IBM — Cognitive Computing\n"
        "• Founding team, Bloomberg Beta\n"
        "• Forbes 30 Under 30 (2015)\n"
        "• OpenAI — founding adviser (2016), board member (2020–2023)\n"
        "• Tesla — Project Director, Autopilot & chip design (2017–2019)\n"
        "• Neuralink — Director of Operations & Special Projects\n\n"
        "She has operated at the intersection of AI, compute infrastructure, and capital for over a decade.\n\n"
        "The Indonesia AI Grid is her infrastructure thesis — build the compute layer for Southeast Asia before the market prices it.\n\n"
        "📩 Institutional inquiries: **contact@aigrid.id**"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Plain-language risk disclosure"""
    text = (
        "⚠️ **Risk Disclosure**\n"
        "───────────────────────────────\n"
        "• This is a private, forward-looking infrastructure investment.\n"
        "• **Capital is at risk.** No returns are guaranteed.\n"
        "• All metrics (20% preferred, 42.5% IRR, 3.8x MOIC) are **targets**, not promises.\n"
        "• The investment is illiquid — 3-year term, no early withdrawal.\n"
        "• Payouts are tied to asset performance, not new capital inflows.\n"
        "• Participation runs through **AI Grid Batam Infrastructure SPV**.\n\n"
        "Full disclosure: https://ai-gr.netlify.app\n\n"
        "📩 Diligence pack: **contact@aigrid.id**"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Current project milestones"""
    text = (
        "📊 **Project Status**\n"
        "───────────────────────────────\n"
        "• **Land** — Allocation in progress within Batam SEZ\n"
        "• **Power** — 150kV dual-feed framework with PLN Batam\n"
        "• **Cooling** — Direct-to-chip architecture finalized, PUE < 1.15\n"
        "• **Syndication** — Phase 1 open\n"
        "• **Founding Board** — 10 seats being formalized\n\n"
        "Detailed milestone schedule and diligence pack are provided to qualified participants.\n\n"
        "📩 Diligence pack: **contact@aigrid.id**"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

# Handlers
custom_amount_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(prompt_custom_amount, pattern="^amount_custom$")],
    states={
        WAITING_CUSTOM_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_custom_amount)]
    },
    fallbacks=[CommandHandler("start", start)]
)

telegram_app.add_handler(CommandHandler("start", start))
# [ADDED — NEW COMMANDS]
telegram_app.add_handler(CommandHandler("founder", cmd_founder))
telegram_app.add_handler(CommandHandler("risk", cmd_risk))
telegram_app.add_handler(CommandHandler("status", cmd_status))
telegram_app.add_handler(custom_amount_handler)
telegram_app.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
telegram_app.add_handler(CallbackQueryHandler(show_tiers, pattern="^show_tiers$"))
telegram_app.add_handler(CallbackQueryHandler(show_calculator, pattern="^show_calculator$"))
telegram_app.add_handler(CallbackQueryHandler(allocate_menu, pattern="^allocate_menu$"))
# [UPDATED — amount callbacks now include 25000 and 100000]
telegram_app.add_handler(CallbackQueryHandler(select_payment_method, pattern="^amount_(1000|5000|25000|100000)$"))
telegram_app.add_handler(CallbackQueryHandler(generate_invoice, pattern="^pay_"))
telegram_app.add_handler(CallbackQueryHandler(show_qr_handler, pattern="^show_qr$"))
telegram_app.add_handler(CallbackQueryHandler(back_to_invoice_handler, pattern="^back_to_invoice$"))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
