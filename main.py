import os
import logging
import httpx
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Executive Dynamic Onboarding Flow"""
    welcome_text = (
        "🚀 **AI GRID INDONESIA | Sovereign Compute Syndicate**\n"
        "───────────────────────────────\n"
        "Welcome to the official capital allocation portal for Batam's **$1B, 50MW High-Density AI Data Center**.\n\n"
        "📊 **Key Financial Highlights:**\n"
        "• **Preferred Dividend:** 20.0% Cash Yield (Distributed every 30 days)\n"
        "• **Liquidity Term:** Flexible 30-Day Cycle (Exit principal or roll over)\n"
        "• **Target Net IRR:** 42.5%\n"
        "• **Projected MOIC:** 3.8x\n"
        "• **Infrastructure:** Direct-to-chip liquid cooling for NVIDIA Blackwell clusters\n\n"
        "Select an option below to explore or allocate capital:"
    )
    
    keyboard = [
        [InlineKeyboardButton("📈 View Investment Tiers", callback_data="show_tiers")],
        [InlineKeyboardButton("𝄠 Interactive ROI Calculator", callback_data="show_calculator")],
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
        "🔹 **Tier 1 — Edge Node ($1,000 USD)**\n"
        "• 20% Preferred Dividend (Monthly $200 payout)\n"
        "• Standard 30-Day Liquidity Cycle\n\n"
        "🔹 **Tier 2 — Rack Suite ($5,000 USD)**\n"
        "• 20% Preferred Dividend (Monthly $1,000 payout)\n"
        "• Priority Compute Allocation Discount (15% off cloud rates)\n\n"
        "🔹 **Tier 3 — GPU Cluster ($10,000 USD)**\n"
        "• 20% Preferred Dividend (Monthly $2,000 payout) + Equity Upside\n"
        "• Monthly Executive Briefing Access\n\n"
        "🔹 **Tier 4 — Institutional Vault ($50,000+ USD)**\n"
        "• 20% Preferred Dividend (Monthly $10,000+ payout)\n"
        "• Custom Liquidity Terms & Direct On-Site Batam SEZ Inspection"
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
        "𝄠 **Yield Projections Summary (20.0% Paid Every 30 Days)**\n\n"
        "• **$1,000 Allocation:** **$200.00** / 30 days ($2,400 / year)\n"
        "• **$5,000 Allocation:** **$1,000.00** / 30 days ($12,000 / year)\n"
        "• **$10,000 Allocation:** **$2,000.00** / 30 days ($24,000 / year)\n"
        "• **$50,000 Allocation:** **$10,000.00** / 30 days ($120,000 / year)\n\n"
        "💡 *Investors receive cash payouts every 30 days. At the end of each cycle, you can withdraw your principal or roll it over into the next 30-day tranche.*"
    )
    
    keyboard = [
        [InlineKeyboardButton("💳 Allocate Capital Now", callback_data="allocate_menu")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
    ]
    await query.edit_message_text(calc_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def allocate_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    menu_text = "💳 **Select your investment amount or enter a custom sum:**"
    keyboard = [
        [InlineKeyboardButton("$1,000 USD (Node)", callback_data="amount_1000")],
        [InlineKeyboardButton("$5,000 USD (Rack)", callback_data="amount_5000")],
        [InlineKeyboardButton("$10,000 USD (Cluster)", callback_data="amount_10000")],
        [InlineKeyboardButton("$50,000 USD (Institutional)", callback_data="amount_50000")],
        [InlineKeyboardButton("✍️ Custom Investment Amount", callback_data="amount_custom")],
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
        "✍️ **Custom Investment Amount**\n\n"
        "Please reply with the exact dollar amount (USD) you wish to invest (e.g. `2500` or `75000`).\n\n"
        "*(Minimum investment: $100 USD)*",
        parse_mode="Markdown"
    )
    return WAITING_CUSTOM_AMOUNT

async def receive_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().replace("$", "").replace(",", "")
    try:
        val = float(text)
        if val < 100:
            await update.message.reply_text("❌ Minimum investment amount is $100 USD. Please enter a higher value:")
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
                
                # Save into session data for toggling views/QR codes
                context.user_data["pay_address"] = pay_address
                context.user_data["pay_amount"] = pay_amount
                context.user_data["pay_currency"] = pay_currency
                context.user_data["crypto_label"] = crypto_info["label"]
                
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
    
    qr_explanation_text = (
        f"📱 **Why Use a QR Code?**\n"
        f"───────────────────────────────\n"
        f"Scanning a QR code directly from your crypto wallet app completely eliminates manual typing mistakes and ensures funds route to the correct destination safely.\n\n"
        f"You can scan your wallet camera directly against an invoice QR code or use a QR tool for this address:\n"
        f"`{pay_address}`"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Wallet Address", callback_data="back_to_invoice")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(qr_explanation_text, parse_mode="Markdown", reply_markup=reply_markup)

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
    
    await query.edit_message_text(invoice_text, parse_mode="Markdown", reply_markup=reply_markup)

# Handlers
custom_amount_handler = ConversationHandler(
    entry_points=[CallbackQueryHandler(prompt_custom_amount, pattern="^amount_custom$")],
    states={
        WAITING_CUSTOM_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_custom_amount)]
    },
    fallbacks=[CommandHandler("start", start)]
)

telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(custom_amount_handler)
telegram_app.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
telegram_app.add_handler(CallbackQueryHandler(show_tiers, pattern="^show_tiers$"))
telegram_app.add_handler(CallbackQueryHandler(show_calculator, pattern="^show_calculator$"))
telegram_app.add_handler(CallbackQueryHandler(allocate_menu, pattern="^allocate_menu$"))
telegram_app.add_handler(CallbackQueryHandler(select_payment_method, pattern="^amount_(1000|5000|10000|50000)$"))
telegram_app.add_handler(CallbackQueryHandler(generate_invoice, pattern="^pay_"))
telegram_app.add_handler(CallbackQueryHandler(show_qr_handler, pattern="^show_qr$"))
telegram_app.add_handler(CallbackQueryHandler(back_to_invoice_handler, pattern="^back_to_invoice$"))
