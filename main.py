import os
import logging
import httpx
from fastapi import FastAPI, Request, Response
import uvicorn
import threading

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

NOWPAYMENTS_API_KEY = os.environ.get("NOWPAYMENTS_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
NOWPAYMENTS_API_URL = "https://api.nowpayments.io/v1"

app = FastAPI()

@app.post("/webhook/nowpayments")
async def nowpayments_webhook(request: Request):
    data = await request.json()
    payment_status = data.get("payment_status")
    order_id = data.get("order_id")
    
    if payment_status == "finished" and order_id:
        try:
            telegram_user_id = int(order_id.split("_")[-1])
            logging.info(f"Payment confirmed successfully for user {telegram_user_id}!")
        except Exception as e:
            logging.error(f"Error processing webhook user ID: {e}")
            
    return Response(status_code=200)

def run_web_server():
    uvicorn.run(app, host="0.0.0.0", port=8000)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("💳 Join AI Grid Indonesia ($100 USDT)", callback_data="buy_program")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Welcome to AI Grid Indonesia.\n\n"
        "Click the button below to purchase program access securely via crypto.",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "buy_program":
        user_id = query.from_user.id
        await query.edit_message_text("🔄 Generating your secure crypto invoice...")
        
        headers = {"x-api-key": NOWPAYMENTS_API_KEY, "Content-Type": "application/json"}
        payload = {
            "price_amount": 100.0,
            "price_currency": "usd",
            "pay_currency": "usdttrc20",
            "order_id": f"ai_grid_{user_id}",
            "order_description": "AI Grid Indonesia Access"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{NOWPAYMENTS_API_URL}/payment", json=payload, headers=headers)
            data = response.json()
            
            if "pay_address" in data:
                pay_address = data["pay_address"]
                pay_amount = data["pay_amount"]
                pay_currency = data["pay_currency"].upper()
                
                invoice_text = (
                    f"✅ **Invoice Generated Successfully**\n\n"
                    f"Please send exactly {pay_amount} {pay_currency} to the address below:\n\n"
                    f"`{pay_address}`\n\n"
                    f"*Note: Your account will unlock automatically once the blockchain transaction confirms.*"
                )
                await query.edit_message_text(invoice_text, parse_mode="Markdown")
            else:
                await query.edit_message_text("❌ Error generating payment link. Please try again later.")

def main():
    server_thread = threading.Thread(target=run_web_server, daemon=True)
    server_thread.start()
    
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    logging.info("AI Grid Indonesia Telegram Bot is up and running...")
    application.run_polling()

if __name__ == "__main__":
    main()
