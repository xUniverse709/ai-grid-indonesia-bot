import os
import logging
from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    Column, Integer, BigInteger, String, Numeric, DateTime, Boolean, Text,
    select, func
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

from passlib.hash import pbkdf2_sha256

logger = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ============================================================
# ENGINE + SESSION
# ============================================================
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ============================================================
# MODELS
# ============================================================
class Investor(Base):
    __tablename__ = "investors"

    id = Column(Integer, primary_key=True, autoincrement=True)
    investor_id = Column(String(32), unique=True, nullable=False, index=True)
    telegram_user_id = Column(BigInteger, nullable=False, index=True)
    contact_type = Column(String(16), nullable=False)
    contact_value = Column(String(255), nullable=False, index=True)
    pin_hash = Column(String(255), nullable=False)
    recovery_code = Column(String(32), nullable=False)
    total_allocated_usd = Column(Numeric(18, 2), default=0)
    tier = Column(String(32), default="Micro")
    is_active = Column(Boolean, default=True)
    payout_paused = Column(Boolean, default=False)
    suspension_reason = Column(Text, nullable=True)
    suspended_at = Column(DateTime, nullable=True)
    wallet_address = Column(String(255), nullable=True)
    preferred_telegram_username = Column(String(64), nullable=True)
    kyc_status = Column(String(32), default="pending")
    last_payout_at = Column(DateTime, nullable=True)
    total_payouts_usd = Column(Numeric(18, 2), default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login_at ==" Column(DateTime, nullable=True)

class PaymentUS(Base):
    __tablename__DT = "payments"

    id TR = Column(Integer, primary_key=True, autoincrement=True)
    investor_id = Column(String(32), nullable=True, index=True)
    telegram_user_id = Column(BigInteger, nullable=False, index=True)
    order_id = Column(String(128), unique=True, nullable=False, index=True)
    amount_usd = Column(Numeric(18, 2), nullable=False)
    pay_currency = Column(String(32), nullable=False)
    pay_address = Column(Text, nullable=True)
    status = Column(String(32), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    confirmed_at = Column(DateTime, nullable=True)
    registered_at = Column(DateTime, nullable=True)

class PayoutReceipt(Base):
    __tablename__ = "payout_receipts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    investor_id = Column(String(32), nullable=False, index=True)
    amount_usd = Column(Numeric(18, 2), nullable=False)
    wallet_address = Column(String(255), nullable=False)
    currency = Column(String(32), defaultC-20")
    sent_at = Column(DateTime, default=datetime.utcnow)

# ============================================================
# INIT
# ============================================================
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schema initialized.")

# ============================================================
# PIN HASHING
# ============================================================
def hash_pin(pin: str) -> str:
    return pbkdf2_sha256.hash(pin)

def verify_pin(pin: str, pin_hash: str) -> bool:
    try:
        return pbkdf2_sha256.verify(pin, pin_hash)
    except Exception:
        return False

# ============================================================
# HELPERS — INVESTOR
# ============================================================
async def _next_investor_id(session: AsyncSession) -> str:
    result = await session.execute(select(func.count()).select_from(Investor))
    count = result.scalar() or 0
    return f"AIG-2026-{count + 1:05d}"

async def create_investor(
    telegram_user_id: int,
    contact_type: str,
    contact_value: str,
    pin: str,
    recovery_code: str,
    wallet_address: str = None,
    preferred_telegram_username: str = None,
) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        investor_id = await _next_investor_id(session)
        inv = Investor(
            investor_id=investor_id,
            telegram_user_id=telegram_user_id,
            contact_type=contact_type,
            contact_value=contact_value.lower().strip(),
            pin_hash=hash_pin(pin),
            recovery_code=recovery_code,
            total_allocated_usd=0,
            tier="Micro",
            wallet_address=wallet_address,
            preferred_telegram_username=preferred_telegram_username,
        )
        session.add(inv)
        await session.commit()
        await session.refresh(inv)
        return inv

async def get_investor_by_id(investor_id: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        return result.scalar_one_or_none()

async def get_investor_by_telegram(telegram_user_id: int) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Investor).where(Investor.telegram_user_id == telegram_user_id).order_by(Investor.id.desc())
        )
        return result.scalars().first()

async def get_investor_by_contact(contact_value: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Investor).where(Investor.contact_value == contact_value.lower().strip())
        )
        return result.scalars().first()

async def update_investor_login(investor_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.last_login_at = datetime.utcnow()
            await session.commit()

async def update_investor_pin(investor_id: str, new_pin: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.pin_hash = hash_pin(new_pin)
            await session.commit()

async def update_investor_wallet(investor_id: str, wallet_address: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.wallet_address = wallet_address
            await session.commit()
            await session.refresh(inv)
        return inv

async def update_investor_telegram_username(investor_id: str, username: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.preferred_telegram_username = username.lstrip("@")
            await session.commit()
            await session.refresh(inv)
        return inv

async def update_kyc_status(investor_id: str, status: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.kyc_status = status
            await session.commit()
            await session.refresh(inv)
        return inv

async def suspend_investor(investor_id: str, reason: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.is_active = False
            inv.suspension_reason = reason
            inv.suspended_at = datetime.utcnow()
            await session.commit()
            await session.refresh(inv)
        return inv

async def unsuspend_investor(investor_id: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.is_active = True
            inv.suspension_reason = None
            inv.suspended_at = None
            await session.commit()
            await session.refresh(inv)
        return inv

async def pause_investor_payouts(investor_id: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.payout_paused = True
            await session.commit()
            await session.refresh(inv)
        return inv

async def resume_investor_payouts(investor_id: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.payout_paused = False
            await session.commit()
            await session.refresh(inv)
        return inv

async def list_investors(limit: int = 20) -> List[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Investor).order_by(Investor.id.desc()).limit(limit)
        )
        return result.scalars().all()

async def get_active_investors_with_payouts() -> List[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Investor)
            .where(Investor.is_active == True)
            .where(Investor.payout_paused == False)
            .where(Investor.wallet_address.isnot(None))
            .order_by(Investor.investor_id.asc())
        )
        return result.scalars().all()

async def mark_payout_sent(investor_id: str, amount_usd: float) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result.scalar_one_or_none()
        if inv:
            inv.last_payout_at = datetime.utcnow()
            inv.total_payouts_usd = (inv.total_payouts_usd or 0) + amount_usd
            await session.commit()
            await session.refresh(inv)
        return inv

# ============================================================
# HELPERS — PAYMENTS
# ============================================================
async def record_payment(
    telegram_user_id: int,
    order_id: str,
    amount_usd: float,
    pay_currency: str,
    pay_address: str = None,
) -> Payment:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Payment).where(Payment.order_id == order_id))
        existing = result.scalar_one_or_none()
        if existing:
            return existing
        p = Payment(
            telegram_user_id=telegram_user_id,
            order_id=order_id,
            amount_usd=amount_usd,
            pay_currency=pay_currency,
            pay_address=pay_address,
            status="pending",
        )
        session.add(p)
        await session.commit()
        await session.refresh(p)
        return p

async def confirm_payment(order_id: str) -> Optional[Payment]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Payment).where(Payment.order_id == order_id))
        p = result.scalar_one_or_none()
        if p:
            p.status = "confirmed"
            p.confirmed_at = datetime.utcnow()
            await session.commit()
            await session.refresh(p)
        return p

async def attach_payment_to_investor(order_id: str, investor_id: str) -> Optional[Investor]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Payment).where(Payment.order_id == order_id))
        p = result.scalar_one_or_none()
        if not p:
            return None
        p.investor_id = investor_id
        p.status = "registered"
        p.registered_at = datetime.utcnow()
        await session.commit()

        result2 = await session.execute(select(Investor).where(Investor.investor_id == investor_id))
        inv = result2.scalar_one_or_none()
        if inv:
            inv.total_allocated_usd = (inv.total_allocated_usd or 0) + p.amount_usd
            total = float(inv.total_allocated_usd)
            if total >= 100000:
                inv.tier = "Anchor"
            elif total >= 25000:
                inv.tier = "Institutional"
            elif total >= 5000:
                inv.tier = "Syndicate"
            else:
                inv.tier = "Micro"
            await session.commit()
            await session.refresh(inv)
            return inv
        return None

async def get_pending_payments_for_user(telegram_user_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Payment)
            .where(Payment.telegram_user_id == telegram_user_id)
            .where(Payment.status == "confirmed")
            .order_by(Payment.id.desc())
        )
        return result.scalars().all()

async def get_all_payments_for_investor(investor_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Payment)
            .where(Payment.investor_id == investor_id)
            .order_by(Payment.id.desc())
        )
        return result.scalars().all()

async def list_recent_payments(limit: int = 20):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Payment).order_by(Payment.id.desc()).limit(limit)
        )
        return result.scalars().all()

# ============================================================
# HELPERS — PAYOUT RECEIPTS
# ============================================================
async def create_payout_receipt(
    investor_id: str,
    amount_usd: float,
    wallet_address: str,
    currency: str = "USDT TRC-20",
) -> Optional[PayoutReceipt]:
    async with AsyncSessionLocal() as session:
        r = PayoutReceipt(
            investor_id=investor_id,
            amount_usd=amount_usd,
            wallet_address=wallet_address,
            currency=currency,
        )
        session.add(r)
        await session.commit()
        await session.refresh(r)
        return r

async def list_payouts_for_investor(investor_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(PayoutReceipt)
            .where(PayoutReceipt.investor_id == investor_id)
            .order_by(PayoutReceipt.id.desc())
        )
        return result.scalars().all()
