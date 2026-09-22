import os
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, Integer, BigInteger, String, Numeric, DateTime, Boolean, Text, select, func
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

from passlib.hash import pbkdf2_sha256

logger = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Railway gives postgres:// — SQLAlchemy async needs postgresql+asyncpg://
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
    contact_type = Column(String(16), nullable=False)   # "email" or "phone"
    contact_value = Column(String(255), nullable=False, index=True)
    pin_hash = Column(String(255), nullable=False)
    recovery_code = Column(String(32), nullable=False)
    total_allocated_usd = Column(Numeric(18, 2), default=0)
    tier = Column(String(32), default="Micro")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login_at = Column(DateTime, nullable=True)

class Payment(Base):
    __tablename__ = "payments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    investor_id = Column(String(32), nullable=True, index=True)
    telegram_user_id = Column(BigInteger, nullable=False, index=True)
    order_id = Column(String(128), unique=True, nullable=False, index=True)
    amount_usd = Column(Numeric(18, 2), nullable=False)
    pay_currency = Column(String(32), nullable=False)
    pay_address = Column(Text, nullable=True)
    status = Column(String(32), default="pending")     # pending | confirmed | registered
    created_at = Column(DateTime, default=datetime.utcnow)
    confirmed_at = Column(DateTime, nullable=True)
    registered_at = Column(DateTime, nullable=True)

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
# HELPERS
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

        # Update investor totals
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
    """Returns payments confirmed but not yet registered to an investor."""
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
