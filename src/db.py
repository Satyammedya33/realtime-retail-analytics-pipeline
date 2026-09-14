"""SQLAlchemy models and upsert helpers for the retail analytics warehouse."""
from __future__ import annotations

import os
from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


class RevenueWindow(Base):
    __tablename__ = "agg_revenue_by_minute"

    id = Column(Integer, primary_key=True, autoincrement=True)
    window_start = Column(DateTime, nullable=False)
    category = Column(String(64), nullable=False)
    order_count = Column(Integer, nullable=False, default=0)
    revenue = Column(Float, nullable=False, default=0.0)
    total_quantity = Column(Integer, nullable=False, default=0)
    average_order_value = Column(Float, nullable=False, default=0.0)

    __table_args__ = (UniqueConstraint("window_start", "category", name="uq_window_category"),)


class FraudAlert(Base):
    __tablename__ = "fraud_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(64), nullable=False)
    category = Column(String(64), nullable=False)
    amount = Column(Float, nullable=False)
    z_score = Column(Float, nullable=False)
    reason = Column(String(128), nullable=False)
    detected_at = Column(DateTime, nullable=False, default=datetime.utcnow)


def get_engine(url: str | None = None):
    url = url or os.environ.get(
        "DATABASE_URL", "postgresql+psycopg2://retail:retail@localhost:5432/retail"
    )
    return create_engine(url, pool_pre_ping=True)


def init_db(engine) -> None:
    Base.metadata.create_all(engine)


def get_session(engine):
    return sessionmaker(bind=engine)()


def upsert_window(session, window: dict) -> None:
    existing = (
        session.query(RevenueWindow)
        .filter_by(window_start=window["window_start"], category=window["category"])
        .one_or_none()
    )
    if existing:
        existing.order_count = window["order_count"]
        existing.revenue = window["revenue"]
        existing.total_quantity = window["total_quantity"]
        existing.average_order_value = window["average_order_value"]
    else:
        session.add(RevenueWindow(**window))
    session.commit()


def insert_alert(session, alert: dict) -> None:
    session.add(
        FraudAlert(
            event_id=alert["event_id"],
            category=alert["category"],
            amount=alert["amount"],
            z_score=alert["z_score"],
            reason=alert["reason"],
        )
    )
    session.commit()
