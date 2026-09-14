"""Synthetic e-commerce event generator.

Produces realistic order events with an injected anomalous-transaction rate so
downstream fraud-detection logic has something real to catch. Can be used
standalone (writes JSON Lines to a file/stdout) or imported by producer.py to
stream events onto a Kafka topic.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

try:
    from faker import Faker

    _fake = Faker()
except ImportError:  # pragma: no cover - faker is an optional convenience dep
    _fake = None

CATEGORIES = [
    "electronics",
    "home_and_kitchen",
    "fashion",
    "beauty",
    "sports",
    "books",
    "grocery",
    "toys",
]

COUNTRIES = ["IN", "US", "GB", "DE", "AE", "SG", "AU", "CA"]

# Typical order value ranges (currency-agnostic units) per category, used to
# generate *normal* traffic. Anomalies are sampled far outside this range.
CATEGORY_PRICE_RANGE = {
    "electronics": (1500, 60000),
    "home_and_kitchen": (300, 12000),
    "fashion": (250, 6000),
    "beauty": (150, 3000),
    "sports": (400, 9000),
    "books": (99, 1500),
    "grocery": (50, 2500),
    "toys": (150, 4000),
}


@dataclass
class RetailEvent:
    event_id: str
    user_id: str
    order_id: str
    category: str
    quantity: int
    unit_price: float
    total_amount: float
    country: str
    device: str
    event_type: str
    is_seed_anomaly: bool
    timestamp: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def _random_user_id() -> str:
    return f"user_{random.randint(1, 50_000)}"


def generate_event(anomaly_rate: float = 0.02) -> RetailEvent:
    category = random.choice(CATEGORIES)
    low, high = CATEGORY_PRICE_RANGE[category]
    is_anomaly = random.random() < anomaly_rate

    quantity = random.randint(1, 5)
    if is_anomaly:
        # Anomalies: wildly inflated basket size and/or price -> looks like
        # card-testing / bulk-fraud behaviour for the anomaly detector to flag.
        quantity = random.randint(15, 80)
        unit_price = round(random.uniform(high * 3, high * 12), 2)
    else:
        unit_price = round(random.uniform(low, high), 2)

    total_amount = round(unit_price * quantity, 2)

    return RetailEvent(
        event_id=str(uuid.uuid4()),
        user_id=_random_user_id(),
        order_id=str(uuid.uuid4()),
        category=category,
        quantity=quantity,
        unit_price=unit_price,
        total_amount=total_amount,
        country=random.choice(COUNTRIES),
        device=random.choice(["mobile", "desktop", "tablet"]),
        event_type=random.choices(
            ["order_placed", "order_placed", "order_placed", "cart_abandoned"],
            weights=[6, 6, 6, 1],
        )[0],
        is_seed_anomaly=is_anomaly,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def event_stream(n: int, anomaly_rate: float = 0.02):
    for _ in range(n):
        yield generate_event(anomaly_rate=anomaly_rate)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic retail events")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--anomaly-rate", type=float, default=0.02)
    parser.add_argument("--out", type=str, default=None, help="Output file (JSONL). Defaults to stdout.")
    args = parser.parse_args()

    out = open(args.out, "w") if args.out else sys.stdout
    try:
        for event in event_stream(args.events, args.anomaly_rate):
            out.write(event.to_json() + "\n")
    finally:
        if args.out:
            out.close()


if __name__ == "__main__":
    main()
