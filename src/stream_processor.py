"""Kafka consumer: windows events, detects anomalies, persists to Postgres.

    python -m src.stream_processor
"""
from __future__ import annotations

import json
import os
from datetime import datetime

from kafka import KafkaConsumer

from .aggregation import AnomalyDetector, TumblingWindowAggregator
from .db import get_engine, get_session, init_db, insert_alert, upsert_window

TOPIC = os.environ.get("KAFKA_TOPIC", "retail-events")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "60"))
Z_THRESHOLD = float(os.environ.get("Z_THRESHOLD", "3.0"))


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="retail-stream-processor",
    )


def run() -> None:
    engine = get_engine()
    init_db(engine)
    session = get_session(engine)

    aggregator = TumblingWindowAggregator(window_seconds=WINDOW_SECONDS)
    detector = AnomalyDetector(z_threshold=Z_THRESHOLD)

    consumer = build_consumer()
    print(f"Consuming '{TOPIC}' from {BOOTSTRAP_SERVERS}, {WINDOW_SECONDS}s tumbling windows...")

    for message in consumer:
        event = message.value
        ts = datetime.fromisoformat(event["timestamp"])

        emitted = aggregator.add_event(
            category=event["category"],
            amount=event["total_amount"],
            quantity=event["quantity"],
            ts=ts,
        )
        if emitted:
            upsert_window(session, emitted.to_dict())
            print(f"[WINDOW] {emitted.to_dict()}")

        alert = detector.check(event["category"], event["total_amount"])
        if alert:
            alert["event_id"] = event["event_id"]
            insert_alert(session, alert)
            print(f"[ALERT] {alert}")


if __name__ == "__main__":
    run()
