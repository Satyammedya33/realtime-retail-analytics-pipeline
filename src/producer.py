"""Kafka producer: streams synthetic retail events onto the `retail-events` topic.

Run with a live Kafka broker (see docker-compose.yml):
    python -m src.producer --rate 20 --anomaly-rate 0.02
"""
from __future__ import annotations

import argparse
import os
import time

from kafka import KafkaProducer

from .data_generator import generate_event

TOPIC = os.environ.get("KAFKA_TOPIC", "retail-events")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=20,
    )


def run(rate_per_second: float, anomaly_rate: float) -> None:
    producer = build_producer()
    delay = 1.0 / rate_per_second if rate_per_second > 0 else 0
    print(f"Publishing to topic '{TOPIC}' @ {BOOTSTRAP_SERVERS} (~{rate_per_second}/s)...")
    try:
        while True:
            event = generate_event(anomaly_rate=anomaly_rate)
            producer.send(TOPIC, key=event.user_id, value=event.to_json())
            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=20.0, help="events/sec")
    parser.add_argument("--anomaly-rate", type=float, default=0.02)
    args = parser.parse_args()
    run(args.rate, args.anomaly_rate)
