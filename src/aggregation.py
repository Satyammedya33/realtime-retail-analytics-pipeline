"""Pure, dependency-free stream-processing logic.

Kept separate from Kafka I/O (see stream_processor.py) so the windowing and
anomaly-detection algorithms can be unit-tested deterministically without a
running broker — this is the part of the codebase the test suite exercises.
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Dict, Iterable, List, Optional


def _floor_to_window(ts: datetime, window_seconds: int) -> datetime:
    epoch = ts.timestamp()
    floored = epoch - (epoch % window_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


@dataclass
class WindowAggregate:
    window_start: datetime
    category: str
    order_count: int = 0
    revenue: float = 0.0
    total_quantity: int = 0

    @property
    def average_order_value(self) -> float:
        return round(self.revenue / self.order_count, 2) if self.order_count else 0.0

    def to_dict(self) -> dict:
        return {
            "window_start": self.window_start.isoformat(),
            "category": self.category,
            "order_count": self.order_count,
            "revenue": round(self.revenue, 2),
            "total_quantity": self.total_quantity,
            "average_order_value": self.average_order_value,
        }


class TumblingWindowAggregator:
    """Groups events into fixed, non-overlapping (tumbling) time windows per
    category and accumulates revenue / order-count / quantity.

    Emits a finalized ``WindowAggregate`` whenever an event arrives for a
    window strictly later than the current one for that category (i.e. the
    previous window has closed) — this mirrors how a real streaming engine
    (Kafka Streams / Flink) would emit on watermark advance.
    """

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        self._current: Dict[str, WindowAggregate] = {}

    def add_event(self, category: str, amount: float, quantity: int, ts: datetime) -> Optional[WindowAggregate]:
        window_start = _floor_to_window(ts, self.window_seconds)
        existing = self._current.get(category)

        emitted: Optional[WindowAggregate] = None
        if existing is not None and existing.window_start != window_start:
            emitted = existing
            existing = None

        if existing is None:
            existing = WindowAggregate(window_start=window_start, category=category)
            self._current[category] = existing

        existing.order_count += 1
        existing.revenue += amount
        existing.total_quantity += quantity
        return emitted

    def flush(self) -> List[WindowAggregate]:
        """Force-close all open windows (call at shutdown / end-of-stream)."""
        results = list(self._current.values())
        self._current.clear()
        return results


@dataclass
class AnomalyDetector:
    """Rolling z-score outlier detector over per-category order values.

    Maintains a bounded sliding window of recent order amounts per category
    and flags any new amount whose z-score exceeds ``z_threshold`` once the
    window has enough samples to compute a meaningful mean/stddev.
    """

    window_size: int = 200
    z_threshold: float = 3.0
    min_samples: int = 30
    _history: Dict[str, Deque[float]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=200)))

    def __post_init__(self):
        # rebuild deques with the configured maxlen (dataclass default above
        # is hard-coded because default_factory can't see other fields)
        self._history = defaultdict(lambda: deque(maxlen=self.window_size))

    def score(self, category: str, amount: float) -> float:
        history = self._history[category]
        if len(history) < self.min_samples:
            return 0.0
        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        stddev = math.sqrt(variance)
        if stddev == 0:
            # Zero-variance history (e.g. identical warm-up values): any
            # deviation at all is infinitely anomalous. Return a large,
            # correctly-signed sentinel instead of silently passing it through.
            if amount == mean:
                return 0.0
            return math.copysign(1e6, amount - mean)
        return (amount - mean) / stddev

    def check(self, category: str, amount: float) -> Optional[dict]:
        """Score the amount, THEN add it to history. Returns an alert dict if
        the z-score exceeds the threshold, else None."""
        z = self.score(category, amount)
        self._history[category].append(amount)
        if abs(z) >= self.z_threshold:
            return {
                "category": category,
                "amount": amount,
                "z_score": round(z, 3),
                "reason": "order_value_outlier",
            }
        return None


def process_batch(events: Iterable[dict], window_seconds: int = 60, z_threshold: float = 3.0):
    """Convenience driver used by tests and CLI tools: runs a batch of event
    dicts through both the aggregator and the anomaly detector."""
    aggregator = TumblingWindowAggregator(window_seconds=window_seconds)
    detector = AnomalyDetector(z_threshold=z_threshold)

    emitted_windows: List[WindowAggregate] = []
    alerts: List[dict] = []

    for event in events:
        ts = datetime.fromisoformat(event["timestamp"])
        emitted = aggregator.add_event(
            category=event["category"],
            amount=event["total_amount"],
            quantity=event["quantity"],
            ts=ts,
        )
        if emitted:
            emitted_windows.append(emitted)

        alert = detector.check(event["category"], event["total_amount"])
        if alert:
            alert["event_id"] = event["event_id"]
            alerts.append(alert)

    emitted_windows.extend(aggregator.flush())
    return emitted_windows, alerts
