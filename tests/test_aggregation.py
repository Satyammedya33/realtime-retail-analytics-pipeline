from datetime import datetime, timedelta, timezone

from src.aggregation import AnomalyDetector, TumblingWindowAggregator, process_batch


def _ts(base: datetime, seconds: float) -> str:
    return (base + timedelta(seconds=seconds)).isoformat()


def test_tumbling_window_accumulates_within_window():
    agg = TumblingWindowAggregator(window_seconds=60)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    emitted1 = agg.add_event("electronics", 1000.0, 1, base)
    emitted2 = agg.add_event("electronics", 500.0, 2, base + timedelta(seconds=30))

    assert emitted1 is None
    assert emitted2 is None

    windows = agg.flush()
    assert len(windows) == 1
    w = windows[0]
    assert w.order_count == 2
    assert w.revenue == 1500.0
    assert w.total_quantity == 3
    assert w.average_order_value == 750.0


def test_tumbling_window_emits_on_rollover():
    agg = TumblingWindowAggregator(window_seconds=60)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    agg.add_event("fashion", 200.0, 1, base)
    emitted = agg.add_event("fashion", 300.0, 1, base + timedelta(seconds=61))

    assert emitted is not None
    assert emitted.order_count == 1
    assert emitted.revenue == 200.0

    remaining = agg.flush()
    assert len(remaining) == 1
    assert remaining[0].revenue == 300.0


def test_windows_are_independent_per_category():
    agg = TumblingWindowAggregator(window_seconds=60)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    agg.add_event("books", 100.0, 1, base)
    agg.add_event("toys", 400.0, 1, base)

    windows = {w.category: w for w in agg.flush()}
    assert windows["books"].revenue == 100.0
    assert windows["toys"].revenue == 400.0


def test_anomaly_detector_flags_extreme_value_after_warmup():
    detector = AnomalyDetector(window_size=200, z_threshold=3.0, min_samples=30)

    # Warm up with consistent "normal" values.
    for _ in range(40):
        assert detector.check("electronics", 2000.0) is None

    # A wildly larger amount should be flagged as an outlier.
    alert = detector.check("electronics", 500_000.0)
    assert alert is not None
    assert alert["category"] == "electronics"
    assert alert["z_score"] > 3.0


def test_anomaly_detector_silent_before_min_samples():
    detector = AnomalyDetector(min_samples=30)
    # Even an extreme value shouldn't fire before we have enough history.
    for _ in range(10):
        alert = detector.check("beauty", 999999.0)
    assert alert is None


def test_process_batch_end_to_end():
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    events = []
    for i in range(50):
        events.append(
            {
                "event_id": f"e{i}",
                "category": "electronics",
                "total_amount": 2000.0 + (i % 5) * 10,
                "quantity": 1,
                "timestamp": _ts(base, i),
            }
        )
    # Inject one clear outlier near the end (window still open).
    events.append(
        {
            "event_id": "e-outlier",
            "category": "electronics",
            "total_amount": 900000.0,
            "quantity": 1,
            "timestamp": _ts(base, 51),
        }
    )

    windows, alerts = process_batch(events, window_seconds=60, z_threshold=3.0)

    assert sum(w.order_count for w in windows) == len(events)
    assert any(a["event_id"] == "e-outlier" for a in alerts)
