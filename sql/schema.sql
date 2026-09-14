-- Retail analytics warehouse schema
CREATE TABLE IF NOT EXISTS agg_revenue_by_minute (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    category VARCHAR(64) NOT NULL,
    order_count INTEGER NOT NULL DEFAULT 0,
    revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_quantity INTEGER NOT NULL DEFAULT 0,
    average_order_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    UNIQUE (window_start, category)
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    reason VARCHAR(128) NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_revenue_window_start ON agg_revenue_by_minute (window_start);
CREATE INDEX IF NOT EXISTS idx_alerts_detected_at ON fraud_alerts (detected_at);
