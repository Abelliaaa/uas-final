-- tabel event (dedup)
CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, event_id)
);

-- tabel statistik (async-safe)
CREATE TABLE IF NOT EXISTS stats_counter (
    id INT PRIMARY KEY DEFAULT 1,
    received BIGINT NOT NULL DEFAULT 0,
    unique_processed BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped BIGINT NOT NULL DEFAULT 0
);

INSERT INTO stats_counter (id)
VALUES (1)
ON CONFLICT DO NOTHING;
