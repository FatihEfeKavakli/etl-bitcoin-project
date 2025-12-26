CREATE TABLE IF NOT EXISTS whale_trades (
    id SERIAL PRIMARY KEY,
    trade_id BIGINT UNIQUE,
    symbol VARCHAR(20),
    side VARCHAR(4),
    price NUMERIC(18, 2),
    qty NUMERIC(18, 8),
    notional_usd NUMERIC(20, 2),
    trade_time TIMESTAMPTZ,
    insert_time TIMESTAMPTZ DEFAULT NOW()
);

