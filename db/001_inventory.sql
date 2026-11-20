BEGIN;

CREATE TABLE items (
    sku text PRIMARY KEY CHECK (char_length(sku) BETWEEN 1 AND 64),
    capacity integer NOT NULL CHECK (capacity >= 0)
);

CREATE TABLE reservations (
    id uuid PRIMARY KEY,
    sku text NOT NULL REFERENCES items (sku),
    idempotency_key text NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 128),
    quantity integer NOT NULL CHECK (quantity > 0),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at),
    UNIQUE (sku, idempotency_key)
);

CREATE INDEX reservations_by_sku_expiry ON reservations (sku, expires_at) INCLUDE (quantity);

COMMIT;
