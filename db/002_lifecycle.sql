BEGIN;

-- A hold ends in exactly one of three ways: confirmed (sold), cancelled (released early),
-- or neither, in which case it stops counting at expires_at.
ALTER TABLE reservations
    ADD COLUMN confirmed_at timestamptz,
    ADD COLUMN cancelled_at timestamptz,
    ADD CONSTRAINT reservations_one_outcome CHECK (confirmed_at IS NULL OR cancelled_at IS NULL),
    ADD CONSTRAINT reservations_confirmed_in_time CHECK (confirmed_at IS NULL OR confirmed_at < expires_at),
    ADD CONSTRAINT reservations_cancelled_after_creation CHECK (cancelled_at IS NULL OR cancelled_at >= created_at);

-- Stock in use = confirmed units + unexpired open holds. One index per half of that sum.
DROP INDEX reservations_by_sku_expiry;
CREATE INDEX reservations_open_by_sku_expiry ON reservations (sku, expires_at) INCLUDE (quantity)
    WHERE confirmed_at IS NULL AND cancelled_at IS NULL;
CREATE INDEX reservations_confirmed_by_sku ON reservations (sku) INCLUDE (quantity)
    WHERE confirmed_at IS NOT NULL;

COMMIT;
