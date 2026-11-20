# Inventory Reservation API

**1,000 HTTP requests compete for 100 units of the same SKU.** The acceptance test requires exactly 100 successful reservations, 900 stock conflicts, and 100 units held in PostgreSQL. It then retries every winning request and compares the returned receipts with the stored rows.

[![PostgreSQL acceptance checks](https://github.com/DanielAPayne/inventory-reservation-api/actions/workflows/verify.yml/badge.svg?branch=main)](https://github.com/DanielAPayne/inventory-reservation-api/actions/workflows/verify.yml)

## Run the proof

Run this from the repository root with Docker Compose v2 and Bash installed.

```sh
./verify
```

This starts two FastAPI workers and PostgreSQL 17 in an isolated Compose project. The verifier releases 1,000 client tasks at a barrier, sends real HTTP requests, then connects directly to PostgreSQL to check the results. It also races 40 callers using one idempotency key and waits for a real two-unit hold to expire. The command removes its test database and network on exit. GitHub Actions runs the same command and publishes the measured counts in the run summary.

## Try the API

```sh
docker compose up --build --detach --wait

curl -i -X PUT http://127.0.0.1:8000/items/desk-lamp \
  -H 'Content-Type: application/json' -d '{"capacity":2}'

curl -i http://127.0.0.1:8000/items/desk-lamp/reservations \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: checkout-42' \
  -d '{"quantity":1,"ttl_seconds":30}'

curl -s http://127.0.0.1:8000/items/desk-lamp
```

The first reservation returns `201 Created` with a receipt and a `Location` header. Repeat it with the same key and body to receive the original receipt with `200 OK`. Visit [interactive API docs](http://127.0.0.1:8000/docs) to try other requests. The development API is unauthenticated and binds to loopback. Set `INVENTORY_PORT` to use another local port. Stop it with `docker compose down`.

| Operation | Contract |
|---|---|
| `PUT /items/{sku}` | Set initial capacity. An identical definition returns 200. A different capacity returns 409 and preserves existing stock. |
| `GET /items/{sku}` | Read capacity, reserved units and available units in one database snapshot. |
| `POST /items/{sku}/reservations` | Reserve the entire requested quantity or return `409 insufficient_stock`. Quantity defaults to 1. TTL defaults to 300 seconds and accepts 1–86,400 seconds. |
| `GET /reservations/{id}` | Read the original receipt plus its current `active` or `expired` status. |
| `GET /ready` | Check connectivity to the initialized database. |

A key belongs to one SKU. A successful key reused with a different quantity or TTL returns `409 idempotency_conflict`. An unsuccessful stock request leaves the key available for another attempt. The API reports conflicts in the JSON `detail` field.

## How it holds up under contention

1. **Serialize allocation per SKU.** The reservation transaction locks the item row with `SELECT … FOR UPDATE`. All API workers share this database lock. Other SKUs have their own rows.
2. **Check for a retry under the lock.** The transaction looks up the original receipt before checking stock. A unique constraint on `(sku, idempotency_key)` backs that lookup. An expired receipt can still be replayed with its original ID and expiry.
3. **Recalculate stock after acquiring the lock.** Under PostgreSQL `READ COMMITTED`, the subsequent statement sees reservations committed by the preceding writer. It sums active quantities using the database clock, then inserts the new hold only if the full quantity fits.
4. **Derive expiry from one timestamp.** A hold counts while `expires_at > statement_timestamp()`. At expiry it drops out of the sum. Repeated reads and retries cannot add a second credit because there is no stock counter to increment.

The tradeoff is throughput for one busy SKU. Writers wait their turn, and allocation sums its current holds using an index on `(sku, expires_at)`. Historical receipts are retained for replay, so storage grows with successful reservations. The burst test checks correctness under contention. Its elapsed time describes that particular run.

[Reservation transaction](inventory/api.py) · [Schema and constraints](db/001_inventory.sql) · [Executable checks](tests/test_reservations.py)
