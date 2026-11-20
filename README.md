# Inventory Reservation API

**1,000 HTTP requests compete for 100 units of the same SKU.** The acceptance test requires exactly 100 successful reservations, 900 stock conflicts, and 100 units held in PostgreSQL. It then retries every winning request and compares the returned receipts with the stored rows.

[![PostgreSQL acceptance checks](https://github.com/DanielAPayne/inventory-reservation-api/actions/workflows/verify.yml/badge.svg?branch=main)](https://github.com/DanielAPayne/inventory-reservation-api/actions/workflows/verify.yml)

## Run the proof

Run this from the repository root with Docker Compose v2 and Bash installed.

```sh
./verify
```

This starts two FastAPI workers and PostgreSQL 17 in an isolated Compose project. The verifier releases 1,000 client tasks at a barrier, sends real HTTP requests, then connects directly to PostgreSQL to check the results. It also races 40 callers using one idempotency key, waits for a real two-unit hold to expire, and confirms 40 holds within 200 ms of their expiry while other buyers compete for the same unit. The command removes its test database and network on exit. GitHub Actions runs the same command and publishes the measured counts in the run summary.

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
| `GET /items/{sku}` | Read capacity, units in use (`reserved`, which includes `confirmed`) and available units in one database snapshot. |
| `POST /items/{sku}/reservations` | Reserve the entire requested quantity or return `409 insufficient_stock`. Quantity defaults to 1. TTL defaults to 300 seconds and accepts 1–86,400 seconds. |
| `GET /reservations/{id}` | Read the original receipt plus its current status: `active`, `expired`, `confirmed` or `cancelled`. |
| `POST /reservations/{id}/confirm` | Turn an active hold into a sale. A confirmed hold keeps its units after `expires_at`. Repeating it returns 200 unchanged. An expired or cancelled hold returns `409 reservation_expired` or `409 reservation_cancelled`. |
| `POST /reservations/{id}/cancel` | Release an active hold now. Repeating it, or cancelling an expired hold, returns 200 and changes nothing. A confirmed hold returns `409 reservation_confirmed`. |
| `GET /ready` | Check connectivity to the initialized database. |

A key belongs to one SKU. A successful key reused with a different quantity or TTL returns `409 idempotency_conflict`. An unsuccessful stock request leaves the key available for another attempt. The API reports conflicts in the JSON `detail` field.

## How it holds up under contention

1. **Serialize allocation per SKU.** The reservation transaction locks the item row with `SELECT … FOR UPDATE`. All API workers share this database lock. Other SKUs have their own rows.
2. **Check for a retry under the lock.** The transaction looks up the original receipt before checking stock. A unique constraint on `(sku, idempotency_key)` backs that lookup. An expired receipt can still be replayed with its original ID and expiry.
3. **Recalculate stock after acquiring the lock.** Under PostgreSQL `READ COMMITTED`, the subsequent statement sees reservations committed by the preceding writer. It sums active quantities using the database clock, then inserts the new hold only if the full quantity fits.
4. **Derive expiry from one timestamp.** An open hold counts while `expires_at > statement_timestamp()`. At expiry it drops out of the sum. Repeated reads and retries cannot add a second credit because there is no stock counter to increment.
5. **Decide confirm and cancel in one statement, under the same lock.** A single `UPDATE … WHERE expires_at > statement_timestamp()` both decides whether the hold is still open and records when, so the decision and the stored timestamp cannot disagree. Confirm takes the item lock first. Without it, a confirm whose statement started just before expiry could commit after a new buyer had already counted those units as free.

## Proof the lock matters

Passing tests alone do not show that the lock is what makes them pass. Two checks do:

- **A control in the suite.** `test_lock_control_shows_the_oversell_it_prevents` calls the API's own allocation function on two database connections and pauses buyer A between its stock check and its insert. With `FOR UPDATE` removed, buyer B gets through while A is paused, and both receive the last unit: 2 sold of 1. With the lock, B waits for A to commit and is refused. The pause makes both outcomes deterministic rather than a matter of timing.
- **A mutation run.** Changing the API's allocation to skip the lock and rerunning the 1,000-request test failed it: `Counter({409: 895, 201: 105})`, five units oversold. This was run by hand against local PostgreSQL 17 on 2026-11-20 and is not part of `./verify`.

The confirm-versus-expiry test spreads 40 confirms from 200 ms before to 200 ms after each hold's expiry, with five rival buyers per item. In the first local runs about half confirmed in time and half expired first. On every item, either the confirm won and every rival was refused, or the confirm was refused and at most one rival got the unit.

The tradeoff is throughput for one busy SKU. Every writer for a SKU, including confirm and cancel, takes its turn on one row lock. Writers wait their turn, and allocation sums its current holds using an index on `(sku, expires_at)`. Historical receipts are retained for replay, so storage grows with successful reservations. The burst test checks correctness under contention. Its elapsed time describes that particular run.

[Reservation transaction](inventory/api.py) · [Schema and constraints](db/001_inventory.sql) · [Executable checks](tests/test_reservations.py)
