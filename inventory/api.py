"""HTTP API for time-limited inventory holds backed by PostgreSQL."""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Annotated, Awaitable, Callable, Literal
from uuid import UUID, uuid4

import asyncpg
from fastapi import FastAPI, Header, HTTPException, Path, Request, Response
from pydantic import BaseModel, ConfigDict, Field


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with asyncpg.create_pool(
        os.environ["DATABASE_URL"], min_size=1, max_size=8, command_timeout=30
    ) as pool:
        app.state.pool = pool
        yield


app = FastAPI(title="Inventory Reservation API", version="0.1.0", lifespan=lifespan)
Sku = Annotated[str, Path(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")]
Key = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128,
                            pattern=r"^[A-Za-z0-9._:-]+$")]


class InputModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class ItemInput(InputModel):
    capacity: int = Field(ge=0, le=2_147_483_647)


class HoldInput(InputModel):
    quantity: int = Field(default=1, ge=1, le=2_147_483_647)
    ttl_seconds: int = Field(default=300, ge=1, le=86_400)


class Item(BaseModel):
    sku: str
    capacity: int


class Inventory(Item):
    reserved: int
    confirmed: int
    available: int


class Receipt(BaseModel):
    id: UUID
    sku: str
    quantity: int
    created_at: datetime
    expires_at: datetime


class Reservation(Receipt):
    status: Literal["active", "expired", "confirmed", "cancelled"]
    confirmed_at: datetime | None
    cancelled_at: datetime | None


# Units unavailable to new holds: confirmed sales plus open holds that have not expired.
# Every read and write of stock uses this one predicate.
IN_USE = """r.cancelled_at IS NULL
            AND (r.confirmed_at IS NOT NULL OR r.expires_at > statement_timestamp())"""

STATUS = """CASE WHEN r.confirmed_at IS NOT NULL THEN 'confirmed'
                 WHEN r.cancelled_at IS NOT NULL THEN 'cancelled'
                 WHEN r.expires_at > statement_timestamp() THEN 'active'
                 ELSE 'expired' END"""

RESERVATION = f"""SELECT r.id, r.sku, r.quantity, r.created_at, r.expires_at,
                         r.confirmed_at, r.cancelled_at, {STATUS} AS status
                  FROM reservations AS r WHERE r.id = $1"""


@app.get("/ready", include_in_schema=False)
async def ready(request: Request):
    try:
        async with request.app.state.pool.acquire(timeout=2) as conn:
            initialized = await conn.fetchval(
                """SELECT to_regclass('public.items') IS NOT NULL
                      AND to_regclass('public.reservations') IS NOT NULL""", timeout=2
            )
    except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
        initialized = False
    if not initialized:
        raise HTTPException(503, detail="database_unavailable")
    return {"status": "ready"}


@app.put("/items/{sku}", response_model=Item, status_code=201,
         responses={200: {"model": Item}, 409: {"description": "SKU already has another capacity"}})
async def define_item(sku: Sku, body: ItemInput, request: Request, response: Response):
    """Set initial stock. Repeating the same definition is safe while reservations are active."""
    async with request.app.state.pool.acquire() as conn, conn.transaction(isolation="read_committed"):
        created = await conn.fetchval(
            """INSERT INTO items (sku, capacity) VALUES ($1, $2)
               ON CONFLICT (sku) DO NOTHING RETURNING sku""", sku, body.capacity
        )
        if created is None:
            capacity = await conn.fetchval("SELECT capacity FROM items WHERE sku = $1", sku)
            if capacity != body.capacity:
                raise HTTPException(409, detail="capacity_conflict")
            response.status_code = 200
    response.headers["Location"] = f"/items/{sku}"
    return {"sku": sku, "capacity": body.capacity}


@app.get("/items/{sku}", response_model=Inventory)
async def get_item(sku: Sku, request: Request):
    """Read capacity, units in use and confirmed units in one database snapshot."""
    row = await request.app.state.pool.fetchrow(
        f"""SELECT i.sku, i.capacity,
                   COALESCE(SUM(r.quantity), 0) AS reserved,
                   COALESCE(SUM(r.quantity) FILTER (WHERE r.confirmed_at IS NOT NULL), 0) AS confirmed
            FROM items AS i
            LEFT JOIN reservations AS r ON r.sku = i.sku AND {IN_USE}
            WHERE i.sku = $1
            GROUP BY i.sku, i.capacity""", sku
    )
    if row is None:
        raise HTTPException(404, detail="item_not_found")
    return {**dict(row), "available": row["capacity"] - row["reserved"]}


async def allocate(conn, sku: str, key: str, quantity: int, duration: timedelta, *,
                   lock: bool = True, after_check: Callable[[], Awaitable[None]] | None = None):
    """Run inside a READ COMMITTED transaction. Returns (receipt, created).

    lock=False and after_check exist only so the test suite can show what the lock prevents:
    it pauses one caller between the stock check and the insert. The API always locks.
    """
    # All writers for a SKU take this lock first. The queries AFTER it get fresh
    # READ COMMITTED snapshots, including any change committed by the previous writer.
    capacity = await conn.fetchval(
        "SELECT capacity FROM items WHERE sku = $1" + (" FOR UPDATE" if lock else ""), sku
    )
    if capacity is None:
        raise HTTPException(404, detail="item_not_found")

    receipt = await conn.fetchrow(
        """SELECT id, sku, quantity, created_at, expires_at FROM reservations
           WHERE sku = $1 AND idempotency_key = $2""", sku, key
    )
    if receipt is not None:
        if receipt["quantity"] != quantity or receipt["expires_at"] - receipt["created_at"] != duration:
            raise HTTPException(409, detail="idempotency_conflict")
        return receipt, False

    stock = await conn.fetchrow(
        f"""SELECT statement_timestamp() AS checked_at, COALESCE(SUM(r.quantity), 0) AS in_use
            FROM reservations AS r WHERE r.sku = $1 AND {IN_USE}""", sku
    )
    if quantity > capacity - stock["in_use"]:
        raise HTTPException(409, detail="insufficient_stock")
    if after_check is not None:
        await after_check()

    created_at = stock["checked_at"]
    receipt = await conn.fetchrow(
        """INSERT INTO reservations (id, sku, idempotency_key, quantity, created_at, expires_at)
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING id, sku, quantity, created_at, expires_at""",
        uuid4(), sku, key, quantity, created_at, created_at + duration
    )
    return receipt, True


@app.post("/items/{sku}/reservations", response_model=Receipt, status_code=201,
          responses={200: {"model": Receipt, "description": "Original receipt for a successful retry"},
                     409: {"description": "Insufficient stock or conflicting idempotency key"}})
async def reserve(sku: Sku, body: HoldInput, key: Key, request: Request, response: Response):
    """Reserve units until expiry. A key identifies one successful request per SKU."""
    async with request.app.state.pool.acquire() as conn, conn.transaction(isolation="read_committed"):
        receipt, created = await allocate(conn, sku, key, body.quantity, timedelta(seconds=body.ttl_seconds))
    if not created:
        response.status_code = 200
    response.headers["Location"] = f"/reservations/{receipt['id']}"
    return dict(receipt)


@app.get("/reservations/{reservation_id}", response_model=Reservation)
async def get_reservation(reservation_id: UUID, request: Request):
    """Read the original receipt and its current status according to the database clock."""
    row = await request.app.state.pool.fetchrow(RESERVATION, reservation_id)
    if row is None:
        raise HTTPException(404, detail="reservation_not_found")
    return dict(row)


async def finish(request: Request, reservation_id: UUID, column: str):
    """Set confirmed_at or cancelled_at on a hold that is still open at this database instant."""
    async with request.app.state.pool.acquire() as conn, conn.transaction(isolation="read_committed"):
        sku = await conn.fetchval("SELECT sku FROM reservations WHERE id = $1", reservation_id)
        if sku is None:
            raise HTTPException(404, detail="reservation_not_found")
        # Same lock as allocation. Without it, a confirm whose statement started just before
        # expiry could commit after a new hold had already counted these units as free.
        await conn.execute("SELECT 1 FROM items WHERE sku = $1 FOR UPDATE", sku)
        # One statement decides and records the outcome, so both use the same timestamp.
        await conn.execute(
            f"""UPDATE reservations SET {column} = statement_timestamp()
                WHERE id = $1 AND confirmed_at IS NULL AND cancelled_at IS NULL
                  AND expires_at > statement_timestamp()""", reservation_id
        )
        return dict(await conn.fetchrow(RESERVATION, reservation_id))


@app.post("/reservations/{reservation_id}/confirm", response_model=Reservation,
          responses={409: {"description": "The hold expired or was cancelled first"}})
async def confirm(reservation_id: UUID, request: Request):
    """Turn an active hold into a sale. Confirming a confirmed hold returns it unchanged."""
    row = await finish(request, reservation_id, "confirmed_at")
    if row["status"] != "confirmed":
        raise HTTPException(409, detail=f"reservation_{row['status']}")
    return row


@app.post("/reservations/{reservation_id}/cancel", response_model=Reservation,
          responses={409: {"description": "The hold was already confirmed"}})
async def cancel(reservation_id: UUID, request: Request):
    """Release an active hold now. Cancelling a cancelled or expired hold changes nothing."""
    row = await finish(request, reservation_id, "cancelled_at")
    if row["status"] == "confirmed":
        raise HTTPException(409, detail="reservation_confirmed")
    return row
