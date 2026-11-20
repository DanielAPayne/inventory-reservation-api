"""HTTP API for time-limited inventory holds backed by PostgreSQL."""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Annotated, Literal
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
    available: int


class Receipt(BaseModel):
    id: UUID
    sku: str
    quantity: int
    created_at: datetime
    expires_at: datetime


class Reservation(Receipt):
    status: Literal["active", "expired"]


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
    """Read capacity and active reserved units in one database snapshot."""
    row = await request.app.state.pool.fetchrow(
        """SELECT i.sku, i.capacity, COALESCE(SUM(r.quantity), 0) AS reserved
           FROM items AS i
           LEFT JOIN reservations AS r
             ON r.sku = i.sku AND r.expires_at > statement_timestamp()
           WHERE i.sku = $1
           GROUP BY i.sku, i.capacity""", sku
    )
    if row is None:
        raise HTTPException(404, detail="item_not_found")
    return {**dict(row), "available": row["capacity"] - row["reserved"]}


@app.post("/items/{sku}/reservations", response_model=Receipt, status_code=201,
          responses={200: {"model": Receipt, "description": "Original receipt for a successful retry"},
                     409: {"description": "Insufficient stock or conflicting idempotency key"}})
async def reserve(sku: Sku, body: HoldInput, key: Key, request: Request, response: Response):
    """Reserve units until expiry. A key identifies one successful request per SKU."""
    duration = timedelta(seconds=body.ttl_seconds)
    async with request.app.state.pool.acquire() as conn, conn.transaction(isolation="read_committed"):
        # All writers for a SKU take this lock first. The queries AFTER it get fresh
        # READ COMMITTED snapshots, including any hold committed by the previous writer.
        capacity = await conn.fetchval("SELECT capacity FROM items WHERE sku = $1 FOR UPDATE", sku)
        if capacity is None:
            raise HTTPException(404, detail="item_not_found")

        receipt = await conn.fetchrow(
            """SELECT id, sku, quantity, created_at, expires_at FROM reservations
               WHERE sku = $1 AND idempotency_key = $2""", sku, key
        )
        if receipt is not None:
            if receipt["quantity"] != body.quantity or receipt["expires_at"] - receipt["created_at"] != duration:
                raise HTTPException(409, detail="idempotency_conflict")
            response.status_code = 200
        else:
            stock = await conn.fetchrow(
                """SELECT statement_timestamp() AS checked_at, COALESCE(SUM(quantity), 0) AS reserved
                   FROM reservations WHERE sku = $1 AND expires_at > statement_timestamp()""", sku
            )
            if body.quantity > capacity - stock["reserved"]:
                raise HTTPException(409, detail="insufficient_stock")

            created_at = stock["checked_at"]
            receipt = await conn.fetchrow(
                """INSERT INTO reservations (id, sku, idempotency_key, quantity, created_at, expires_at)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   RETURNING id, sku, quantity, created_at, expires_at""",
                uuid4(), sku, key, body.quantity, created_at, created_at + duration
            )
    response.headers["Location"] = f"/reservations/{receipt['id']}"
    return dict(receipt)


@app.get("/reservations/{reservation_id}", response_model=Reservation)
async def get_reservation(reservation_id: UUID, request: Request):
    """Read the original receipt and its current status according to the database clock."""
    row = await request.app.state.pool.fetchrow(
        """SELECT id, sku, quantity, created_at, expires_at,
                  CASE WHEN expires_at > statement_timestamp() THEN 'active' ELSE 'expired' END AS status
           FROM reservations WHERE id = $1""", reservation_id
    )
    if row is None:
        raise HTTPException(404, detail="reservation_not_found")
    return dict(row)
