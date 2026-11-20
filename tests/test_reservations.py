"""Acceptance checks against the live HTTP service and its actual PostgreSQL database."""

import asyncio
import os
import time
import unittest
from collections import Counter
from typing import NamedTuple
from uuid import uuid4

import aiohttp
import asyncpg


class Reply(NamedTuple):
    status: int
    body: dict
    location: str | None


class ReservationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        asyncio.get_running_loop().set_debug(False)
        self.http = aiohttp.ClientSession(
            base_url=os.environ["API_BASE_URL"],
            connector=aiohttp.TCPConnector(limit=0),
            timeout=aiohttp.ClientTimeout(total=120),
        )
        self.addAsyncCleanup(self.http.close)
        self.db = await asyncpg.connect(os.environ["DATABASE_URL"])
        self.addAsyncCleanup(self.db.close)
        self.sku = f"test-{uuid4().hex}"

    async def request(self, method, path, **kwargs):
        async with self.http.request(method, path, **kwargs) as response:
            return Reply(response.status, await response.json(), response.headers.get("Location"))

    async def define(self, capacity):
        result = await self.request("PUT", f"/items/{self.sku}", json={"capacity": capacity})
        self.assertEqual(result.status, 201, result.body)

    async def hold(self, key, quantity=1, ttl=300):
        return await self.request(
            "POST", f"/items/{self.sku}/reservations",
            headers={"Idempotency-Key": key}, json={"quantity": quantity, "ttl_seconds": ttl}
        )

    async def inventory(self):
        reply = await self.request("GET", f"/items/{self.sku}")
        self.assertEqual(reply.status, 200, reply.body)
        return reply.body

    async def burst(self, keys, quantity=1):
        # Every client coroutine reaches the barrier before any HTTP request is sent.
        gate = asyncio.Barrier(len(keys))

        async def compete(key):
            await gate.wait()
            return await self.hold(key, quantity)

        return await asyncio.gather(*(compete(key) for key in keys))

    async def test_1000_requests_for_100_units(self):
        capacity = 100
        await self.define(capacity)
        keys = [f"buyer-{i}" for i in range(1000)]
        started = time.monotonic()
        replies = await self.burst(keys)
        elapsed = time.monotonic() - started

        counts = Counter(r.status for r in replies)
        self.assertEqual(counts, {201: capacity, 409: len(keys) - capacity})
        for r in replies:
            if r.status == 409:
                self.assertEqual(r.body["detail"], "insufficient_stock")
        winners = [(k, r) for k, r in zip(keys, replies, strict=True) if r.status == 201]
        replays = await asyncio.gather(*(self.hold(k) for k, _ in winners))
        for (_, original), replay in zip(winners, replays, strict=True):
            self.assertEqual(replay, Reply(200, original.body, original.location))
            self.assertEqual(original.location, f"/reservations/{original.body['id']}")

        # Inspect persisted rows independently of the API's stock calculation.
        rows = await self.db.fetch(
            "SELECT id, idempotency_key, quantity FROM reservations WHERE sku = $1", self.sku
        )
        self.assertEqual({(str(row["id"]), row["idempotency_key"]) for row in rows},
                         {(r.body["id"], k) for k, r in winners})
        units = sum(row["quantity"] for row in rows)
        self.assertEqual((len(rows), units), (capacity, capacity))
        self.assertEqual(await self.inventory(), {
            "sku": self.sku, "capacity": capacity, "reserved": capacity, "available": 0
        })
        version = await self.db.fetchval("SHOW server_version")
        print(f"- Burst: {len(replies):,} HTTP requests, {counts[201]} created, "
              f"{counts[409]} insufficient-stock responses, {units} units held in PostgreSQL {version}. "
              f"All {len(replays)} successful keys replayed. Burst duration: {elapsed:.2f} s.", flush=True)

    async def test_parallel_retries_share_one_receipt(self):
        await self.define(2)
        replies = await self.burst(["checkout-42"] * 40, quantity=2)
        self.assertEqual(Counter(r.status for r in replies), {201: 1, 200: 39})
        self.assertTrue(all(r.body == replies[0].body for r in replies))
        self.assertEqual(await self.db.fetchval(
            "SELECT count(*) FROM reservations WHERE sku = $1", self.sku
        ), 1)
        self.assertEqual((await self.inventory())["reserved"], 2)
        print("- Retry race: 40 requests sharing a key produced one database row and one receipt.", flush=True)

    async def test_expiry_releases_exactly_once(self):
        await self.define(2)
        old = await self.hold("old", quantity=2, ttl=3)
        self.assertEqual(old.status, 201)
        blocked = await self.hold("next")
        self.assertEqual((blocked.status, blocked.body["detail"]), (409, "insufficient_stock"))
        state = await self.request("GET", old.location)
        self.assertEqual(state.body["status"], "active")

        deadline = time.monotonic() + 15
        while state.body["status"] == "active" and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
            state = await self.request("GET", old.location)
        self.assertEqual((state.status, state.body["status"]), (200, "expired"))
        for _ in range(3):
            replay = await self.hold("old", quantity=2, ttl=3)
            self.assertEqual(replay, Reply(200, old.body, old.location))
            self.assertEqual((await self.inventory())["available"], 2)

        # A previously refused key may try again. Replaying the old receipt never extends it.
        self.assertEqual((await self.hold("next")).status, 201)
        contenders = await self.burst([f"replacement-{i}" for i in range(20)])
        self.assertEqual(Counter(r.status for r in contenders), {201: 1, 409: 19})
        self.assertEqual((await self.inventory())["available"], 0)
        self.assertEqual((await self.request("GET", old.location)).body["status"], "expired")
        self.assertEqual(await self.db.fetchval(
            "SELECT count(*) FROM reservations WHERE sku = $1", self.sku
        ), 3)
        print("- Expiry: one two-unit hold expired. Repeated reads and retries made exactly two units reusable.",
              flush=True)

    async def test_conflicting_retry_does_not_change_inventory(self):
        await self.define(5)
        original = await self.hold("order", quantity=2, ttl=60)
        self.assertEqual(original.status, 201)
        for quantity, ttl in [(1, 60), (2, 61)]:
            conflict = await self.hold("order", quantity, ttl)
            self.assertEqual((conflict.status, conflict.body["detail"]), (409, "idempotency_conflict"))
        self.assertEqual((await self.hold("order", 2, 60)).body, original.body)
        self.assertEqual((await self.inventory())["available"], 3)
        first_sku = self.sku
        self.sku = f"other-{uuid4().hex}"
        await self.define(1)
        other = await self.hold("order")
        self.assertEqual(other.status, 201)
        self.assertNotEqual(other.body["id"], original.body["id"])
        self.assertEqual((await self.request("GET", f"/items/{first_sku}")).body["available"], 3)

    async def test_redefining_a_sku_never_resets_live_stock(self):
        await self.define(3)
        self.assertEqual((await self.hold("first")).status, 201)
        same = await self.request("PUT", f"/items/{self.sku}", json={"capacity": 3})
        self.assertEqual(same.status, 200)
        changed = await self.request("PUT", f"/items/{self.sku}", json={"capacity": 9})
        self.assertEqual((changed.status, changed.body["detail"]), (409, "capacity_conflict"))
        self.assertEqual((await self.inventory())["available"], 2)

    async def test_multi_unit_request_is_all_or_nothing(self):
        await self.define(5)
        self.assertEqual((await self.hold("first", quantity=3)).status, 201)
        refused = await self.hold("second", quantity=3)
        self.assertEqual((refused.status, refused.body["detail"]), (409, "insufficient_stock"))
        self.assertEqual((await self.inventory())["available"], 2)
        self.assertEqual((await self.hold("third", quantity=2)).status, 201)
        self.assertEqual((await self.inventory())["available"], 0)

    async def test_invalid_inputs_and_missing_resources(self):
        self.assertEqual((await self.request("GET", f"/items/{self.sku}")).status, 404)
        self.assertEqual((await self.hold("missing")).status, 404)
        self.assertEqual((await self.request("GET", f"/reservations/{uuid4()}")).status, 404)
        item_path = f"/items/{self.sku}"
        self.assertEqual((await self.request("PUT", item_path, json={"capacity": -1})).status, 422)
        await self.define(0)
        route = item_path + "/reservations"
        self.assertEqual((await self.request("POST", route, json={})).status, 422)
        for bad in [{"quantity": 0}, {"quantity": True}, {"ttl_seconds": 0}, {"ttl_seconds": 86401}]:
            result = await self.request("POST", route, json=bad, headers={"Idempotency-Key": "test"})
            self.assertEqual(result.status, 422, result.body)
        refused = await self.hold("empty")
        self.assertEqual((refused.status, refused.body["detail"]), (409, "insufficient_stock"))
