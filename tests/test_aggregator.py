import os
import tempfile
import time

import pytest
from httpx import ASGITransport, AsyncClient

from src.dedup_store import DedupStore
from src.main import app


def make_event(topic: str = "logs", event_id: str = "evt-001", source: str = "svc-a"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": "2024-01-01T00:00:00Z",
        "source": source,
        "payload": {"msg": "hello"},
    }


@pytest.mark.asyncio
async def test_publish_single_event():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/publish", json=make_event())
        assert response.status_code == 200
        assert response.json()["queued"] == 1


@pytest.mark.asyncio
async def test_publish_batch_event():
    events = [make_event(event_id=f"batch-{i}") for i in range(5)]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/publish", json=events)
        assert response.status_code == 200
        assert response.json()["queued"] == 5


@pytest.mark.asyncio
async def test_dedup_duplicate_dropped():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DedupStore(db_path=os.path.join(tmpdir, "dedup.db"))
        await store.init()

        from src.models import Event

        event = Event(**make_event())

        result1 = await store.mark_processed(event)
        result2 = await store.mark_processed(event)

        assert result1 is True
        assert result2 is False


@pytest.mark.asyncio
async def test_dedup_persistence():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "dedup.db")

        store1 = DedupStore(db_path=db_path)
        await store1.init()

        from src.models import Event

        event = Event(**make_event(event_id="persist-test"))
        await store1.mark_processed(event)

        store2 = DedupStore(db_path=db_path)
        await store2.init()
        result = await store2.mark_processed(event)

        assert result is False


@pytest.mark.asyncio
async def test_schema_validation_missing_field():
    bad_event = {
        "event_id": "x",
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "svc",
    }
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/publish", json=bad_event)
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_schema_validation_bad_timestamp():
    bad_event = make_event()
    bad_event["timestamp"] = "bukan-tanggal"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/publish", json=bad_event)
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_stats_structure():
    from src.main import dedup_store

    await dedup_store.init()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/stats")
        assert response.status_code == 200
        data = response.json()
        for field in [
            "received",
            "unique_processed",
            "duplicate_dropped",
            "topics",
            "uptime_seconds",
        ]:
            assert field in data


@pytest.mark.asyncio
async def test_get_events_by_topic():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DedupStore(db_path=os.path.join(tmpdir, "dedup.db"))
        await store.init()

        from src.models import Event

        e1 = Event(**make_event(topic="topicA", event_id="a1"))
        e2 = Event(**make_event(topic="topicB", event_id="b1"))
        await store.mark_processed(e1)
        await store.mark_processed(e2)

        results = await store.get_events(topic="topicA")
        assert len(results) == 1
        assert results[0]["topic"] == "topicA"


@pytest.mark.asyncio
async def test_stress_5000_events():
    with tempfile.TemporaryDirectory() as tmpdir:
        store = DedupStore(db_path=os.path.join(tmpdir, "dedup.db"))
        await store.init()

        from src.models import Event

        events = []
        for i in range(4000):
            events.append(Event(**make_event(event_id=f"stress-{i}")))
        for i in range(1000):
            events.append(Event(**make_event(event_id=f"stress-{i}")))

        start = time.time()
        new_count = 0
        dup_count = 0
        for event in events:
            result = await store.mark_processed(event)
            if result:
                new_count += 1
            else:
                dup_count += 1
        elapsed = time.time() - start

        assert new_count == 4000
        assert dup_count == 1000
        # Performance on Docker Desktop with bind mounts can vary across hosts.
        assert elapsed < 180


@pytest.mark.asyncio
async def test_health_endpoint():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
