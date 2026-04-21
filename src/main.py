import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Union

from fastapi import FastAPI, Query

from .consumer import Consumer
from .dedup_store import DedupStore
from .models import Event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

queue: asyncio.Queue = asyncio.Queue()
dedup_store = DedupStore()
start_time = datetime.now(timezone.utc)

stats = {
    "received": 0,
    "unique_processed": 0,
    "duplicate_dropped": 0,
}

consumer: Consumer | None = None
consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    del app
    global consumer, consumer_task
    await dedup_store.init()
    consumer = Consumer(queue, dedup_store, stats)
    consumer_task = asyncio.create_task(consumer.start())
    yield
    if consumer:
        consumer.running = False
    if consumer_task:
        consumer_task.cancel()


app = FastAPI(title="Pub-Sub Log Aggregator", lifespan=lifespan)


@app.post("/publish")
async def publish(events: Union[Event, list[Event]]):
    if isinstance(events, Event):
        events = [events]

    for event in events:
        stats["received"] += 1
        await queue.put(event)

    return {"queued": len(events), "message": "Event(s) diterima dan masuk ke queue"}


@app.get("/events")
async def get_events(topic: str | None = Query(default=None)):
    events = await dedup_store.get_events(topic)
    return {"count": len(events), "events": events}


@app.get("/stats")
async def get_stats():
    uptime_seconds = (datetime.now(timezone.utc) - start_time).total_seconds()
    all_events = await dedup_store.get_events()
    topics = sorted(list({event["topic"] for event in all_events}))

    return {
        "received": stats["received"],
        "unique_processed": stats["unique_processed"],
        "duplicate_dropped": stats["duplicate_dropped"],
        "topics": topics,
        "uptime_seconds": round(uptime_seconds, 2),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}
