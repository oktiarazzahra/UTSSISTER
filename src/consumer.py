import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class Consumer:
    def __init__(self, queue: asyncio.Queue, dedup_store: Any, stats: dict[str, int]):
        self.queue = queue
        self.dedup_store = dedup_store
        self.stats = stats
        self.running = False

    async def start(self) -> None:
        self.running = True
        logger.info("Consumer started.")
        while self.running:
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self.process(event)
            except asyncio.TimeoutError:
                continue

    async def process(self, event: Any) -> None:
        is_new = await self.dedup_store.mark_processed(event)
        if is_new:
            self.stats["unique_processed"] += 1
            logger.info("[PROCESSED] topic=%s event_id=%s", event.topic, event.event_id)
        else:
            self.stats["duplicate_dropped"] += 1
            logger.warning(
                "[DUPLICATE DROPPED] topic=%s event_id=%s",
                event.topic,
                event.event_id,
            )
