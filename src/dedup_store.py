import asyncio
import json
import os
from datetime import datetime
from typing import Any

import aiosqlite

DB_PATH = os.getenv("DEDUP_DB_PATH", "/tmp/dedup.db")


class DedupStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    def _ensure_db_dir(self) -> None:
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    async def init(self) -> None:
        self._ensure_db_dir()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    source TEXT,
                    payload TEXT,
                    timestamp TEXT,
                    PRIMARY KEY (topic, event_id)
                )
                """
            )
            await db.commit()

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        self._ensure_db_dir()
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
                (topic, event_id),
            )
            row = await cursor.fetchone()
            return row is not None

    async def mark_processed(self, event: Any) -> bool:
        """Return True jika berhasil insert (bukan duplikat), False jika duplikat."""
        async with self._lock:
            try:
                self._ensure_db_dir()
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute(
                        """
                        INSERT INTO processed_events
                        (topic, event_id, received_at, source, payload, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event.topic,
                            event.event_id,
                            datetime.utcnow().isoformat(),
                            event.source,
                            json.dumps(event.payload),
                            event.timestamp,
                        ),
                    )
                    await db.commit()
                    return True
            except aiosqlite.IntegrityError:
                return False

    async def get_events(self, topic: str | None = None) -> list[dict[str, Any]]:
        self._ensure_db_dir()
        async with aiosqlite.connect(self.db_path) as db:
            if topic:
                cursor = await db.execute(
                    """
                    SELECT topic, event_id, timestamp, source, payload
                    FROM processed_events
                    WHERE topic=?
                    ORDER BY received_at
                    """,
                    (topic,),
                )
            else:
                cursor = await db.execute(
                    """
                    SELECT topic, event_id, timestamp, source, payload
                    FROM processed_events
                    ORDER BY received_at
                    """
                )
            rows = await cursor.fetchall()
        return [
            {
                "topic": row[0],
                "event_id": row[1],
                "timestamp": row[2],
                "source": row[3],
                "payload": json.loads(row[4]),
            }
            for row in rows
        ]
