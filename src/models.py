from datetime import datetime
from typing import Any

from pydantic import BaseModel, field_validator


class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict[str, Any] = {}

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        # Validasi ISO8601 termasuk suffix Z.
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("timestamp harus format ISO8601") from exc
        return value

    @field_validator("topic", "event_id", "source")
    @classmethod
    def not_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("field tidak boleh kosong")
        return value
