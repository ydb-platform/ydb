from typing import Any, Optional
from pydantic import BaseModel


class TaskSchema(BaseModel):
    """Schema describing task information in API responses."""

    id: str
    type: str
    status: str
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    delay: Optional[float] = None


class TaskStatsSchema(BaseModel):
    """Schema describing task statistics."""

    total: int
    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int
    queue_size: int
