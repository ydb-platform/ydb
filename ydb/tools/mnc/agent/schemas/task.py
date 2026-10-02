from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class TaskResult:
    success: bool
    message: str
    data: Any


@dataclass
class TaskSchema:
    id: str
    type: str
    status: str
    created_at: float
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    delay: Optional[float] = None


@dataclass
class TaskStatsSchema:
    total: int
    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int
    queue_size: int
    max_inflight: int = 0
    current_inflight: int = 0
