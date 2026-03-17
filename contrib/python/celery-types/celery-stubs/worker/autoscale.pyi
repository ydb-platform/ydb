from typing import Any

from celery.bootsteps import StartStopStep
from celery.concurrency.base import BasePool
from celery.utils.threads import bgThread
from typing_extensions import override

class WorkerComponent(StartStopStep):
    enabled: Any
    def __init__(self, w: Any, **kwargs: Any) -> None: ...
    @override
    def create(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, w: Any
    ) -> Any: ...
    def register_with_event_loop(self, w: Any, hub: Any) -> None: ...
    @override
    def info(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, w: Any
    ) -> Any: ...

class Autoscaler(bgThread):
    pool: BasePool
    mutex: Any
    max_concurrency: int
    min_concurrency: int
    keepalive: int | float
    worker: Any
    def __init__(
        self,
        pool: BasePool,
        max_concurrency: int,
        min_concurrency: int = ...,
        worker: Any | None = ...,
        keepalive: float = ...,
        mutex: Any | None = ...,
    ) -> None: ...
    @override
    def body(self) -> None: ...
    def maybe_scale(self, req: Any | None = ...) -> None: ...
    def update(self, max: Any | None = ..., min: Any | None = ...) -> Any: ...
    def scale_up(self, n: Any) -> Any: ...
    def scale_down(self, n: Any) -> Any: ...
    def info(self) -> Any: ...
    @property
    def qty(self) -> Any: ...
    @property
    def processes(self) -> Any: ...
