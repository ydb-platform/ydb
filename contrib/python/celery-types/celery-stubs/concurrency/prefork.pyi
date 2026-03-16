from celery.concurrency.asynpool import AsynPool
from celery.concurrency.base import BasePool
from typing_extensions import override

__all__ = ["TaskPool"]

class TaskPool(BasePool):
    Pool = AsynPool
    uses_semaphore: bool
    @override
    def on_start(self) -> None: ...
    @override
    def restart(self) -> None: ...
    @override
    def on_stop(self) -> None: ...
    @override
    def on_terminate(self) -> None: ...
    @override
    def on_close(self) -> None: ...
