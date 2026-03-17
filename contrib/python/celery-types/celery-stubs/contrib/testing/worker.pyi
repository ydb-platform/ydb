from collections.abc import Generator
from contextlib import contextmanager
from logging import LogRecord, handlers
from typing import Any

from celery import Celery
from celery.utils.dispatch.signal import Signal
from celery.worker.worker import WorkController
from typing_extensions import override

WORKER_LOGLEVEL: str

test_worker_starting: Signal
test_worker_started: Signal
test_worker_stopped: Signal

class TestWorkController(WorkController):
    __test__: bool
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

    class QueueHandler(handlers.QueueHandler):
        @override
        def prepare(self, record: LogRecord) -> LogRecord: ...
        @override
        def handleError(self, record: LogRecord) -> None: ...

    def start(self) -> Any: ...
    def on_consumer_ready(self, consumer: Any) -> None: ...
    def ensure_started(self) -> None: ...

@contextmanager
def start_worker(
    app: Celery,
    concurrency: int = 1,
    pool: str = "solo",
    loglevel: str | int = ...,
    logfile: str | None = None,
    perform_ping_check: bool = True,
    ping_task_timeout: float = 10.0,
    shutdown_timeout: float = 10.0,
    **kwargs: Any,
) -> Generator[WorkController, None, None]: ...
@contextmanager
def _start_worker_thread(
    app: Celery,
    concurrency: int = 1,
    pool: str = "solo",
    loglevel: str | int = ...,
    logfile: str | None = None,
    WorkController: type[TestWorkController] = ...,
    perform_ping_check: bool = True,
    shutdown_timeout: float = 10.0,
    **kwargs: Any,
) -> Generator[WorkController, None, None]: ...
@contextmanager
def _start_worker_process(
    app: Celery,
    concurrency: int = 1,
    pool: str = "solo",
    loglevel: str | int = ...,
    logfile: str | None = None,
    **kwargs: Any,
) -> Generator[None, None, None]: ...
def setup_app_for_worker(
    app: Celery,
    loglevel: str | int,
    logfile: str,
) -> None: ...
