import threading
from typing import Any

from billiard.common import (
    TERM_SIGNAL,
    human_status,
    pickle_loads,
    reset_signals,
    restart_state,
)
from billiard.compat import get_errno, mem_rss, send_offset
from billiard.dummy import DummyProcess, Process
from billiard.einfo import ExceptionInfo
from billiard.exceptions import (
    CoroStop,
    RestartFreqExceeded,
    SoftTimeLimitExceeded,
    Terminated,
    TimeLimitExceeded,
    TimeoutError,
    WorkerLostError,
)
from billiard.util import debug, warning
from typing_extensions import override

__all__ = [
    "ACK",
    "CLOSE",
    "DEATH",
    "EX_FAILURE",
    "EX_OK",
    "EX_RECYCLE",
    "GUARANTEE_MESSAGE_CONSUMPTION_RETRY_INTERVAL",
    "GUARANTEE_MESSAGE_CONSUMPTION_RETRY_LIMIT",
    "LOST_WORKER_TIMEOUT",
    "MAXMEM_USED_FMT",
    "NACK",
    "READY",
    "RUN",
    "SIGKILL",
    "TASK",
    "TERMINATE",
    "TERM_SIGNAL",
    "ApplyResult",
    "CoroStop",
    "DummyProcess",
    "ExceptionInfo",
    "IMapIterator",
    "IMapUnorderedIterator",
    "LaxBoundedSemaphore",
    "Lock",
    "MapResult",
    "MaybeEncodingError",
    "Pool",
    "PoolThread",
    "Process",
    "RestartFreqExceeded",
    "ResultHandler",
    "SoftTimeLimitExceeded",
    "Supervisor",
    "TaskHandler",
    "Terminated",
    "ThreadPool",
    "TimeLimitExceeded",
    "TimeoutError",
    "TimeoutHandler",
    "Worker",
    "WorkerLostError",
    "WorkersJoined",
    "debug",
    "get_errno",
    "human_status",
    "mem_rss",
    "pickle_loads",
    "reset_signals",
    "restart_state",
    "send_offset",
    "warning",
]

MAXMEM_USED_FMT: str
SIGKILL = TERM_SIGNAL
RUN: int
CLOSE: int
TERMINATE: int
ACK: int
READY: int
TASK: int
NACK: int
DEATH: int
EX_OK: int
EX_FAILURE: int
EX_RECYCLE: int
LOST_WORKER_TIMEOUT: float
GUARANTEE_MESSAGE_CONSUMPTION_RETRY_LIMIT: int
GUARANTEE_MESSAGE_CONSUMPTION_RETRY_INTERVAL: float
Lock = threading.Lock

class LaxBoundedSemaphore(threading.Semaphore):
    def shrink(self) -> None: ...
    def grow(self) -> None: ...
    @override
    def release(self, n: int = ...) -> None: ...
    def clear(self) -> None: ...

class MaybeEncodingError(Exception):
    def __init__(self, exc: object, value: object) -> None: ...

class WorkersJoined(Exception): ...

class Worker:
    def after_fork(self) -> None: ...

class PoolThread(DummyProcess):
    daemon: bool
    def on_stop_not_started(self) -> None: ...
    def terminate(self) -> None: ...
    def close(self) -> None: ...

class Supervisor(PoolThread):
    def body(self) -> None: ...

class TaskHandler(PoolThread):
    def body(self) -> None: ...
    def tell_others(self) -> None: ...
    @override
    def on_stop_not_started(self) -> None: ...

class TimeoutHandler(PoolThread):
    def body(self) -> None: ...

class ResultHandler(PoolThread):
    @override
    def on_stop_not_started(self) -> None: ...
    def body(self) -> None: ...
    def finish_at_shutdown(self, handle_timeouts: bool = ...) -> None: ...

class Pool:
    def shrink(self, n: int = ...) -> None: ...
    def grow(self, n: int = ...) -> None: ...
    def maintain_pool(self) -> None: ...
    @override
    def __reduce__(self) -> str | tuple[Any, ...]: ...
    def close(self) -> None: ...
    def terminate(self) -> None: ...
    def join(self) -> None: ...
    def restart(self) -> None: ...

class ApplyResult:
    def discard(self) -> None: ...
    def handle_timeout(self, soft: bool = ...) -> None: ...

class MapResult(ApplyResult): ...
class IMapIterator: ...
class IMapUnorderedIterator(IMapIterator): ...

class ThreadPool(Pool):
    Process = DummyProcess
    def __init__(self) -> None: ...
