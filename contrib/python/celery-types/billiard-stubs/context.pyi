from billiard import process
from billiard.exceptions import (
    AuthenticationError,
    BufferTooShort,
    ProcessError,
    SoftTimeLimitExceeded,
    TimeLimitExceeded,
    TimeoutError,
    WorkerLostError,
)

__all__ = [
    "W_NO_EXECV",
    "AuthenticationError",
    "BaseContext",
    "BufferTooShort",
    "DefaultContext",
    "ForkContext",
    "ForkProcess",
    "ForkServerContext",
    "ForkServerProcess",
    "Process",
    "ProcessError",
    "SoftTimeLimitExceeded",
    "SpawnContext",
    "SpawnProcess",
    "TimeLimitExceeded",
    "TimeoutError",
    "WorkerLostError",
    "process",
]

W_NO_EXECV: str

class BaseContext:
    def freeze_support(self) -> None: ...
    def allow_connection_pickling(self) -> None: ...

class Process(process.BaseProcess): ...
class DefaultContext(BaseContext): ...
class ForkProcess(process.BaseProcess): ...
class SpawnProcess(process.BaseProcess): ...
class ForkServerProcess(process.BaseProcess): ...

class ForkContext(BaseContext):
    Process = ForkProcess

class SpawnContext(BaseContext):
    Process = SpawnProcess

class ForkServerContext(BaseContext):
    Process = ForkServerProcess
