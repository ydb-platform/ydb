from multiprocessing import (
    AuthenticationError,
    BufferTooShort,
    ProcessError,
    TimeoutError,
)

__all__ = [
    "AuthenticationError",
    "BufferTooShort",
    "CoroStop",
    "ProcessError",
    "RestartFreqExceeded",
    "SoftTimeLimitExceeded",
    "Terminated",
    "TimeLimitExceeded",
    "TimeoutError",
    "WorkerLostError",
]

class TimeLimitExceeded(Exception): ...
class SoftTimeLimitExceeded(Exception): ...
class WorkerLostError(Exception): ...
class Terminated(Exception): ...
class RestartFreqExceeded(Exception): ...
class CoroStop(Exception): ...
