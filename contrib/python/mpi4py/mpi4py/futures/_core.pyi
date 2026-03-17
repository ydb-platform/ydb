import sys
from concurrent.futures import (
    FIRST_COMPLETED as FIRST_COMPLETED,
    FIRST_EXCEPTION as FIRST_EXCEPTION,
    ALL_COMPLETED as ALL_COMPLETED,
    CancelledError as CancelledError,
    TimeoutError as TimeoutError,
    Future as Future,
    Executor as Executor,
    wait as wait,
    as_completed as as_completed,
)

if sys.version_info >= (3, 7):
    from concurrent.futures import BrokenExecutor as BrokenExecutor
else:
    class BrokenExecutor(RuntimeError): ...

if sys.version_info >= (3, 8):
    from concurrent.futures import InvalidStateError as InvalidStateError
else:
    from concurrent.futures._base import Error
    class InvalidStateError(Error): ...
