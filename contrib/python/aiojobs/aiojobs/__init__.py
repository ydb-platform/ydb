"""Jobs scheduler for managing background task (asyncio).

The library gives controlled way for scheduling background tasks for
asyncio applications.

"""

import warnings
from typing import Optional

from ._job import Job
from ._scheduler import ExceptionHandler, Scheduler

__version__ = "1.4.0"


async def create_scheduler(
    *,
    close_timeout: Optional[float] = 0.1,
    limit: Optional[int] = 100,
    pending_limit: int = 10000,
    exception_handler: Optional[ExceptionHandler] = None,
) -> Scheduler:
    warnings.warn("Scheduler can now be instantiated directly.", DeprecationWarning)

    if exception_handler is not None and not callable(exception_handler):
        raise TypeError(
            f"A callable object or None is expected, got {exception_handler!r}"
        )
    return Scheduler(
        close_timeout=close_timeout,
        limit=limit,
        pending_limit=pending_limit,
        exception_handler=exception_handler,
    )


__all__ = ("Job", "Scheduler", "create_scheduler")
