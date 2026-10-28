import asyncio
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar, cast

from temporalio import activity

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def auto_heartbeater(fn: F) -> F:
    """Decorator that heartbeats at half the activity's heartbeat timeout."""

    @wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        heartbeat_timeout = activity.info().heartbeat_timeout
        heartbeat_task = None
        if heartbeat_timeout:
            heartbeat_task = asyncio.create_task(
                _heartbeat_every(heartbeat_timeout.total_seconds() / 2)
            )
        try:
            return await fn(*args, **kwargs)
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

    return cast(F, wrapper)


async def _heartbeat_every(delay: float) -> None:
    while True:
        await asyncio.sleep(delay)
        activity.heartbeat()
