import asyncio
from collections.abc import AsyncIterator, Awaitable, Coroutine
from functools import wraps
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
)

from aiohttp import web

from ._job import Job
from ._scheduler import Scheduler

__all__ = ("setup", "spawn", "get_scheduler", "get_scheduler_from_app", "atomic")

_T = TypeVar("_T")
_FutureLike = Union["asyncio.Future[_T]", Awaitable[_T]]
_RequestView = TypeVar("_RequestView", bound=Union[web.Request, web.View])


AIOJOBS_SCHEDULER = web.AppKey("AIOJOBS_SCHEDULER", Scheduler)


def get_scheduler(request: web.Request) -> Scheduler:
    scheduler = get_scheduler_from_request(request)
    if scheduler is None:
        raise RuntimeError("Call aiojobs.aiohttp.setup() on application initialization")
    return scheduler


def get_scheduler_from_app(app: web.Application) -> Optional[Scheduler]:
    return app.get(AIOJOBS_SCHEDULER)


def get_scheduler_from_request(request: web.Request) -> Optional[Scheduler]:
    return request.config_dict.get(AIOJOBS_SCHEDULER)


async def spawn(request: web.Request, coro: Coroutine[object, object, _T]) -> Job[_T]:
    return await get_scheduler(request).spawn(coro)


def shield(request: web.Request, arg: _FutureLike[_T]) -> "asyncio.Future[_T]":
    return get_scheduler(request).shield(arg)


def atomic(
    coro: Callable[[_RequestView], Coroutine[object, object, _T]],
) -> Callable[[_RequestView], Awaitable[_T]]:
    @wraps(coro)
    async def wrapper(request_or_view: _RequestView) -> _T:  # type: ignore[misc]
        if isinstance(request_or_view, web.View):
            # Class Based View decorated.
            request = request_or_view.request
        else:
            request = request_or_view

        job = await spawn(request, coro(request_or_view))
        return await job.wait()

    return wrapper


def setup(app: web.Application, **kwargs: Any) -> None:
    async def cleanup_context(app: web.Application) -> AsyncIterator[None]:
        app[AIOJOBS_SCHEDULER] = scheduler = Scheduler(**kwargs)
        yield
        await scheduler.wait_and_close()

    app.cleanup_ctx.append(cleanup_context)
