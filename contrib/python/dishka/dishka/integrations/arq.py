__all__ = [
    "DISHKA_APP_CONTAINER_KEY",
    "DISHKA_REQUEST_CONTAINER_KEY",
    "FromDishka",
    "inject",
    "setup_dishka",
]

from collections.abc import Awaitable, Callable
from typing import Any, Final, ParamSpec, TypeVar

from arq import Worker
from arq.typing import StartupShutdown

from dishka import AsyncContainer, FromDishka
from dishka.integrations.base import wrap_injection

T = TypeVar("T")
P = ParamSpec("P")
DISHKA_APP_CONTAINER_KEY: Final = "dishka_app_container"
DISHKA_REQUEST_CONTAINER_KEY: Final = "dishka_request_container"


def inject(func: Callable[P, T]) -> Callable[P, T]:
    return wrap_injection(
        func=func,
        remove_depends=True,
        container_getter=lambda p, _: p[0][DISHKA_REQUEST_CONTAINER_KEY],
        is_async=True,
    )


def job_start(
    hook_func: StartupShutdown | None,
) -> Callable[[dict[Any, Any]], Awaitable[None]]:
    async def wrapper(context: dict[Any, Any]) -> None:
        container: AsyncContainer = context[DISHKA_APP_CONTAINER_KEY]
        sub_container = await container().__aenter__()
        context[DISHKA_REQUEST_CONTAINER_KEY] = sub_container
        if hook_func:
            await hook_func(context)

    return wrapper


def job_end(
    hook_func: StartupShutdown | None,
) -> Callable[[dict[Any, Any]], Awaitable[None]]:
    async def wrapper(context: dict[Any, Any]) -> None:
        if hook_func:
            await hook_func(context)

        sub_container: AsyncContainer = context[DISHKA_REQUEST_CONTAINER_KEY]
        await sub_container.close()

    return wrapper


def setup_dishka(
    container: AsyncContainer,
    worker_settings: dict[Any, Any] | Worker | Any,
) -> None:
    if isinstance(worker_settings, dict):
        if worker_settings.get("ctx"):
            worker_settings["ctx"][DISHKA_APP_CONTAINER_KEY] = container
        else:
            worker_settings["ctx"] = {DISHKA_APP_CONTAINER_KEY: container}

        worker_settings["on_job_start"] = job_start(
            worker_settings.get("on_job_start"),
        )
        worker_settings["on_job_end"] = job_end(
            worker_settings.get("on_job_end"),
        )
    else:
        if hasattr(worker_settings, "ctx"):
            worker_settings.ctx[DISHKA_APP_CONTAINER_KEY] = container
        else:
            worker_settings.ctx = {DISHKA_APP_CONTAINER_KEY: container}

        worker_settings.on_job_start = (
            job_start(worker_settings.on_job_start)
            if hasattr(worker_settings, "on_job_start")
            else job_start(None)
        )
        worker_settings.on_job_end = (
            job_end(worker_settings.on_job_end)
            if hasattr(worker_settings, "on_job_end")
            else job_end(None)
        )
