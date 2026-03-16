__all__ = [
    "CONTAINER_NAME",
    "AiogramMiddlewareData",
    "AiogramProvider",
    "AutoInjectMiddleware",
    "FromDishka",
    "inject",
    "inject_handler",
    "inject_router",
    "setup_dishka",
]

import warnings
from collections.abc import Awaitable, Callable
from functools import partial
from inspect import Parameter, signature
from typing import Any, Final, NewType, ParamSpec, TypeVar, cast

from aiogram import BaseMiddleware, Router
from aiogram.dispatcher.event.handler import HandlerObject
from aiogram.types import TelegramObject

from dishka import AsyncContainer, FromDishka, Provider, Scope, from_context
from .base import InjectFunc, is_dishka_injected, wrap_injection

P = ParamSpec("P")
T = TypeVar("T")
CONTAINER_NAME: Final = "dishka_container"
AiogramMiddlewareData = NewType("AiogramMiddlewareData", dict[str, Any])


def inject(func: Callable[P, T]) -> Callable[P, T]:
    if CONTAINER_NAME in signature(func).parameters:
        additional_params = []
    else:
        additional_params = [Parameter(
            name=CONTAINER_NAME,
            annotation=AsyncContainer,
            kind=Parameter.KEYWORD_ONLY,
        )]

    return wrap_injection(
        func=func,
        is_async=True,
        additional_params=additional_params,
        container_getter=lambda args, kwargs: kwargs[CONTAINER_NAME],
    )


class AiogramProvider(Provider):
    event = from_context(TelegramObject, scope=Scope.REQUEST)
    middleware_data = from_context(AiogramMiddlewareData, scope=Scope.REQUEST)


class ContainerMiddleware(BaseMiddleware):
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        async with self.container(
                {
                    TelegramObject: event,
                    AiogramMiddlewareData: data,
                },
        ) as sub_container:
            data[CONTAINER_NAME] = sub_container
            return await handler(event, data)


class AutoInjectMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        warnings.warn(
            f"{self.__class__.__name__} is slow, "
            "use `setup_dishka` instead if you care about performance",
            UserWarning,
            stacklevel=2,
        )

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        old_handler = cast(HandlerObject, data["handler"])
        if is_dishka_injected(old_handler.callback):
            return await handler(event, data)

        inject_handler(old_handler, inject)
        return await handler(event, data)


def setup_dishka(
    container: AsyncContainer,
    router: Router,
    *,
    auto_inject: bool | InjectFunc[P, T] = False,
) -> None:
    middleware = ContainerMiddleware(container)

    for observer in router.observers.values():
        observer.outer_middleware(middleware)

    if auto_inject is not False:
        inject_func: InjectFunc[P, T]

        if auto_inject is True:
            inject_func = inject
        else:
            inject_func = auto_inject

        callback = partial(
            inject_router,
            router=router,
            inject_func=inject_func,
        )
        router.startup.register(callback)


def inject_router(
    router: Router,
    inject_func: InjectFunc[P, T] = inject,
) -> None:
    """Inject dishka to the router handlers."""
    for sub_router in router.chain_tail:
        for observer in sub_router.observers.values():
            if observer.event_name == "update":
                continue

            for handler in observer.handlers:
                if not is_dishka_injected(handler.callback):
                    inject_handler(handler, inject_func)


def inject_handler(
    handler: HandlerObject,
    inject_func: InjectFunc[P, T] = inject,
) -> HandlerObject:
    """Inject dishka for callback in aiogram's handler."""
    # temp_handler is used to apply original __post_init__ processing
    # for callback object wrapped by injector
    temp_handler = HandlerObject(
        callback=inject_func(handler.callback),
        filters=handler.filters,
        flags=handler.flags,
    )

    # since injector modified callback and params,
    # we should update them in the original handler
    handler.callback = temp_handler.callback
    handler.params = temp_handler.params

    return handler
