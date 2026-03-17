__all__ = [
    "FromDishka",
    "StarletteProvider",
    "inject",
    "setup_dishka",
]

import warnings
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocket

from dishka import AsyncContainer, FromDishka, Provider, from_context
from dishka import Scope as DIScope
from .base import wrap_injection

T = TypeVar("T")
P = ParamSpec("P")

warnings.warn(
    "`dishka.integrations.starlette` will be removed in `dishka==2.0`.\n"
    "Use `starlette-dishka` package instead, as integrations "
    "are now maintained in separate third-party packages for faster updates.",
    DeprecationWarning,
    stacklevel=2,
)


def inject(func: Callable[P, T]) -> Callable[P, T]:
    return wrap_injection(
        func=func,
        is_async=True,
        container_getter=lambda r, _: r[0].scope["state"]["dishka_container"],
    )


class StarletteProvider(Provider):
    request = from_context(Request, scope=DIScope.REQUEST)
    websocket = from_context(WebSocket, scope=DIScope.SESSION)


class ContainerMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)

        request: Request | WebSocket
        context: dict[type[Request | WebSocket], Request | WebSocket]

        if scope["type"] == "http":
            request = Request(scope, receive=receive, send=send)
            context = {Request: request}
            di_scope = DIScope.REQUEST

        else:
            request = WebSocket(scope, receive, send)
            context = {WebSocket: request}
            di_scope = DIScope.SESSION

        async with request.app.state.dishka_container(
            context,
            scope=di_scope,
        ) as request_container:
            request.state.dishka_container = request_container
            return await self.app(scope, receive, send)


class SyncContainerMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)

        request: Request | WebSocket
        context: dict[type[Request | WebSocket], Request | WebSocket]

        if scope["type"] == "http":
            request = Request(scope, receive=receive, send=send)
            context = {Request: request}
            di_scope = DIScope.REQUEST

        else:
            request = WebSocket(scope, receive, send)
            context = {WebSocket: request}
            di_scope = DIScope.SESSION

        with request.app.state.dishka_container(
            context,
            scope=di_scope,
        ) as request_container:
            request.state.dishka_container = request_container
            return await self.app(scope, receive, send)


def setup_dishka(container: AsyncContainer, app: Starlette) -> None:
    app.add_middleware(ContainerMiddleware)
    app.state.dishka_container = container
