__all__ = [
    "DishkaRouter",
    "FromDishka",
    "LitestarProvider",
    "inject",
    "inject_websocket",
    "setup_dishka",
]

from collections.abc import Callable
from functools import wraps
from inspect import Parameter
from typing import ParamSpec, TypeVar, get_type_hints

from litestar import Controller, Litestar, Request, Router, WebSocket
from litestar.enums import ScopeType
from litestar.handlers import (
    BaseRouteHandler,
    HTTPRouteHandler,
    WebsocketListener,
)
from litestar.handlers.websocket_handlers import WebsocketListenerRouteHandler
from litestar.handlers.websocket_handlers._utils import ListenerHandler
from litestar.routes import BaseRoute
from litestar.types import (
    ASGIApp,
    ControllerRouterHandler,
    Receive,
    Scope,
    Send,
)

from dishka import AsyncContainer, FromDishka, Provider, from_context
from dishka import Scope as DIScope
from dishka.integrations.base import wrap_injection

P = ParamSpec("P")
T = TypeVar("T")


def inject(func: Callable[P, T]):
    return _inject_wrapper(func, "request", Request)


def inject_websocket(func: Callable[P, T]):
    return _inject_wrapper(func, "socket", WebSocket)


def _inject_wrapper(
        func: Callable[P, T],
        param_name: str,
        param_annotation: type[Request | WebSocket],
):
    hints = get_type_hints(func)

    request_param = next(
        (name for name in hints if name == param_name),
        None,
    )

    if request_param:
        additional_params = []
    else:
        additional_params = [Parameter(
            name=param_name,
            annotation=param_annotation,
            kind=Parameter.KEYWORD_ONLY,
        )]

    return wrap_injection(
        func=func,
        is_async=True,
        additional_params=additional_params,
        container_getter=lambda _, r: r[param_name].state.dishka_container,
    )


def _inject_based_on_handler_type(
    value: BaseRouteHandler,
) -> BaseRouteHandler:
    if isinstance(value, HTTPRouteHandler):
        value._fn = inject(value._fn)  # noqa: SLF001

    if isinstance(value, WebsocketListenerRouteHandler) and isinstance(
        value._fn,  # noqa: SLF001
        ListenerHandler,
    ):
        value = value(inject_websocket(value._fn._fn))  # noqa: SLF001

    return value


def _inject_route_handlers(
    get_route_handlers: Callable[P, list[BaseRouteHandler]],
) -> Callable[P, list[BaseRouteHandler]]:
    @wraps(get_route_handlers)
    def _wrapper(*args: P.args, **kwargs: P.kwargs) -> list[BaseRouteHandler]:
        return [
            _inject_based_on_handler_type(route)
            for route in get_route_handlers(*args, **kwargs)
        ]

    return _wrapper


def _resolve_value(
    router: Router,
    value: ControllerRouterHandler,
) -> ControllerRouterHandler:
    if isinstance(value, Router):
        return value

    if isinstance(value, BaseRouteHandler):
        return _inject_based_on_handler_type(value)

    if isinstance(value, type):
        if issubclass(value, Controller):
            value.get_route_handlers = _inject_route_handlers(  # type: ignore[method-assign]
                value.get_route_handlers,
            )
        if issubclass(value, WebsocketListener):
            return _inject_based_on_handler_type(value(router).to_handler())

    return value


class DishkaRouter(Router):
    __slots__ = ()

    def register(self, value: ControllerRouterHandler) -> list[BaseRoute]:
        return super().register(_resolve_value(self, value))


class LitestarProvider(Provider):
    request = from_context(Request, scope=DIScope.REQUEST)
    socket = from_context(WebSocket, scope=DIScope.SESSION)


def make_add_request_container_middleware(app: ASGIApp) -> ASGIApp:
    async def middleware(scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") not in (ScopeType.HTTP, ScopeType.WEBSOCKET):
            await app(scope, receive, send)
            return

        if scope.get("type") == ScopeType.HTTP:
            request = Request(scope)  # type: ignore[var-annotated]
            context = {Request: request}
            di_scope = DIScope.REQUEST

        else:
            request = WebSocket(scope)
            context = {WebSocket: request}
            di_scope = DIScope.SESSION

        async with request.app.state.dishka_container(
            context, scope=di_scope,
        ) as request_container:
            request.state.dishka_container = request_container
            await app(scope, receive, send)

    return middleware


def setup_dishka(container: AsyncContainer, app: Litestar) -> None:
    app.asgi_handler = make_add_request_container_middleware(
        app.asgi_handler,
    )
    app.state.dishka_container = container
