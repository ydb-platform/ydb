__all__ = [
    "FlaskProvider",
    "FromDishka",
    "inject",
    "setup_dishka",
]

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, cast

from flask import Flask, Request, g, request
from flask.sansio.scaffold import Scaffold
from flask.typing import RouteCallable

from dishka import Container, FromDishka, Provider, Scope, from_context
from .base import InjectFunc, is_dishka_injected, wrap_injection

T = TypeVar("T")
P = ParamSpec("P")


def inject(func: Callable[P, T]) -> Callable[P, T]:
    return wrap_injection(
        func=func,
        is_async=False,
        container_getter=lambda _, p: g.dishka_container,
    )


class FlaskProvider(Provider):
    request = from_context(Request, scope=Scope.REQUEST)


class ContainerMiddleware:
    def __init__(self, container: Container) -> None:
        self.container = container

    def enter_request(self) -> None:
        g.dishka_container_wrapper = self.container({Request: request})
        g.dishka_container = g.dishka_container_wrapper.__enter__()

    def exit_request(self, *_args: Any, **_kwargs: Any) -> None:
        dishka_container = getattr(g, "dishka_container", None)
        if dishka_container is not None:
            g.dishka_container.close()


def _inject_routes(scaffold: Scaffold, inject_func: InjectFunc[P, T]) -> None:
    for key, func in scaffold.view_functions.items():
        if not is_dishka_injected(func):
            # typing.cast is applied because there
            # are RouteCallable objects in dict value
            scaffold.view_functions[key] = cast(
                RouteCallable,
                inject_func(func),
            )


def setup_dishka(
        container: Container,
        app: Flask,
        *,
        auto_inject: bool | InjectFunc[P, T] = False,
) -> None:
    middleware = ContainerMiddleware(container)
    app.before_request(middleware.enter_request)
    app.teardown_appcontext(middleware.exit_request)
    if auto_inject is not False:
        inject_func: InjectFunc[P, T]
        if auto_inject is True:
            inject_func = inject
        else:
            inject_func = auto_inject

        _inject_routes(app, inject_func)
        for blueprint in app.blueprints.values():
            _inject_routes(blueprint, inject_func)
