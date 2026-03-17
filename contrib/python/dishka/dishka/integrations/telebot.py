__all__ = [
    "FromDishka",
    "TelebotProvider",
    "inject",
    "setup_dishka",
]

from collections.abc import Callable
from inspect import Parameter
from typing import Any, NewType, ParamSpec, TypeVar

import telebot  # type: ignore[import-untyped]
from telebot import BaseMiddleware, TeleBot

from dishka import Container, FromDishka, Provider, Scope, from_context
from .base import wrap_injection

CONTAINER_NAME = "dishka_container"

T = TypeVar("T")
P = ParamSpec("P")
TelebotEvent = NewType("TelebotEvent", object)


def inject(func: Callable[P, T]) -> Callable[P, T]:
    additional_params = [Parameter(
        name=CONTAINER_NAME,
        annotation=Container,
        kind=Parameter.KEYWORD_ONLY,
    )]

    return wrap_injection(
        func=func,
        additional_params=additional_params,
        container_getter=lambda _, p: p[CONTAINER_NAME],
    )


class TelebotProvider(Provider):
    message = from_context(TelebotEvent, scope=Scope.REQUEST)


class ContainerMiddleware(BaseMiddleware):  # type: ignore[misc]
    update_types = telebot.util.update_types

    def __init__(self, container: Container) -> None:
        super().__init__()
        self.container = container

    def pre_process(
        self,
        message: Any,
        data: dict[str, Any],
    ) -> None:
        dishka_container_wrapper = self.container(
            {TelebotEvent(type(message)): message},
        )
        data[CONTAINER_NAME + "_wrapper"] = dishka_container_wrapper
        data[CONTAINER_NAME] = dishka_container_wrapper.__enter__()

    def post_process(
        self,
        message: Any,
        data: dict[str, Any],
        exception: Exception,
    ) -> None:
        data[CONTAINER_NAME + "_wrapper"].__exit__(None, None, None)


def setup_dishka(container: Container, bot: TeleBot) -> Container:
    middleware = ContainerMiddleware(container)
    bot.setup_middleware(middleware)
    return container
