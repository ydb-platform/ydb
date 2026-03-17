__all__ = [
    "DishkaTask",
    "FromDishka",
    "inject",
    "setup_dishka",
]

from collections.abc import Callable
from typing import Final, ParamSpec, TypeVar

from celery import Celery, Task, current_app
from celery.utils.functional import head_from_fun

from dishka import Container, FromDishka
from dishka.integrations.base import is_dishka_injected, wrap_injection

CONTAINER_NAME: Final = "dishka_container"


T = TypeVar("T")
P = ParamSpec("P")


def inject(func: Callable[P, T]) -> Callable[P, T]:
    return wrap_injection(
        func=func,
        is_async=False,
        container_getter=lambda args, kwargs: current_app.conf[
            CONTAINER_NAME
        ],
        manage_scope=True,
    )


def setup_dishka(container: Container, app: Celery):
    app.conf[CONTAINER_NAME] = container


class DishkaTask(Task):
    def __init__(self) -> None:
        super().__init__()

        run = self.run

        if not is_dishka_injected(run):
            injected_func = inject(run)
            self.run = injected_func

            self.__header__ = head_from_fun(injected_func)
