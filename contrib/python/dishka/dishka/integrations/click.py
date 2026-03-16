__all__ = [
    "FromDishka",
    "inject",
    "setup_dishka",
]

from collections.abc import Callable
from typing import Final, TypeVar

from click import (
    Command,
    Context,
    Group,
    get_current_context,
)

from dishka import Container, FromDishka
from .base import InjectFunc, is_dishka_injected, wrap_injection

T = TypeVar("T")
CONTAINER_NAME: Final = "dishka_container"


def inject(func: Callable[..., T]) -> Callable[..., T]:
    return wrap_injection(
        func=func,
        container_getter=lambda _, __: get_current_context().meta[
            CONTAINER_NAME
        ],
        remove_depends=True,
        is_async=False,
    )


def _inject_commands(
    context: Context,
    command: Command | None,
    inject_func: InjectFunc[..., T],
) -> None:
    if isinstance(command, Command) and not is_dishka_injected(
        command.callback,  # type: ignore[arg-type]
    ):
        command.callback = inject_func(command.callback)   # type: ignore[arg-type]

    if isinstance(command, Group):
        for command_name in command.list_commands(context):
            child_command = command.get_command(context, command_name)
            _inject_commands(context, child_command, inject_func)


def setup_dishka(
    container: Container,
    context: Context,
    *,
    finalize_container: bool = True,
    auto_inject: bool | InjectFunc[..., T] = False,
) -> None:
    context.meta[CONTAINER_NAME] = container

    if finalize_container:
        context.call_on_close(container.close)

    if auto_inject is not False:
        inject_func: InjectFunc[..., T]

        if auto_inject is True:
            inject_func = inject
        else:
            inject_func = auto_inject

        _inject_commands(context, context.command, inject_func)
