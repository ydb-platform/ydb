__all__ = [
    "FromDishka",
    "TaskiqProvider",
    "inject",
    "setup_dishka",
]

import warnings
from collections.abc import Callable, Generator
from functools import partial
from inspect import Parameter
from typing import Annotated, Any, Final, TypeVar, overload

from taskiq import (
    AsyncBroker,
    Context,
    TaskiqDepends,
    TaskiqMessage,
    TaskiqMiddleware,
    TaskiqResult,
    TaskiqState,
)

from dishka import AsyncContainer, FromDishka, Provider, Scope, from_context
from dishka.integrations.base import wrap_injection

CONTAINER_NAME: Final = "dishka_container"
CONTAINER_REGISTRY: Final = "dishka_container_registry"
CONTAINER_ID: Final = "dishka_container_id"


_F = TypeVar("_F", bound=Callable[..., Any])


class TaskiqProvider(Provider):
    event = from_context(TaskiqMessage, scope=Scope.REQUEST)


class ContainerMiddleware(TaskiqMiddleware):
    def __init__(self, container: AsyncContainer) -> None:
        super().__init__()
        self._container = container

    async def pre_execute(
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage:
        if CONTAINER_REGISTRY not in self.broker.state:
            self.broker.state[CONTAINER_REGISTRY] = {}

        container = await self._container(
            context={TaskiqMessage: message},
        ).__aenter__()

        container_id = id(container)
        container_registry = self.broker.state[CONTAINER_REGISTRY]
        container_registry[container_id] = container

        message.labels[CONTAINER_ID] = container_id
        return message

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        if CONTAINER_ID not in message.labels:
            return

        container_id = message.labels[CONTAINER_ID]
        container_registry = self.broker.state[CONTAINER_REGISTRY]
        if container_id not in container_registry:
            return

        await container_registry[container_id].close()
        del container_registry[container_id]
        del message.labels[CONTAINER_ID]

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        if CONTAINER_ID not in message.labels:
            return

        container_id = message.labels[CONTAINER_ID]
        container_registry = self.broker.state[CONTAINER_REGISTRY]
        if container_id not in container_registry:
            return

        await container_registry[container_id].close()
        del container_registry[container_id]
        del message.labels[CONTAINER_ID]


def _get_container(
    state: Annotated[TaskiqState, TaskiqDepends()],
    context: Annotated[Context, TaskiqDepends()],
) -> Generator[AsyncContainer, None, None]:
    container_id = context.message.labels[CONTAINER_ID]
    yield state[CONTAINER_REGISTRY][container_id]


@overload
def inject(
    func: _F, *, patch_module: bool = False,
) -> _F: ...


@overload
def inject(
    func: None = ..., *, patch_module: bool = False,
) -> Callable[[_F], _F]: ...


def inject(
    func: _F | None = None,
    *,
    patch_module: bool = False,
) -> _F | Callable[[_F], _F]:
    if func is None:
        return partial(_inject_wrapper, patch_module=patch_module)

    return _inject_wrapper(func, patch_module=patch_module)


def _inject_wrapper(
    func: Callable[..., Any],
    *,
    patch_module: bool,
) -> Callable[..., Any]:
    annotation = Annotated[
        AsyncContainer, TaskiqDepends(_get_container),
    ]
    additional_params = [Parameter(
        name=CONTAINER_NAME,
        annotation=annotation,
        kind=Parameter.KEYWORD_ONLY,
    )]

    wrapper = wrap_injection(
        func=func,
        is_async=True,
        remove_depends=True,
        additional_params=additional_params,
        container_getter=lambda _, p: p[CONTAINER_NAME],
    )

    if not patch_module:
        warnings.warn(
            "Behavior without patch module is deprecated"
            ", use patch_module = True",
            DeprecationWarning,
            stacklevel=2,
        )

        wrapper.__module__ = wrap_injection.__module__

    return wrapper


def setup_dishka(
    container: AsyncContainer,
    broker: AsyncBroker,
) -> None:
    broker.add_middlewares(ContainerMiddleware(container))
