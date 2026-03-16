__all__ = (
    "FastStreamProvider",
    "inject",
    "setup_dishka",
)

import warnings
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import (
    Any,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
)

from faststream import BaseMiddleware, FastStream, context
from faststream.broker.core.usecase import BrokerUsecase as BrokerType
from faststream.broker.message import StreamMessage
from faststream.types import DecodedMessage
from faststream.utils.context import ContextRepo

from dishka import AsyncContainer, Provider, Scope, from_context
from dishka.integrations.base import InjectFunc, wrap_injection

_ReturnT = TypeVar("_ReturnT")
_ParamsP = ParamSpec("_ParamsP")


class FastStreamProvider(Provider):
    context = from_context(ContextRepo, scope=Scope.REQUEST)
    message = from_context(StreamMessage, scope=Scope.REQUEST)


try:
    # AsgiFastStream was introduced in FastStream 0.5.16
    from faststream.asgi import AsgiFastStream

except ImportError:
    Application: TypeAlias = FastStream  # type: ignore[no-redef,misc]

else:
    Application: TypeAlias = FastStream | AsgiFastStream  # type: ignore[no-redef,misc]

try:
    from faststream.broker.fastapi import StreamRouter
except ImportError:
    pass
else:
    Application |= StreamRouter  # type: ignore[assignment]


class ApplicationLike(Protocol):
    broker: BrokerType[Any, Any]


def setup_dishka(
    container: AsyncContainer,
    app: "Application | ApplicationLike | None" = None,
    broker: "BrokerType[Any, Any] | None" = None,
    *,
    finalize_container: bool = True,
    auto_inject: bool | InjectFunc[_ParamsP, _ReturnT] = False,
) -> None:
    """
    Setup dishka integration with FastStream.
    You must provide either app or broker.

    Args:
        container: AsyncContainer instance.
        app: FastStream Application or StreamRouter instance.
        broker: FastStream broker instance.
        finalize_container: bool. Can be used only with app.
        auto_inject: bool or custom inject decorator.
    """
    if (app and broker) or (not app and not broker):
        raise ValueError(  # noqa: TRY003
            "You must provide either app or broker "
            "to setup dishka integration.",
        )

    if finalize_container:
        if getattr(app, "after_shutdown", None):
            app.after_shutdown(container.close)  # type: ignore[union-attr]

        else:
            warnings.warn(
                "For use `finalize_container=True` "
                "you must provide `app: FastStream | AsgiFastStream` "
                "argument.",
                category=RuntimeWarning,
                stacklevel=2,
            )

    broker = broker or getattr(app, "broker", None)
    assert broker  # noqa: S101

    if getattr(broker, "add_middleware", None):
        # FastStream 0.5.6 and higher
        broker.add_middleware(DishkaMiddleware(container))

    else:
        # FastStream 0.5 - 0.5.5 versions backport
        broker._middlewares = (  # noqa: SLF001
            DishkaMiddleware(container),
            *broker._middlewares,  # noqa: SLF001
        )

        for subscriber in broker._subscribers.values():  # noqa: SLF001
            subscriber._broker_middlewares = (  # noqa: SLF001
                DishkaMiddleware(container),
                *subscriber._broker_middlewares,  # noqa: SLF001
            )

        for publisher in broker._publishers.values():  # noqa: SLF001
            publisher._broker_middlewares = (  # noqa: SLF001
                DishkaMiddleware(container),
                *publisher._broker_middlewares,  # noqa: SLF001
            )

    if auto_inject is not False:
        inject_func: InjectFunc[_ParamsP, _ReturnT]

        if auto_inject is True:
            inject_func = inject
        else:
            inject_func = auto_inject

        broker._call_decorators = (  # noqa: SLF001
            inject_func,
            *broker._call_decorators,  # noqa: SLF001
        )


def inject(func: Callable[_ParamsP, _ReturnT]) -> Callable[_ParamsP, _ReturnT]:
    return wrap_injection(
        func=func,
        is_async=True,
        container_getter=lambda *_: context.get_local("dishka"),
    )


class DishkaMiddleware:
    def __init__(self, container: AsyncContainer) -> None:
        self.container = container

    def __call__(self, *args: Any, **kwargs: Any) -> "_DishkaMiddleware":
        return _DishkaMiddleware(self.container, *args, **kwargs)


class _DishkaMiddleware(BaseMiddleware):
    def __init__(
        self,
        container: AsyncContainer,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.container = container
        super().__init__(*args, **kwargs)

    async def consume_scope(  # type: ignore[misc]
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: StreamMessage[Any],
    ) -> AsyncIterator[DecodedMessage]:
        async with self.container(
            {
                StreamMessage: msg,
                ContextRepo: context,
                type(msg): msg,
            },
        ) as request_container:
            with context.scope("dishka", request_container):
                return cast(
                    AsyncIterator[DecodedMessage],
                    await call_next(msg),
                )
