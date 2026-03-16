__all__ = [
    "AnyFlow",
    "Flow",
    "async_flow",
    "flow",
    "flow_expr",
    "run_async_flow",
    "run_sync_flow",
    "sync_flow",
]

from collections.abc import Awaitable, Callable, Generator
from functools import wraps
from typing import Any, TypeAlias, cast

from typing_extensions import ParamSpec, TypeVar

from fastapi_pagination.utils import await_if_coro, is_coro

P = ParamSpec("P")

TArg = TypeVar("TArg")
R = TypeVar("R", default=Any)

Flow: TypeAlias = Generator[
    Awaitable[TArg] | TArg,
    TArg,
    R,
]
AnyFlow: TypeAlias = Flow[Any, R]

TFlow = TypeVar("TFlow", bound=Flow[Any, Any])


def flow(func: Callable[P, TFlow]) -> Callable[P, TFlow]:
    return func


def _check_not_coro(obj: Any) -> None:
    if is_coro(obj):
        raise TypeError(f"Coroutine {obj} is not allowed in sync flow")


def run_sync_flow(gen: Flow[Any, R], /) -> R:
    try:
        res = gen.send(None)
        _check_not_coro(res)

        while True:
            try:
                res = gen.send(res)
                _check_not_coro(res)
            except StopIteration:  # noqa: PERF203
                raise
            except BaseException as exc:  # noqa: BLE001
                res = gen.throw(exc)
    except StopIteration as exc:
        return cast(R, exc.value)


async def run_async_flow(gen: Flow[Any, R], /) -> R:
    try:
        res = gen.send(None)

        while True:
            try:
                res = await await_if_coro(res)
                res = gen.send(res)
            except StopIteration:  # noqa: PERF203
                raise
            except BaseException as exc:  # noqa: BLE001
                res = gen.throw(exc)
    except StopIteration as exc:
        return cast(R, exc.value)


def sync_flow(func: Callable[P, Flow[Any, R]]) -> Callable[P, R]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return run_sync_flow(func(*args, **kwargs))

    return wrapper


def async_flow(func: Callable[P, Flow[Any, R]]) -> Callable[P, Awaitable[R]]:
    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return await run_async_flow(func(*args, **kwargs))

    return wrapper


def flow_expr(expr: Callable[P, Awaitable[R] | R]) -> Callable[P, Flow[Any, R]]:
    @wraps(expr)
    def flow_wrapper(*args: P.args, **kwargs: P.kwargs) -> Flow[Any, R]:
        res = yield expr(*args, **kwargs)
        return cast(R, res)

    return flow_wrapper
