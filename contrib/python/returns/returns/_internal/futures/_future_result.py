from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar

from returns.io import IO, IOResult
from returns.primitives.hkt import Kind2, dekind
from returns.result import Failure, Result, Success

if TYPE_CHECKING:
    from returns.future import Future, FutureResult


_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')
_ErrorType_co = TypeVar('_ErrorType_co', covariant=True)
_NewErrorType = TypeVar('_NewErrorType')


async def async_swap(
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_ErrorType_co, _ValueType_co]:
    """Swaps value and error types in ``Result``."""
    return (await inner_value).swap()


async def async_map(
    function: Callable[[_ValueType_co], _NewValueType],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async maps a function over a value."""
    return (await inner_value).map(function)


async def async_apply(
    container: FutureResult[
        Callable[[_ValueType_co], _NewValueType], _ErrorType_co
    ],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async maps a function over a value."""
    return (await inner_value).apply((await container)._inner_value)  # noqa: SLF001


async def async_bind(
    function: Callable[
        [_ValueType_co],
        Kind2[FutureResult, _NewValueType, _ErrorType_co],
    ],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a container over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return (await dekind(function(container.unwrap())))._inner_value  # noqa: SLF001
    return container  # type: ignore[return-value]


async def async_bind_awaitable(
    function: Callable[[_ValueType_co], Awaitable[_NewValueType]],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a coroutine over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return Result.from_value(await function(container.unwrap()))
    return container  # type: ignore[return-value]


async def async_bind_async(
    function: Callable[
        [_ValueType_co],
        Awaitable[Kind2[FutureResult, _NewValueType, _ErrorType_co]],
    ],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a coroutine with container over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return await dekind(await function(container.unwrap()))._inner_value  # noqa: SLF001
    return container  # type: ignore[return-value]


async def async_bind_result(
    function: Callable[[_ValueType_co], Result[_NewValueType, _ErrorType_co]],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a container returning ``Result`` over a value."""
    return (await inner_value).bind(function)


async def async_bind_ioresult(
    function: Callable[[_ValueType_co], IOResult[_NewValueType, _ErrorType_co]],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a container returning ``IOResult`` over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return function(container.unwrap())._inner_value  # noqa: SLF001
    return container  # type: ignore[return-value]


async def async_bind_io(
    function: Callable[[_ValueType_co], IO[_NewValueType]],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a container returning ``IO`` over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return Success(function(container.unwrap())._inner_value)  # noqa: SLF001
    return container  # type: ignore[return-value]


async def async_bind_future(
    function: Callable[[_ValueType_co], Future[_NewValueType]],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a container returning ``IO`` over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return await async_from_success(function(container.unwrap()))
    return container  # type: ignore[return-value]


async def async_bind_async_future(
    function: Callable[[_ValueType_co], Awaitable[Future[_NewValueType]]],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a container returning ``IO`` over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return await async_from_success(await function(container.unwrap()))
    return container  # type: ignore[return-value]


async def async_alt(
    function: Callable[[_ErrorType_co], _NewErrorType],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_ValueType_co, _NewErrorType]:
    """Async alts a function over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return container
    return Failure(function(container.failure()))


async def async_lash(
    function: Callable[
        [_ErrorType_co],
        Kind2[FutureResult, _ValueType_co, _NewErrorType],
    ],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_ValueType_co, _NewErrorType]:
    """Async lashes a function returning a container over a value."""
    container = await inner_value
    if isinstance(container, Success):
        return container
    return (await dekind(function(container.failure())))._inner_value  # noqa: SLF001


async def async_from_success(
    container: Future[_NewValueType],
) -> Result[_NewValueType, Any]:
    """Async success unit factory."""
    return Success((await container)._inner_value)  # noqa: SLF001


async def async_from_failure(
    container: Future[_NewErrorType],
) -> Result[Any, _NewErrorType]:
    """Async failure unit factory."""
    return Failure((await container)._inner_value)  # noqa: SLF001


async def async_compose_result(
    function: Callable[
        [Result[_ValueType_co, _ErrorType_co]],
        Kind2[FutureResult, _NewValueType, _ErrorType_co],
    ],
    inner_value: Awaitable[Result[_ValueType_co, _ErrorType_co]],
) -> Result[_NewValueType, _ErrorType_co]:
    """Async composes ``Result`` based function."""
    return (await dekind(function(await inner_value)))._inner_value  # noqa: SLF001
