from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeVar

from returns.io import IO
from returns.primitives.hkt import Kind1, dekind

if TYPE_CHECKING:
    from returns.future import Future

_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')


async def async_map(
    function: Callable[[_ValueType_co], _NewValueType],
    inner_value: Awaitable[_ValueType_co],
) -> _NewValueType:
    """Async maps a function over a value."""
    return function(await inner_value)


async def async_apply(
    container: 'Future[Callable[[_ValueType_co], _NewValueType]]',
    inner_value: Awaitable[_ValueType_co],
) -> _NewValueType:
    """Async applies a container with function over a value."""
    return (await container)._inner_value(await inner_value)  # noqa: SLF001


async def async_bind(
    function: Callable[[_ValueType_co], Kind1['Future', _NewValueType]],
    inner_value: Awaitable[_ValueType_co],
) -> _NewValueType:
    """Async binds a container over a value."""
    return (await dekind(function(await inner_value)))._inner_value  # noqa: SLF001


async def async_bind_awaitable(
    function: Callable[[_ValueType_co], Awaitable[_NewValueType]],
    inner_value: Awaitable[_ValueType_co],
) -> _NewValueType:
    """Async binds a coroutine over a value."""
    return await function(await inner_value)


async def async_bind_async(
    function: Callable[
        [_ValueType_co],
        Awaitable[Kind1['Future', _NewValueType]],
    ],
    inner_value: Awaitable[_ValueType_co],
) -> _NewValueType:
    """Async binds a coroutine with container over a value."""
    inner_io = dekind(await function(await inner_value))._inner_value  # noqa: SLF001
    return await inner_io


async def async_bind_io(
    function: Callable[[_ValueType_co], IO[_NewValueType]],
    inner_value: Awaitable[_ValueType_co],
) -> _NewValueType:
    """Async binds a container over a value."""
    return function(await inner_value)._inner_value  # noqa: SLF001
