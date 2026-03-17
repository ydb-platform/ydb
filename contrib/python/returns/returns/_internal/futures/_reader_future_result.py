from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeVar

from returns.primitives.hkt import Kind3, dekind
from returns.result import Result, Success

if TYPE_CHECKING:
    from returns.context import RequiresContextFutureResult

_ValueType_co = TypeVar('_ValueType_co', covariant=True)
_NewValueType = TypeVar('_NewValueType')
_ErrorType_co = TypeVar('_ErrorType_co', covariant=True)
_EnvType = TypeVar('_EnvType')


async def async_bind_async(
    function: Callable[
        [_ValueType_co],
        Awaitable[
            Kind3[
                RequiresContextFutureResult,
                _NewValueType,
                _ErrorType_co,
                _EnvType,
            ],
        ],
    ],
    container: RequiresContextFutureResult[
        _ValueType_co, _ErrorType_co, _EnvType
    ],
    deps: _EnvType,
) -> Result[_NewValueType, _ErrorType_co]:
    """Async binds a coroutine with container over a value."""
    inner_value = await container(deps)._inner_value  # noqa: SLF001
    if isinstance(inner_value, Success):
        return await dekind(  # noqa: SLF001
            await function(inner_value.unwrap()),
        )(deps)._inner_value
    return inner_value  # type: ignore[return-value]


async def async_compose_result(
    function: Callable[
        [Result[_ValueType_co, _ErrorType_co]],
        Kind3[
            RequiresContextFutureResult,
            _NewValueType,
            _ErrorType_co,
            _EnvType,
        ],
    ],
    container: RequiresContextFutureResult[
        _ValueType_co, _ErrorType_co, _EnvType
    ],
    deps: _EnvType,
) -> Result[_NewValueType, _ErrorType_co]:
    """Async composes ``Result`` based function."""
    new_container = dekind(function((await container(deps))._inner_value))  # noqa: SLF001
    return (await new_container(deps))._inner_value  # noqa: SLF001
