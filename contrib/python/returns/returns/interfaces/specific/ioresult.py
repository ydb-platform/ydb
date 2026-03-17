"""
An interface for types that do ``IO`` and can fail.

It is a base interface for both sync and async ``IO`` stacks.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import Never

from returns.interfaces.specific import io, result
from returns.primitives.hkt import KindN

if TYPE_CHECKING:
    from returns.io import IO, IOResult  # noqa: WPS433
    from returns.result import Result  # noqa: WPS433

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_ValueType = TypeVar('_ValueType')
_ErrorType = TypeVar('_ErrorType')

_IOResultLikeType = TypeVar('_IOResultLikeType', bound='IOResultLikeN')


class IOResultLikeN(
    io.IOLikeN[_FirstType, _SecondType, _ThirdType],
    result.ResultLikeN[_FirstType, _SecondType, _ThirdType],
):
    """
    Base type for types that look like ``IOResult`` but cannot be unwrapped.

    Like ``FutureResult`` or ``RequiresContextIOResult``.
    """

    __slots__ = ()

    @abstractmethod
    def bind_ioresult(
        self: _IOResultLikeType,
        function: Callable[[_FirstType], IOResult[_UpdatedType, _SecondType]],
    ) -> KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Runs ``IOResult`` returning function over a container."""

    @abstractmethod
    def compose_result(
        self: _IOResultLikeType,
        function: Callable[
            [Result[_FirstType, _SecondType]],
            KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType],
        ],
    ) -> KindN[_IOResultLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Allows to compose the underlying ``Result`` with a function."""

    @classmethod
    @abstractmethod
    def from_ioresult(
        cls: type[_IOResultLikeType],
        inner_value: IOResult[_ValueType, _ErrorType],
    ) -> KindN[_IOResultLikeType, _ValueType, _ErrorType, _ThirdType]:
        """Unit method to create new containers from ``IOResult`` type."""

    @classmethod
    @abstractmethod
    def from_failed_io(
        cls: type[_IOResultLikeType],
        inner_value: IO[_ErrorType],
    ) -> KindN[_IOResultLikeType, _FirstType, _ErrorType, _ThirdType]:
        """Unit method to create new containers from failed ``IO``."""


#: Type alias for kinds with two type arguments.
IOResultLike2 = IOResultLikeN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
IOResultLike3 = IOResultLikeN[_FirstType, _SecondType, _ThirdType]


class IOResultBasedN(
    IOResultLikeN[_FirstType, _SecondType, _ThirdType],
    io.IOBasedN[_FirstType, _SecondType, _ThirdType],
    result.UnwrappableResult[
        _FirstType,
        _SecondType,
        _ThirdType,
        # Unwraps:
        'IO[_FirstType]',
        'IO[_SecondType]',
    ],
):
    """
    Base type for real ``IOResult`` types.

    Can be unwrapped.
    """

    __slots__ = ()


#: Type alias for kinds with two type arguments.
IOResultBased2 = IOResultBasedN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
IOResultBased3 = IOResultBasedN[_FirstType, _SecondType, _ThirdType]
