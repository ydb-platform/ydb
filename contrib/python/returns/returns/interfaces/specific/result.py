"""
An interface that represents a pure computation result.

For impure result see
:class:`returns.interfaces.specific.ioresult.IOResultLikeN` type.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import Never

from returns.interfaces import equable, failable, unwrappable
from returns.primitives.hkt import KindN

if TYPE_CHECKING:
    from returns.result import Result  # noqa: WPS433

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_ResultLikeType = TypeVar('_ResultLikeType', bound='ResultLikeN')

# New values:
_ValueType = TypeVar('_ValueType')
_ErrorType = TypeVar('_ErrorType')

# Unwrappable:
_FirstUnwrappableType = TypeVar('_FirstUnwrappableType')
_SecondUnwrappableType = TypeVar('_SecondUnwrappableType')


class ResultLikeN(
    failable.DiverseFailableN[_FirstType, _SecondType, _ThirdType],
):
    """
    Base types for types that looks like ``Result`` but cannot be unwrapped.

    Like ``RequiresContextResult`` or ``FutureResult``.
    """

    __slots__ = ()

    @abstractmethod
    def bind_result(
        self: _ResultLikeType,
        function: Callable[[_FirstType], Result[_UpdatedType, _SecondType]],
    ) -> KindN[_ResultLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Runs ``Result`` returning function over a container."""

    @classmethod
    @abstractmethod
    def from_result(
        cls: type[_ResultLikeType],
        inner_value: Result[_ValueType, _ErrorType],
    ) -> KindN[_ResultLikeType, _ValueType, _ErrorType, _ThirdType]:
        """Unit method to create new containers from any raw value."""


#: Type alias for kinds with two type arguments.
ResultLike2 = ResultLikeN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
ResultLike3 = ResultLikeN[_FirstType, _SecondType, _ThirdType]


class UnwrappableResult(
    ResultLikeN[_FirstType, _SecondType, _ThirdType],
    unwrappable.Unwrappable[_FirstUnwrappableType, _SecondUnwrappableType],
    equable.Equable,
):
    """
    Intermediate type with 5 type arguments that represents unwrappable result.

    It is a raw type and should not be used directly.
    Use ``ResultBasedN`` and ``IOResultBasedN`` instead.
    """

    __slots__ = ()


class ResultBasedN(
    UnwrappableResult[
        _FirstType,
        _SecondType,
        _ThirdType,
        # Unwraps:
        _FirstType,
        _SecondType,
    ],
):
    """
    Base type for real ``Result`` types.

    Can be unwrapped.
    """

    __slots__ = ()


#: Type alias for kinds with two type arguments.
ResultBased2 = ResultBasedN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
ResultBased3 = ResultBasedN[_FirstType, _SecondType, _ThirdType]
