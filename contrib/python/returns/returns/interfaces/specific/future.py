"""
Represents the base interfaces for types that do fearless async operations.

This type means that ``Future`` cannot fail.
Don't use this type for async that can. Instead, use
:class:`returns.interfaces.specific.future_result.FutureResultBasedN` type.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Awaitable, Callable, Generator
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import Never

from returns.interfaces.specific import io
from returns.primitives.hkt import KindN

if TYPE_CHECKING:
    from returns.future import Future  # noqa: WPS433

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_FutureLikeType = TypeVar('_FutureLikeType', bound='FutureLikeN')
_AsyncFutureType = TypeVar('_AsyncFutureType', bound='AwaitableFutureN')


class FutureLikeN(io.IOLikeN[_FirstType, _SecondType, _ThirdType]):
    """
    Base type for ones that does look like ``Future``.

    But at the time this is not a real ``Future`` and cannot be awaited.
    """

    __slots__ = ()

    @abstractmethod
    def bind_future(
        self: _FutureLikeType,
        function: Callable[[_FirstType], Future[_UpdatedType]],
    ) -> KindN[_FutureLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Allows to bind ``Future`` returning function over a container."""

    @abstractmethod
    def bind_async_future(
        self: _FutureLikeType,
        function: Callable[[_FirstType], Awaitable[Future[_UpdatedType]]],
    ) -> KindN[_FutureLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Allows to bind async ``Future`` returning function over container."""

    @abstractmethod
    def bind_async(
        self: _FutureLikeType,
        function: Callable[
            [_FirstType],
            Awaitable[
                KindN[_FutureLikeType, _UpdatedType, _SecondType, _ThirdType],
            ],
        ],
    ) -> KindN[_FutureLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Binds async function returning the same type of container."""

    @abstractmethod
    def bind_awaitable(
        self: _FutureLikeType,
        function: Callable[[_FirstType], Awaitable[_UpdatedType]],
    ) -> KindN[_FutureLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Allows to bind async function over container."""

    @classmethod
    @abstractmethod
    def from_future(
        cls: type[_FutureLikeType],
        inner_value: Future[_UpdatedType],
    ) -> KindN[_FutureLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Unit method to create new containers from successful ``Future``."""


#: Type alias for kinds with one type argument.
FutureLike1 = FutureLikeN[_FirstType, Never, Never]

#: Type alias for kinds with two type arguments.
FutureLike2 = FutureLikeN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
FutureLike3 = FutureLikeN[_FirstType, _SecondType, _ThirdType]


class AwaitableFutureN(Generic[_FirstType, _SecondType, _ThirdType]):
    """
    Type that provides the required API for ``Future`` to be async.

    Should not be used directly. Use ``FutureBasedN`` instead.
    """

    __slots__ = ()

    @abstractmethod
    def __await__(
        self: _AsyncFutureType,
    ) -> Generator[
        Any,
        Any,
        io.IOLikeN[_FirstType, _SecondType, _ThirdType],
    ]:
        """Magic method to allow ``await`` expression."""

    @abstractmethod
    async def awaitable(
        self: _AsyncFutureType,
    ) -> io.IOLikeN[_FirstType, _SecondType, _ThirdType]:
        """Underling logic under ``await`` expression."""


#: Type alias for kinds with one type argument.
AsyncFuture1 = AwaitableFutureN[_FirstType, Never, Never]

#: Type alias for kinds with two type arguments.
AsyncFuture2 = AwaitableFutureN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
AsyncFuture3 = AwaitableFutureN[_FirstType, _SecondType, _ThirdType]


class FutureBasedN(
    FutureLikeN[_FirstType, _SecondType, _ThirdType],
    AwaitableFutureN[_FirstType, _SecondType, _ThirdType],
):
    """
    Base type for real ``Future`` objects.

    They can be awaited.
    """

    __slots__ = ()


#: Type alias for kinds with one type argument.
FutureBased1 = FutureBasedN[_FirstType, Never, Never]

#: Type alias for kinds with two type arguments.
FutureBased2 = FutureBasedN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
FutureBased3 = FutureBasedN[_FirstType, _SecondType, _ThirdType]
