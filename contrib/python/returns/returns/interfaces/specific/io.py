from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import Never

from returns.interfaces import container, equable
from returns.primitives.hkt import KindN

if TYPE_CHECKING:
    from returns.io import IO  # noqa: WPS433

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_IOLikeType = TypeVar('_IOLikeType', bound='IOLikeN')


class IOLikeN(container.ContainerN[_FirstType, _SecondType, _ThirdType]):
    """
    Represents interface for types that looks like fearless ``IO``.

    This type means that ``IO`` cannot fail. Like random numbers, date, etc.
    Don't use this type for ``IO`` that can. Instead, use
    :class:`returns.interfaces.specific.ioresult.IOResultBasedN` type.

    """

    __slots__ = ()

    @abstractmethod
    def bind_io(
        self: _IOLikeType,
        function: Callable[[_FirstType], IO[_UpdatedType]],
    ) -> KindN[_IOLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Allows to apply a wrapped function over a container."""

    @classmethod
    @abstractmethod
    def from_io(
        cls: type[_IOLikeType],
        inner_value: IO[_UpdatedType],
    ) -> KindN[_IOLikeType, _UpdatedType, _SecondType, _ThirdType]:
        """Unit method to create new containers from successful ``IO``."""


#: Type alias for kinds with one type argument.
IOLike1 = IOLikeN[_FirstType, Never, Never]

#: Type alias for kinds with two type arguments.
IOLike2 = IOLikeN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
IOLike3 = IOLikeN[_FirstType, _SecondType, _ThirdType]


class IOBasedN(
    IOLikeN[_FirstType, _SecondType, _ThirdType],
    equable.Equable,
):
    """
    Represents the base interface for types that do fearless ``IO``.

    This type means that ``IO`` cannot fail. Like random numbers, date, etc.
    Don't use this type for ``IO`` that can. Instead, use
    :class:`returns.interfaces.specific.ioresult.IOResultBasedN` type.

    This interface also supports direct comparison of two values.
    While ``IOLikeN`` is different. It can be lazy and cannot be compared.

    """

    __slots__ = ()


#: Type alias for kinds with one type argument.
IOBased1 = IOBasedN[_FirstType, Never, Never]

#: Type alias for kinds with two type arguments.
IOBased2 = IOBasedN[_FirstType, _SecondType, Never]

#: Type alias for kinds with three type arguments.
IOBased3 = IOBasedN[_FirstType, _SecondType, _ThirdType]
