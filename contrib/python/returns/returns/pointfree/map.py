from collections.abc import Callable
from typing import TypeVar

from returns.interfaces.mappable import MappableN
from returns.primitives.hkt import Kinded, KindN, kinded

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_MappableKind = TypeVar('_MappableKind', bound=MappableN)


def map_(
    function: Callable[[_FirstType], _UpdatedType],
) -> Kinded[
    Callable[
        [KindN[_MappableKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_MappableKind, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Lifts function to be wrapped in a container for better composition.

    In other words, it modifies the function's
    signature from:
    ``a -> b``
    to: ``Container[a] -> Container[b]``

    This is how it should be used:

    .. code:: python

        >>> from returns.io import IO
        >>> from returns.pointfree import map_

        >>> def example(argument: int) -> float:
        ...     return argument / 2

        >>> assert map_(example)(IO(1)) == IO(0.5)

    Note, that this function works for all containers with ``.map`` method.
    See :class:`returns.primitives.interfaces.mappable.MappableN` for more info.

    See also:
        - https://wiki.haskell.org/Lifting

    """

    @kinded
    def factory(
        container: KindN[_MappableKind, _FirstType, _SecondType, _ThirdType],
    ) -> KindN[_MappableKind, _UpdatedType, _SecondType, _ThirdType]:
        return container.map(function)

    return factory
