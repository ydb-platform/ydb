from collections.abc import Callable
from typing import TypeVar

from returns.interfaces.applicative import ApplicativeN
from returns.primitives.hkt import Kinded, KindN, kinded

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_ApplicativeKind = TypeVar('_ApplicativeKind', bound=ApplicativeN)


def apply(
    container: KindN[
        _ApplicativeKind,
        Callable[[_FirstType], _UpdatedType],
        _SecondType,
        _ThirdType,
    ],
) -> Kinded[
    Callable[
        [KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Turns container containing a function into a callable.

    In other words, it modifies the function
    signature from:
    ``Container[a -> b]``
    to:
    ``Container[a] -> Container[b]``

    This is how it should be used:

    .. code:: python

      >>> from returns.pointfree import apply
      >>> from returns.maybe import Some, Nothing

      >>> def example(argument: int) -> int:
      ...     return argument + 1

      >>> assert apply(Some(example))(Some(1)) == Some(2)
      >>> assert apply(Some(example))(Nothing) == Nothing
      >>> assert apply(Nothing)(Some(1)) == Nothing
      >>> assert apply(Nothing)(Nothing) == Nothing

    Note, that this function works for all containers with ``.apply`` method.
    See :class:`returns.interfaces.applicative.ApplicativeN` for more info.

    """

    @kinded
    def factory(
        other: KindN[_ApplicativeKind, _FirstType, _SecondType, _ThirdType],
    ) -> KindN[_ApplicativeKind, _UpdatedType, _SecondType, _ThirdType]:
        return other.apply(container)

    return factory
