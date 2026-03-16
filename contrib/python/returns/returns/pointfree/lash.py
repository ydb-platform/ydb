from collections.abc import Callable
from typing import TypeVar

from returns.interfaces.lashable import LashableN
from returns.primitives.hkt import Kinded, KindN, kinded

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_LashableKind = TypeVar('_LashableKind', bound=LashableN)


def lash(
    function: Callable[
        [_SecondType],
        KindN[_LashableKind, _FirstType, _UpdatedType, _ThirdType],
    ],
) -> Kinded[
    Callable[
        [KindN[_LashableKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_LashableKind, _FirstType, _UpdatedType, _ThirdType],
    ]
]:
    """
    Turns function's input parameter from a regular value to a container.

    In other words, it modifies the function
    signature from:
    ``a -> Container[b]``
    to:
    ``Container[a] -> Container[b]``

    Similar to :func:`returns.pointfree.bind`, but works for failed containers.

    This is how it should be used:

    .. code:: python

      >>> from returns.pointfree import lash
      >>> from returns.result import Success, Failure, Result

      >>> def example(argument: int) -> Result[str, int]:
      ...     return Success(argument + 1)

      >>> assert lash(example)(Success('a')) == Success('a')
      >>> assert lash(example)(Failure(1)) == Success(2)

    Note, that this function works for all containers with ``.lash`` method.
    See :class:`returns.interfaces.lashable.Lashable` for more info.

    """

    @kinded
    def factory(
        container: KindN[_LashableKind, _FirstType, _SecondType, _ThirdType],
    ) -> KindN[_LashableKind, _FirstType, _UpdatedType, _ThirdType]:
        return container.lash(function)

    return factory
