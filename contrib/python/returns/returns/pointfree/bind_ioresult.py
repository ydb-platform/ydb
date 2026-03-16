from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from returns.interfaces.specific.ioresult import IOResultLikeN
from returns.primitives.hkt import Kinded, KindN, kinded

if TYPE_CHECKING:
    from returns.io import IOResult  # noqa: WPS433

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_IOResultLikeKind = TypeVar('_IOResultLikeKind', bound=IOResultLikeN)


def bind_ioresult(
    function: Callable[[_FirstType], IOResult[_UpdatedType, _SecondType]],
) -> Kinded[
    Callable[
        [KindN[_IOResultLikeKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_IOResultLikeKind, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Composes successful container with a function that returns a container.

    In other words, it modifies the function's
    signature from:
    ``a -> IOResult[b, c]``
    to:
    ``Container[a, c] -> Container[b, c]``

    .. code:: python

      >>> from returns.io import IOResult, IOSuccess
      >>> from returns.context import RequiresContextIOResult
      >>> from returns.pointfree import bind_ioresult

      >>> def returns_ioresult(arg: int) -> IOResult[int, str]:
      ...     return IOSuccess(arg + 1)

      >>> bound = bind_ioresult(returns_ioresult)
      >>> assert bound(IOSuccess(1)) == IOSuccess(2)
      >>> assert bound(
      ...     RequiresContextIOResult.from_value(1),
      ... )(...) == IOSuccess(2)

    """

    @kinded
    def factory(
        container: KindN[
            _IOResultLikeKind,
            _FirstType,
            _SecondType,
            _ThirdType,
        ],
    ) -> KindN[_IOResultLikeKind, _UpdatedType, _SecondType, _ThirdType]:
        return container.bind_ioresult(function)

    return factory
