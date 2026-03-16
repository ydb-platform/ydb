from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from returns.interfaces.specific.reader_result import ReaderResultLikeN
from returns.primitives.hkt import Kinded, KindN, kinded

if TYPE_CHECKING:
    from returns.context import ReaderResult  # noqa: WPS433

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')
_UpdatedType = TypeVar('_UpdatedType')

_ReaderResultLikeKind = TypeVar(
    '_ReaderResultLikeKind',
    bound=ReaderResultLikeN,
)


def bind_context_result(
    function: Callable[
        [_FirstType],
        ReaderResult[_UpdatedType, _SecondType, _ThirdType],
    ],
) -> Kinded[
    Callable[
        [KindN[_ReaderResultLikeKind, _FirstType, _SecondType, _ThirdType]],
        KindN[_ReaderResultLikeKind, _UpdatedType, _SecondType, _ThirdType],
    ]
]:
    """
    Composes successful container with a function that returns a container.

    In other words, it modifies the function's
    signature from:
    ``a -> ReaderResult[b, c, e]``
    to:
    ``Container[a, c, e] -> Container[b, c, e]``

    .. code:: python

      >>> from returns.pointfree import bind_context_result
      >>> from returns.context import ReaderIOResult, ReaderResult
      >>> from returns.io import IOSuccess, IOFailure

      >>> def example(argument: int) -> ReaderResult[int, str, str]:
      ...     return ReaderResult.from_value(argument + 1)

      >>> assert bind_context_result(example)(
      ...     ReaderIOResult.from_value(1),
      ... )(...) == IOSuccess(2)
      >>> assert bind_context_result(example)(
      ...     ReaderIOResult.from_failure('a'),
      ... )(...) == IOFailure('a')

    """

    @kinded
    def factory(
        container: KindN[
            _ReaderResultLikeKind,
            _FirstType,
            _SecondType,
            _ThirdType,
        ],
    ) -> KindN[_ReaderResultLikeKind, _UpdatedType, _SecondType, _ThirdType]:
        return container.bind_context_result(function)

    return factory
