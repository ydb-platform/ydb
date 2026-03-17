from collections.abc import Callable
from typing import TypeVar

from returns.interfaces.failable import DiverseFailableN
from returns.primitives.hkt import Kinded, KindN, kinded

_FirstType = TypeVar('_FirstType')
_NewFirstType = TypeVar('_NewFirstType')
_SecondType = TypeVar('_SecondType')
_NewSecondType = TypeVar('_NewSecondType')
_ThirdType = TypeVar('_ThirdType')
_NewThirdType = TypeVar('_NewThirdType')

_DiverseFailableKind = TypeVar('_DiverseFailableKind', bound=DiverseFailableN)


def unify(  # noqa: WPS234
    function: Callable[
        [_FirstType],
        KindN[
            _DiverseFailableKind,
            _NewFirstType,
            _NewSecondType,
            _NewThirdType,
        ],
    ],
) -> Kinded[
    Callable[
        [KindN[_DiverseFailableKind, _FirstType, _SecondType, _ThirdType]],
        KindN[
            _DiverseFailableKind,
            _NewFirstType,
            _SecondType | _NewSecondType,
            _NewThirdType,
        ],
    ]
]:
    """
    Composes successful container with a function that returns a container.

    Similar to :func:`~returns.pointfree.bind` but has different type.
    It returns ``Result[ValueType, Union[OldErrorType, NewErrorType]]``
    instead of ``Result[ValueType, OldErrorType]``.

    So, it can be more useful in some situations.
    Probably with specific exceptions.

    .. code:: python

      >>> from returns.methods import cond
      >>> from returns.pointfree import unify
      >>> from returns.result import Result, Success, Failure

      >>> def bindable(arg: int) -> Result[int, int]:
      ...     return cond(Result, arg % 2 == 0, arg + 1, arg - 1)

      >>> assert unify(bindable)(Success(2)) == Success(3)
      >>> assert unify(bindable)(Success(1)) == Failure(0)
      >>> assert unify(bindable)(Failure(42)) == Failure(42)

    """

    @kinded
    def factory(
        container: KindN[
            _DiverseFailableKind,
            _FirstType,
            _SecondType,
            _ThirdType,
        ],
    ) -> KindN[
        _DiverseFailableKind,
        _NewFirstType,
        _SecondType | _NewSecondType,
        _NewThirdType,
    ]:
        return container.bind(function)  # type: ignore

    return factory
