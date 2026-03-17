from typing import TypeVar, overload

from returns.context import NoDeps
from returns.interfaces.failable import DiverseFailableN, SingleFailableN
from returns.primitives.hkt import KindN, kinded

_ValueType = TypeVar('_ValueType')
_ErrorType = TypeVar('_ErrorType')

_SingleFailableKind = TypeVar('_SingleFailableKind', bound=SingleFailableN)
_DiverseFailableKind = TypeVar('_DiverseFailableKind', bound=DiverseFailableN)


@overload
def internal_cond(
    container_type: type[_SingleFailableKind],
    is_success: bool,  # noqa: FBT001
    success_value: _ValueType,
) -> KindN[_SingleFailableKind, _ValueType, _ErrorType, NoDeps]: ...


@overload
def internal_cond(
    container_type: type[_DiverseFailableKind],
    is_success: bool,  # noqa: FBT001
    success_value: _ValueType,
    error_value: _ErrorType,
) -> KindN[_DiverseFailableKind, _ValueType, _ErrorType, NoDeps]: ...


def internal_cond(
    container_type: (type[_SingleFailableKind] | type[_DiverseFailableKind]),
    is_success: bool,  # noqa: FBT001
    success_value: _ValueType,
    error_value: _ErrorType | None = None,
):
    """
    Reduce the boilerplate when choosing paths.

    Works with ``SingleFailableN`` (e.g. ``Maybe``)
    and ``DiverseFailableN`` (e.g. ``Result``).

    Example using ``cond`` with the ``Result`` container:

    .. code:: python

      >>> from returns.methods import cond
      >>> from returns.result import Failure, Result, Success

      >>> def is_numeric(string: str) -> Result[str, str]:
      ...     return cond(
      ...         Result,
      ...         string.isnumeric(),
      ...         'It is a number',
      ...         'It is not a number',
      ...     )

      >>> assert is_numeric('42') == Success('It is a number')
      >>> assert is_numeric('non numeric') == Failure('It is not a number')

    Example using ``cond`` with the ``Maybe`` container:

    .. code:: python

      >>> from returns.maybe import Maybe, Some, Nothing

      >>> def is_positive(number: int) -> Maybe[int]:
      ...     return cond(Maybe, number > 0, number)

      >>> assert is_positive(10) == Some(10)
      >>> assert is_positive(-10) == Nothing

    """
    if is_success:
        return container_type.from_value(success_value)

    if issubclass(container_type, DiverseFailableN):
        return container_type.from_failure(error_value)
    return container_type.empty


#: Kinded version of :func:`~internal_cond`, use it to infer real return type.
cond = kinded(internal_cond)
