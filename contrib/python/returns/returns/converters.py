from typing import TypeVar, overload

from returns.functions import identity
from returns.interfaces.bindable import BindableN
from returns.maybe import Maybe, Nothing, Some
from returns.pipeline import is_successful
from returns.primitives.hkt import KindN, kinded
from returns.result import Failure, Result, Success

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')
_ThirdType = TypeVar('_ThirdType')

_BindableKind = TypeVar('_BindableKind', bound=BindableN)


@kinded
def flatten(
    container: KindN[
        _BindableKind,
        KindN[_BindableKind, _FirstType, _SecondType, _ThirdType],
        _SecondType,
        _ThirdType,
    ],
) -> KindN[_BindableKind, _FirstType, _SecondType, _ThirdType]:
    """
    Joins two nested containers together.

    Please, note that it will not join
    two ``Failure`` for ``Result`` case
    or two ``Nothing`` for ``Maybe`` case
    (or basically any two error types) together.

    .. code:: python

      >>> from returns.converters import flatten
      >>> from returns.io import IO
      >>> from returns.result import Failure, Success

      >>> assert flatten(IO(IO(1))) == IO(1)

      >>> assert flatten(Success(Success(1))) == Success(1)
      >>> assert flatten(Failure(Failure(1))) == Failure(Failure(1))

    See also:
        - https://bit.ly/2sIviUr

    """
    return container.bind(identity)


def result_to_maybe(
    result_container: Result[_FirstType, _SecondType],
) -> Maybe[_FirstType]:
    """
    Converts ``Result`` container to ``Maybe`` container.

    .. code:: python

      >>> from returns.maybe import Some, Nothing
      >>> from returns.result import Failure, Success

      >>> assert result_to_maybe(Success(1)) == Some(1)
      >>> assert result_to_maybe(Success(None)) == Some(None)
      >>> assert result_to_maybe(Failure(1)) == Nothing
      >>> assert result_to_maybe(Failure(None)) == Nothing

    """
    if is_successful(result_container):
        return Some(result_container.unwrap())
    return Nothing


@overload
def maybe_to_result(
    maybe_container: Maybe[_FirstType],
) -> Result[_FirstType, None]: ...


@overload
def maybe_to_result(
    maybe_container: Maybe[_FirstType],
    default_error: _SecondType,
) -> Result[_FirstType, _SecondType]: ...


def maybe_to_result(
    maybe_container: Maybe[_FirstType],
    default_error: _SecondType | None = None,
) -> Result[_FirstType, _SecondType | None]:
    """
    Converts ``Maybe`` container to ``Result`` container.

    With optional ``default_error`` to be used for ``Failure``'s error value.

    .. code:: python

      >>> from returns.maybe import Some, Nothing
      >>> from returns.result import Failure, Success

      >>> assert maybe_to_result(Some(1)) == Success(1)
      >>> assert maybe_to_result(Some(None)) == Success(None)
      >>> assert maybe_to_result(Nothing) == Failure(None)

      >>> assert maybe_to_result(Nothing, 'error') == Failure('error')

    """
    if is_successful(maybe_container):
        return Success(maybe_container.unwrap())
    return Failure(default_error)
