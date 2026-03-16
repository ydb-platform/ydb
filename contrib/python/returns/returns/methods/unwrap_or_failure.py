from typing import TypeVar

from returns.interfaces.unwrappable import Unwrappable
from returns.pipeline import is_successful

_FirstType = TypeVar('_FirstType')
_SecondType = TypeVar('_SecondType')


def unwrap_or_failure(
    container: Unwrappable[_FirstType, _SecondType],
) -> _FirstType | _SecondType:
    """
    Unwraps either successful or failed value.

    .. code:: python

      >>> from returns.io import IO, IOSuccess, IOFailure
      >>> from returns.methods import unwrap_or_failure

      >>> assert unwrap_or_failure(IOSuccess(1)) == IO(1)
      >>> assert unwrap_or_failure(IOFailure('a')) == IO('a')

    """
    if is_successful(container):
        return container.unwrap()
    return container.failure()
