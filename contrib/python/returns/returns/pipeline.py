from typing import Any

from returns._internal.pipeline.flow import flow as flow
from returns._internal.pipeline.managed import managed as managed
from returns._internal.pipeline.pipe import pipe as pipe
from returns.interfaces.unwrappable import Unwrappable
from returns.primitives.exceptions import UnwrapFailedError


# TODO: add overloads for specific types, so it can narrow them with `TypeIs`
def is_successful(container: Unwrappable[Any, Any]) -> bool:
    """
    Determines if a container was successful or not.

    .. code:: python

      >>> from returns.maybe import Some, Nothing
      >>> from returns.result import Failure, Success
      >>> from returns.io import IOSuccess, IOFailure

      >>> assert is_successful(Some(1))
      >>> assert not is_successful(Nothing)

      >>> assert is_successful(Success(1))
      >>> assert not is_successful(Failure(1))

      >>> assert is_successful(IOSuccess(1))
      >>> assert not is_successful(IOFailure(1))

    This function can work with containers
    that are instance of :class:`returns.interfaces.unwrappable.Unwrappable`.

    """
    try:
        container.unwrap()
    except UnwrapFailedError:
        return False
    return True
