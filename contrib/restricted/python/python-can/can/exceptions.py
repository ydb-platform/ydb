"""
There are several specific :class:`Exception` classes to allow user
code to react to specific scenarios related to CAN buses::

    Exception (Python standard library)
     +-- ...
     +-- CanError (python-can)
         +-- CanInterfaceNotImplementedError
         +-- CanInitializationError
         +-- CanOperationError
         +-- CanTimeoutError

Keep in mind that some functions and methods may raise different exceptions.
For example, validating typical arguments and parameters might result in a
:class:`ValueError`. This should always be documented for the function at hand.
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Optional


class CanError(Exception):
    """Base class for all CAN related exceptions.

    If specified, the error code is automatically appended to the message:

    .. testsetup:: canerror

        from can import CanError, CanOperationError

    .. doctest:: canerror

        >>> # With an error code (it also works with a specific error):
        >>> error = CanOperationError(message="Failed to do the thing", error_code=42)
        >>> str(error)
        'Failed to do the thing [Error Code 42]'
        >>>
        >>> # Missing the error code:
        >>> plain_error = CanError(message="Something went wrong ...")
        >>> str(plain_error)
        'Something went wrong ...'

    :param error_code:
        An optional error code to narrow down the cause of the fault

    :arg error_code:
        An optional error code to narrow down the cause of the fault
    """

    def __init__(
        self,
        message: str = "",
        error_code: Optional[int] = None,
    ) -> None:
        self.error_code = error_code
        super().__init__(
            message if error_code is None else f"{message} [Error Code {error_code}]"
        )


class CanInterfaceNotImplementedError(CanError, NotImplementedError):
    """Indicates that the interface is not supported on the current platform.

    Example scenarios:
      - No interface with that name exists
      - The interface is unsupported on the current operating system or interpreter
      - The driver could not be found or has the wrong version
    """


class CanInitializationError(CanError):
    """Indicates an error the occurred while initializing a :class:`can.BusABC`.

    If initialization fails due to a driver or platform missing/being unsupported,
    a :exc:`~can.exceptions.CanInterfaceNotImplementedError` is raised instead.
    If initialization fails due to a value being out of range, a :class:`ValueError`
    is raised.

    Example scenarios:
      - Try to open a non-existent device and/or channel
      - Try to use an invalid setting, which is ok by value, but not ok for the interface
      - The device or other resources are already used
    """


class CanOperationError(CanError):
    """Indicates an error while in operation.

    Example scenarios:
      - A call to a library function results in an unexpected return value
      - An invalid message was received
      - The driver rejected a message that was meant to be sent
      - Cyclic redundancy check (CRC) failed
      - A message remained unacknowledged
      - A buffer is full
    """


class CanTimeoutError(CanError, TimeoutError):
    """Indicates the timeout of an operation.

    Example scenarios:
      - Some message could not be sent after the timeout elapsed
      - No message was read within the given time
    """


@contextmanager
def error_check(
    error_message: Optional[str] = None,
    exception_type: type[CanError] = CanOperationError,
) -> Generator[None, None, None]:
    """Catches any exceptions and turns them into the new type while preserving the stack trace."""
    try:
        yield
    except Exception as error:  # pylint: disable=broad-except
        if error_message is None:
            raise exception_type(str(error)) from error
        else:
            raise exception_type(error_message) from error
