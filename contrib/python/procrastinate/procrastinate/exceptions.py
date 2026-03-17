from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from procrastinate.retry import RetryDecision


class ProcrastinateException(Exception):
    """
    Unexpected Procrastinate error.
    """

    def __init__(self, message: str | None = None):
        if not message:
            message = self.__doc__
        super().__init__(message)


class TaskNotFound(ProcrastinateException):
    """
    Task cannot be imported.
    """


class UnboundTaskError(ProcrastinateException):
    """
    The `Task` was used before it was bound to an `App`.
    If the task was defined on a `Blueprint`, ensure that you called
    `App.add_tasks_from` before deferring the task.
    """


class TaskAlreadyRegistered(ProcrastinateException):
    """
    A task with this name was already registered.
    """


class LoadFromPathError(ProcrastinateException, ImportError):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """
    App was not found at the provided path, or the loaded object is not an App.
    """


class JobRetry(ProcrastinateException):
    """
    Job should be retried.
    """

    def __init__(self, retry_decision: RetryDecision):
        self.retry_decision = retry_decision
        super().__init__()


class JobAborted(ProcrastinateException):
    """
    Job was aborted.
    """


class AppNotOpen(ProcrastinateException):
    """
    App was not open. Procrastinate App needs to be opened using:

    - ``app.open()``,
    - ``await app.open_async()``,
    - ``with app.open():``,
    - ``async with app.open_async():``.
    """


class ConnectorException(ProcrastinateException):
    """
    Database error.
    """

    # The precise error can be seen with ``exception.__cause__``.


class AlreadyEnqueued(ProcrastinateException):
    """
    There is already a job waiting in the queue with the same queueing lock.
    """


class UniqueViolation(ConnectorException):
    """
    A unique constraint is violated. The constraint name is available in
    ``exception.constraint_name``.
    """

    def __init__(
        self, *args: Any, constraint_name: str | None, queueing_lock: str | None
    ):
        super().__init__(*args)
        self.constraint_name = constraint_name
        self.queueing_lock = queueing_lock


class NoResult(ConnectorException):
    """
    No result was returned by the database query.
    """


class MissingApp(ProcrastinateException):
    """
    Missing app. This most probably happened because procrastinate needs an
    app via --app or the PROCRASTINATE_APP environment variable.
    """


class SyncConnectorConfigurationError(ProcrastinateException):
    """
    A synchronous connector was used, but the operation
    needs an asynchronous connector. Please check your App
    configuration.
    """


class CallerModuleUnknown(ProcrastinateException):
    """
    Unable to determine the module name of the caller.
    """


class RunTaskError(ProcrastinateException):
    """One of the specified coroutines ended with an exception"""


class InvalidTimestamp(ProcrastinateException):
    """
    Periodic task launched with a timestamp kwarg not matching the
    defer_timestamp arg
    """


class FunctionPathError(ProcrastinateException):
    """Couldn't automatically generate a unique name for a function"""


class MovedElsewhere(ProcrastinateException):
    """The object has been moved elsewhere"""
