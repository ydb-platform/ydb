"""Common Temporal exceptions."""

import asyncio
from datetime import timedelta
from enum import IntEnum
from typing import Any, Optional, Sequence, Tuple

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.failure.v1


class TemporalError(Exception):
    """Base for all Temporal exceptions."""

    @property
    def cause(self) -> Optional[BaseException]:
        """Cause of the exception.

        This is the same as ``Exception.__cause__``.
        """
        return self.__cause__


class FailureError(TemporalError):
    """Base for runtime failures during workflow/activity execution."""

    def __init__(
        self,
        message: str,
        *,
        failure: Optional[temporalio.api.failure.v1.Failure] = None,
        exc_args: Optional[Tuple] = None,
    ) -> None:
        """Initialize a failure error."""
        if exc_args is None:
            exc_args = (message,)
        super().__init__(*exc_args)
        self._message = message
        self._failure = failure

    @property
    def message(self) -> str:
        """Message."""
        return self._message

    @property
    def failure(self) -> Optional[temporalio.api.failure.v1.Failure]:
        """Underlying protobuf failure object."""
        return self._failure


class WorkflowAlreadyStartedError(FailureError):
    """Thrown by a client or workflow when a workflow execution has already started.

    Attributes:
        workflow_id: ID of the already-started workflow.
        workflow_type: Workflow type name of the already-started workflow.
        run_id: Run ID of the already-started workflow if this was raised by the
            client.
    """

    def __init__(
        self, workflow_id: str, workflow_type: str, *, run_id: Optional[str] = None
    ) -> None:
        """Initialize a workflow already started error."""
        super().__init__("Workflow execution already started")
        self.workflow_id = workflow_id
        self.workflow_type = workflow_type
        self.run_id = run_id


class ApplicationErrorCategory(IntEnum):
    """Severity category for your application error. Maps to corresponding client-side logging/metrics behaviors"""

    UNSPECIFIED = int(
        temporalio.api.enums.v1.ApplicationErrorCategory.APPLICATION_ERROR_CATEGORY_UNSPECIFIED
    )

    BENIGN = int(
        temporalio.api.enums.v1.ApplicationErrorCategory.APPLICATION_ERROR_CATEGORY_BENIGN
    )
    """BENIGN category errors emit DEBUG level logs and do not record metrics"""


class ApplicationError(FailureError):
    """Error raised during workflow/activity execution."""

    def __init__(
        self,
        message: str,
        *details: Any,
        type: Optional[str] = None,
        non_retryable: bool = False,
        next_retry_delay: Optional[timedelta] = None,
        category: ApplicationErrorCategory = ApplicationErrorCategory.UNSPECIFIED,
    ) -> None:
        """Initialize an application error."""
        super().__init__(
            message,
            # If there is a type, prepend it to the message on the string repr
            exc_args=(message if not type else f"{type}: {message}",),
        )
        self._details = details
        self._type = type
        self._non_retryable = non_retryable
        self._next_retry_delay = next_retry_delay
        self._category = category

    @property
    def details(self) -> Sequence[Any]:
        """User-defined details on the error."""
        return self._details

    @property
    def type(self) -> Optional[str]:
        """General error type."""
        return self._type

    @property
    def non_retryable(self) -> bool:
        """Whether the error was set as non-retryable when created.

        Note: This is not whether the error is non-retryable via other means
        such as retry policy. This is just whether the error was marked
        non-retryable upon creation by the user.
        """
        return self._non_retryable

    @property
    def next_retry_delay(self) -> Optional[timedelta]:
        """Delay before the next activity retry attempt.

        User activity code may set this when raising ApplicationError to specify
        a delay before the next activity retry.
        """
        return self._next_retry_delay

    @property
    def category(self) -> ApplicationErrorCategory:
        """Severity category of the application error"""
        return self._category


class CancelledError(FailureError):
    """Error raised on workflow/activity cancellation."""

    def __init__(self, message: str = "Cancelled", *details: Any) -> None:
        """Initialize a cancelled error."""
        super().__init__(message)
        self._details = details

    @property
    def details(self) -> Sequence[Any]:
        """User-defined details on the error."""
        return self._details


class TerminatedError(FailureError):
    """Error raised on workflow cancellation."""

    def __init__(self, message: str, *details: Any) -> None:
        """Initialize a terminated error."""
        super().__init__(message)
        self._details = details

    @property
    def details(self) -> Sequence[Any]:
        """User-defined details on the error."""
        return self._details


class TimeoutType(IntEnum):
    """Type of timeout for :py:class:`TimeoutError`."""

    START_TO_CLOSE = int(
        temporalio.api.enums.v1.TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE
    )
    SCHEDULE_TO_START = int(
        temporalio.api.enums.v1.TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_START
    )
    SCHEDULE_TO_CLOSE = int(
        temporalio.api.enums.v1.TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
    )
    HEARTBEAT = int(temporalio.api.enums.v1.TimeoutType.TIMEOUT_TYPE_HEARTBEAT)


class TimeoutError(FailureError):
    """Error raised on workflow/activity timeout."""

    def __init__(
        self,
        message: str,
        *,
        type: Optional[TimeoutType],
        last_heartbeat_details: Sequence[Any],
    ) -> None:
        """Initialize a timeout error."""
        super().__init__(message)
        self._type = type
        self._last_heartbeat_details = last_heartbeat_details

    @property
    def type(self) -> Optional[TimeoutType]:
        """Type of timeout error."""
        return self._type

    @property
    def last_heartbeat_details(self) -> Sequence[Any]:
        """Last heartbeat details if this is for an activity heartbeat."""
        return self._last_heartbeat_details


class ServerError(FailureError):
    """Error originating in the Temporal server."""

    def __init__(self, message: str, *, non_retryable: bool = False) -> None:
        """Initialize a server error."""
        super().__init__(message)
        self._non_retryable = non_retryable

    @property
    def non_retryable(self) -> bool:
        """Whether this error is non-retryable."""
        return self._non_retryable


class RetryState(IntEnum):
    """Current retry state of the workflow/activity during error."""

    IN_PROGRESS = int(temporalio.api.enums.v1.RetryState.RETRY_STATE_IN_PROGRESS)
    NON_RETRYABLE_FAILURE = int(
        temporalio.api.enums.v1.RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE
    )
    TIMEOUT = int(temporalio.api.enums.v1.RetryState.RETRY_STATE_TIMEOUT)
    MAXIMUM_ATTEMPTS_REACHED = int(
        temporalio.api.enums.v1.RetryState.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
    )
    RETRY_POLICY_NOT_SET = int(
        temporalio.api.enums.v1.RetryState.RETRY_STATE_RETRY_POLICY_NOT_SET
    )
    INTERNAL_SERVER_ERROR = int(
        temporalio.api.enums.v1.RetryState.RETRY_STATE_INTERNAL_SERVER_ERROR
    )
    CANCEL_REQUESTED = int(
        temporalio.api.enums.v1.RetryState.RETRY_STATE_CANCEL_REQUESTED
    )


class ActivityError(FailureError):
    """Error raised on activity failure."""

    def __init__(
        self,
        message: str,
        *,
        scheduled_event_id: int,
        started_event_id: int,
        identity: str,
        activity_type: str,
        activity_id: str,
        retry_state: Optional[RetryState],
    ) -> None:
        """Initialize an activity error."""
        super().__init__(message)
        self._scheduled_event_id = scheduled_event_id
        self._started_event_id = started_event_id
        self._identity = identity
        self._activity_type = activity_type
        self._activity_id = activity_id
        self._retry_state = retry_state

    @property
    def scheduled_event_id(self) -> int:
        """Scheduled event ID for this error."""
        return self._scheduled_event_id

    @property
    def started_event_id(self) -> int:
        """Started event ID for this error."""
        return self._started_event_id

    @property
    def identity(self) -> str:
        """Identity for this error."""
        return self._identity

    @property
    def activity_type(self) -> str:
        """Activity type for this error."""
        return self._activity_type

    @property
    def activity_id(self) -> str:
        """Activity ID for this error."""
        return self._activity_id

    @property
    def retry_state(self) -> Optional[RetryState]:
        """Retry state for this error."""
        return self._retry_state


class ChildWorkflowError(FailureError):
    """Error raised on child workflow failure."""

    def __init__(
        self,
        message: str,
        *,
        namespace: str,
        workflow_id: str,
        run_id: str,
        workflow_type: str,
        initiated_event_id: int,
        started_event_id: int,
        retry_state: Optional[RetryState],
    ) -> None:
        """Initialize a child workflow error."""
        super().__init__(message)
        self._namespace = namespace
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._workflow_type = workflow_type
        self._initiated_event_id = initiated_event_id
        self._started_event_id = started_event_id
        self._retry_state = retry_state

    @property
    def namespace(self) -> str:
        """Namespace for this error."""
        return self._namespace

    @property
    def workflow_id(self) -> str:
        """Workflow ID for this error."""
        return self._workflow_id

    @property
    def run_id(self) -> str:
        """Run ID for this error."""
        return self._run_id

    @property
    def workflow_type(self) -> str:
        """Workflow type for this error."""
        return self._workflow_type

    @property
    def initiated_event_id(self) -> int:
        """Initiated event ID for this error."""
        return self._initiated_event_id

    @property
    def started_event_id(self) -> int:
        """Started event ID for this error."""
        return self._started_event_id

    @property
    def retry_state(self) -> Optional[RetryState]:
        """Retry state for this error."""
        return self._retry_state


class NexusOperationError(FailureError):
    """Error raised on Nexus operation failure inside a Workflow."""

    def __init__(
        self,
        message: str,
        *,
        scheduled_event_id: int,
        endpoint: str,
        service: str,
        operation: str,
        operation_token: str,
    ):
        """Initialize a Nexus operation error.

        Args:
            message: The error message.
            scheduled_event_id: The NexusOperationScheduled event ID for the failed operation.
            endpoint: The endpoint name for the failed operation.
            service: The service name for the failed operation.
            operation: The name of the failed operation.
            operation_token: The operation token returned by the failed operation.
        """
        super().__init__(message)
        self._scheduled_event_id = scheduled_event_id
        self._endpoint = endpoint
        self._service = service
        self._operation = operation
        self._operation_token = operation_token

    @property
    def scheduled_event_id(self) -> int:
        """The NexusOperationScheduled event ID for the failed operation."""
        return self._scheduled_event_id

    @property
    def endpoint(self) -> str:
        """The endpoint name for the failed operation."""
        return self._endpoint

    @property
    def service(self) -> str:
        """The service name for the failed operation."""
        return self._service

    @property
    def operation(self) -> str:
        """The name of the failed operation."""
        return self._operation

    @property
    def operation_token(self) -> str:
        """The operation token returned by the failed operation."""
        return self._operation_token


def is_cancelled_exception(exception: BaseException) -> bool:
    """Check whether the given exception is considered a cancellation exception
    according to Temporal.

    This is often used in a conditional of a catch clause to check whether a
    cancel occurred inside of a workflow. This can occur from
    :py:class:`asyncio.CancelledError` or :py:class:`CancelledError` or either
    :py:class:`ActivityError` or :py:class:`ChildWorkflowError` if either of
    those latter two have a :py:class:`CancelledError` cause.

    Args:
        exception: Exception to check.

    Returns:
        True if a cancelled exception, false if not.
    """
    return (
        isinstance(exception, asyncio.CancelledError)
        or isinstance(exception, CancelledError)
        or (
            (
                isinstance(exception, ActivityError)
                or isinstance(exception, ChildWorkflowError)
                or isinstance(exception, NexusOperationError)
            )
            and isinstance(exception.cause, CancelledError)
        )
    )
