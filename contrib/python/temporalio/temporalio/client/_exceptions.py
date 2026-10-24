"""Client support for accessing Temporal."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
)

import temporalio.exceptions
from temporalio.activity import ActivityCancellationDetails

if TYPE_CHECKING:
    from ._workflow import WorkflowExecutionStatus


class WorkflowFailureError(temporalio.exceptions.TemporalError):
    """Error that occurs when a workflow is unsuccessful."""

    def __init__(self, *, cause: BaseException) -> None:
        """Create workflow failure error."""
        super().__init__("Workflow execution failed")
        self.__cause__ = cause

    @property
    def cause(self) -> BaseException:
        """Cause of the workflow failure."""
        assert self.__cause__
        return self.__cause__


class WorkflowContinuedAsNewError(temporalio.exceptions.TemporalError):
    """Error that occurs when a workflow was continued as new."""

    def __init__(self, new_execution_run_id: str) -> None:
        """Create workflow continue as new error."""
        super().__init__("Workflow continued as new")
        self._new_execution_run_id = new_execution_run_id

    @property
    def new_execution_run_id(self) -> str:
        """New execution run ID the workflow continued to"""
        return self._new_execution_run_id


class WorkflowQueryRejectedError(temporalio.exceptions.TemporalError):
    """Error that occurs when a query was rejected."""

    def __init__(self, status: WorkflowExecutionStatus | None) -> None:
        """Create workflow query rejected error."""
        super().__init__(f"Query rejected, status: {status}")
        self._status = status

    @property
    def status(self) -> WorkflowExecutionStatus | None:
        """Get workflow execution status causing rejection."""
        return self._status


class WorkflowQueryFailedError(temporalio.exceptions.TemporalError):
    """Error that occurs when a query fails."""

    def __init__(self, message: str) -> None:
        """Create workflow query failed error."""
        super().__init__(message)
        self._message = message

    @property
    def message(self) -> str:
        """Get query failed message."""
        return self._message


class WorkflowUpdateFailedError(temporalio.exceptions.TemporalError):
    """Error that occurs when an update fails."""

    def __init__(self, cause: BaseException) -> None:
        """Create workflow update failed error."""
        super().__init__("Workflow update failed")
        self.__cause__ = cause

    @property
    def cause(self) -> BaseException:
        """Cause of the update failure."""
        assert self.__cause__
        return self.__cause__


class RPCTimeoutOrCancelledError(temporalio.exceptions.TemporalError):
    """Error that occurs on some client calls that timeout or get cancelled."""

    pass


class WorkflowUpdateRPCTimeoutOrCancelledError(RPCTimeoutOrCancelledError):
    """Error that occurs when update RPC call times out or is cancelled.

    Note, this is not related to any general concept of timing out or cancelling
    a running update, this is only related to the client call itself.
    """

    def __init__(self) -> None:
        """Create workflow update timeout or cancelled error."""
        super().__init__("Timeout or cancellation waiting for update")


class ActivityFailureError(temporalio.exceptions.TemporalError):
    """Error that occurs when an activity is unsuccessful.

    .. warning::
       This API is experimental.
    """

    def __init__(self, *, cause: BaseException) -> None:
        """Create activity failure error."""
        super().__init__("Activity execution failed")
        self.__cause__ = cause

    @property
    def cause(self) -> BaseException:
        """Cause of the activity failure."""
        assert self.__cause__
        return self.__cause__


class AsyncActivityCancelledError(temporalio.exceptions.TemporalError):
    """Error that occurs when async activity attempted heartbeat but was cancelled."""

    def __init__(self, details: ActivityCancellationDetails | None = None) -> None:
        """Create async activity cancelled error."""
        super().__init__("Activity cancelled")
        self.details = details


class ScheduleAlreadyRunningError(temporalio.exceptions.TemporalError):
    """Error when a schedule is already running."""

    def __init__(self) -> None:
        """Create schedule already running error."""
        super().__init__("Schedule already running")
