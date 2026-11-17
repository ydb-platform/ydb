from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from enum import IntEnum
from typing import Any, Concatenate, Generic, NoReturn, TypedDict, overload

import temporalio.bridge.proto.child_workflow
import temporalio.common

from ..types import (
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MethodSyncOrAsyncNoParam,
    MethodSyncOrAsyncSingleParam,
    MultiParamSpec,
    ParamType,
    ReturnType,
    SelfType,
)
from ._activities import _AsyncioTask
from ._context import _Runtime, uuid4
from ._exceptions import ContinueAsNewVersioningBehavior, VersioningIntent

__all__ = [
    "ChildWorkflowCancellationType",
    "ChildWorkflowConfig",
    "ChildWorkflowHandle",
    "ContinueAsNewError",
    "ExternalWorkflowHandle",
    "ParentClosePolicy",
    "all_handlers_finished",
    "continue_as_new",
    "execute_child_workflow",
    "get_dynamic_query_handler",
    "get_dynamic_signal_handler",
    "get_dynamic_update_handler",
    "get_external_workflow_handle",
    "get_external_workflow_handle_for",
    "get_query_handler",
    "get_signal_handler",
    "get_update_handler",
    "set_dynamic_query_handler",
    "set_dynamic_signal_handler",
    "set_dynamic_update_handler",
    "set_query_handler",
    "set_signal_handler",
    "set_update_handler",
    "start_child_workflow",
]


class ChildWorkflowHandle(_AsyncioTask[ReturnType], Generic[SelfType, ReturnType]):  # type: ignore[type-var]
    """Handle for interacting with a child workflow.

    This is created via :py:func:`start_child_workflow`.

    This extends :py:class:`asyncio.Task` and supports all task features.
    """

    @property
    def id(self) -> str:
        """ID for the workflow."""
        raise NotImplementedError

    @property
    def first_execution_run_id(self) -> str | None:
        """Run ID for the workflow."""
        raise NotImplementedError

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncNoParam[SelfType, None],
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncSingleParam[SelfType, ParamType, None],
        arg: ParamType,
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[None] | None],
        *,
        args: Sequence[Any],
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None: ...

    async def signal(
        self,
        signal: str | Callable,  # type: ignore[reportUnusedParameter]
        arg: Any = temporalio.common._arg_unset,  # type: ignore[reportUnusedParameter]
        *,
        args: Sequence[Any] = [],  # type: ignore[reportUnusedParameter]
    ) -> None:
        """Signal this child workflow.

        Args:
            signal: Name or method reference for the signal.
            arg: Single argument to the signal.
            args: Multiple arguments to the signal. Cannot be set if arg is.

        """
        raise NotImplementedError


class ChildWorkflowCancellationType(IntEnum):
    """How a child workflow cancellation should be handled."""

    ABANDON = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.ABANDON
    )
    TRY_CANCEL = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.TRY_CANCEL
    )
    WAIT_CANCELLATION_COMPLETED = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED
    )
    WAIT_CANCELLATION_REQUESTED = int(
        temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.WAIT_CANCELLATION_REQUESTED
    )


class ParentClosePolicy(IntEnum):
    """How a child workflow should be handled when the parent closes."""

    UNSPECIFIED = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_UNSPECIFIED
    )
    TERMINATE = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE
    )
    ABANDON = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON
    )
    REQUEST_CANCEL = int(
        temporalio.bridge.proto.child_workflow.ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL
    )


class ChildWorkflowConfig(TypedDict, total=False):
    """TypedDict of config that can be used for :py:func:`start_child_workflow`
    and :py:func:`execute_child_workflow`.
    """

    id: str | None
    task_queue: str | None
    cancellation_type: ChildWorkflowCancellationType
    parent_close_policy: ParentClosePolicy
    execution_timeout: timedelta | None
    run_timeout: timedelta | None
    task_timeout: timedelta | None
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    retry_policy: temporalio.common.RetryPolicy | None
    cron_schedule: str
    memo: Mapping[str, Any] | None
    search_attributes: None | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    )
    versioning_intent: VersioningIntent | None
    static_summary: str | None
    static_details: str | None
    priority: temporalio.common.Priority


# Overload for no-param workflow
@overload
async def start_child_workflow(
    workflow: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[SelfType, ReturnType]: ...


# Overload for single-param workflow
@overload
async def start_child_workflow(
    workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[SelfType, ReturnType]: ...


# Overload for multi-param workflow
@overload
async def start_child_workflow(
    workflow: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[SelfType, ReturnType]: ...


# Overload for string-name workflow
@overload
async def start_child_workflow(
    workflow: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str | None = None,
    task_queue: str | None = None,
    result_type: type | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[Any, Any]: ...


async def start_child_workflow(
    workflow: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str | None = None,
    task_queue: str | None = None,
    result_type: type | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ChildWorkflowHandle[Any, Any]:
    """Start a child workflow and return its handle.

    Args:
        workflow: String name or class method decorated with ``@workflow.run``
            for the workflow to start.
        arg: Single argument to the child workflow.
        args: Multiple arguments to the child workflow. Cannot be set if arg is.
        id: Optional unique identifier for the workflow execution. If not set,
            defaults to :py:func:`uuid4`.
        task_queue: Task queue to run the workflow on. Defaults to the current
            workflow's task queue.
        result_type: For string workflows, this can set the specific result type
            hint to deserialize into.
        cancellation_type: How the child workflow will react to cancellation.
        parent_close_policy: How to handle the child workflow when the parent
            workflow closes.
        execution_timeout: Total workflow execution timeout including
            retries and continue as new.
        run_timeout: Timeout of a single workflow run.
        task_timeout: Timeout of a single workflow task.
        id_reuse_policy: How already-existing IDs are treated.
        retry_policy: Retry policy for the workflow.
        cron_schedule: See https://docs.temporal.io/docs/content/what-is-a-temporal-cron-job/
        memo: Memo for the workflow.
        search_attributes: Search attributes for the workflow. The dictionary
            form of this is DEPRECATED.
        versioning_intent:  When using the Worker Versioning feature, specifies whether this Child
            Workflow should run on a worker with a compatible Build Id or not.
            Deprecated: Use Worker Deployment versioning instead.
        static_summary: A single-line fixed summary for this child workflow execution that may appear
            in the UI/CLI. This can be in single-line Temporal markdown format.
        static_details: General fixed details for this child workflow execution that may appear in
            UI/CLI. This can be in Temporal markdown format and can span multiple lines. This is
            a fixed value on the workflow that cannot be updated. For details that can be
            updated, use :py:meth:`get_current_details` within the workflow.
        priority: Priority to use for this workflow.

    Returns:
        A workflow handle to the started/existing workflow.
    """
    temporalio.common._warn_on_deprecated_search_attributes(search_attributes)
    return await _Runtime.current().workflow_start_child_workflow(
        workflow,
        *temporalio.common._arg_or_args(arg, args),
        id=id or str(uuid4()),
        task_queue=task_queue,
        result_type=result_type,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
    )


# Overload for no-param workflow
@overload
async def execute_child_workflow(
    workflow: MethodAsyncNoParam[SelfType, ReturnType],
    *,
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for single-param workflow
@overload
async def execute_child_workflow(
    workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
    arg: ParamType,
    *,
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for multi-param workflow
@overload
async def execute_child_workflow(
    workflow: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]],
    *,
    args: Sequence[Any],
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ReturnType: ...


# Overload for string-name workflow
@overload
async def execute_child_workflow(
    workflow: str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str | None = None,
    task_queue: str | None = None,
    result_type: type | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any: ...


async def execute_child_workflow(
    workflow: Any,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str | None = None,
    task_queue: str | None = None,
    result_type: type | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start a child workflow and wait for completion.

    This is a shortcut for ``await (await`` :py:meth:`start_child_workflow` ``)``.
    """
    temporalio.common._warn_on_deprecated_search_attributes(search_attributes)
    # We call the runtime directly instead of top-level start_child_workflow to
    # ensure we don't miss new parameters
    handle = await _Runtime.current().workflow_start_child_workflow(
        workflow,
        *temporalio.common._arg_or_args(arg, args),
        id=id or str(uuid4()),
        task_queue=task_queue,
        result_type=result_type,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
    )
    return await handle


class ExternalWorkflowHandle(Generic[SelfType]):
    """Handle for interacting with an external workflow.

    This is created via :py:func:`get_external_workflow_handle` or
    :py:func:`get_external_workflow_handle_for`.
    """

    @property
    def id(self) -> str:
        """ID for the workflow."""
        raise NotImplementedError

    @property
    def run_id(self) -> str | None:
        """Run ID for the workflow if any."""
        raise NotImplementedError

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncNoParam[SelfType, None],
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncSingleParam[SelfType, ParamType, None],
        arg: ParamType,
    ) -> None: ...

    @overload
    async def signal(
        self,
        signal: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None: ...

    async def signal(
        self,
        signal: str | Callable,  # type: ignore[reportUnusedParameter]
        arg: Any = temporalio.common._arg_unset,  # type: ignore[reportUnusedParameter]
        *,
        args: Sequence[Any] = [],  # type: ignore[reportUnusedParameter]
    ) -> None:
        """Signal this external workflow.

        Args:
            signal: Name or method reference for the signal.
            arg: Single argument to the signal.
            args: Multiple arguments to the signal. Cannot be set if arg is.

        """
        raise NotImplementedError

    async def cancel(self, *, reason: str = "") -> None:  # pyright: ignore[reportUnusedParameter]
        """Send a cancellation request to this external workflow.

        This will fail if the workflow cannot accept the request (e.g. if the
        workflow is not found).

        Args:
            reason: Reason recorded with the cancellation request. Available in
                the target workflow via :py:func:`cancellation_reason`.
        """
        raise NotImplementedError


def get_external_workflow_handle(
    workflow_id: str,
    *,
    run_id: str | None = None,
) -> ExternalWorkflowHandle[Any]:
    """Get a workflow handle to an existing workflow by its ID.

    Args:
        workflow_id: Workflow ID to get a handle to.
        run_id: Optional run ID for the workflow.

    Returns:
        The external workflow handle.
    """
    return _Runtime.current().workflow_get_external_workflow_handle(
        workflow_id, run_id=run_id
    )


def get_external_workflow_handle_for(
    workflow: MethodAsyncNoParam[SelfType, Any]  # type: ignore[reportUnusedParameter]
    | MethodAsyncSingleParam[SelfType, Any, Any],
    workflow_id: str,
    *,
    run_id: str | None = None,
) -> ExternalWorkflowHandle[SelfType]:
    """Get a typed workflow handle to an existing workflow by its ID.

    This is the same as :py:func:`get_external_workflow_handle` but typed. Note,
    the workflow type given is not validated, it is only for typing.

    Args:
        workflow: The workflow run method to use for typing the handle.
        workflow_id: Workflow ID to get a handle to.
        run_id: Optional run ID for the workflow.

    Returns:
        The external workflow handle.
    """
    return get_external_workflow_handle(workflow_id, run_id=run_id)


class ContinueAsNewError(BaseException):
    """Error thrown by :py:func:`continue_as_new`.

    This should not be caught, but instead be allowed to throw out of the
    workflow which then triggers the continue as new. This should never be
    instantiated directly.
    """

    def __init__(self, *args: object) -> None:
        """Direct instantiation is disabled. Use :py:func:`continue_as_new`."""
        if type(self) is ContinueAsNewError:
            raise RuntimeError("Cannot instantiate ContinueAsNewError directly")
        super().__init__(*args)


# Overload for self (unfortunately, cannot type args)
@overload
def continue_as_new(
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    backoff_start_interval: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    initial_versioning_behavior: ContinueAsNewVersioningBehavior | None = None,
) -> NoReturn: ...


# Overload for no-param workflow
@overload
def continue_as_new(
    *,
    workflow: MethodAsyncNoParam[SelfType, Any],
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    backoff_start_interval: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    initial_versioning_behavior: ContinueAsNewVersioningBehavior | None = None,
) -> NoReturn: ...


# Overload for single-param workflow
@overload
def continue_as_new(
    arg: ParamType,
    *,
    workflow: MethodAsyncSingleParam[SelfType, ParamType, Any],
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    backoff_start_interval: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    initial_versioning_behavior: ContinueAsNewVersioningBehavior | None = None,
) -> NoReturn: ...


# Overload for multi-param workflow
@overload
def continue_as_new(
    *,
    workflow: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[Any]],
    args: Sequence[Any],
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    backoff_start_interval: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    initial_versioning_behavior: ContinueAsNewVersioningBehavior | None = None,
) -> NoReturn: ...


# Overload for string-name workflow
@overload
def continue_as_new(
    *,
    workflow: str,
    args: Sequence[Any] = [],
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    backoff_start_interval: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    initial_versioning_behavior: ContinueAsNewVersioningBehavior | None = None,
) -> NoReturn: ...


def continue_as_new(
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    workflow: None | Callable | str = None,
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    backoff_start_interval: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    memo: Mapping[str, Any] | None = None,
    search_attributes: None
    | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ) = None,
    versioning_intent: VersioningIntent | None = None,
    initial_versioning_behavior: ContinueAsNewVersioningBehavior | None = None,
) -> NoReturn:
    """Stop the workflow immediately and continue as new.

    Args:
        arg: Single argument to the continued workflow.
        args: Multiple arguments to the continued workflow. Cannot be set if arg
            is.
        workflow: Specific workflow to continue to. Defaults to the current
            workflow.
        task_queue: Task queue to run the workflow on. Defaults to the current
            workflow's task queue.
        run_timeout: Timeout of a single workflow run. Defaults to the current
            workflow's run timeout.
        task_timeout: Timeout of a single workflow task. Defaults to the current
            workflow's task timeout.
        backoff_start_interval: Delay before the first workflow task of the
            continued run is scheduled.
        memo: Memo for the workflow. Defaults to the current workflow's memo.
        search_attributes: Search attributes for the workflow. Defaults to the
            current workflow's search attributes. The dictionary form of this is
            DEPRECATED.
        versioning_intent: When using the Worker Versioning feature, specifies whether this Workflow
            should Continue-as-New onto a worker with a compatible Build Id or not.
            Deprecated: Use Worker Deployment versioning instead.

    Returns:
        Never returns, always raises a :py:class:`ContinueAsNewError`.

    Raises:
        ContinueAsNewError: Always raised by this function. Should not be caught
            but instead be allowed to
    """
    temporalio.common._warn_on_deprecated_search_attributes(search_attributes)
    _Runtime.current().workflow_continue_as_new(
        *temporalio.common._arg_or_args(arg, args),
        workflow=workflow,
        task_queue=task_queue,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        backoff_start_interval=backoff_start_interval,
        retry_policy=retry_policy,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        initial_versioning_behavior=initial_versioning_behavior,
    )


def get_signal_handler(name: str) -> Callable | None:
    """Get the signal handler for the given name if any.

    This includes handlers created via the ``@workflow.signal`` decorator.

    Args:
        name: Name of the signal.

    Returns:
        Callable for the signal if any. If a handler is not found for the name,
        this will not return the dynamic handler even if there is one.
    """
    return _Runtime.current().workflow_get_signal_handler(name)


def set_signal_handler(name: str, handler: Callable | None) -> None:
    """Set or unset the signal handler for the given name.

    This overrides any existing handlers for the given name, including handlers
    created via the ``@workflow.signal`` decorator.

    When set, all unhandled past signals for the given name are immediately sent
    to the handler.

    Args:
        name: Name of the signal.
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_signal_handler(name, handler)


def get_dynamic_signal_handler() -> Callable | None:
    """Get the dynamic signal handler if any.

    This includes dynamic handlers created via the ``@workflow.signal``
    decorator.

    Returns:
        Callable for the dynamic signal handler if any.
    """
    return _Runtime.current().workflow_get_signal_handler(None)


def set_dynamic_signal_handler(handler: Callable | None) -> None:
    """Set or unset the dynamic signal handler.

    This overrides the existing dynamic handler even if it was created via the
    ``@workflow.signal`` decorator.

    When set, all unhandled past signals are immediately sent to the handler.

    Args:
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_signal_handler(None, handler)


def get_query_handler(name: str) -> Callable | None:
    """Get the query handler for the given name if any.

    This includes handlers created via the ``@workflow.query`` decorator.

    Args:
        name: Name of the query.

    Returns:
        Callable for the query if any. If a handler is not found for the name,
        this will not return the dynamic handler even if there is one.
    """
    return _Runtime.current().workflow_get_query_handler(name)


def set_query_handler(name: str, handler: Callable | None) -> None:
    """Set or unset the query handler for the given name.

    This overrides any existing handlers for the given name, including handlers
    created via the ``@workflow.query`` decorator.

    Args:
        name: Name of the query.
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_query_handler(name, handler)


def get_dynamic_query_handler() -> Callable | None:
    """Get the dynamic query handler if any.

    This includes dynamic handlers created via the ``@workflow.query``
    decorator.

    Returns:
        Callable for the dynamic query handler if any.
    """
    return _Runtime.current().workflow_get_query_handler(None)


def set_dynamic_query_handler(handler: Callable | None) -> None:
    """Set or unset the dynamic query handler.

    This overrides the existing dynamic handler even if it was created via the
    ``@workflow.query`` decorator.

    Args:
        handler: Callable to set or None to unset.
    """
    _Runtime.current().workflow_set_query_handler(None, handler)


def get_update_handler(name: str) -> Callable | None:
    """Get the update handler for the given name if any.

    This includes handlers created via the ``@workflow.update`` decorator.

    Args:
        name: Name of the update.

    Returns:
        Callable for the update if any. If a handler is not found for the name,
        this will not return the dynamic handler even if there is one.
    """
    return _Runtime.current().workflow_get_update_handler(name)


def set_update_handler(
    name: str, handler: Callable | None, *, validator: Callable | None = None
) -> None:
    """Set or unset the update handler for the given name.

    This overrides any existing handlers for the given name, including handlers
    created via the ``@workflow.update`` decorator.

    Args:
        name: Name of the update.
        handler: Callable to set or None to unset.
        validator: Callable to set or None to unset as the update validator.
    """
    _Runtime.current().workflow_set_update_handler(name, handler, validator)


def get_dynamic_update_handler() -> Callable | None:
    """Get the dynamic update handler if any.

    This includes dynamic handlers created via the ``@workflow.update``
    decorator.

    Returns:
        Callable for the dynamic update handler if any.
    """
    return _Runtime.current().workflow_get_update_handler(None)


def set_dynamic_update_handler(
    handler: Callable | None, *, validator: Callable | None = None
) -> None:
    """Set or unset the dynamic update handler.

    This overrides the existing dynamic handler even if it was created via the
    ``@workflow.update`` decorator.

    Args:
        handler: Callable to set or None to unset.
        validator: Callable to set or None to unset as the update validator.
    """
    _Runtime.current().workflow_set_update_handler(None, handler, validator)


def all_handlers_finished() -> bool:
    """Whether update and signal handlers have finished executing.

    Consider waiting on this condition before workflow return or continue-as-new, to prevent
    interruption of in-progress handlers by workflow exit:
    ``await workflow.wait_condition(lambda: workflow.all_handlers_finished())``

    Returns:
        True if there are no in-progress update or signal handler executions.
    """
    return _Runtime.current().workflow_all_handlers_finished()
