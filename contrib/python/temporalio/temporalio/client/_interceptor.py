"""Client support for accessing Temporal."""

from __future__ import annotations

from collections.abc import (
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
)

import temporalio.api.common.v1
import temporalio.api.workflowservice.v1
import temporalio.common
from temporalio.converter import (
    DataConverter,
)

if TYPE_CHECKING:
    from ._activity import (
        ActivityExecutionAsyncIterator,
        ActivityExecutionCount,
        ActivityExecutionDescription,
        ActivityHandle,
        AsyncActivityIDReference,
    )
    from ._nexus import (
        NexusOperationExecutionAsyncIterator,
        NexusOperationExecutionCount,
        NexusOperationExecutionDescription,
        NexusOperationHandle,
    )
    from ._schedule import (
        Schedule,
        ScheduleAsyncIterator,
        ScheduleBackfill,
        ScheduleDescription,
        ScheduleHandle,
        ScheduleOverlapPolicy,
        ScheduleUpdate,
        ScheduleUpdateInput,
    )
    from ._worker_versioning import (
        BuildIdOp,
        TaskReachabilityType,
        WorkerBuildIdVersionSets,
        WorkerTaskReachability,
    )
    from ._workflow import (
        WorkflowExecutionAsyncIterator,
        WorkflowExecutionCount,
        WorkflowExecutionDescription,
        WorkflowHandle,
        WorkflowHistoryEventAsyncIterator,
        WorkflowHistoryEventFilterType,
        WorkflowUpdateHandle,
        WorkflowUpdateStage,
    )


@dataclass
class StartWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.start_workflow`."""

    workflow: str
    args: Sequence[Any]
    id: str
    task_queue: str
    execution_timeout: timedelta | None
    run_timeout: timedelta | None
    task_timeout: timedelta | None
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy
    retry_policy: temporalio.common.RetryPolicy | None
    cron_schedule: str
    memo: Mapping[str, Any] | None
    search_attributes: None | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    )
    start_delay: timedelta | None
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    start_signal: str | None
    start_signal_args: Sequence[Any]
    static_summary: str | None
    static_details: str | None
    # Type may be absent
    ret_type: type | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    request_eager_start: bool
    priority: temporalio.common.Priority
    versioning_override: temporalio.common.VersioningOverride | None = None


@dataclass
class CancelWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.cancel_workflow`."""

    id: str
    run_id: str | None
    first_execution_run_id: str | None
    reason: str
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class DescribeWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.describe_workflow`."""

    id: str
    run_id: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class FetchWorkflowHistoryEventsInput:
    """Input for :py:meth:`OutboundInterceptor.fetch_workflow_history_events`."""

    id: str
    run_id: str | None
    page_size: int | None
    next_page_token: bytes | None
    wait_new_event: bool
    event_filter_type: WorkflowHistoryEventFilterType
    skip_archival: bool
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class ListWorkflowsInput:
    """Input for :py:meth:`OutboundInterceptor.list_workflows`."""

    query: str | None
    page_size: int
    next_page_token: bytes | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    limit: int | None


@dataclass
class CountWorkflowsInput:
    """Input for :py:meth:`OutboundInterceptor.count_workflows`."""

    query: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class QueryWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.query_workflow`."""

    id: str
    run_id: str | None
    query: str
    args: Sequence[Any]
    reject_condition: temporalio.common.QueryRejectCondition | None
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    # Type may be absent
    ret_type: type | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class SignalWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.signal_workflow`."""

    id: str
    run_id: str | None
    signal: str
    args: Sequence[Any]
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class TerminateWorkflowInput:
    """Input for :py:meth:`OutboundInterceptor.terminate_workflow`."""

    id: str
    run_id: str | None
    first_execution_run_id: str | None
    args: Sequence[Any]
    reason: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class StartActivityInput:
    """Input for :py:meth:`OutboundInterceptor.start_activity`.

    .. warning::
       This API is experimental.
    """

    activity_type: str
    args: Sequence[Any]
    id: str
    task_queue: str
    result_type: type | None
    schedule_to_close_timeout: timedelta | None
    start_to_close_timeout: timedelta | None
    schedule_to_start_timeout: timedelta | None
    heartbeat_timeout: timedelta | None
    id_reuse_policy: temporalio.common.ActivityIDReusePolicy
    id_conflict_policy: temporalio.common.ActivityIDConflictPolicy
    retry_policy: temporalio.common.RetryPolicy | None
    priority: temporalio.common.Priority
    search_attributes: temporalio.common.TypedSearchAttributes | None
    summary: str | None
    start_delay: timedelta | None
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class CancelActivityInput:
    """Input for :py:meth:`OutboundInterceptor.cancel_activity`.

    .. warning::
       This API is experimental.
    """

    activity_id: str
    activity_run_id: str | None
    reason: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class TerminateActivityInput:
    """Input for :py:meth:`OutboundInterceptor.terminate_activity`.

    .. warning::
       This API is experimental.
    """

    activity_id: str
    activity_run_id: str | None
    reason: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class DescribeActivityInput:
    """Input for :py:meth:`OutboundInterceptor.describe_activity`.

    .. warning::
       This API is experimental.
    """

    activity_id: str
    activity_run_id: str | None
    long_poll_token: bytes | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class ListActivitiesInput:
    """Input for :py:meth:`OutboundInterceptor.list_activities`.

    .. warning::
       This API is experimental.
    """

    query: str | None
    page_size: int
    next_page_token: bytes | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    limit: int | None


@dataclass
class CountActivitiesInput:
    """Input for :py:meth:`OutboundInterceptor.count_activities`.

    .. warning::
       This API is experimental.
    """

    query: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class StartWorkflowUpdateInput:
    """Input for :py:meth:`OutboundInterceptor.start_workflow_update`."""

    id: str
    run_id: str | None
    first_execution_run_id: str | None
    update_id: str | None
    update: str
    args: Sequence[Any]
    wait_for_stage: WorkflowUpdateStage
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    ret_type: type | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class UpdateWithStartUpdateWorkflowInput:
    """Update input for :py:meth:`OutboundInterceptor.start_update_with_start_workflow`."""

    update_id: str | None
    update: str
    args: Sequence[Any]
    wait_for_stage: WorkflowUpdateStage
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    ret_type: type | None


@dataclass
class UpdateWithStartStartWorkflowInput:
    """StartWorkflow input for :py:meth:`OutboundInterceptor.start_update_with_start_workflow`."""

    # Similar to StartWorkflowInput but without e.g. run_id, start_signal,
    # start_signal_args, request_eager_start.

    workflow: str
    args: Sequence[Any]
    id: str
    task_queue: str
    execution_timeout: timedelta | None
    run_timeout: timedelta | None
    task_timeout: timedelta | None
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy
    retry_policy: temporalio.common.RetryPolicy | None
    cron_schedule: str
    memo: Mapping[str, Any] | None
    search_attributes: None | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    )
    start_delay: timedelta | None
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    static_summary: str | None
    static_details: str | None
    # Type may be absent
    ret_type: type | None
    priority: temporalio.common.Priority
    versioning_override: temporalio.common.VersioningOverride | None = None


@dataclass
class StartWorkflowUpdateWithStartInput:
    """Input for :py:meth:`OutboundInterceptor.start_update_with_start_workflow`.

    The ``rpc_metadata`` and ``rpc_timeout`` fields are authoritative for the
    ``execute_multi_operation`` gRPC call.  Interceptors that wish to set RPC
    metadata should modify :py:attr:`rpc_metadata` on this object.
    """

    start_workflow_input: UpdateWithStartStartWorkflowInput
    update_workflow_input: UpdateWithStartUpdateWorkflowInput
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    _on_start: Callable[
        [temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse], None
    ]
    _on_start_error: Callable[[BaseException], None]


@dataclass
class HeartbeatAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.heartbeat_async_activity`."""

    id_or_token: AsyncActivityIDReference | bytes
    details: Sequence[Any]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    data_converter_override: DataConverter | None = None


@dataclass
class CompleteAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.complete_async_activity`."""

    id_or_token: AsyncActivityIDReference | bytes
    result: Any | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    data_converter_override: DataConverter | None = None


@dataclass
class FailAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.fail_async_activity`."""

    id_or_token: AsyncActivityIDReference | bytes
    error: Exception
    last_heartbeat_details: Sequence[Any]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    data_converter_override: DataConverter | None = None


@dataclass
class ReportCancellationAsyncActivityInput:
    """Input for :py:meth:`OutboundInterceptor.report_cancellation_async_activity`."""

    id_or_token: AsyncActivityIDReference | bytes
    details: Sequence[Any]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    data_converter_override: DataConverter | None = None


@dataclass
class CreateScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.create_schedule`."""

    id: str
    schedule: Schedule
    trigger_immediately: bool
    backfill: Sequence[ScheduleBackfill]
    memo: Mapping[str, Any] | None
    search_attributes: None | (
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    )
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class ListSchedulesInput:
    """Input for :py:meth:`OutboundInterceptor.list_schedules`."""

    page_size: int
    next_page_token: bytes | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    query: str | None = None


@dataclass
class BackfillScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.backfill_schedule`."""

    id: str
    backfills: Sequence[ScheduleBackfill]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class DeleteScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.delete_schedule`."""

    id: str
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class DescribeScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.describe_schedule`."""

    id: str
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class PauseScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.pause_schedule`."""

    id: str
    note: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class TriggerScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.trigger_schedule`."""

    id: str
    overlap: ScheduleOverlapPolicy | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class UnpauseScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.unpause_schedule`."""

    id: str
    note: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class UpdateScheduleInput:
    """Input for :py:meth:`OutboundInterceptor.update_schedule`."""

    id: str
    updater: Callable[
        [ScheduleUpdateInput],
        ScheduleUpdate | None | Awaitable[ScheduleUpdate | None],
    ]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class UpdateWorkerBuildIdCompatibilityInput:
    """Input for :py:meth:`OutboundInterceptor.update_worker_build_id_compatibility`."""

    task_queue: str
    operation: BuildIdOp
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class GetWorkerBuildIdCompatibilityInput:
    """Input for :py:meth:`OutboundInterceptor.get_worker_build_id_compatibility`."""

    task_queue: str
    max_sets: int | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class GetWorkerTaskReachabilityInput:
    """Input for :py:meth:`OutboundInterceptor.get_worker_task_reachability`."""

    build_ids: Sequence[str]
    task_queues: Sequence[str]
    reachability: TaskReachabilityType | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class StartNexusOperationInput:
    """Input for :py:meth:`OutboundInterceptor.start_nexus_operation`.

    .. warning::
       This API is experimental and unstable.
    """

    operation: str
    arg: Any
    id: str
    endpoint: str
    service: str
    result_type: type | None
    schedule_to_close_timeout: timedelta | None
    schedule_to_start_timeout: timedelta | None
    start_to_close_timeout: timedelta | None
    id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy
    id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy
    search_attributes: temporalio.common.TypedSearchAttributes | None
    summary: str | None
    headers: Mapping[str, str]
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class DescribeNexusOperationInput:
    """Input for :py:meth:`OutboundInterceptor.describe_nexus_operation`.

    .. warning::
       This API is experimental and unstable.
    """

    operation_id: str
    run_id: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class GetNexusOperationResultInput:
    """Input for :py:meth:`OutboundInterceptor.get_nexus_operation_result`.

    .. warning::
        This API is experimental and unstable.
    """

    operation_id: str
    run_id: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    result_type: type[Any] | None


@dataclass
class CancelNexusOperationInput:
    """Input for :py:meth:`OutboundInterceptor.cancel_nexus_operation`.

    .. warning::
       This API is experimental and unstable.
    """

    operation_id: str
    run_id: str | None
    reason: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class TerminateNexusOperationInput:
    """Input for :py:meth:`OutboundInterceptor.terminate_nexus_operation`.

    .. warning::
       This API is experimental and unstable.
    """

    operation_id: str
    run_id: str | None
    reason: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class ListNexusOperationsInput:
    """Input for :py:meth:`OutboundInterceptor.list_nexus_operations`.

    .. warning::
       This API is experimental and unstable.
    """

    query: str | None
    page_size: int
    next_page_token: bytes | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None
    limit: int | None


@dataclass
class CountNexusOperationsInput:
    """Input for :py:meth:`OutboundInterceptor.count_nexus_operations`.

    .. warning::
       This API is experimental and unstable.
    """

    query: str | None
    rpc_metadata: Mapping[str, str | bytes]
    rpc_timeout: timedelta | None


@dataclass
class Interceptor:
    """Interceptor for clients.

    This should be extended by any client interceptors.
    """

    def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
        """Method called for intercepting a client.

        Args:
            next: The underlying outbound interceptor this interceptor should
                delegate to.

        Returns:
            The new interceptor that will be called for each client call.
        """
        return next


class OutboundInterceptor:
    """OutboundInterceptor for intercepting client calls.

    This should be extended by any client outbound interceptors.
    """

    def __init__(self, next: OutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    ### Workflow calls

    async def start_workflow(
        self, input: StartWorkflowInput
    ) -> WorkflowHandle[Any, Any]:
        """Called for every :py:meth:`Client.start_workflow` call."""
        return await self.next.start_workflow(input)

    async def cancel_workflow(self, input: CancelWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.cancel` call."""
        await self.next.cancel_workflow(input)

    async def describe_workflow(
        self, input: DescribeWorkflowInput
    ) -> WorkflowExecutionDescription:
        """Called for every :py:meth:`WorkflowHandle.describe` call."""
        return await self.next.describe_workflow(input)

    def fetch_workflow_history_events(
        self, input: FetchWorkflowHistoryEventsInput
    ) -> WorkflowHistoryEventAsyncIterator:
        """Called for every :py:meth:`WorkflowHandle.fetch_history_events` call."""
        return self.next.fetch_workflow_history_events(input)

    def list_workflows(
        self, input: ListWorkflowsInput
    ) -> WorkflowExecutionAsyncIterator:
        """Called for every :py:meth:`Client.list_workflows` call."""
        return self.next.list_workflows(input)

    async def count_workflows(
        self, input: CountWorkflowsInput
    ) -> WorkflowExecutionCount:
        """Called for every :py:meth:`Client.count_workflows` call."""
        return await self.next.count_workflows(input)

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:
        """Called for every :py:meth:`WorkflowHandle.query` call."""
        return await self.next.query_workflow(input)

    async def signal_workflow(self, input: SignalWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.signal` call."""
        await self.next.signal_workflow(input)

    async def terminate_workflow(self, input: TerminateWorkflowInput) -> None:
        """Called for every :py:meth:`WorkflowHandle.terminate` call."""
        await self.next.terminate_workflow(input)

    ### Activity calls

    async def start_activity(self, input: StartActivityInput) -> ActivityHandle[Any]:
        """Called for every :py:meth:`Client.start_activity` call.

        .. warning::
           This API is experimental.
        """
        return await self.next.start_activity(input)

    async def cancel_activity(self, input: CancelActivityInput) -> None:
        """Called for every :py:meth:`ActivityHandle.cancel` call.

        .. warning::
           This API is experimental.
        """
        await self.next.cancel_activity(input)

    async def terminate_activity(self, input: TerminateActivityInput) -> None:
        """Called for every :py:meth:`ActivityHandle.terminate` call.

        .. warning::
           This API is experimental.
        """
        await self.next.terminate_activity(input)

    async def describe_activity(
        self, input: DescribeActivityInput
    ) -> ActivityExecutionDescription:
        """Called for every :py:meth:`ActivityHandle.describe` call.

        .. warning::
           This API is experimental.
        """
        return await self.next.describe_activity(input)

    def list_activities(
        self, input: ListActivitiesInput
    ) -> ActivityExecutionAsyncIterator:
        """Called for every :py:meth:`Client.list_activities` call.

        .. warning::
           This API is experimental.
        """
        return self.next.list_activities(input)

    async def count_activities(
        self, input: CountActivitiesInput
    ) -> ActivityExecutionCount:
        """Called for every :py:meth:`Client.count_activities` call.

        .. warning::
           This API is experimental.
        """
        return await self.next.count_activities(input)

    async def start_workflow_update(
        self, input: StartWorkflowUpdateInput
    ) -> WorkflowUpdateHandle[Any]:
        """Called for every :py:meth:`WorkflowHandle.start_update` and :py:meth:`WorkflowHandle.execute_update` call."""
        return await self.next.start_workflow_update(input)

    async def start_update_with_start_workflow(
        self, input: StartWorkflowUpdateWithStartInput
    ) -> WorkflowUpdateHandle[Any]:
        """Called for every :py:meth:`Client.start_update_with_start_workflow` and :py:meth:`Client.execute_update_with_start_workflow` call."""
        return await self.next.start_update_with_start_workflow(input)

    ### Async activity calls

    async def heartbeat_async_activity(
        self, input: HeartbeatAsyncActivityInput
    ) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.heartbeat` call."""
        await self.next.heartbeat_async_activity(input)

    async def complete_async_activity(self, input: CompleteAsyncActivityInput) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.complete` call."""
        await self.next.complete_async_activity(input)

    async def fail_async_activity(self, input: FailAsyncActivityInput) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.fail` call."""
        await self.next.fail_async_activity(input)

    async def report_cancellation_async_activity(
        self, input: ReportCancellationAsyncActivityInput
    ) -> None:
        """Called for every :py:meth:`AsyncActivityHandle.report_cancellation` call."""
        await self.next.report_cancellation_async_activity(input)

    ### Schedule calls

    async def create_schedule(self, input: CreateScheduleInput) -> ScheduleHandle:
        """Called for every :py:meth:`Client.create_schedule` call."""
        return await self.next.create_schedule(input)

    def list_schedules(self, input: ListSchedulesInput) -> ScheduleAsyncIterator:
        """Called for every :py:meth:`Client.list_schedules` call."""
        return self.next.list_schedules(input)

    async def backfill_schedule(self, input: BackfillScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.backfill` call."""
        await self.next.backfill_schedule(input)

    async def delete_schedule(self, input: DeleteScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.delete` call."""
        await self.next.delete_schedule(input)

    async def describe_schedule(
        self, input: DescribeScheduleInput
    ) -> ScheduleDescription:
        """Called for every :py:meth:`ScheduleHandle.describe` call."""
        return await self.next.describe_schedule(input)

    async def pause_schedule(self, input: PauseScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.pause` call."""
        await self.next.pause_schedule(input)

    async def trigger_schedule(self, input: TriggerScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.trigger` call."""
        await self.next.trigger_schedule(input)

    async def unpause_schedule(self, input: UnpauseScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.unpause` call."""
        await self.next.unpause_schedule(input)

    async def update_schedule(self, input: UpdateScheduleInput) -> None:
        """Called for every :py:meth:`ScheduleHandle.update` call."""
        await self.next.update_schedule(input)

    async def update_worker_build_id_compatibility(
        self, input: UpdateWorkerBuildIdCompatibilityInput
    ) -> None:
        """Called for every :py:meth:`Client.update_worker_build_id_compatibility` call."""
        await self.next.update_worker_build_id_compatibility(input)

    async def get_worker_build_id_compatibility(
        self, input: GetWorkerBuildIdCompatibilityInput
    ) -> WorkerBuildIdVersionSets:
        """Called for every :py:meth:`Client.get_worker_build_id_compatibility` call."""
        return await self.next.get_worker_build_id_compatibility(input)

    async def get_worker_task_reachability(
        self, input: GetWorkerTaskReachabilityInput
    ) -> WorkerTaskReachability:
        """Called for every :py:meth:`Client.get_worker_task_reachability` call."""
        return await self.next.get_worker_task_reachability(input)

    ### Nexus operation calls

    async def start_nexus_operation(
        self, input: StartNexusOperationInput
    ) -> NexusOperationHandle[Any]:
        """Called for every :py:meth:`NexusClient.start_operation` call.

        .. warning::
           This API is experimental and unstable.
        """
        return await self.next.start_nexus_operation(input)

    async def describe_nexus_operation(
        self, input: DescribeNexusOperationInput
    ) -> NexusOperationExecutionDescription:
        """Called for every :py:meth:`NexusOperationHandle.describe` call.

        .. warning::
           This API is experimental and unstable.
        """
        return await self.next.describe_nexus_operation(input)

    async def get_nexus_operation_result(
        self, input: GetNexusOperationResultInput
    ) -> Any:
        """Called for every :py:meth:`NexusOperationHandle.result` call.

        .. warning::
           This API is experimental and unstable.
        """
        return await self.next.get_nexus_operation_result(input)

    async def cancel_nexus_operation(self, input: CancelNexusOperationInput) -> None:
        """Called for every :py:meth:`NexusOperationHandle.cancel` call.

        .. warning::
           This API is experimental and unstable.
        """
        await self.next.cancel_nexus_operation(input)

    async def terminate_nexus_operation(
        self, input: TerminateNexusOperationInput
    ) -> None:
        """Called for every :py:meth:`NexusOperationHandle.terminate` call.

        .. warning::
           This API is experimental and unstable.
        """
        await self.next.terminate_nexus_operation(input)

    def list_nexus_operations(
        self, input: ListNexusOperationsInput
    ) -> NexusOperationExecutionAsyncIterator:
        """Called for every :py:meth:`Client.list_nexus_operations` call.

        .. warning::
           This API is experimental and unstable.
        """
        return self.next.list_nexus_operations(input)

    async def count_nexus_operations(
        self, input: CountNexusOperationsInput
    ) -> NexusOperationExecutionCount:
        """Called for every :py:meth:`Client.count_nexus_operations` call.

        .. warning::
           This API is experimental and unstable.
        """
        return await self.next.count_nexus_operations(input)
