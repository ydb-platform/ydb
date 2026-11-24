"""Client support for accessing Temporal."""

from __future__ import annotations

import asyncio
import inspect
import uuid
import warnings
from collections.abc import (
    Callable,
    Mapping,
)
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from google.protobuf.internal.containers import MessageMap

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.errordetails.v1
import temporalio.api.failure.v1
import temporalio.api.schedule.v1
import temporalio.api.taskqueue.v1
import temporalio.api.update.v1
import temporalio.api.workflowservice.v1
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.nexus
import temporalio.nexus._operation_context
from temporalio.activity import ActivityCancellationDetails
from temporalio.converter import (
    ActivitySerializationContext,
    StorageDriverActivityInfo,
    StorageDriverStoreContext,
    StorageDriverWorkflowInfo,
    WorkflowSerializationContext,
)
from temporalio.service import (
    RPCError,
    RPCStatusCode,
)

from ..common import HeaderCodecBehavior
from ._activity import (
    ActivityExecutionAsyncIterator,
    ActivityExecutionCount,
    ActivityExecutionDescription,
    ActivityHandle,
    AsyncActivityIDReference,
)
from ._exceptions import (
    AsyncActivityCancelledError,
    ScheduleAlreadyRunningError,
    WorkflowQueryFailedError,
    WorkflowQueryRejectedError,
    WorkflowUpdateRPCTimeoutOrCancelledError,
)
from ._helpers import _apply_headers, _encode_user_metadata
from ._interceptor import (
    BackfillScheduleInput,
    CancelActivityInput,
    CancelNexusOperationInput,
    CancelWorkflowInput,
    CompleteAsyncActivityInput,
    CountActivitiesInput,
    CountNexusOperationsInput,
    CountWorkflowsInput,
    CreateScheduleInput,
    DeleteScheduleInput,
    DescribeActivityInput,
    DescribeNexusOperationInput,
    DescribeScheduleInput,
    DescribeWorkflowInput,
    FailAsyncActivityInput,
    FetchWorkflowHistoryEventsInput,
    GetNexusOperationResultInput,
    GetWorkerBuildIdCompatibilityInput,
    GetWorkerTaskReachabilityInput,
    HeartbeatAsyncActivityInput,
    ListActivitiesInput,
    ListNexusOperationsInput,
    ListSchedulesInput,
    ListWorkflowsInput,
    OutboundInterceptor,
    PauseScheduleInput,
    QueryWorkflowInput,
    ReportCancellationAsyncActivityInput,
    SignalWorkflowInput,
    StartActivityInput,
    StartNexusOperationInput,
    StartWorkflowInput,
    StartWorkflowUpdateInput,
    StartWorkflowUpdateWithStartInput,
    TerminateActivityInput,
    TerminateNexusOperationInput,
    TerminateWorkflowInput,
    TriggerScheduleInput,
    UnpauseScheduleInput,
    UpdateScheduleInput,
    UpdateWithStartStartWorkflowInput,
    UpdateWithStartUpdateWorkflowInput,
    UpdateWorkerBuildIdCompatibilityInput,
)
from ._nexus import (
    NexusOperationExecutionAsyncIterator,
    NexusOperationExecutionCount,
    NexusOperationExecutionDescription,
    NexusOperationFailureError,
    NexusOperationHandle,
)
from ._schedule import (
    ScheduleAsyncIterator,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from ._worker_versioning import WorkerBuildIdVersionSets, WorkerTaskReachability
from ._workflow import (
    WorkflowExecutionAsyncIterator,
    WorkflowExecutionCount,
    WorkflowExecutionDescription,
    WorkflowExecutionStatus,
    WorkflowHandle,
    WorkflowHistoryEventAsyncIterator,
    WorkflowUpdateHandle,
    WorkflowUpdateStage,
)

if TYPE_CHECKING:
    from ._client import Client


class _ClientImpl(OutboundInterceptor):  # pyright: ignore[reportUnusedClass]
    def __init__(self, client: Client) -> None:  # type: ignore
        # We are intentionally not calling the base class's __init__ here
        self._client = client

    ### Workflow calls

    async def start_workflow(
        self, input: StartWorkflowInput
    ) -> WorkflowHandle[Any, Any]:
        req: (
            temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest
            | temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest
        )
        if input.start_signal is not None:
            req = await self._build_signal_with_start_workflow_execution_request(input)
        else:
            req = await self._build_start_workflow_execution_request(input)

        resp: (
            temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse
            | temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse
        )
        first_execution_run_id = None
        eagerly_started = False
        try:
            if isinstance(
                req,
                temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest,
            ):
                resp = await self._client.workflow_service.signal_with_start_workflow_execution(
                    req,
                    retry=True,
                    metadata=input.rpc_metadata,
                    timeout=input.rpc_timeout,
                )
            else:
                resp = await self._client.workflow_service.start_workflow_execution(
                    req,
                    retry=True,
                    metadata=input.rpc_metadata,
                    timeout=input.rpc_timeout,
                )
                first_execution_run_id = resp.run_id
                eagerly_started = resp.HasField("eager_workflow_task")
        except RPCError as err:
            # If the status is ALREADY_EXISTS and the details can be extracted
            # as already started, use a different exception
            if err.status == RPCStatusCode.ALREADY_EXISTS and err.grpc_status.details:
                details = temporalio.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure()
                if err.grpc_status.details[0].Unpack(details):
                    raise temporalio.exceptions.WorkflowAlreadyStartedError(
                        input.id, input.workflow, run_id=details.run_id
                    )
            raise
        handle: WorkflowHandle[Any, Any] = WorkflowHandle(
            self._client,
            req.workflow_id,
            result_run_id=resp.run_id,
            first_execution_run_id=first_execution_run_id,
            result_type=input.ret_type,
            start_workflow_response=resp,
        )
        setattr(handle, "__temporal_eagerly_started", eagerly_started)
        temporalio.nexus._operation_context._apply_start_workflow_response_to_nexus_context(
            handle
        )
        return handle

    async def _build_start_workflow_execution_request(
        self, input: StartWorkflowInput
    ) -> temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest:
        req = temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest()
        await self._populate_start_workflow_execution_request(req, input)
        # _populate_start_workflow_execution_request is used for both StartWorkflowInput
        # and UpdateWithStartStartWorkflowInput. UpdateWithStartStartWorkflowInput does
        # not have the following two fields so they are handled here.
        req.request_eager_execution = input.request_eager_start

        temporalio.nexus._operation_context._apply_nexus_context_to_start_workflow_request(
            req
        )

        return req

    async def _build_signal_with_start_workflow_execution_request(
        self, input: StartWorkflowInput
    ) -> temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest:
        assert input.start_signal
        data_converter = self._client.data_converter._with_contexts(
            WorkflowSerializationContext(
                namespace=self._client.namespace,
                workflow_id=input.id,
            ),
            StorageDriverStoreContext(
                target=StorageDriverWorkflowInfo(
                    id=input.id, type=input.workflow, namespace=self._client.namespace
                ),
            ),
        )
        req = temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest(
            signal_name=input.start_signal
        )
        if input.start_signal_args:
            req.signal_input.payloads.extend(
                await data_converter.encode(input.start_signal_args)
            )
        await self._populate_start_workflow_execution_request(req, input)
        temporalio.nexus._operation_context._apply_nexus_context_to_signal_with_start_workflow_request(
            req
        )
        return req

    async def _build_update_with_start_start_workflow_execution_request(
        self, input: UpdateWithStartStartWorkflowInput
    ) -> temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest:
        req = temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest()
        await self._populate_start_workflow_execution_request(req, input)
        return req

    async def _populate_start_workflow_execution_request(
        self,
        req: (
            temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest
            | temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest
        ),
        input: StartWorkflowInput | UpdateWithStartStartWorkflowInput,
    ) -> None:
        data_converter = self._client.data_converter._with_contexts(
            WorkflowSerializationContext(
                namespace=self._client.namespace,
                workflow_id=input.id,
            ),
            StorageDriverStoreContext(
                target=StorageDriverWorkflowInfo(
                    id=input.id, type=input.workflow, namespace=self._client.namespace
                ),
            ),
        )
        req.namespace = self._client.namespace
        req.workflow_id = input.id
        req.workflow_type.name = input.workflow
        req.task_queue.name = input.task_queue
        if input.args:
            req.input.payloads.extend(await data_converter.encode(input.args))
        if input.execution_timeout is not None:
            req.workflow_execution_timeout.FromTimedelta(input.execution_timeout)
        if input.run_timeout is not None:
            req.workflow_run_timeout.FromTimedelta(input.run_timeout)
        if input.task_timeout is not None:
            req.workflow_task_timeout.FromTimedelta(input.task_timeout)
        req.identity = self._client.identity
        req.request_id = str(uuid.uuid4())
        req.workflow_id_reuse_policy = cast(
            "temporalio.api.enums.v1.WorkflowIdReusePolicy.ValueType",
            int(input.id_reuse_policy),
        )
        req.workflow_id_conflict_policy = cast(
            "temporalio.api.enums.v1.WorkflowIdConflictPolicy.ValueType",
            int(input.id_conflict_policy),
        )

        if input.retry_policy is not None:
            input.retry_policy.apply_to_proto(req.retry_policy)
        req.cron_schedule = input.cron_schedule
        if input.memo is not None:
            await data_converter._encode_memo_existing(input.memo, req.memo)
        if input.search_attributes is not None:
            temporalio.converter.encode_search_attributes(
                input.search_attributes, req.search_attributes
            )
        metadata = await _encode_user_metadata(
            data_converter, input.static_summary, input.static_details
        )
        if metadata is not None:
            req.user_metadata.CopyFrom(metadata)
        if input.start_delay is not None:
            req.workflow_start_delay.FromTimedelta(input.start_delay)
        if input.headers is not None:  # type:ignore[reportUnnecessaryComparison]
            await self._apply_headers(input.headers, req.header.fields)
        if input.priority is not None:  # type:ignore[reportUnnecessaryComparison]
            req.priority.CopyFrom(input.priority._to_proto())
        if input.versioning_override is not None:
            req.versioning_override.CopyFrom(input.versioning_override._to_proto())

    async def cancel_workflow(self, input: CancelWorkflowInput) -> None:
        await self._client.workflow_service.request_cancel_workflow_execution(
            temporalio.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest(
                namespace=self._client.namespace,
                workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                    workflow_id=input.id,
                    run_id=input.run_id or "",
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
                first_execution_run_id=input.first_execution_run_id or "",
                reason=input.reason,
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def describe_workflow(
        self, input: DescribeWorkflowInput
    ) -> WorkflowExecutionDescription:
        return await WorkflowExecutionDescription._from_raw_description(
            await self._client.workflow_service.describe_workflow_execution(
                temporalio.api.workflowservice.v1.DescribeWorkflowExecutionRequest(
                    namespace=self._client.namespace,
                    execution=temporalio.api.common.v1.WorkflowExecution(
                        workflow_id=input.id,
                        run_id=input.run_id or "",
                    ),
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            ),
            namespace=self._client.namespace,
            converter=self._client.data_converter.with_context(
                WorkflowSerializationContext(
                    namespace=self._client.namespace,
                    workflow_id=input.id,
                )
            ),
        )

    def fetch_workflow_history_events(
        self, input: FetchWorkflowHistoryEventsInput
    ) -> WorkflowHistoryEventAsyncIterator:
        return WorkflowHistoryEventAsyncIterator(self._client, input)

    def list_workflows(
        self, input: ListWorkflowsInput
    ) -> WorkflowExecutionAsyncIterator:
        return WorkflowExecutionAsyncIterator(self._client, input)

    async def count_workflows(
        self, input: CountWorkflowsInput
    ) -> WorkflowExecutionCount:
        return WorkflowExecutionCount._from_raw(
            await self._client.workflow_service.count_workflow_executions(
                temporalio.api.workflowservice.v1.CountWorkflowExecutionsRequest(
                    namespace=self._client.namespace,
                    query=input.query or "",
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        )

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:
        data_converter = self._client.data_converter._with_contexts(
            WorkflowSerializationContext(
                namespace=self._client.namespace,
                workflow_id=input.id,
            ),
            StorageDriverStoreContext(
                target=StorageDriverWorkflowInfo(
                    id=input.id,
                    run_id=input.run_id or None,
                    namespace=self._client.namespace,
                ),
            ),
        )
        req = temporalio.api.workflowservice.v1.QueryWorkflowRequest(
            namespace=self._client.namespace,
            execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=input.id,
                run_id=input.run_id or "",
            ),
        )
        if input.reject_condition:
            req.query_reject_condition = cast(
                "temporalio.api.enums.v1.QueryRejectCondition.ValueType",
                int(input.reject_condition),
            )
        req.query.query_type = input.query
        if input.args:
            req.query.query_args.payloads.extend(
                await data_converter.encode(input.args)
            )
        if input.headers is not None:  # type:ignore[reportUnnecessaryComparison]
            await self._apply_headers(input.headers, req.query.header.fields)
        try:
            resp = await self._client.workflow_service.query_workflow(
                req,
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        except RPCError as err:
            # If the status is INVALID_ARGUMENT, we can assume it's a query
            # failed error
            if err.status == RPCStatusCode.INVALID_ARGUMENT:
                raise WorkflowQueryFailedError(err.message)
            else:
                raise
        if resp.HasField("query_rejected"):
            raise WorkflowQueryRejectedError(
                WorkflowExecutionStatus(resp.query_rejected.status)
                if resp.query_rejected.status
                else None
            )
        if not resp.query_result.payloads:
            return None
        type_hints = [input.ret_type] if input.ret_type else None
        results = await data_converter.decode(resp.query_result.payloads, type_hints)
        if not results:
            return None
        elif len(results) > 1:
            warnings.warn(f"Expected single query result, got {len(results)}")
        return results[0]

    async def signal_workflow(self, input: SignalWorkflowInput) -> None:
        data_converter = self._client.data_converter._with_contexts(
            WorkflowSerializationContext(
                namespace=self._client.namespace,
                workflow_id=input.id,
            ),
            StorageDriverStoreContext(
                target=StorageDriverWorkflowInfo(
                    id=input.id,
                    run_id=input.run_id or None,
                    namespace=self._client.namespace,
                ),
            ),
        )
        req = temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest(
            namespace=self._client.namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=input.id,
                run_id=input.run_id or "",
            ),
            signal_name=input.signal,
            identity=self._client.identity,
            request_id=str(uuid.uuid4()),
        )
        if input.args:
            req.input.payloads.extend(await data_converter.encode(input.args))
        if input.headers is not None:  # type:ignore[reportUnnecessaryComparison]
            await self._apply_headers(input.headers, req.header.fields)
        temporalio.nexus._operation_context._apply_nexus_context_to_signal_workflow_request(
            req
        )
        resp = await self._client.workflow_service.signal_workflow_execution(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )
        temporalio.nexus._operation_context._apply_signal_workflow_response_to_nexus_context(
            resp
        )

    async def terminate_workflow(self, input: TerminateWorkflowInput) -> None:
        data_converter = self._client.data_converter._with_contexts(
            WorkflowSerializationContext(
                namespace=self._client.namespace,
                workflow_id=input.id,
            ),
            StorageDriverStoreContext(
                target=StorageDriverWorkflowInfo(
                    id=input.id,
                    run_id=input.run_id or None,
                    namespace=self._client.namespace,
                ),
            ),
        )
        req = temporalio.api.workflowservice.v1.TerminateWorkflowExecutionRequest(
            namespace=self._client.namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=input.id,
                run_id=input.run_id or "",
            ),
            reason=input.reason or "",
            identity=self._client.identity,
            first_execution_run_id=input.first_execution_run_id or "",
        )
        if input.args:
            req.details.payloads.extend(await data_converter.encode(input.args))
        await self._client.workflow_service.terminate_workflow_execution(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )

    async def start_activity(self, input: StartActivityInput) -> ActivityHandle[Any]:
        """Start an activity and return a handle to it."""
        if not (input.start_to_close_timeout or input.schedule_to_close_timeout):
            raise ValueError(
                "Activity must have start_to_close_timeout or schedule_to_close_timeout"
            )
        if input.start_delay is not None and input.start_delay < timedelta(0):
            raise ValueError("start_delay must be non-negative")
        req = await self._build_start_activity_execution_request(input)

        resp: temporalio.api.workflowservice.v1.StartActivityExecutionResponse
        try:
            resp = await self._client.workflow_service.start_activity_execution(
                req,
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        except RPCError as err:
            # If the status is ALREADY_EXISTS and the details can be extracted
            # as already started, use a different exception
            if err.status == RPCStatusCode.ALREADY_EXISTS and err.grpc_status.details:
                details = temporalio.api.errordetails.v1.ActivityExecutionAlreadyStartedFailure()
                if err.grpc_status.details[0].Unpack(details):
                    raise temporalio.exceptions.ActivityAlreadyStartedError(
                        input.id, input.activity_type, run_id=details.run_id
                    )
            raise

        # Apply StartActivity response elements to the current Nexus context.
        # No-ops if called outside a Nexus context.
        temporalio.nexus._operation_context._apply_start_activity_response_to_nexus_context(
            input.id, resp
        )

        return ActivityHandle(
            self._client,
            input.id,
            run_id=resp.run_id,
            result_type=input.result_type,
        )

    async def _build_start_activity_execution_request(
        self, input: StartActivityInput
    ) -> temporalio.api.workflowservice.v1.StartActivityExecutionRequest:
        """Build StartActivityExecutionRequest from input."""
        data_converter = self._client.data_converter._with_contexts(
            ActivitySerializationContext(
                namespace=self._client.namespace,
                activity_id=input.id,
                activity_type=input.activity_type,
                activity_task_queue=input.task_queue,
                is_local=False,
                workflow_id=None,
                workflow_type=None,
            ),
            StorageDriverStoreContext(
                target=StorageDriverActivityInfo(
                    id=input.id,
                    type=input.activity_type,
                    namespace=self._client.namespace,
                ),
            ),
        )

        req = temporalio.api.workflowservice.v1.StartActivityExecutionRequest(
            namespace=self._client.namespace,
            identity=self._client.identity,
            request_id=str(uuid.uuid4()),
            activity_id=input.id,
            activity_type=temporalio.api.common.v1.ActivityType(
                name=input.activity_type
            ),
            task_queue=temporalio.api.taskqueue.v1.TaskQueue(name=input.task_queue),
            id_reuse_policy=cast(
                "temporalio.api.enums.v1.ActivityIdReusePolicy.ValueType",
                int(input.id_reuse_policy),
            ),
            id_conflict_policy=cast(
                "temporalio.api.enums.v1.ActivityIdConflictPolicy.ValueType",
                int(input.id_conflict_policy),
            ),
        )

        if input.schedule_to_close_timeout is not None:
            req.schedule_to_close_timeout.FromTimedelta(input.schedule_to_close_timeout)
        if input.start_to_close_timeout is not None:
            req.start_to_close_timeout.FromTimedelta(input.start_to_close_timeout)
        if input.schedule_to_start_timeout is not None:
            req.schedule_to_start_timeout.FromTimedelta(input.schedule_to_start_timeout)
        if input.heartbeat_timeout is not None:
            req.heartbeat_timeout.FromTimedelta(input.heartbeat_timeout)
        if input.start_delay is not None:
            req.start_delay.FromTimedelta(input.start_delay)
        if input.retry_policy is not None:
            input.retry_policy.apply_to_proto(req.retry_policy)

        # Set input payloads
        if input.args:
            req.input.payloads.extend(await data_converter.encode(input.args))

        # Set search attributes
        if input.search_attributes is not None:
            temporalio.converter.encode_search_attributes(
                input.search_attributes, req.search_attributes
            )

        # Set user metadata
        metadata = await _encode_user_metadata(data_converter, input.summary, None)
        if metadata is not None:
            req.user_metadata.CopyFrom(metadata)

        # Set headers
        if input.headers:
            await self._apply_headers(input.headers, req.header.fields)

        # Set priority
        req.priority.CopyFrom(input.priority._to_proto())

        # Nexus starts use the inbound request ID so retries across the Nexus
        # boundary resolve to the same activity execution.
        temporalio.nexus._operation_context._apply_nexus_context_to_start_activity_request(
            req
        )

        return req

    async def cancel_activity(self, input: CancelActivityInput) -> None:
        """Cancel an activity."""
        await self._client.workflow_service.request_cancel_activity_execution(
            temporalio.api.workflowservice.v1.RequestCancelActivityExecutionRequest(
                namespace=self._client.namespace,
                activity_id=input.activity_id,
                run_id=input.activity_run_id or "",
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
                reason=input.reason or "",
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def terminate_activity(self, input: TerminateActivityInput) -> None:
        """Terminate an activity."""
        await self._client.workflow_service.terminate_activity_execution(
            temporalio.api.workflowservice.v1.TerminateActivityExecutionRequest(
                namespace=self._client.namespace,
                activity_id=input.activity_id,
                run_id=input.activity_run_id or "",
                reason=input.reason or "",
                identity=self._client.identity,
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def describe_activity(
        self, input: DescribeActivityInput
    ) -> ActivityExecutionDescription:
        """Describe an activity."""
        resp = await self._client.workflow_service.describe_activity_execution(
            temporalio.api.workflowservice.v1.DescribeActivityExecutionRequest(
                namespace=self._client.namespace,
                activity_id=input.activity_id,
                run_id=input.activity_run_id or "",
                long_poll_token=input.long_poll_token or b"",
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )
        return await ActivityExecutionDescription._from_execution_info(
            info=resp.info,
            long_poll_token=resp.long_poll_token or None,
            namespace=self._client.namespace,
            data_converter=self._client.data_converter.with_context(
                ActivitySerializationContext(
                    namespace=self._client.namespace,
                    activity_id=resp.info.activity_id,
                    activity_task_queue=resp.info.task_queue,
                    activity_type=resp.info.activity_type.name,
                    workflow_id=None,
                    workflow_type=None,
                    is_local=False,
                )
            ),
            callbacks=resp.callbacks,
        )

    def list_activities(
        self, input: ListActivitiesInput
    ) -> ActivityExecutionAsyncIterator:
        return ActivityExecutionAsyncIterator(self._client, input)

    async def count_activities(
        self, input: CountActivitiesInput
    ) -> ActivityExecutionCount:
        return ActivityExecutionCount._from_raw(
            await self._client.workflow_service.count_activity_executions(
                temporalio.api.workflowservice.v1.CountActivityExecutionsRequest(
                    namespace=self._client.namespace,
                    query=input.query or "",
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        )

    async def start_workflow_update(
        self, input: StartWorkflowUpdateInput
    ) -> WorkflowUpdateHandle[Any]:
        workflow_id = input.id
        req = await self._build_update_workflow_execution_request(input, workflow_id)

        # Repeatedly try to invoke UpdateWorkflowExecution until the update is durable.
        resp: temporalio.api.workflowservice.v1.UpdateWorkflowExecutionResponse
        while True:
            try:
                resp = await self._client.workflow_service.update_workflow_execution(
                    req,
                    retry=True,
                    metadata=input.rpc_metadata,
                    timeout=input.rpc_timeout,
                )
            except RPCError as err:
                if (
                    err.status == RPCStatusCode.DEADLINE_EXCEEDED
                    or err.status == RPCStatusCode.CANCELLED
                ):
                    raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
                else:
                    raise
            except asyncio.CancelledError as err:
                raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
            if (
                resp.stage
                >= temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
            ):
                break

        temporalio.nexus._operation_context._apply_start_workflow_update_response_to_nexus_context(
            resp
        )

        # Build the handle. If the user's wait stage is COMPLETED, make sure we
        # poll for result.
        handle: WorkflowUpdateHandle[Any] = WorkflowUpdateHandle(
            client=self._client,
            id=req.request.meta.update_id,
            workflow_id=workflow_id,
            workflow_run_id=resp.update_ref.workflow_execution.run_id,
            result_type=input.ret_type,
        )
        if resp.HasField("outcome"):
            handle._known_outcome = resp.outcome
        if input.wait_for_stage == WorkflowUpdateStage.COMPLETED:
            await handle._poll_until_outcome()
        return handle

    async def _build_update_workflow_execution_request(
        self,
        input: StartWorkflowUpdateInput | UpdateWithStartUpdateWorkflowInput,
        workflow_id: str,
    ) -> temporalio.api.workflowservice.v1.UpdateWorkflowExecutionRequest:
        data_converter = self._client.data_converter._with_contexts(
            WorkflowSerializationContext(
                namespace=self._client.namespace,
                workflow_id=workflow_id,
            ),
            StorageDriverStoreContext(
                target=StorageDriverWorkflowInfo(
                    id=workflow_id,
                    run_id=(input.run_id or None)
                    if isinstance(input, StartWorkflowUpdateInput)
                    else None,
                    namespace=self._client.namespace,
                ),
            ),
        )
        run_id, first_execution_run_id = (
            (
                input.run_id,
                input.first_execution_run_id,
            )
            if isinstance(input, StartWorkflowUpdateInput)
            else (None, None)
        )
        req = temporalio.api.workflowservice.v1.UpdateWorkflowExecutionRequest(
            namespace=self._client.namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=workflow_id,
                run_id=run_id or "",
            ),
            first_execution_run_id=first_execution_run_id or "",
            request=temporalio.api.update.v1.Request(
                meta=temporalio.api.update.v1.Meta(
                    update_id=input.update_id or str(uuid.uuid4()),
                    identity=self._client.identity,
                ),
                input=temporalio.api.update.v1.Input(
                    name=input.update,
                ),
            ),
            wait_policy=temporalio.api.update.v1.WaitPolicy(
                lifecycle_stage=temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.ValueType(
                    input.wait_for_stage
                )
            ),
        )
        if isinstance(input, StartWorkflowUpdateInput):
            temporalio.nexus._operation_context._apply_nexus_context_to_start_workflow_update_request(
                req
            )
        if input.args:
            req.request.input.args.payloads.extend(
                await data_converter.encode(input.args)
            )
        if input.headers is not None:  # type:ignore[reportUnnecessaryComparison]
            await self._apply_headers(input.headers, req.request.input.header.fields)
        return req

    async def start_update_with_start_workflow(
        self, input: StartWorkflowUpdateWithStartInput
    ) -> WorkflowUpdateHandle[Any]:
        seen_start = False

        def on_start(
            start_response: temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse,
        ):
            nonlocal seen_start
            if not seen_start:
                input._on_start(start_response)
                seen_start = True

        err: BaseException | None = None

        try:
            return await self._start_workflow_update_with_start(
                input.start_workflow_input,
                input.update_workflow_input,
                input.rpc_metadata,
                input.rpc_timeout,
                on_start,
            )
        except asyncio.CancelledError as _err:
            err = _err
            raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
        except RPCError as _err:
            err = _err
            if err.status in [
                RPCStatusCode.DEADLINE_EXCEEDED,
                RPCStatusCode.CANCELLED,
            ]:
                raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
            else:
                multiop_failure = (
                    temporalio.api.errordetails.v1.MultiOperationExecutionFailure()
                )
                if err.grpc_status.details and err.grpc_status.details[0].Unpack(
                    multiop_failure
                ):
                    status = next(
                        (
                            st
                            for st in multiop_failure.statuses
                            if (
                                st.code != RPCStatusCode.OK
                                and not (
                                    st.details
                                    and st.details[0].Is(
                                        temporalio.api.failure.v1.MultiOperationExecutionAborted.DESCRIPTOR
                                    )
                                )
                            )
                        ),
                        None,
                    )
                    if status and status.code in list(RPCStatusCode):
                        if (
                            status.code == RPCStatusCode.ALREADY_EXISTS
                            and status.details
                        ):
                            details = temporalio.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure()
                            if status.details[0].Unpack(details):
                                err = temporalio.exceptions.WorkflowAlreadyStartedError(
                                    input.start_workflow_input.id,
                                    input.start_workflow_input.workflow,
                                    run_id=details.run_id,
                                )
                        else:
                            err = RPCError(
                                status.message,
                                RPCStatusCode(status.code),
                                err.raw_grpc_status,
                            )
                raise err
        finally:
            if err and not seen_start:
                input._on_start_error(err)

    async def _start_workflow_update_with_start(
        self,
        start_input: UpdateWithStartStartWorkflowInput,
        update_input: UpdateWithStartUpdateWorkflowInput,
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
        on_start: Callable[
            [temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse], None
        ],
    ) -> WorkflowUpdateHandle[Any]:
        start_req = (
            await self._build_update_with_start_start_workflow_execution_request(
                start_input
            )
        )
        update_req = await self._build_update_workflow_execution_request(
            update_input, workflow_id=start_input.id
        )
        multiop_req = temporalio.api.workflowservice.v1.ExecuteMultiOperationRequest(
            namespace=self._client.namespace,
            operations=[
                temporalio.api.workflowservice.v1.ExecuteMultiOperationRequest.Operation(
                    start_workflow=start_req
                ),
                temporalio.api.workflowservice.v1.ExecuteMultiOperationRequest.Operation(
                    update_workflow=update_req
                ),
            ],
        )

        # Repeatedly try to invoke ExecuteMultiOperation until the update is durable
        while True:
            multiop_response = (
                await self._client.workflow_service.execute_multi_operation(
                    multiop_req,
                    retry=True,
                    metadata=rpc_metadata,
                    timeout=rpc_timeout,
                )
            )
            start_response = multiop_response.responses[0].start_workflow
            update_response = multiop_response.responses[1].update_workflow
            on_start(start_response)
            known_outcome = (
                update_response.outcome if update_response.HasField("outcome") else None
            )
            if (
                update_response.stage
                >= temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
            ):
                break

        handle: WorkflowUpdateHandle[Any] = WorkflowUpdateHandle(
            client=self._client,
            id=update_req.request.meta.update_id,
            workflow_id=start_input.id,
            workflow_run_id=start_response.run_id,
            known_outcome=known_outcome,
            result_type=update_input.ret_type,
        )
        if update_input.wait_for_stage == WorkflowUpdateStage.COMPLETED:
            await handle._poll_until_outcome()

        return handle

    ### Async activity calls

    def _get_async_activity_store_context(
        self, id_or_token: AsyncActivityIDReference | bytes
    ) -> StorageDriverStoreContext:
        if isinstance(id_or_token, AsyncActivityIDReference):
            if id_or_token.workflow_id:
                return StorageDriverStoreContext(
                    target=StorageDriverWorkflowInfo(
                        id=id_or_token.workflow_id or None,
                        run_id=id_or_token.run_id or None,
                        namespace=self._client.namespace,
                    ),
                )
            return StorageDriverStoreContext(
                target=StorageDriverActivityInfo(
                    id=id_or_token.activity_id,
                    run_id=id_or_token.run_id or None,
                    namespace=self._client.namespace,
                ),
            )
        else:
            return StorageDriverStoreContext(target=None)

    async def heartbeat_async_activity(
        self, input: HeartbeatAsyncActivityInput
    ) -> None:
        data_converter = (
            input.data_converter_override or self._client.data_converter
        )._with_store_context(self._get_async_activity_store_context(input.id_or_token))
        details = (
            None
            if not input.details
            else await data_converter.encode_wrapper(input.details)
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            resp_by_id = await self._client.workflow_service.record_activity_task_heartbeat_by_id(
                temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdRequest(
                    workflow_id=input.id_or_token.workflow_id or "",
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
            if (
                resp_by_id.cancel_requested
                or resp_by_id.activity_paused
                or resp_by_id.activity_reset
            ):
                raise AsyncActivityCancelledError(
                    details=ActivityCancellationDetails(
                        cancel_requested=resp_by_id.cancel_requested,
                        paused=resp_by_id.activity_paused,
                        reset=resp_by_id.activity_reset,
                    )
                )

        else:
            resp = await self._client.workflow_service.record_activity_task_heartbeat(
                temporalio.api.workflowservice.v1.RecordActivityTaskHeartbeatRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
            if resp.cancel_requested or resp.activity_paused:
                raise AsyncActivityCancelledError(
                    details=ActivityCancellationDetails(
                        cancel_requested=resp.cancel_requested,
                        paused=resp.activity_paused,
                        reset=resp.activity_reset,
                    )
                )

    async def complete_async_activity(self, input: CompleteAsyncActivityInput) -> None:
        data_converter = (
            input.data_converter_override or self._client.data_converter
        )._with_store_context(self._get_async_activity_store_context(input.id_or_token))
        result = (
            None
            if input.result is temporalio.common._arg_unset
            else await data_converter.encode_wrapper([input.result])
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            await self._client.workflow_service.respond_activity_task_completed_by_id(
                temporalio.api.workflowservice.v1.RespondActivityTaskCompletedByIdRequest(
                    workflow_id=input.id_or_token.workflow_id or "",
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    result=result,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        else:
            await self._client.workflow_service.respond_activity_task_completed(
                temporalio.api.workflowservice.v1.RespondActivityTaskCompletedRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    result=result,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )

    async def fail_async_activity(self, input: FailAsyncActivityInput) -> None:
        data_converter = (
            input.data_converter_override or self._client.data_converter
        )._with_store_context(self._get_async_activity_store_context(input.id_or_token))

        failure = temporalio.api.failure.v1.Failure()
        await data_converter.encode_failure(input.error, failure)
        last_heartbeat_details = (
            await data_converter.encode_wrapper(input.last_heartbeat_details)
            if input.last_heartbeat_details
            else None
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            await self._client.workflow_service.respond_activity_task_failed_by_id(
                temporalio.api.workflowservice.v1.RespondActivityTaskFailedByIdRequest(
                    workflow_id=input.id_or_token.workflow_id or "",
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    failure=failure,
                    last_heartbeat_details=last_heartbeat_details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        else:
            await self._client.workflow_service.respond_activity_task_failed(
                temporalio.api.workflowservice.v1.RespondActivityTaskFailedRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    failure=failure,
                    last_heartbeat_details=last_heartbeat_details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )

    async def report_cancellation_async_activity(
        self, input: ReportCancellationAsyncActivityInput
    ) -> None:
        data_converter = (
            input.data_converter_override or self._client.data_converter
        )._with_store_context(self._get_async_activity_store_context(input.id_or_token))
        details = (
            None
            if not input.details
            else await data_converter.encode_wrapper(input.details)
        )
        if isinstance(input.id_or_token, AsyncActivityIDReference):
            await self._client.workflow_service.respond_activity_task_canceled_by_id(
                temporalio.api.workflowservice.v1.RespondActivityTaskCanceledByIdRequest(
                    workflow_id=input.id_or_token.workflow_id or "",
                    run_id=input.id_or_token.run_id or "",
                    activity_id=input.id_or_token.activity_id,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        else:
            await self._client.workflow_service.respond_activity_task_canceled(
                temporalio.api.workflowservice.v1.RespondActivityTaskCanceledRequest(
                    task_token=input.id_or_token,
                    namespace=self._client.namespace,
                    identity=self._client.identity,
                    details=details,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )

    ### Schedule calls

    async def create_schedule(self, input: CreateScheduleInput) -> ScheduleHandle:
        # Limited actions must be false if remaining actions is 0 and must be
        # true if remaining actions is non-zero
        if (
            input.schedule.state.limited_actions
            and not input.schedule.state.remaining_actions
        ):
            raise ValueError(
                "Must set limited actions to false if there are no remaining actions set"
            )
        if (
            not input.schedule.state.limited_actions
            and input.schedule.state.remaining_actions
        ):
            raise ValueError(
                "Must set limited actions to true if there are remaining actions set"
            )

        initial_patch: temporalio.api.schedule.v1.SchedulePatch | None = None
        if input.trigger_immediately or input.backfill:
            initial_patch = temporalio.api.schedule.v1.SchedulePatch(
                trigger_immediately=temporalio.api.schedule.v1.TriggerImmediatelyRequest(
                    overlap_policy=temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                        input.schedule.policy.overlap
                    ),
                )
                if input.trigger_immediately
                else None,
                backfill_request=[b._to_proto() for b in input.backfill]
                if input.backfill
                else None,
            )
        try:
            request = temporalio.api.workflowservice.v1.CreateScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                schedule=await input.schedule._to_proto(self._client),
                initial_patch=initial_patch,
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
                memo=await self._client.data_converter._encode_memo(input.memo)
                if input.memo
                else None,
            )
            if input.search_attributes:
                temporalio.converter.encode_search_attributes(
                    input.search_attributes, request.search_attributes
                )
            await self._client.workflow_service.create_schedule(
                request,
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        except RPCError as err:
            already_started = (
                err.status == RPCStatusCode.ALREADY_EXISTS
                and err.grpc_status.details
                and err.grpc_status.details[0].Is(
                    temporalio.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure.DESCRIPTOR
                )
            )
            if already_started:
                raise ScheduleAlreadyRunningError()
            raise
        return ScheduleHandle(self._client, input.id)

    def list_schedules(self, input: ListSchedulesInput) -> ScheduleAsyncIterator:
        return ScheduleAsyncIterator(self._client, input)

    async def backfill_schedule(self, input: BackfillScheduleInput) -> None:
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    backfill_request=[b._to_proto() for b in input.backfills],
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def delete_schedule(self, input: DeleteScheduleInput) -> None:
        await self._client.workflow_service.delete_schedule(
            temporalio.api.workflowservice.v1.DeleteScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                identity=self._client.identity,
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def describe_schedule(
        self, input: DescribeScheduleInput
    ) -> ScheduleDescription:
        return ScheduleDescription._from_proto(
            input.id,
            await self._client.workflow_service.describe_schedule(
                temporalio.api.workflowservice.v1.DescribeScheduleRequest(
                    namespace=self._client.namespace,
                    schedule_id=input.id,
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            ),
            self._client.data_converter,
        )

    async def pause_schedule(self, input: PauseScheduleInput) -> None:
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    pause=input.note or "Paused via Python SDK",
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def trigger_schedule(self, input: TriggerScheduleInput) -> None:
        overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
        if input.overlap:
            overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                input.overlap
            )
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    trigger_immediately=temporalio.api.schedule.v1.TriggerImmediatelyRequest(
                        overlap_policy=overlap_policy,
                    ),
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def unpause_schedule(self, input: UnpauseScheduleInput) -> None:
        await self._client.workflow_service.patch_schedule(
            temporalio.api.workflowservice.v1.PatchScheduleRequest(
                namespace=self._client.namespace,
                schedule_id=input.id,
                patch=temporalio.api.schedule.v1.SchedulePatch(
                    unpause=input.note or "Unpaused via Python SDK",
                ),
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def update_schedule(self, input: UpdateScheduleInput) -> None:
        # TODO(cretz): This is supposed to be a retry-conflict loop, but we do
        # not yet have a way to know update failure is due to conflict token
        # mismatch
        update = input.updater(
            ScheduleUpdateInput(
                description=ScheduleDescription._from_proto(
                    input.id,
                    await self._client.workflow_service.describe_schedule(
                        temporalio.api.workflowservice.v1.DescribeScheduleRequest(
                            namespace=self._client.namespace,
                            schedule_id=input.id,
                        ),
                        retry=True,
                        metadata=input.rpc_metadata,
                        timeout=input.rpc_timeout,
                    ),
                    self._client.data_converter,
                )
            )
        )
        if inspect.iscoroutine(update):
            update = await update
        if not update:
            return
        assert isinstance(update, ScheduleUpdate)
        request = temporalio.api.workflowservice.v1.UpdateScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=input.id,
            schedule=await update.schedule._to_proto(self._client),
            identity=self._client.identity,
            request_id=str(uuid.uuid4()),
        )
        if update.search_attributes is not None:
            request.search_attributes.indexed_fields.clear()  # Ensure that we at least create an empty map
            temporalio.converter.encode_search_attributes(
                update.search_attributes, request.search_attributes
            )
        await self._client.workflow_service.update_schedule(
            request,
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def update_worker_build_id_compatibility(
        self, input: UpdateWorkerBuildIdCompatibilityInput
    ) -> None:
        req = input.operation._as_partial_proto()
        req.namespace = self._client.namespace
        req.task_queue = input.task_queue
        await self._client.workflow_service.update_worker_build_id_compatibility(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )

    async def get_worker_build_id_compatibility(
        self, input: GetWorkerBuildIdCompatibilityInput
    ) -> WorkerBuildIdVersionSets:
        req = temporalio.api.workflowservice.v1.GetWorkerBuildIdCompatibilityRequest(
            namespace=self._client.namespace,
            task_queue=input.task_queue,
            max_sets=input.max_sets or 0,
        )
        resp = await self._client.workflow_service.get_worker_build_id_compatibility(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )
        return WorkerBuildIdVersionSets._from_proto(resp)

    async def get_worker_task_reachability(
        self, input: GetWorkerTaskReachabilityInput
    ) -> WorkerTaskReachability:
        req = temporalio.api.workflowservice.v1.GetWorkerTaskReachabilityRequest(
            namespace=self._client.namespace,
            build_ids=input.build_ids,
            task_queues=input.task_queues,
            reachability=input.reachability._to_proto()
            if input.reachability
            else temporalio.api.enums.v1.TaskReachability.TASK_REACHABILITY_UNSPECIFIED,
        )
        resp = await self._client.workflow_service.get_worker_task_reachability(
            req, retry=True, metadata=input.rpc_metadata, timeout=input.rpc_timeout
        )
        return WorkerTaskReachability._from_proto(resp)

    ### Nexus operation calls

    async def start_nexus_operation(
        self, input: StartNexusOperationInput
    ) -> NexusOperationHandle[Any]:
        """Start a nexus operation and return a handle to it."""
        req = temporalio.api.workflowservice.v1.StartNexusOperationExecutionRequest(
            namespace=self._client.namespace,
            identity=self._client.identity,
            request_id=str(uuid.uuid4()),
            operation_id=input.id,
            endpoint=input.endpoint,
            service=input.service,
            operation=input.operation,
            id_reuse_policy=cast(
                "temporalio.api.enums.v1.NexusOperationIdReusePolicy.ValueType",
                int(input.id_reuse_policy),
            ),
            id_conflict_policy=cast(
                "temporalio.api.enums.v1.NexusOperationIdConflictPolicy.ValueType",
                int(input.id_conflict_policy),
            ),
        )

        if input.schedule_to_close_timeout is not None:
            req.schedule_to_close_timeout.FromTimedelta(input.schedule_to_close_timeout)
        if input.schedule_to_start_timeout is not None:
            req.schedule_to_start_timeout.FromTimedelta(input.schedule_to_start_timeout)
        if input.start_to_close_timeout is not None:
            req.start_to_close_timeout.FromTimedelta(input.start_to_close_timeout)

        # Set input payload
        encoded = await self._client.data_converter.encode([input.arg])
        if encoded:
            req.input.CopyFrom(encoded[0])

        # Set search attributes
        if input.search_attributes is not None:
            temporalio.converter.encode_search_attributes(
                input.search_attributes, req.search_attributes
            )

        # Set user metadata
        metadata = await _encode_user_metadata(
            self._client.data_converter, input.summary, None
        )
        if metadata is not None:
            req.user_metadata.CopyFrom(metadata)

        # Set nexus headers
        if input.headers:
            for k, v in input.headers.items():
                req.nexus_header[k] = v

        resp: temporalio.api.workflowservice.v1.StartNexusOperationExecutionResponse
        try:
            resp = await self._client.workflow_service.start_nexus_operation_execution(
                req,
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        except RPCError as err:
            if err.status == RPCStatusCode.ALREADY_EXISTS and err.grpc_status.details:
                details = temporalio.api.errordetails.v1.NexusOperationExecutionAlreadyStartedFailure()
                if err.grpc_status.details[0].Unpack(details):
                    raise temporalio.exceptions.NexusOperationAlreadyStartedError(
                        input.id, run_id=details.run_id
                    )
            raise
        return NexusOperationHandle(
            self._client,
            input.id,
            run_id=resp.run_id or None,
            result_type=input.result_type,
            endpoint=input.endpoint,
            service=input.service,
        )

    async def describe_nexus_operation(
        self, input: DescribeNexusOperationInput
    ) -> NexusOperationExecutionDescription:
        """Describe a nexus operation."""
        req = temporalio.api.workflowservice.v1.DescribeNexusOperationExecutionRequest(
            namespace=self._client.namespace,
            operation_id=input.operation_id,
            run_id=input.run_id or "",
        )
        resp = await self._client.workflow_service.describe_nexus_operation_execution(
            req=req,
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )
        return await NexusOperationExecutionDescription._from_execution_info(
            info=resp.info,
            data_converter=self._client.data_converter,
        )

    async def get_nexus_operation_result(
        self, input: GetNexusOperationResultInput
    ) -> Any:
        """Poll for nexus operation result until it's available."""
        req = temporalio.api.workflowservice.v1.PollNexusOperationExecutionRequest(
            namespace=self._client.namespace,
            operation_id=input.operation_id,
            run_id=input.run_id or "",
            wait_stage=temporalio.api.enums.v1.NexusOperationWaitStage.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
        )

        # Continue polling as long as we have no outcome
        while True:
            try:
                res = (
                    await self._client.workflow_service.poll_nexus_operation_execution(
                        req,
                        retry=True,
                        metadata=input.rpc_metadata,
                        timeout=input.rpc_timeout,
                    )
                )
                match res.WhichOneof("outcome"):
                    case "result":
                        type_hints = [input.result_type] if input.result_type else None
                        [result] = await self._client.data_converter.decode(
                            [res.result], type_hints
                        )
                        return result

                    case "failure":
                        raise NexusOperationFailureError(
                            cause=await self._client.data_converter.decode_failure(
                                res.failure
                            )
                        )

                    case None:
                        # poll again
                        pass
            except RPCError as err:
                match err.status:
                    case RPCStatusCode.DEADLINE_EXCEEDED:
                        # Deadline exceeded is expected with long polling; retry
                        continue
                    case RPCStatusCode.CANCELLED:
                        raise asyncio.CancelledError() from err
                    case _:
                        raise

    async def cancel_nexus_operation(self, input: CancelNexusOperationInput) -> None:
        """Cancel a nexus operation."""
        await self._client.workflow_service.request_cancel_nexus_operation_execution(
            temporalio.api.workflowservice.v1.RequestCancelNexusOperationExecutionRequest(
                namespace=self._client.namespace,
                operation_id=input.operation_id,
                run_id=input.run_id or "",
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
                reason=input.reason or "",
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    async def terminate_nexus_operation(
        self, input: TerminateNexusOperationInput
    ) -> None:
        """Terminate a nexus operation."""
        await self._client.workflow_service.terminate_nexus_operation_execution(
            temporalio.api.workflowservice.v1.TerminateNexusOperationExecutionRequest(
                namespace=self._client.namespace,
                operation_id=input.operation_id,
                run_id=input.run_id or "",
                reason=input.reason or "",
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
            ),
            retry=True,
            metadata=input.rpc_metadata,
            timeout=input.rpc_timeout,
        )

    def list_nexus_operations(
        self, input: ListNexusOperationsInput
    ) -> NexusOperationExecutionAsyncIterator:
        return NexusOperationExecutionAsyncIterator(self._client, input)

    async def count_nexus_operations(
        self, input: CountNexusOperationsInput
    ) -> NexusOperationExecutionCount:
        return NexusOperationExecutionCount._from_raw(
            await self._client.workflow_service.count_nexus_operation_executions(
                temporalio.api.workflowservice.v1.CountNexusOperationExecutionsRequest(
                    namespace=self._client.namespace,
                    query=input.query or "",
                ),
                retry=True,
                metadata=input.rpc_metadata,
                timeout=input.rpc_timeout,
            )
        )

    async def _apply_headers(
        self,
        source: Mapping[str, temporalio.api.common.v1.Payload] | None,
        dest: MessageMap[str, temporalio.api.common.v1.Payload],
    ) -> None:
        await _apply_headers(
            source,
            dest,
            self._client.config(active_config=True)["header_codec_behavior"]
            == HeaderCodecBehavior.CODEC,
            self._client.data_converter,
        )
