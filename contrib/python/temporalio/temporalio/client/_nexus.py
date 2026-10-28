from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Generic, cast, overload

import nexusrpc
from nexusrpc import InputT, OutputT
from typing_extensions import Self

import temporalio.api.nexus.v1
import temporalio.api.workflowservice.v1
import temporalio.common
import temporalio.converter
import temporalio.converter._search_attributes
import temporalio.exceptions
import temporalio.nexus._util
from temporalio.types import NexusServiceType, ReturnType

from ._helpers import _decode_user_metadata
from ._interceptor import (
    CancelNexusOperationInput,
    DescribeNexusOperationInput,
    GetNexusOperationResultInput,
    ListNexusOperationsInput,
    StartNexusOperationInput,
    TerminateNexusOperationInput,
)

if TYPE_CHECKING:
    from ._client import Client


@dataclass
class NexusOperationExecutionCancellationInfo:
    """Cancellation information for a Nexus Operation.

    .. warning::
       This API is experimental and unstable.
    """

    raw: temporalio.api.nexus.v1.NexusOperationExecutionCancellationInfo
    """Underlying protobuf cancellation info."""

    requested_time: datetime | None
    """The time when cancellation was requested."""

    state: temporalio.common.NexusOperationCancellationState
    """The current state of the cancellation request."""

    attempt: int
    """The number of attempts made to deliver the cancel operation request."""

    last_attempt_complete_time: datetime | None
    """The time when the last attempt completed."""

    next_attempt_schedule_time: datetime | None
    """The time when the next attempt is scheduled."""

    last_attempt_failure: BaseException | None
    """The last attempt's failure, if any."""

    blocked_reason: str
    """Blocked reason provides additional information if the cancellation state is BLOCKED."""

    reason: str
    """The reason specified in the cancellation request."""

    @classmethod
    async def _from_cancellation_info(
        cls,
        info: temporalio.api.nexus.v1.NexusOperationExecutionCancellationInfo,
        data_converter: temporalio.converter.DataConverter,
    ) -> Self:
        """Create from raw proto nexus operation cancellation info."""
        return cls(
            raw=info,
            requested_time=(
                info.requested_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("requested_time")
                else None
            ),
            state=(
                temporalio.common.NexusOperationCancellationState(info.state)
                if info.state
                else temporalio.common.NexusOperationCancellationState.UNSPECIFIED
            ),
            attempt=info.attempt,
            last_attempt_complete_time=(
                info.last_attempt_complete_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("last_attempt_complete_time")
                else None
            ),
            next_attempt_schedule_time=(
                info.next_attempt_schedule_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("next_attempt_schedule_time")
                else None
            ),
            last_attempt_failure=(
                cast(
                    BaseException | None,
                    await data_converter.decode_failure(info.last_attempt_failure),
                )
                if info.HasField("last_attempt_failure")
                else None
            ),
            blocked_reason=info.blocked_reason,
            reason=info.reason,
        )


@dataclass
class NexusOperationExecution:
    """Info for a standalone Nexus operation execution, from list response.

    .. warning::
       This API is experimental and unstable.
    """

    operation_id: str
    """Unique identifier of this operation."""

    run_id: str
    """Run ID of the standalone Nexus operation."""

    endpoint: str
    """Endpoint name."""

    service: str
    """Service name."""

    operation: str
    """Operation name."""

    schedule_time: datetime | None
    """Time the operation was originally scheduled."""

    close_time: datetime | None
    """Time the operation reached a terminal status, if closed."""

    status: temporalio.common.NexusOperationExecutionStatus
    """Current status of the operation."""

    search_attributes: temporalio.common.TypedSearchAttributes
    """Current set of search attributes if any."""

    state_transition_count: int
    """Number of state transitions."""

    execution_duration: timedelta | None
    """Duration from scheduled to close time, only populated if closed."""

    raw_info: (
        temporalio.api.nexus.v1.NexusOperationExecutionListInfo
        | temporalio.api.nexus.v1.NexusOperationExecutionInfo
    )
    """Underlying protobuf info."""

    @classmethod
    def _from_raw_info(
        cls, info: temporalio.api.nexus.v1.NexusOperationExecutionListInfo
    ) -> Self:
        """Create from raw proto nexus operation list info."""
        return cls(
            operation_id=info.operation_id,
            run_id=info.run_id,
            endpoint=info.endpoint,
            service=info.service,
            operation=info.operation,
            schedule_time=(
                info.schedule_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("schedule_time")
                else None
            ),
            close_time=(
                info.close_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("close_time")
                else None
            ),
            status=(
                temporalio.common.NexusOperationExecutionStatus(info.status)
                if info.status
                else temporalio.common.NexusOperationExecutionStatus.UNSPECIFIED
            ),
            search_attributes=temporalio.converter.decode_typed_search_attributes(
                info.search_attributes
            ),
            state_transition_count=info.state_transition_count,
            execution_duration=(
                info.execution_duration.ToTimedelta()
                if info.HasField("execution_duration")
                else None
            ),
            raw_info=info,
        )


@dataclass
class NexusOperationExecutionDescription(NexusOperationExecution):
    """Detailed information about a standalone Nexus operation execution.

    .. warning::
       This API is experimental and unstable.
    """

    raw_description: temporalio.api.nexus.v1.NexusOperationExecutionInfo
    """Underlying protobuf description info."""

    state: temporalio.common.PendingNexusOperationExecutionState
    """More detailed breakdown if status is :py:attr:`NexusOperationExecutionStatus.RUNNING`."""

    schedule_to_close_timeout: timedelta | None
    """Schedule-to-close timeout for this operation."""

    schedule_to_start_timeout: timedelta | None
    """Schedule-to-start timeout for this operation."""

    start_to_close_timeout: timedelta | None
    """Start-to-close timeout for this operation."""

    attempt: int
    """Current attempt number."""

    expiration_time: datetime | None
    """Scheduled time plus schedule_to_close_timeout."""

    last_attempt_complete_time: datetime | None
    """Time when the last attempt completed."""

    next_attempt_schedule_time: datetime | None
    """Time when the next attempt will be scheduled."""

    last_attempt_failure: BaseException | None
    """Failure from the last failed attempt, if any."""

    blocked_reason: str | None
    """Reason the operation is blocked, if any."""

    request_id: str
    """Server-generated request ID used as an idempotency token."""

    operation_token: str | None
    """Operation token is only set for asynchronous operations after a successful start_operation call."""

    identity: str
    """Identity of the client that started this operation."""

    cancellation_info: NexusOperationExecutionCancellationInfo | None
    """Cancellation info if cancellation was requested."""

    _data_converter: temporalio.converter.DataConverter = field(
        kw_only=True, compare=False, repr=False
    )
    _static_summary: str | None = field(
        kw_only=True, default=None, compare=False, repr=False
    )
    _static_details: str | None = field(
        kw_only=True, default=None, compare=False, repr=False
    )
    _metadata_decoded: bool = field(
        kw_only=True, default=False, compare=False, repr=False
    )

    async def static_summary(self) -> str | None:
        """Gets the single-line fixed summary for this Nexus operation execution that may appear in
        UI/CLI. This can be in single-line Temporal markdown format.
        """
        if not self._metadata_decoded:
            await self._decode_metadata()
        return self._static_summary

    async def static_details(self) -> str | None:
        """Gets the general fixed details for this Nexus operation execution that may appear in UI/CLI.
        This can be in Temporal markdown format and can span multiple lines.
        """
        if not self._metadata_decoded:
            await self._decode_metadata()
        return self._static_details

    async def _decode_metadata(self) -> None:
        """Internal method to decode metadata lazily."""
        self._static_summary, self._static_details = await _decode_user_metadata(
            self._data_converter, self.raw_description.user_metadata
        )
        self._metadata_decoded = True

    @classmethod
    async def _from_execution_info(
        cls,
        info: temporalio.api.nexus.v1.NexusOperationExecutionInfo,
        data_converter: temporalio.converter.DataConverter,
    ) -> Self:
        """Create from raw proto nexus operation execution info."""
        return cls(
            _data_converter=data_converter,
            operation_id=info.operation_id,
            run_id=info.run_id,
            endpoint=info.endpoint,
            service=info.service,
            operation=info.operation,
            schedule_time=(
                info.schedule_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("schedule_time")
                else None
            ),
            close_time=(
                info.close_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("close_time")
                else None
            ),
            status=(
                temporalio.common.NexusOperationExecutionStatus(info.status)
                if info.status
                else temporalio.common.NexusOperationExecutionStatus.UNSPECIFIED
            ),
            search_attributes=temporalio.converter.decode_typed_search_attributes(
                info.search_attributes
            ),
            state_transition_count=info.state_transition_count,
            execution_duration=(
                info.execution_duration.ToTimedelta()
                if info.HasField("execution_duration")
                else None
            ),
            raw_info=info,
            raw_description=info,
            state=(
                temporalio.common.PendingNexusOperationExecutionState(info.state)
                if info.state
                else temporalio.common.PendingNexusOperationExecutionState.UNSPECIFIED
            ),
            schedule_to_close_timeout=(
                info.schedule_to_close_timeout.ToTimedelta()
                if info.HasField("schedule_to_close_timeout")
                else None
            ),
            schedule_to_start_timeout=(
                info.schedule_to_start_timeout.ToTimedelta()
                if info.HasField("schedule_to_start_timeout")
                else None
            ),
            start_to_close_timeout=(
                info.start_to_close_timeout.ToTimedelta()
                if info.HasField("start_to_close_timeout")
                else None
            ),
            attempt=info.attempt,
            expiration_time=(
                info.expiration_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("expiration_time")
                else None
            ),
            last_attempt_complete_time=(
                info.last_attempt_complete_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("last_attempt_complete_time")
                else None
            ),
            last_attempt_failure=(
                cast(
                    BaseException | None,
                    await data_converter.decode_failure(info.last_attempt_failure),
                )
                if info.HasField("last_attempt_failure")
                else None
            ),
            next_attempt_schedule_time=(
                info.next_attempt_schedule_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("next_attempt_schedule_time")
                else None
            ),
            blocked_reason=info.blocked_reason if info.blocked_reason else None,
            request_id=info.request_id,
            operation_token=info.operation_token if info.operation_token else None,
            identity=info.identity,
            cancellation_info=(
                await NexusOperationExecutionCancellationInfo._from_cancellation_info(
                    info.cancellation_info, data_converter
                )
                if info.HasField("cancellation_info")
                else None
            ),
        )


@dataclass(frozen=True)
class NexusOperationExecutionCountAggregationGroup:
    """A single aggregation group from a count nexus operations call.

    .. warning::
       This API is experimental and unstable.
    """

    count: int
    """Count for this group."""

    group_values: Sequence[temporalio.common.SearchAttributeValue]
    """Values that define this group."""

    @staticmethod
    def _from_raw(
        raw: temporalio.api.workflowservice.v1.CountNexusOperationExecutionsResponse.AggregationGroup,
    ) -> NexusOperationExecutionCountAggregationGroup:
        return NexusOperationExecutionCountAggregationGroup(
            count=raw.count,
            group_values=[
                temporalio.converter._search_attributes._decode_search_attribute_value(
                    v
                )
                for v in raw.group_values
            ],
        )


@dataclass
class NexusOperationExecutionCount:
    """Representation of a count from a count nexus operations call.

    .. warning::
       This API is experimental and unstable.
    """

    count: int
    """Approximate number of operations matching the original query.

    If the query had a group-by clause, this is simply the sum of all the counts
    in :py:attr:`groups`.
    """

    groups: Sequence[NexusOperationExecutionCountAggregationGroup]
    """Groups if the query had a group-by clause, or empty if not."""

    @staticmethod
    def _from_raw(
        resp: temporalio.api.workflowservice.v1.CountNexusOperationExecutionsResponse,
    ) -> NexusOperationExecutionCount:
        """Create from raw proto response."""
        return NexusOperationExecutionCount(
            count=resp.count,
            groups=[
                NexusOperationExecutionCountAggregationGroup._from_raw(g)
                for g in resp.groups
            ],
        )


class NexusOperationFailureError(temporalio.exceptions.TemporalError):
    """Error that occurs when a Nexus operation is unsuccessful.

    .. warning::
       This API is experimental and unstable.
    """

    def __init__(self, *, cause: BaseException) -> None:
        """Create Nexus operation failure error."""
        super().__init__("Nexus operation execution failed")
        self.__cause__ = cause

    @property
    def cause(self) -> BaseException:
        """Cause of the Nexus operation failure."""
        assert self.__cause__
        return self.__cause__


class NexusClient(ABC, Generic[NexusServiceType]):
    """Client for starting standalone Nexus operations.

    .. warning::
       This API is experimental and unstable.

    Use :py:meth:`Client.create_nexus_client` to create a client.
    """

    # Overload for nexusrpc.Operation
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: nexusrpc.Operation[InputT, OutputT],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for string operation name
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: str,
        arg: Any,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        result_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for workflow_run_operation methods
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType, temporalio.nexus.WorkflowRunOperationContext, InputT],
            Awaitable[temporalio.nexus.WorkflowHandle[OutputT]],
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for sync_operation methods (async def)
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            Awaitable[OutputT],
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for sync_operation methods (def)
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            OutputT,
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for operation_handler
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType], nexusrpc.handler.OperationHandler[InputT, OutputT]
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for temporal_operation methods
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [
                NexusServiceType,
                temporalio.nexus.TemporalStartOperationContext,
                temporalio.nexus.TemporalNexusClient,
                InputT,
            ],
            Awaitable[temporalio.nexus.TemporalOperationResult[OutputT]],
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    @abstractmethod
    async def start_operation(
        self,
        operation: nexusrpc.Operation[Any, Any] | str | Callable[..., Any],
        arg: Any,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[Any]:
        """Start a Nexus operation and return a handle.

        .. warning::
           This API is experimental and unstable.

        Args:
            operation: The operation to start. Can be a ``nexusrpc.Operation``,
                a callable operation method, or a string name.
            arg: Input argument for the operation.
            id: Unique identifier for this operation.
            id_reuse_policy: Policy for reusing operation IDs.
            id_conflict_policy: Policy for handling ID conflicts.
            result_type: For string operation names, this can set the specific
                result type hint to deserialize into.
            schedule_to_close_timeout: End-to-end timeout for the Nexus
                operation. If unset, defaults to the maximum allowed by the
                Temporal server.
            schedule_to_start_timeout: Maximum time to wait for the operation
                to be started (or completed, if synchronous) by the handler. If
                unset, no schedule-to-start timeout is enforced.
            start_to_close_timeout: Maximum time to wait for an asynchronous
                operation to complete after it has been started. Only applies to
                asynchronous operations and is ignored for synchronous
                operations. If unset, no start-to-close timeout is enforced.
            search_attributes: Search attributes for the operation.
            summary: Summary for the operation.
            headers: Headers to attach to the Nexus request.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            A handle to the started operation.
        """
        ...

    # Overload for nexusrpc.Operation
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: nexusrpc.Operation[InputT, OutputT],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> OutputT: ...

    # Overload for string operation name
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: str,
        arg: Any,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        result_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> OutputT: ...

    # Overload for workflow_run_operation methods
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType, temporalio.nexus.WorkflowRunOperationContext, InputT],
            Awaitable[temporalio.nexus.WorkflowHandle[OutputT]],
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> OutputT: ...

    # Overload for sync_operation methods (async def)
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            Awaitable[OutputT],
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> OutputT: ...

    # Overload for sync_operation methods (async def)
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            OutputT,
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> OutputT: ...

    # Overload for operation_handler
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType],
            nexusrpc.handler.OperationHandler[InputT, OutputT],
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> OutputT: ...

    # Overload for temporal_operation methods
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [
                NexusServiceType,
                temporalio.nexus.TemporalStartOperationContext,
                temporalio.nexus.TemporalNexusClient,
                InputT,
            ],
            Awaitable[temporalio.nexus.TemporalOperationResult[OutputT]],
        ],
        arg: InputT,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> OutputT: ...

    @abstractmethod
    async def execute_operation(
        self,
        operation: nexusrpc.Operation[Any, Any] | str | Callable[..., Any],
        arg: Any,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any:
        """Start a Nexus operation and wait for its result.

        .. warning::
           This API is experimental and unstable.

        This is a shortcut for ``await (await nexus_client.start_operation(...)).result()``.

        Args:
            operation: The operation to execute. Can be a ``nexusrpc.Operation``,
                a callable operation method, or a string name.
            arg: Input argument for the operation.
            id: Unique identifier for this operation.
            id_reuse_policy: Policy for reusing operation IDs.
            id_conflict_policy: Policy for handling ID conflicts.
            result_type: For string operation names, this can set the specific
                result type hint to deserialize into.
            schedule_to_close_timeout: End-to-end timeout for the Nexus
                operation. If unset, defaults to the maximum allowed by the
                Temporal server.
            schedule_to_start_timeout: Maximum time to wait for the operation
                to be started (or completed, if synchronous) by the handler. If
                unset, no schedule-to-start timeout is enforced.
            start_to_close_timeout: Maximum time to wait for an asynchronous
                operation to complete after it has been started. Only applies to
                asynchronous operations and is ignored for synchronous
                operations. If unset, no start-to-close timeout is enforced.
            search_attributes: Search attributes for the operation.
            summary: Summary for the operation.
            headers: Headers to attach to the Nexus request.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            The result of the operation.
        """
        ...


class _NexusClient(NexusClient[NexusServiceType]):  # pyright: ignore[reportUnusedClass]
    """Concrete implementation of NexusClient."""

    def __init__(
        self,
        client: Client,
        service: type[NexusServiceType] | str,
        endpoint: str,
    ) -> None:
        self._client = client
        if isinstance(service, str):
            self._service_name = service
        elif service_defn := nexusrpc.get_service_definition(service):
            self._service_name = service_defn.name
        else:
            self._service_name = service.__name__
        self._endpoint = endpoint

    def _resolve_operation(
        self,
        operation: nexusrpc.Operation[Any, Any] | str | Callable[..., Any],
    ) -> tuple[str, type | None]:
        """Resolve an operation to its name and output type."""
        if isinstance(operation, str):
            return operation, None
        elif isinstance(operation, nexusrpc.Operation):
            return operation.name, operation.output_type
        elif callable(operation):
            _, op = temporalio.nexus._util.get_operation_factory(operation)
            if isinstance(op, nexusrpc.Operation):
                return op.name, op.output_type
            else:
                raise ValueError(
                    f"Operation callable is not a Nexus operation: {operation}"
                )
        else:
            raise ValueError(  # pyright: ignore[reportUnreachable]
                f"Operation is not resolvable as a Nexus operation: {operation}"
            )

    async def start_operation(
        self,
        operation: nexusrpc.Operation[Any, Any] | str | Callable[..., Any],
        arg: Any,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationHandle[Any]:
        """Start a Nexus operation and return a handle.

        .. warning::
           This API is experimental and unstable.
        """
        op_name, output_type = self._resolve_operation(operation)
        final_result_type: type | None = (
            result_type if isinstance(operation, str) else output_type
        )

        return await self._client._impl.start_nexus_operation(
            StartNexusOperationInput(
                operation=op_name,
                arg=arg,
                id=id,
                endpoint=self._endpoint,
                service=self._service_name,
                result_type=final_result_type,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                id_reuse_policy=id_reuse_policy,
                id_conflict_policy=id_conflict_policy,
                search_attributes=search_attributes,
                summary=summary,
                headers=dict(headers) if headers else {},
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def execute_operation(
        self,
        operation: nexusrpc.Operation[Any, Any] | str | Callable[..., Any],
        arg: Any,
        *,
        id: str,
        id_reuse_policy: temporalio.common.NexusOperationIDReusePolicy = temporalio.common.NexusOperationIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.NexusOperationIDConflictPolicy = temporalio.common.NexusOperationIDConflictPolicy.FAIL,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        search_attributes: temporalio.common.TypedSearchAttributes | None = None,
        summary: str | None = None,
        headers: Mapping[str, str] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any:
        """Start a Nexus operation and wait for its result.

        .. warning::
           This API is experimental and unstable.
        """
        handle = await self.start_operation(
            operation,
            arg,
            id=id,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            result_type=result_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            search_attributes=search_attributes,
            summary=summary,
            headers=headers,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()


class NexusOperationHandle(Generic[ReturnType]):
    """Handle representing a standalone Nexus operation execution.

    .. warning::
       This API is experimental and unstable.
    """

    def __init__(
        self,
        client: Client,
        operation_id: str,
        *,
        run_id: str | None = None,
        result_type: type | None = None,
        endpoint: str = "",
        service: str = "",
    ) -> None:
        """Create nexus operation handle."""
        self._client = client
        self._operation_id = operation_id
        self._run_id = run_id
        self._result_type = result_type
        self._endpoint = endpoint
        self._service = service
        # the default value is `_arg_unset` because ReturnType could be None
        self._known_outcome: ReturnType | NexusOperationFailureError | object = (
            temporalio.common._arg_unset
        )

    @property
    def operation_id(self) -> str:
        """ID of the operation."""
        return self._operation_id

    @property
    def run_id(self) -> str | None:
        """Run ID of the operation."""
        return self._run_id

    @property
    def endpoint(self) -> str:
        """Endpoint name."""
        return self._endpoint

    @property
    def service(self) -> str:
        """Service name."""
        return self._service

    async def result(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType:
        """Wait for result of the Nexus operation.

        .. warning::
           This API is experimental and unstable.

        The result may already be known if this method has been called before,
        in which case no network call is made. Otherwise the result will be
        polled for until it is available.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call. Note:
                this is the timeout for each RPC call while polling, not a
                timeout for the function as a whole. If an individual RPC
                times out, it will be retried until the result is available.

        Returns:
            The result of the operation.

        Raises:
            NexusOperationFailureError: If the operation completed with a failure.
            RPCError: Operation result could not be fetched for some reason.
        """
        if self._known_outcome is temporalio.common._arg_unset:
            try:
                self._known_outcome = (
                    await self._client._impl.get_nexus_operation_result(
                        GetNexusOperationResultInput(
                            operation_id=self._operation_id,
                            run_id=self._run_id,
                            result_type=self._result_type,
                            rpc_metadata=rpc_metadata,
                            rpc_timeout=rpc_timeout,
                        )
                    )
                )
                return cast(ReturnType, self._known_outcome)
            except NexusOperationFailureError as failure:
                self._known_outcome = failure
                raise
        elif isinstance(self._known_outcome, NexusOperationFailureError):
            raise self._known_outcome
        else:
            return cast(ReturnType, self._known_outcome)

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> NexusOperationExecutionDescription:
        """Describe the Nexus operation execution.

        .. warning::
           This API is experimental and unstable.

        Args:
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Nexus operation execution description.
        """
        return await self._client._impl.describe_nexus_operation(
            DescribeNexusOperationInput(
                operation_id=self._operation_id,
                run_id=self._run_id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def cancel(
        self,
        *,
        reason: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Request cancellation of the Nexus operation.

        .. warning::
           This API is experimental and unstable.

        Args:
            reason: Reason for the cancellation.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.cancel_nexus_operation(
            CancelNexusOperationInput(
                operation_id=self._operation_id,
                run_id=self._run_id,
                reason=reason,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def terminate(
        self,
        *,
        reason: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Terminate the Nexus operation execution immediately.

        .. warning::
           This API is experimental and unstable.

        Args:
            reason: Reason for the termination.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.terminate_nexus_operation(
            TerminateNexusOperationInput(
                operation_id=self._operation_id,
                run_id=self._run_id,
                reason=reason,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )


class NexusOperationExecutionAsyncIterator:
    """Asynchronous iterator for Nexus operation execution values.

    You should typically use ``async for`` on this iterator and not call any of its methods.

    .. warning::
       This API is experimental and unstable.
    """

    def __init__(
        self,
        client: Client,
        input: ListNexusOperationsInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`Client.list_nexus_operations`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: Sequence[NexusOperationExecution] | None = None
        self._current_page_index = 0
        self._limit = input.limit
        self._yielded = 0

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page that will be returned from
        the next :py:meth:`__anext__` call.
        """
        return self._current_page_index

    @property
    def current_page(self) -> Sequence[NexusOperationExecution] | None:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> bytes | None:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: int | None = None) -> None:
        """Fetch the next page of results.

        Args:
            page_size: Override the page size this iterator was originally
                created with.
        """
        page_size = page_size or self._input.page_size
        if self._limit is not None and self._limit - self._yielded < page_size:
            page_size = self._limit - self._yielded

        resp = await self._client.workflow_service.list_nexus_operation_executions(
            temporalio.api.workflowservice.v1.ListNexusOperationExecutionsRequest(
                namespace=self._client.namespace,
                page_size=page_size,
                next_page_token=self._next_page_token or b"",
                query=self._input.query or "",
            ),
            retry=True,
            metadata=self._input.rpc_metadata,
            timeout=self._input.rpc_timeout,
        )

        self._current_page = [
            NexusOperationExecution._from_raw_info(v) for v in resp.operations
        ]
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> NexusOperationExecutionAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> NexusOperationExecution:
        """Get the next execution on this iterator, fetching next page if
        necessary.
        """
        if self._limit is not None and self._yielded >= self._limit:
            raise StopAsyncIteration
        while True:
            # No page? fetch and continue
            if self._current_page is None:
                await self.fetch_next_page()
                continue
            # No more left in page?
            if self._current_page_index >= len(self._current_page):
                # If there is a next page token, try to get another page and try
                # again
                if self._next_page_token is not None:
                    await self.fetch_next_page()
                    continue
                # No more pages means we're done
                raise StopAsyncIteration
            # Get current, increment page index, and return
            ret = self._current_page[self._current_page_index]
            self._current_page_index += 1
            self._yielded += 1
            return ret
