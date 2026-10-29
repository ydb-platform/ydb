"""Client support for accessing Temporal."""

from __future__ import annotations

import asyncio
import functools
import warnings
from collections.abc import (
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    cast,
)

from typing_extensions import Self

import temporalio.api.activity.v1
import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.workflowservice.v1
import temporalio.common
import temporalio.converter
import temporalio.converter._search_attributes
from temporalio.converter import (
    ActivitySerializationContext,
    DataConverter,
    SerializationContext,
    WithSerializationContext,
)
from temporalio.service import (
    RPCError,
    RPCStatusCode,
)

from ..types import (
    ReturnType,
)
from ._exceptions import ActivityFailureError
from ._interceptor import (
    CancelActivityInput,
    CompleteAsyncActivityInput,
    DescribeActivityInput,
    FailAsyncActivityInput,
    HeartbeatAsyncActivityInput,
    ReportCancellationAsyncActivityInput,
    TerminateActivityInput,
)

if TYPE_CHECKING:
    from ._client import Client
    from ._interceptor import ListActivitiesInput


class ActivityExecutionAsyncIterator:
    """Asynchronous iterator for activity execution values.

    You should typically use ``async for`` on this iterator and not call any of its methods.

    .. warning::
       This API is experimental.
    """

    def __init__(
        self,
        client: Client,
        input: ListActivitiesInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`Client.list_activities`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: Sequence[ActivityExecution] | None = None
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
    def current_page(self) -> Sequence[ActivityExecution] | None:
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

        resp = await self._client.workflow_service.list_activity_executions(
            temporalio.api.workflowservice.v1.ListActivityExecutionsRequest(
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
            ActivityExecution._from_raw_info(v, self._client.namespace)
            for v in resp.executions
        ]
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> ActivityExecutionAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> ActivityExecution:
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


@dataclass(frozen=True)
class ActivityExecution:
    """Info for an activity execution not started by a workflow, from list response.

    .. warning::
       This API is experimental.
    """

    activity_id: str
    """Activity ID."""

    activity_run_id: str | None
    """Run ID of the activity."""

    activity_type: str
    """Type name of the activity."""

    close_time: datetime | None
    """Time the activity reached a terminal status, if closed."""

    execution_duration: timedelta | None
    """Duration from scheduled to close time, only populated if closed."""

    namespace: str
    """Namespace of the activity (copied from calling client)."""

    raw_info: (
        temporalio.api.activity.v1.ActivityExecutionListInfo
        | temporalio.api.activity.v1.ActivityExecutionInfo
    )
    """Underlying protobuf info."""

    scheduled_time: datetime
    """Time the activity was originally scheduled."""

    state_transition_count: int | None
    """Number of state transitions, if available."""

    status: ActivityExecutionStatus
    """Current status of the activity."""

    task_queue: str
    """Task queue the activity was scheduled on."""

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Current set of search attributes if any."""

    @classmethod
    def _from_raw_info(
        cls, info: temporalio.api.activity.v1.ActivityExecutionListInfo, namespace: str
    ) -> Self:
        """Create from raw proto activity list info."""
        return cls(
            activity_id=info.activity_id,
            activity_run_id=info.run_id or None,
            activity_type=(
                info.activity_type.name if info.HasField("activity_type") else ""
            ),
            close_time=(
                info.close_time.ToDatetime().replace(tzinfo=timezone.utc)
                if info.HasField("close_time")
                else None
            ),
            execution_duration=(
                info.execution_duration.ToTimedelta()
                if info.HasField("execution_duration")
                else None
            ),
            namespace=namespace,
            raw_info=info,
            scheduled_time=(
                info.schedule_time.ToDatetime().replace(tzinfo=timezone.utc)
                if info.HasField("schedule_time")
                else datetime.min
            ),
            state_transition_count=(
                info.state_transition_count if info.state_transition_count else None
            ),
            status=(
                ActivityExecutionStatus(info.status)
                if info.status
                else ActivityExecutionStatus.UNSPECIFIED
            ),
            task_queue=info.task_queue,
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                info.search_attributes
            ),
        )


@dataclass(frozen=True)
class ActivityExecutionDescription(ActivityExecution):
    """Detailed information about an activity execution not started by a workflow.

    .. warning::
       This API is experimental.
    """

    attempt: int
    """Current attempt number."""

    canceled_reason: str | None
    """Reason for cancellation, if cancel was requested."""

    current_retry_interval: timedelta | None
    """Time until the next retry, if applicable."""

    eager_execution_requested: bool
    """Whether eager execution was requested for this activity."""

    expiration_time: datetime
    """Scheduled time plus schedule_to_close_timeout."""

    last_attempt_complete_time: datetime | None
    """Time when the last attempt completed."""

    last_failure: Exception | None
    """Failure from the last failed attempt, if any."""

    last_heartbeat_time: datetime | None
    """Time of the last heartbeat."""

    last_started_time: datetime | None
    """Time the last attempt was started."""

    last_worker_identity: str
    """Identity of the last worker that processed the activity."""

    next_attempt_schedule_time: datetime | None
    """Time when the next attempt will be scheduled."""

    paused: bool
    """Whether the activity is paused."""

    raw_heartbeat_details: Sequence[temporalio.api.common.v1.Payload]
    """Details from the last heartbeat."""

    retry_policy: temporalio.common.RetryPolicy | None
    """Retry policy for the activity."""

    run_state: PendingActivityState | None
    """More detailed breakdown if status is RUNNING."""

    long_poll_token: bytes | None
    """Token for follow-on long-poll requests. None if the activity is complete."""

    raw_callbacks: Sequence[temporalio.api.activity.v1.CallbackInfo]
    """Underlying protobuf callbacks"""

    @classmethod
    async def _from_execution_info(
        cls,
        info: temporalio.api.activity.v1.ActivityExecutionInfo,
        long_poll_token: bytes | None,
        namespace: str,
        data_converter: temporalio.converter.DataConverter,
        callbacks: Sequence[temporalio.api.activity.v1.CallbackInfo],
    ) -> Self:
        """Create from raw proto activity execution info."""
        # Decode heartbeat details if present
        decoded_heartbeat_details: Sequence[temporalio.api.common.v1.Payload] = (
            info.heartbeat_details.payloads
        )
        if decoded_heartbeat_details and data_converter.payload_codec:
            decoded_heartbeat_details = await data_converter.payload_codec.decode(
                decoded_heartbeat_details
            )

        return cls(
            activity_id=info.activity_id,
            activity_run_id=info.run_id or None,
            activity_type=(
                info.activity_type.name if info.HasField("activity_type") else ""
            ),
            attempt=info.attempt,
            canceled_reason=info.canceled_reason or None,
            close_time=(
                info.close_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("close_time")
                else None
            ),
            current_retry_interval=(
                info.current_retry_interval.ToTimedelta()
                if info.HasField("current_retry_interval")
                else None
            ),
            eager_execution_requested=getattr(info, "eager_execution_requested", False),
            execution_duration=(
                info.execution_duration.ToTimedelta()
                if info.HasField("execution_duration")
                else None
            ),
            expiration_time=(
                info.expiration_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("expiration_time")
                else datetime.min
            ),
            last_attempt_complete_time=(
                info.last_attempt_complete_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("last_attempt_complete_time")
                else None
            ),
            last_failure=(
                cast(
                    Exception | None,
                    await data_converter.decode_failure(info.last_failure),
                )
                if info.HasField("last_failure")
                else None
            ),
            last_heartbeat_time=(
                info.last_heartbeat_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("last_heartbeat_time")
                else None
            ),
            last_started_time=(
                info.last_started_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("last_started_time")
                else None
            ),
            last_worker_identity=info.last_worker_identity,
            long_poll_token=long_poll_token or None,
            namespace=namespace,
            next_attempt_schedule_time=(
                info.next_attempt_schedule_time.ToDatetime(tzinfo=timezone.utc)
                if info.HasField("next_attempt_schedule_time")
                else None
            ),
            paused=getattr(info, "paused", False),
            raw_heartbeat_details=decoded_heartbeat_details,
            raw_info=info,
            retry_policy=temporalio.common.RetryPolicy.from_proto(info.retry_policy)
            if info.HasField("retry_policy")
            else None,
            run_state=(
                PendingActivityState(info.run_state) if info.run_state else None
            ),
            scheduled_time=(info.schedule_time.ToDatetime(tzinfo=timezone.utc)),
            state_transition_count=(
                info.state_transition_count if info.state_transition_count else None
            ),
            status=(
                ActivityExecutionStatus(info.status)
                if info.status
                else ActivityExecutionStatus.UNSPECIFIED
            ),
            task_queue=info.task_queue,
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                info.search_attributes
            ),
            raw_callbacks=callbacks,
        )


class ActivityExecutionStatus(IntEnum):
    """Status of an activity execution.

    .. warning::
       This API is experimental.

    See :py:class:`temporalio.api.enums.v1.ActivityExecutionStatus`.
    """

    UNSPECIFIED = int(
        temporalio.api.enums.v1.ActivityExecutionStatus.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
    )
    RUNNING = int(
        temporalio.api.enums.v1.ActivityExecutionStatus.ACTIVITY_EXECUTION_STATUS_RUNNING
    )
    COMPLETED = int(
        temporalio.api.enums.v1.ActivityExecutionStatus.ACTIVITY_EXECUTION_STATUS_COMPLETED
    )
    FAILED = int(
        temporalio.api.enums.v1.ActivityExecutionStatus.ACTIVITY_EXECUTION_STATUS_FAILED
    )
    CANCELED = int(
        temporalio.api.enums.v1.ActivityExecutionStatus.ACTIVITY_EXECUTION_STATUS_CANCELED
    )
    TERMINATED = int(
        temporalio.api.enums.v1.ActivityExecutionStatus.ACTIVITY_EXECUTION_STATUS_TERMINATED
    )
    TIMED_OUT = int(
        temporalio.api.enums.v1.ActivityExecutionStatus.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
    )


class PendingActivityState(IntEnum):
    """Detailed state of an activity execution that is in ACTIVITY_EXECUTION_STATUS_RUNNING.

    .. warning::
       This API is experimental.

    See :py:class:`temporalio.api.enums.v1.PendingActivityState`.
    """

    UNSPECIFIED = int(
        temporalio.api.enums.v1.PendingActivityState.PENDING_ACTIVITY_STATE_UNSPECIFIED
    )
    SCHEDULED = int(
        temporalio.api.enums.v1.PendingActivityState.PENDING_ACTIVITY_STATE_SCHEDULED
    )
    STARTED = int(
        temporalio.api.enums.v1.PendingActivityState.PENDING_ACTIVITY_STATE_STARTED
    )
    CANCEL_REQUESTED = int(
        temporalio.api.enums.v1.PendingActivityState.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
    )
    PAUSED = int(
        temporalio.api.enums.v1.PendingActivityState.PENDING_ACTIVITY_STATE_PAUSED
    )
    PAUSE_REQUESTED = int(
        temporalio.api.enums.v1.PendingActivityState.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
    )


@dataclass(frozen=True)
class ActivityExecutionCount:
    """Representation of a count from a count activities call.

    .. warning::
       This API is experimental.
    """

    count: int
    """Total count matching the filter, if any."""

    groups: Sequence[ActivityExecutionCountAggregationGroup]
    """Aggregation groups if requested."""

    @staticmethod
    def _from_raw(
        resp: temporalio.api.workflowservice.v1.CountActivityExecutionsResponse,
    ) -> ActivityExecutionCount:
        """Create from raw proto response."""
        return ActivityExecutionCount(
            count=resp.count,
            groups=[
                ActivityExecutionCountAggregationGroup._from_raw(g) for g in resp.groups
            ],
        )


@dataclass(frozen=True)
class ActivityExecutionCountAggregationGroup:
    """A single aggregation group from a count activities call.

    .. warning::
       This API is experimental.
    """

    count: int
    """Count for this group."""

    group_values: Sequence[temporalio.common.SearchAttributeValue]
    """Values that define this group."""

    @staticmethod
    def _from_raw(
        raw: temporalio.api.workflowservice.v1.CountActivityExecutionsResponse.AggregationGroup,
    ) -> ActivityExecutionCountAggregationGroup:
        return ActivityExecutionCountAggregationGroup(
            count=raw.count,
            group_values=[
                temporalio.converter._search_attributes._decode_search_attribute_value(
                    v
                )
                for v in raw.group_values
            ],
        )


@dataclass(frozen=True)
class AsyncActivityIDReference:
    """Reference to an async activity by its qualified ID."""

    workflow_id: str | None
    run_id: str | None
    activity_id: str


class AsyncActivityHandle(WithSerializationContext):
    """Handle representing an external activity for completion and heartbeat."""

    def __init__(
        self,
        client: Client,
        id_or_token: AsyncActivityIDReference | bytes,
        data_converter_override: DataConverter | None = None,
    ) -> None:
        """Create an async activity handle."""
        self._client = client
        self._id_or_token = id_or_token
        self._data_converter_override = data_converter_override

    async def heartbeat(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Record a heartbeat for the activity.

        Args:
            details: Details of the heartbeat.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.heartbeat_async_activity(
            HeartbeatAsyncActivityInput(
                id_or_token=self._id_or_token,
                details=details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                data_converter_override=self._data_converter_override,
            ),
        )

    async def complete(
        self,
        result: Any | None = temporalio.common._arg_unset,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Complete the activity.

        Args:
            result: Result of the activity if any.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.complete_async_activity(
            CompleteAsyncActivityInput(
                id_or_token=self._id_or_token,
                result=result,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                data_converter_override=self._data_converter_override,
            ),
        )

    async def fail(
        self,
        error: Exception,
        *,
        last_heartbeat_details: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Fail the activity.

        Args:
            error: Error for the activity.
            last_heartbeat_details: Last heartbeat details for the activity.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.fail_async_activity(
            FailAsyncActivityInput(
                id_or_token=self._id_or_token,
                error=error,
                last_heartbeat_details=last_heartbeat_details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                data_converter_override=self._data_converter_override,
            ),
        )

    async def report_cancellation(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Report the activity as cancelled.

        Args:
            details: Cancellation details.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.report_cancellation_async_activity(
            ReportCancellationAsyncActivityInput(
                id_or_token=self._id_or_token,
                details=details,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                data_converter_override=self._data_converter_override,
            ),
        )

    def with_context(self, context: SerializationContext) -> Self:
        """Create a new AsyncActivityHandle with a different serialization context.

        Payloads received by the activity will be decoded and deserialized using a data converter
        with :py:class:`ActivitySerializationContext` set as context. If you are using a custom data
        converter that makes use of this context then you can use this method to supply matching
        context data to the data converter used to serialize and encode the outbound payloads.
        """
        data_converter = self._client.data_converter.with_context(context)
        if data_converter is self._client.data_converter:
            return self
        cls = type(self)
        if cls.__init__ is not AsyncActivityHandle.__init__:
            raise TypeError(
                "If you have subclassed AsyncActivityHandle and overridden the __init__ method "
                "then you must override with_context to return an instance of your class."
            )
        return cls(
            self._client,
            self._id_or_token,
            data_converter,
        )


class ActivityHandle(Generic[ReturnType]):
    """Handle representing an activity execution not started by a workflow.

    .. warning::
       This API is experimental.
    """

    def __init__(
        self,
        client: Client,
        id: str,
        *,
        run_id: str | None = None,
        result_type: type | None = None,
    ) -> None:
        """Create activity handle."""
        self._client = client
        self._id = id
        self._run_id = run_id
        self._result_type = result_type
        self._known_outcome: (
            temporalio.api.activity.v1.ActivityExecutionOutcome | None
        ) = None

    @functools.cached_property
    def _data_converter(self) -> temporalio.converter.DataConverter:
        return self._client.data_converter.with_context(
            ActivitySerializationContext(
                namespace=self._client.namespace,
                activity_id=self._id,
                activity_type=None,
                activity_task_queue=None,
                is_local=False,
                workflow_id=None,
                workflow_type=None,
            )
        )

    @property
    def id(self) -> str:
        """ID of the activity."""
        return self._id

    @property
    def run_id(self) -> str | None:
        """Run ID of the activity."""
        return self._run_id

    async def result(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType:
        """Wait for result of the activity.

        .. warning::
           This API is experimental.

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
            The result of the activity.

        Raises:
            ActivityFailureError: If the activity completed with a failure.
            RPCError: Activity result could not be fetched for some reason.
        """
        await self._poll_until_outcome(
            rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
        )

        # Convert outcome to failure or value
        assert self._known_outcome
        if self._known_outcome.HasField("failure"):
            raise ActivityFailureError(
                cause=await self._data_converter.decode_failure(
                    self._known_outcome.failure
                ),
            )
        if not self._known_outcome.result.payloads:
            return None  # type: ignore
        type_hints = [self._result_type] if self._result_type else None
        results = await self._data_converter.decode(
            self._known_outcome.result.payloads, type_hints
        )
        if not results:
            return None  # type: ignore
        elif len(results) > 1:
            warnings.warn(f"Expected single activity result, got {len(results)}")
        return results[0]

    async def _poll_until_outcome(
        self,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Poll for activity result until it's available."""
        if self._known_outcome:
            return

        req = temporalio.api.workflowservice.v1.PollActivityExecutionRequest(
            namespace=self._client.namespace,
            activity_id=self._id,
            run_id=self._run_id or "",
        )

        # Continue polling as long as we have no outcome
        while True:
            try:
                res = await self._client.workflow_service.poll_activity_execution(
                    req,
                    retry=True,
                    metadata=rpc_metadata,
                    timeout=rpc_timeout,
                )
                if res.HasField("outcome"):
                    self._known_outcome = res.outcome
                    return
            except RPCError as err:
                if err.status == RPCStatusCode.DEADLINE_EXCEEDED:
                    # Deadline exceeded is expected with long polling; retry
                    continue
                elif err.status == RPCStatusCode.CANCELLED:
                    raise asyncio.CancelledError() from err
                else:
                    raise
            except asyncio.CancelledError:
                raise

    async def cancel(
        self,
        *,
        reason: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Request cancellation of the activity.

        .. warning::
           This API is experimental.

        Requesting cancellation of an activity does not automatically transition the activity to
        canceled status. If the activity is heartbeating, a :py:class:`exceptions.CancelledError`
        exception will be raised when receiving the heartbeat response; if the activity allows this
        exception to bubble out, the activity will transition to canceled status. If the activity it
        is not heartbeating, this method will have no effect on activity status.

        Args:
            reason: Reason for the cancellation. Recorded and available via describe.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.cancel_activity(
            CancelActivityInput(
                activity_id=self._id,
                activity_run_id=self._run_id,
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
        """Terminate the activity execution immediately.

        .. warning::
           This API is experimental.

        Termination does not reach the worker and the activity code cannot react to it.
        A terminated activity may have a running attempt and will be requested to be
        canceled by the server when it heartbeats.

        Args:
            reason: Reason for the termination.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.terminate_activity(
            TerminateActivityInput(
                activity_id=self._id,
                activity_run_id=self._run_id,
                reason=reason,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def describe(
        self,
        *,
        long_poll_token: bytes | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ActivityExecutionDescription:
        """Describe the activity execution.

        .. warning::
           This API is experimental.

        Args:
            long_poll_token: Token from a previous describe response. If provided,
                the request will long-poll until the activity state changes.
            rpc_metadata: Headers used on the RPC call.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Activity execution description.
        """
        return await self._client._impl.describe_activity(
            DescribeActivityInput(
                activity_id=self._id,
                activity_run_id=self._run_id,
                long_poll_token=long_poll_token,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )
