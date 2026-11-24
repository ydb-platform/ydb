"""Client support for accessing Temporal."""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import (
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    overload,
)

import google.protobuf.duration_pb2
import google.protobuf.timestamp_pb2

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.schedule.v1
import temporalio.api.taskqueue.v1
import temporalio.api.workflow.v1
import temporalio.api.workflowservice.v1
import temporalio.common
import temporalio.converter
import temporalio.workflow
from temporalio.converter import (
    StorageDriverStoreContext,
    StorageDriverWorkflowInfo,
    WorkflowSerializationContext,
)

from ..common import HeaderCodecBehavior
from ..types import (
    AnyType,
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MultiParamSpec,
    ParamType,
    ReturnType,
    SelfType,
)
from ._helpers import _apply_headers, _encode_user_metadata
from ._interceptor import (
    BackfillScheduleInput,
    DeleteScheduleInput,
    DescribeScheduleInput,
    PauseScheduleInput,
    TriggerScheduleInput,
    UnpauseScheduleInput,
    UpdateScheduleInput,
)

if TYPE_CHECKING:
    from ._client import Client
    from ._interceptor import ListSchedulesInput


class ScheduleHandle:
    """Handle for interacting with a schedule.

    This is usually created via :py:meth:`Client.get_schedule_handle` or
    returned from :py:meth:`Client.create_schedule`.

    Attributes:
        id: ID of the schedule.
    """

    def __init__(self, client: Client, id: str) -> None:
        """Create schedule handle."""
        self._client = client
        self.id = id

    async def backfill(
        self,
        *backfill: ScheduleBackfill,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Backfill the schedule by going through the specified time periods as
        if they passed right now.

        Args:
            backfill: Backfill periods.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        if not backfill:
            raise ValueError("At least one backfill required")
        await self._client._impl.backfill_schedule(
            BackfillScheduleInput(
                id=self.id,
                backfills=backfill,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def delete(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Delete this schedule.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.delete_schedule(
            DeleteScheduleInput(
                id=self.id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ScheduleDescription:
        """Fetch this schedule's description.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        return await self._client._impl.describe_schedule(
            DescribeScheduleInput(
                id=self.id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def pause(
        self,
        *,
        note: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Pause the schedule and set a note.

        Args:
            note: Note to set on the schedule.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.pause_schedule(
            PauseScheduleInput(
                id=self.id,
                note=note,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def trigger(
        self,
        *,
        overlap: ScheduleOverlapPolicy | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Trigger an action on this schedule to happen immediately.

        Args:
            overlap: If set, overrides the schedule's overlap policy.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.trigger_schedule(
            TriggerScheduleInput(
                id=self.id,
                overlap=overlap,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    async def unpause(
        self,
        *,
        note: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Unpause the schedule and set a note.

        Args:
            note: Note to set on the schedule.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.
        """
        await self._client._impl.unpause_schedule(
            UnpauseScheduleInput(
                id=self.id,
                note=note,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )

    @overload
    async def update(
        self,
        updater: Callable[[ScheduleUpdateInput], ScheduleUpdate | None],
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None: ...

    @overload
    async def update(
        self,
        updater: Callable[[ScheduleUpdateInput], Awaitable[ScheduleUpdate | None]],
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None: ...

    async def update(
        self,
        updater: Callable[
            [ScheduleUpdateInput],
            ScheduleUpdate | None | Awaitable[ScheduleUpdate | None],
        ],
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Update a schedule using a callback to build the update from the
        description.

        The callback may be invoked multiple times in a conflict-resolution
        loop.

        Args:
            updater: Callback that returns the update. It accepts a
                :py:class:`ScheduleUpdateInput` and returns a
                :py:class:`ScheduleUpdate`. If None is returned or an error
                occurs, the update is not attempted. This may be called multiple
                times.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys. This is for every call made
                within.
            rpc_timeout: Optional RPC deadline to set for the RPC call. This is
                for each call made within, not overall.
        """
        await self._client._impl.update_schedule(
            UpdateScheduleInput(
                id=self.id,
                updater=updater,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )


@dataclass
class ScheduleSpec:
    """Specification of the times scheduled actions may occur.

    The times are the union of :py:attr:`calendars`, :py:attr:`intervals`, and
    :py:attr:`cron_expressions` excluding anything in :py:attr:`skip`.
    """

    calendars: Sequence[ScheduleCalendarSpec] = dataclasses.field(default_factory=list)
    """Calendar-based specification of times."""

    intervals: Sequence[ScheduleIntervalSpec] = dataclasses.field(default_factory=list)
    """Interval-based specification of times."""

    cron_expressions: Sequence[str] = dataclasses.field(default_factory=list)
    """Cron-based specification of times.

    This is provided for easy migration from legacy string-based cron
    scheduling. New uses should use :py:attr:`calendars` instead. These
    expressions will be translated to calendar-based specifications on the
    server.
    """

    skip: Sequence[ScheduleCalendarSpec] = dataclasses.field(default_factory=list)
    """Set of matching calendar times that will be skipped."""

    start_at: datetime | None = None
    """Time before which any matching times will be skipped."""

    end_at: datetime | None = None
    """Time after which any matching times will be skipped."""

    jitter: timedelta | None = None
    """Jitter to apply each action.

    An action's scheduled time will be incremented by a random value between 0
    and this value if present (but not past the next schedule).
    """

    time_zone_name: str | None = None
    """IANA time zone name, for example ``US/Central``."""

    @staticmethod
    def _from_proto(spec: temporalio.api.schedule.v1.ScheduleSpec) -> ScheduleSpec:
        return ScheduleSpec(
            calendars=[
                ScheduleCalendarSpec._from_proto(c) for c in spec.structured_calendar
            ],
            intervals=[ScheduleIntervalSpec._from_proto(i) for i in spec.interval],
            cron_expressions=spec.cron_string,
            skip=[
                ScheduleCalendarSpec._from_proto(c)
                for c in spec.exclude_structured_calendar
            ],
            start_at=spec.start_time.ToDatetime().replace(tzinfo=timezone.utc)
            if spec.HasField("start_time")
            else None,
            end_at=spec.end_time.ToDatetime().replace(tzinfo=timezone.utc)
            if spec.HasField("end_time")
            else None,
            jitter=spec.jitter.ToTimedelta() if spec.HasField("jitter") else None,
            time_zone_name=spec.timezone_name or None,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.ScheduleSpec:
        start_time: google.protobuf.timestamp_pb2.Timestamp | None = None
        if self.start_at:
            start_time = google.protobuf.timestamp_pb2.Timestamp()
            start_time.FromDatetime(self.start_at)
        end_time: google.protobuf.timestamp_pb2.Timestamp | None = None
        if self.end_at:
            end_time = google.protobuf.timestamp_pb2.Timestamp()
            end_time.FromDatetime(self.end_at)
        jitter: google.protobuf.duration_pb2.Duration | None = None
        if self.jitter:
            jitter = google.protobuf.duration_pb2.Duration()
            jitter.FromTimedelta(self.jitter)
        return temporalio.api.schedule.v1.ScheduleSpec(
            structured_calendar=[cal._to_proto() for cal in self.calendars],
            cron_string=self.cron_expressions,
            interval=[i._to_proto() for i in self.intervals],
            exclude_structured_calendar=[cal._to_proto() for cal in self.skip],
            start_time=start_time,
            end_time=end_time,
            jitter=jitter,
            timezone_name=self.time_zone_name or "",
        )


@dataclass(frozen=True)
class ScheduleRange:
    """Inclusive range for a schedule match value."""

    start: int
    """Inclusive start of the range."""

    end: int = 0
    """Inclusive end of the range.

    If unset or less than start, defaults to start.
    """

    step: int = 0
    """
    Step to take between each value.

    Unset or 0 defaults as 1.
    """

    def __post_init__(self):
        """Set field defaults."""
        # Class is frozen, so we must setattr bypassing dataclass setattr
        if self.end < self.start:
            object.__setattr__(self, "end", self.start)
        if self.step == 0:
            object.__setattr__(self, "step", 1)

    @staticmethod
    def _from_protos(
        ranges: Sequence[temporalio.api.schedule.v1.Range],
    ) -> Sequence[ScheduleRange]:
        return tuple(ScheduleRange._from_proto(r) for r in ranges)

    @staticmethod
    def _from_proto(range: temporalio.api.schedule.v1.Range) -> ScheduleRange:
        return ScheduleRange(start=range.start, end=range.end, step=range.step)

    @staticmethod
    def _to_protos(
        ranges: Sequence[ScheduleRange],
    ) -> Sequence[temporalio.api.schedule.v1.Range]:
        return tuple(r._to_proto() for r in ranges)

    def _to_proto(self) -> temporalio.api.schedule.v1.Range:
        return temporalio.api.schedule.v1.Range(
            start=self.start, end=self.end, step=self.step
        )


@dataclass
class ScheduleCalendarSpec:
    """Specification relative to calendar time when to run an action.

    A timestamp matches if at least one range of each field matches except for
    year. If year is missing, that means all years match. For all fields besides
    year, at least one range must be present to match anything.
    """

    second: Sequence[ScheduleRange] = (ScheduleRange(0),)
    """Second range to match, 0-59. Default matches 0."""

    minute: Sequence[ScheduleRange] = (ScheduleRange(0),)
    """Minute range to match, 0-59. Default matches 0."""

    hour: Sequence[ScheduleRange] = (ScheduleRange(0),)
    """Hour range to match, 0-23. Default matches 0."""

    day_of_month: Sequence[ScheduleRange] = (ScheduleRange(1, 31),)
    """Day of month range to match, 1-31. Default matches all days."""

    month: Sequence[ScheduleRange] = (ScheduleRange(1, 12),)
    """Month range to match, 1-12. Default matches all months."""

    year: Sequence[ScheduleRange] = ()
    """Optional year range to match. Default of empty matches all years."""

    day_of_week: Sequence[ScheduleRange] = (ScheduleRange(0, 6),)
    """Day of week range to match, 0-6, 0 is Sunday. Default matches all
    days."""

    comment: str | None = None
    """Description of this schedule."""

    @staticmethod
    def _from_proto(
        spec: temporalio.api.schedule.v1.StructuredCalendarSpec,
    ) -> ScheduleCalendarSpec:
        return ScheduleCalendarSpec(
            second=ScheduleRange._from_protos(spec.second),
            minute=ScheduleRange._from_protos(spec.minute),
            hour=ScheduleRange._from_protos(spec.hour),
            day_of_month=ScheduleRange._from_protos(spec.day_of_month),
            month=ScheduleRange._from_protos(spec.month),
            year=ScheduleRange._from_protos(spec.year),
            day_of_week=ScheduleRange._from_protos(spec.day_of_week),
            comment=spec.comment or None,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.StructuredCalendarSpec:
        return temporalio.api.schedule.v1.StructuredCalendarSpec(
            second=ScheduleRange._to_protos(self.second),
            minute=ScheduleRange._to_protos(self.minute),
            hour=ScheduleRange._to_protos(self.hour),
            day_of_month=ScheduleRange._to_protos(self.day_of_month),
            month=ScheduleRange._to_protos(self.month),
            year=ScheduleRange._to_protos(self.year),
            day_of_week=ScheduleRange._to_protos(self.day_of_week),
            comment=self.comment or "",
        )


@dataclass
class ScheduleIntervalSpec:
    """Specification for scheduling on an interval.

    Matches times expressed as epoch + (n * every) + offset.
    """

    every: timedelta
    """Period to repeat the interval."""

    offset: timedelta | None = None
    """Fixed offset added to each interval period."""

    @staticmethod
    def _from_proto(
        spec: temporalio.api.schedule.v1.IntervalSpec,
    ) -> ScheduleIntervalSpec:
        return ScheduleIntervalSpec(
            every=spec.interval.ToTimedelta(),
            offset=spec.phase.ToTimedelta() if spec.HasField("phase") else None,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.IntervalSpec:
        interval = google.protobuf.duration_pb2.Duration()
        interval.FromTimedelta(self.every)
        phase: google.protobuf.duration_pb2.Duration | None = None
        if self.offset:
            phase = google.protobuf.duration_pb2.Duration()
            phase.FromTimedelta(self.offset)
        return temporalio.api.schedule.v1.IntervalSpec(interval=interval, phase=phase)


class ScheduleAction(ABC):
    """Base class for an action a schedule can take.

    See :py:class:`ScheduleActionStartWorkflow` for the most commonly used
    implementation.
    """

    @staticmethod
    def _from_proto(
        action: temporalio.api.schedule.v1.ScheduleAction,
    ) -> ScheduleAction:
        if action.HasField("start_workflow"):
            return ScheduleActionStartWorkflow._from_proto(action.start_workflow)
        else:
            raise ValueError(f"Unsupported action: {action.WhichOneof('action')}")

    @abstractmethod
    async def _to_proto(
        self, client: Client
    ) -> temporalio.api.schedule.v1.ScheduleAction: ...


@dataclass
class ScheduleActionStartWorkflow(ScheduleAction):
    """Schedule action to start a workflow."""

    workflow: str
    args: Sequence[Any] | Sequence[temporalio.api.common.v1.Payload]
    id: str
    task_queue: str
    execution_timeout: timedelta | None
    run_timeout: timedelta | None
    task_timeout: timedelta | None
    retry_policy: temporalio.common.RetryPolicy | None
    memo: None | (Mapping[str, Any] | Mapping[str, temporalio.api.common.v1.Payload])
    typed_search_attributes: temporalio.common.TypedSearchAttributes
    untyped_search_attributes: temporalio.common.SearchAttributes
    """This is deprecated and is only present in case existing untyped
    attributes already exist for update. This should never be used when
    creating."""
    static_summary: str | temporalio.api.common.v1.Payload | None
    static_details: str | temporalio.api.common.v1.Payload | None
    priority: temporalio.common.Priority

    headers: Mapping[str, temporalio.api.common.v1.Payload] | None
    """
    Headers may still be encoded by the payload codec if present.
    """
    _from_raw: bool = dataclasses.field(compare=False, init=False)

    @staticmethod
    def _from_proto(  # pyright: ignore
        info: temporalio.api.workflow.v1.NewWorkflowExecutionInfo,  # type: ignore[override]
    ) -> ScheduleActionStartWorkflow:
        return ScheduleActionStartWorkflow("<unset>", raw_info=info)

    # Overload for no-param workflow
    @overload
    def __init__(
        self,
        workflow: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        memo: Mapping[str, Any] | None = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for single-param workflow
    @overload
    def __init__(
        self,
        workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        memo: Mapping[str, Any] | None = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for multi-param workflow
    @overload
    def __init__(
        self,
        workflow: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        memo: Mapping[str, Any] | None = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for string-name workflow
    @overload
    def __init__(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        memo: Mapping[str, Any] | None = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None: ...

    # Overload for raw info
    @overload
    def __init__(
        self,
        workflow: str,
        *,
        raw_info: temporalio.api.workflow.v1.NewWorkflowExecutionInfo,
    ) -> None: ...

    def __init__(
        self,
        workflow: str | Callable[..., Awaitable[Any]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str | None = None,
        task_queue: str | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        memo: Mapping[str, Any] | None = None,
        typed_search_attributes: temporalio.common.TypedSearchAttributes = temporalio.common.TypedSearchAttributes.empty,
        untyped_search_attributes: temporalio.common.SearchAttributes = {},
        static_summary: str | None = None,
        static_details: str | None = None,
        headers: Mapping[str, temporalio.api.common.v1.Payload] | None = None,
        raw_info: temporalio.api.workflow.v1.NewWorkflowExecutionInfo | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None:
        """Create a start-workflow action.

        See :py:meth:`Client.start_workflow` for details on these parameter
        values.
        """
        super().__init__()
        if raw_info:
            self._from_raw = True
            # Ignore other fields
            self.workflow = raw_info.workflow_type.name
            self.args = raw_info.input.payloads if raw_info.input else []
            self.id = raw_info.workflow_id
            self.task_queue = raw_info.task_queue.name
            self.execution_timeout = (
                raw_info.workflow_execution_timeout.ToTimedelta()
                if raw_info.HasField("workflow_execution_timeout")
                else None
            )
            self.run_timeout = (
                raw_info.workflow_run_timeout.ToTimedelta()
                if raw_info.HasField("workflow_run_timeout")
                else None
            )
            self.task_timeout = (
                raw_info.workflow_task_timeout.ToTimedelta()
                if raw_info.HasField("workflow_task_timeout")
                else None
            )
            self.retry_policy = (
                temporalio.common.RetryPolicy.from_proto(raw_info.retry_policy)
                if raw_info.HasField("retry_policy")
                else None
            )
            self.memo = raw_info.memo.fields if raw_info.memo.fields else None
            self.typed_search_attributes = (
                temporalio.converter.decode_typed_search_attributes(
                    raw_info.search_attributes
                )
            )
            self.headers = raw_info.header.fields if raw_info.header.fields else None
            # Also set the untyped attributes as the set of attributes from
            # decode with the typed ones removed
            self.untyped_search_attributes = (
                temporalio.converter.decode_search_attributes(
                    raw_info.search_attributes
                )
            )
            for pair in self.typed_search_attributes:
                if pair.key.name in self.untyped_search_attributes:
                    # We know this is mutable here
                    del self.untyped_search_attributes[pair.key.name]  # type: ignore
            self.static_summary = (
                raw_info.user_metadata.summary
                if raw_info.HasField("user_metadata") and raw_info.user_metadata.summary
                else None
            )
            self.static_details = (
                raw_info.user_metadata.details
                if raw_info.HasField("user_metadata") and raw_info.user_metadata.details
                else None
            )
            self.priority = (
                temporalio.common.Priority._from_proto(raw_info.priority)
                if raw_info.HasField("priority") and raw_info.priority
                else temporalio.common.Priority.default
            )
        else:
            self._from_raw = False
            if not id:
                raise ValueError("ID required")
            if not task_queue:
                raise ValueError("Task queue required")
            # Use definition if callable
            if callable(workflow):
                defn = temporalio.workflow._Definition.must_from_run_fn(workflow)
                if not defn.name:
                    raise ValueError("Cannot schedule dynamic workflow explicitly")
                workflow = defn.name
            elif not isinstance(workflow, str):
                raise TypeError("Workflow must be a string or callable")  # type:ignore[reportUnreachable]
            self.workflow = workflow
            self.args = temporalio.common._arg_or_args(arg, args)
            self.id = id
            self.task_queue = task_queue
            self.execution_timeout = execution_timeout
            self.run_timeout = run_timeout
            self.task_timeout = task_timeout
            self.retry_policy = retry_policy
            self.memo = memo
            self.typed_search_attributes = typed_search_attributes
            self.untyped_search_attributes = untyped_search_attributes
            self.headers = headers  # encode here
            self.static_summary = static_summary
            self.static_details = static_details
            self.priority = priority

    async def _to_proto(
        self, client: Client
    ) -> temporalio.api.schedule.v1.ScheduleAction:
        execution_timeout: google.protobuf.duration_pb2.Duration | None = None
        if self.execution_timeout:
            execution_timeout = google.protobuf.duration_pb2.Duration()
            execution_timeout.FromTimedelta(self.execution_timeout)
        run_timeout: google.protobuf.duration_pb2.Duration | None = None
        if self.run_timeout:
            run_timeout = google.protobuf.duration_pb2.Duration()
            run_timeout.FromTimedelta(self.run_timeout)
        task_timeout: google.protobuf.duration_pb2.Duration | None = None
        if self.task_timeout:
            task_timeout = google.protobuf.duration_pb2.Duration()
            task_timeout.FromTimedelta(self.task_timeout)
        retry_policy: temporalio.api.common.v1.RetryPolicy | None = None
        if self.retry_policy:
            retry_policy = temporalio.api.common.v1.RetryPolicy()
            self.retry_policy.apply_to_proto(retry_policy)
        priority: temporalio.api.common.v1.Priority | None = None
        if self.priority:
            priority = self.priority._to_proto()
        data_converter = client.data_converter._with_contexts(
            WorkflowSerializationContext(
                namespace=client.namespace,
                workflow_id=self.id,
            ),
            StorageDriverStoreContext(
                target=StorageDriverWorkflowInfo(
                    id=self.id, type=self.workflow, namespace=client.namespace
                ),
            ),
        )
        action = temporalio.api.schedule.v1.ScheduleAction(
            start_workflow=temporalio.api.workflow.v1.NewWorkflowExecutionInfo(
                workflow_id=self.id,
                workflow_type=temporalio.api.common.v1.WorkflowType(name=self.workflow),
                task_queue=temporalio.api.taskqueue.v1.TaskQueue(name=self.task_queue),
                input=(
                    temporalio.api.common.v1.Payloads(
                        payloads=[
                            a
                            if isinstance(a, temporalio.api.common.v1.Payload)
                            else (await data_converter.encode([a]))[0]
                            for a in self.args
                        ]
                    )
                    if self.args
                    else None
                ),
                workflow_execution_timeout=execution_timeout,
                workflow_run_timeout=run_timeout,
                workflow_task_timeout=task_timeout,
                retry_policy=retry_policy,
                memo=await data_converter._encode_memo(self.memo)
                if self.memo
                else None,
                user_metadata=await _encode_user_metadata(
                    data_converter, self.static_summary, self.static_details
                ),
                priority=priority,
            ),
        )
        # Add any untyped attributes that are not also in the typed set
        untyped_not_in_typed = {
            k: v
            for k, v in self.untyped_search_attributes.items()
            if k not in self.typed_search_attributes
        }
        if untyped_not_in_typed:
            temporalio.converter.encode_search_attributes(
                untyped_not_in_typed, action.start_workflow.search_attributes
            )
        # TODO (dan): confirm whether this be `is not None`
        if self.typed_search_attributes:
            temporalio.converter.encode_search_attributes(
                self.typed_search_attributes,
                action.start_workflow.search_attributes,
            )
        if self.headers:
            await _apply_headers(
                self.headers,
                action.start_workflow.header.fields,
                client.config(active_config=True)["header_codec_behavior"]
                == HeaderCodecBehavior.CODEC
                and not self._from_raw,
                client.data_converter,
            )
        return action


class ScheduleOverlapPolicy(IntEnum):
    """Controls what happens when a workflow would be started by a schedule but
    one is already running.
    """

    SKIP = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP
    )
    """Don't start anything.

    When the workflow completes, the next scheduled event after that time will
    be considered.
    """

    BUFFER_ONE = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
    )
    """Start the workflow again soon as the current one completes, but only
    buffer one start in this way.

    If another start is supposed to happen when the workflow is running, and one
    is already buffered, then only the first one will be started after the
    running workflow finishes.
    """

    BUFFER_ALL = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
    )
    """Buffer up any number of starts to all happen sequentially, immediately
    after the running workflow completes."""

    CANCEL_OTHER = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
    )
    """If there is another workflow running, cancel it, and start the new one
    after the old one completes cancellation."""

    TERMINATE_OTHER = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
    )
    """If there is another workflow running, terminate it and start the new one
    immediately."""

    ALLOW_ALL = int(
        temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
    )
    """Start any number of concurrent workflows.

    Note that with this policy, last completion result and last failure will not
    be available since workflows are not sequential."""


@dataclass
class ScheduleBackfill:
    """Time period and policy for actions taken as if the time passed right
    now.
    """

    start_at: datetime
    """Start of the range to evaluate the schedule in.

    This is exclusive
    """
    end_at: datetime
    overlap: ScheduleOverlapPolicy | None = None

    def _to_proto(self) -> temporalio.api.schedule.v1.BackfillRequest:
        start_time = google.protobuf.timestamp_pb2.Timestamp()
        start_time.FromDatetime(self.start_at)
        end_time = google.protobuf.timestamp_pb2.Timestamp()
        end_time.FromDatetime(self.end_at)
        overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
        if self.overlap:
            overlap_policy = temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                self.overlap
            )
        return temporalio.api.schedule.v1.BackfillRequest(
            start_time=start_time,
            end_time=end_time,
            overlap_policy=overlap_policy,
        )


@dataclass
class SchedulePolicy:
    """Policies of a schedule."""

    overlap: ScheduleOverlapPolicy = dataclasses.field(
        default_factory=lambda: ScheduleOverlapPolicy.SKIP
    )
    """Controls what happens when an action is started while another is still
    running."""

    catchup_window: timedelta = timedelta(days=365)
    """After a Temporal server is unavailable, amount of time in the past to
    execute missed actions."""

    pause_on_failure: bool = False
    """Whether to pause the schedule if an action fails or times out.

    Note: For workflows, this only applies after all retries have been
    exhausted.
    """

    @staticmethod
    def _from_proto(pol: temporalio.api.schedule.v1.SchedulePolicies) -> SchedulePolicy:
        return SchedulePolicy(
            overlap=ScheduleOverlapPolicy(int(pol.overlap_policy)),
            catchup_window=pol.catchup_window.ToTimedelta(),
            pause_on_failure=pol.pause_on_failure,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.SchedulePolicies:
        catchup_window = google.protobuf.duration_pb2.Duration()
        catchup_window.FromTimedelta(self.catchup_window)
        return temporalio.api.schedule.v1.SchedulePolicies(
            overlap_policy=temporalio.api.enums.v1.ScheduleOverlapPolicy.ValueType(
                self.overlap
            ),
            catchup_window=catchup_window,
            pause_on_failure=self.pause_on_failure,
        )


@dataclass
class ScheduleState:
    """State of a schedule."""

    note: str | None = None
    """Human readable message for the schedule.

    The system may overwrite this value on certain conditions like
    pause-on-failure.
    """

    paused: bool = False
    """Whether the schedule is paused."""

    # Cannot be set to True on create
    limited_actions: bool = False
    """
    If true, remaining actions will be decremented for each action taken.

    On schedule create, this must be set to true if :py:attr:`remaining_actions`
    is non-zero and left false if :py:attr:`remaining_actions` is zero.
    """

    remaining_actions: int = 0
    """Actions remaining on this schedule.

    Once this number hits 0, no further actions are scheduled automatically.
    """

    @staticmethod
    def _from_proto(state: temporalio.api.schedule.v1.ScheduleState) -> ScheduleState:
        return ScheduleState(
            note=state.notes or None,
            paused=state.paused,
            limited_actions=state.limited_actions,
            remaining_actions=state.remaining_actions,
        )

    def _to_proto(self) -> temporalio.api.schedule.v1.ScheduleState:
        return temporalio.api.schedule.v1.ScheduleState(
            notes=self.note or "",
            paused=self.paused,
            limited_actions=self.limited_actions,
            remaining_actions=self.remaining_actions,
        )


@dataclass
class Schedule:
    """A schedule for periodically running an action."""

    action: ScheduleAction
    """Action taken when scheduled."""

    spec: ScheduleSpec
    """When the action is taken."""

    policy: SchedulePolicy = dataclasses.field(default_factory=SchedulePolicy)
    """Schedule policies."""

    state: ScheduleState = dataclasses.field(default_factory=ScheduleState)
    """State of the schedule."""

    @staticmethod
    def _from_proto(sched: temporalio.api.schedule.v1.Schedule) -> Schedule:
        return Schedule(
            action=ScheduleAction._from_proto(sched.action),
            spec=ScheduleSpec._from_proto(sched.spec),
            policy=SchedulePolicy._from_proto(sched.policies),
            state=ScheduleState._from_proto(sched.state),
        )

    async def _to_proto(self, client: Client) -> temporalio.api.schedule.v1.Schedule:
        catchup_window = google.protobuf.duration_pb2.Duration()
        catchup_window.FromTimedelta(self.policy.catchup_window)
        return temporalio.api.schedule.v1.Schedule(
            spec=self.spec._to_proto(),
            action=await self.action._to_proto(client),
            policies=self.policy._to_proto(),
            state=self.state._to_proto(),
        )


@dataclass
class ScheduleDescription:
    """Description of a schedule."""

    id: str
    """ID of the schedule."""

    schedule: Schedule
    """Schedule details that can be mutated."""

    info: ScheduleInfo
    """Information about the schedule."""

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Search attributes on the schedule."""

    search_attributes: temporalio.common.SearchAttributes
    """Search attributes on the schedule.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """

    data_converter: temporalio.converter.DataConverter
    """Data converter used for memo decoding."""

    raw_description: temporalio.api.workflowservice.v1.DescribeScheduleResponse
    """Raw description of the schedule."""

    @staticmethod
    def _from_proto(
        id: str,
        desc: temporalio.api.workflowservice.v1.DescribeScheduleResponse,
        converter: temporalio.converter.DataConverter,
    ) -> ScheduleDescription:
        return ScheduleDescription(
            id=id,
            schedule=Schedule._from_proto(desc.schedule),
            info=ScheduleInfo._from_proto(desc.info),
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                desc.search_attributes
            ),
            search_attributes=temporalio.converter.decode_search_attributes(
                desc.search_attributes
            ),
            data_converter=converter,
            raw_description=desc,
        )

    async def memo(self) -> Mapping[str, Any]:
        """Schedule's memo values, converted without type hints.

        Since type hints are not used, the default converted values will come
        back. For example, if the memo was originally created with a dataclass,
        the value will be a dict. To convert using proper type hints, use
        :py:meth:`memo_value`.

        Returns:
            Mapping of all memo keys and they values without type hints.
        """
        return await self.data_converter._decode_memo(self.raw_description.memo)

    @overload
    async def memo_value(
        self, key: str, default: Any = temporalio.common._arg_unset
    ) -> Any: ...

    @overload
    async def memo_value(
        self, key: str, *, type_hint: type[ParamType]
    ) -> ParamType: ...

    @overload
    async def memo_value(
        self, key: str, default: AnyType, *, type_hint: type[ParamType]
    ) -> AnyType | ParamType: ...

    async def memo_value(
        self,
        key: str,
        default: Any = temporalio.common._arg_unset,
        *,
        type_hint: type | None = None,
    ) -> Any:
        """Memo value for the given key, optional default, and optional type
        hint.

        Args:
            key: Key to get memo value for.
            default: Default to use if key is not present. If unset, a
                :py:class:`KeyError` is raised when the key does not exist.
            type_hint: type hint to use when converting.

        Returns:
            Memo value, converted with the type hint if present.

        Raises:
            KeyError: Key not present and default not set.
        """
        return await self.data_converter._decode_memo_field(
            self.raw_description.memo, key, default, type_hint
        )


@dataclass
class ScheduleInfo:
    """Information about a schedule."""

    num_actions: int
    """Number of actions taken by this schedule."""

    num_actions_missed_catchup_window: int
    """Number of times an action was skipped due to missing the catchup
    window."""

    num_actions_skipped_overlap: int
    """Number of actions skipped due to overlap."""

    running_actions: Sequence[ScheduleActionExecution]
    """Currently running actions."""

    recent_actions: Sequence[ScheduleActionResult]
    """10 most recent actions, oldest first."""

    next_action_times: Sequence[datetime]
    """Next 10 scheduled action times."""

    created_at: datetime
    """When the schedule was created."""

    last_updated_at: datetime | None
    """When the schedule was last updated."""

    @staticmethod
    def _from_proto(info: temporalio.api.schedule.v1.ScheduleInfo) -> ScheduleInfo:
        return ScheduleInfo(
            num_actions=info.action_count,
            num_actions_missed_catchup_window=info.missed_catchup_window,
            num_actions_skipped_overlap=info.overlap_skipped,
            running_actions=[
                ScheduleActionExecutionStartWorkflow._from_proto(r)
                for r in info.running_workflows
            ],
            recent_actions=[
                ScheduleActionResult._from_proto(r) for r in info.recent_actions
            ],
            next_action_times=[
                f.ToDatetime().replace(tzinfo=timezone.utc)
                for f in info.future_action_times
            ],
            created_at=info.create_time.ToDatetime().replace(tzinfo=timezone.utc),
            last_updated_at=info.update_time.ToDatetime().replace(tzinfo=timezone.utc)
            if info.HasField("update_time")
            else None,
        )


class ScheduleActionExecution(ABC):
    """Base class for an action execution."""

    pass


@dataclass
class ScheduleActionExecutionStartWorkflow(ScheduleActionExecution):
    """Execution of a scheduled workflow start."""

    workflow_id: str
    """Workflow ID."""

    first_execution_run_id: str
    """Workflow run ID."""

    @staticmethod
    def _from_proto(
        exec: temporalio.api.common.v1.WorkflowExecution,
    ) -> ScheduleActionExecutionStartWorkflow:
        return ScheduleActionExecutionStartWorkflow(
            workflow_id=exec.workflow_id,
            first_execution_run_id=exec.run_id,
        )


@dataclass
class ScheduleActionResult:
    """Information about when an action took place."""

    scheduled_at: datetime
    """Scheduled time of the action including jitter."""

    started_at: datetime
    """When the action actually started."""

    action: ScheduleActionExecution
    """Action that took place."""

    @staticmethod
    def _from_proto(
        res: temporalio.api.schedule.v1.ScheduleActionResult,
    ) -> ScheduleActionResult:
        return ScheduleActionResult(
            scheduled_at=res.schedule_time.ToDatetime().replace(tzinfo=timezone.utc),
            started_at=res.actual_time.ToDatetime().replace(tzinfo=timezone.utc),
            action=ScheduleActionExecutionStartWorkflow._from_proto(
                res.start_workflow_result
            ),
        )


@dataclass
class ScheduleUpdateInput:
    """Parameter for an update callback for :py:meth:`ScheduleHandle.update`."""

    description: ScheduleDescription
    """Current description of the schedule."""


@dataclass
class ScheduleUpdate:
    """Result of an update callback for :py:meth:`ScheduleHandle.update`."""

    schedule: Schedule
    """Schedule to update."""

    search_attributes: temporalio.common.TypedSearchAttributes | None = None
    """Search attributes to update."""


@dataclass
class ScheduleListDescription:
    """Description of a listed schedule."""

    id: str
    """ID of the schedule."""

    schedule: ScheduleListSchedule | None
    """Schedule details that can be mutated.

    This may not be present in older Temporal servers without advanced
    visibility.
    """

    info: ScheduleListInfo | None
    """Information about the schedule.

    This may not be present in older Temporal servers without advanced
    visibility.
    """

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Search attributes on the schedule."""

    search_attributes: temporalio.common.SearchAttributes
    """Search attributes on the schedule.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """

    data_converter: temporalio.converter.DataConverter
    """Data converter used for memo decoding."""

    raw_entry: temporalio.api.schedule.v1.ScheduleListEntry
    """Raw description of the schedule."""

    @staticmethod
    def _from_proto(
        entry: temporalio.api.schedule.v1.ScheduleListEntry,
        converter: temporalio.converter.DataConverter,
    ) -> ScheduleListDescription:
        return ScheduleListDescription(
            id=entry.schedule_id,
            schedule=ScheduleListSchedule._from_proto(entry.info)
            if entry.HasField("info")
            else None,
            info=ScheduleListInfo._from_proto(entry.info)
            if entry.HasField("info")
            else None,
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                entry.search_attributes
            ),
            search_attributes=temporalio.converter.decode_search_attributes(
                entry.search_attributes
            ),
            data_converter=converter,
            raw_entry=entry,
        )

    async def memo(self) -> Mapping[str, Any]:
        """Schedule's memo values, converted without type hints.

        Since type hints are not used, the default converted values will come
        back. For example, if the memo was originally created with a dataclass,
        the value will be a dict. To convert using proper type hints, use
        :py:meth:`memo_value`.

        Returns:
            Mapping of all memo keys and they values without type hints.
        """
        return await self.data_converter._decode_memo(self.raw_entry.memo)

    @overload
    async def memo_value(
        self, key: str, default: Any = temporalio.common._arg_unset
    ) -> Any: ...

    @overload
    async def memo_value(
        self, key: str, *, type_hint: type[ParamType]
    ) -> ParamType: ...

    @overload
    async def memo_value(
        self, key: str, default: AnyType, *, type_hint: type[ParamType]
    ) -> AnyType | ParamType: ...

    async def memo_value(
        self,
        key: str,
        default: Any = temporalio.common._arg_unset,
        *,
        type_hint: type | None = None,
    ) -> Any:
        """Memo value for the given key, optional default, and optional type
        hint.

        Args:
            key: Key to get memo value for.
            default: Default to use if key is not present. If unset, a
                :py:class:`KeyError` is raised when the key does not exist.
            type_hint: type hint to use when converting.

        Returns:
            Memo value, converted with the type hint if present.

        Raises:
            KeyError: Key not present and default not set.
        """
        return await self.data_converter._decode_memo_field(
            self.raw_entry.memo, key, default, type_hint
        )


@dataclass
class ScheduleListSchedule:
    """Details for a listed schedule."""

    action: ScheduleListAction
    """Action taken when scheduled."""

    spec: ScheduleSpec
    """When the action is taken."""

    state: ScheduleListState
    """State of the schedule."""

    @staticmethod
    def _from_proto(
        info: temporalio.api.schedule.v1.ScheduleListInfo,
    ) -> ScheduleListSchedule:
        # Only start workflow supported for now
        if not info.HasField("workflow_type"):
            raise ValueError("Unknown action on schedule")
        return ScheduleListSchedule(
            action=ScheduleListActionStartWorkflow(workflow=info.workflow_type.name),
            spec=ScheduleSpec._from_proto(info.spec),
            state=ScheduleListState._from_proto(info),
        )


class ScheduleListAction(ABC):
    """Base class for an action a listed schedule can take."""

    pass


@dataclass
class ScheduleListActionStartWorkflow(ScheduleListAction):
    """Action to start a workflow on a listed schedule."""

    workflow: str
    """Workflow type name."""


@dataclass
class ScheduleListInfo:
    """Information about a listed schedule."""

    recent_actions: Sequence[ScheduleActionResult]
    """Most recent actions, oldest first.

    This may be a smaller amount than present on
    :py:attr:`ScheduleDescription.info`.
    """

    next_action_times: Sequence[datetime]
    """Next scheduled action times.

    This may be a smaller amount than present on
    :py:attr:`ScheduleDescription.info`.
    """

    @staticmethod
    def _from_proto(
        info: temporalio.api.schedule.v1.ScheduleListInfo,
    ) -> ScheduleListInfo:
        return ScheduleListInfo(
            recent_actions=[
                ScheduleActionResult._from_proto(r) for r in info.recent_actions
            ],
            next_action_times=[
                f.ToDatetime().replace(tzinfo=timezone.utc)
                for f in info.future_action_times
            ],
        )


@dataclass
class ScheduleListState:
    """State of a listed schedule."""

    note: str | None
    """Human readable message for the schedule.

    The system may overwrite this value on certain conditions like
    pause-on-failure.
    """

    paused: bool
    """Whether the schedule is paused."""

    @staticmethod
    def _from_proto(
        info: temporalio.api.schedule.v1.ScheduleListInfo,
    ) -> ScheduleListState:
        return ScheduleListState(
            note=info.notes or None,
            paused=info.paused,
        )


class ScheduleAsyncIterator:
    """Asynchronous iterator for :py:class:`ScheduleListDescription` values.

    Most users should use ``async for`` on this iterator and not call any of the
    methods within.
    """

    def __init__(
        self,
        client: Client,
        input: ListSchedulesInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`Client.list_schedules`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: Sequence[ScheduleListDescription] | None = None
        self._current_page_index = 0

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page that will be returned from
        the next :py:meth:`__anext__` call.
        """
        return self._current_page_index

    @property
    def current_page(self) -> Sequence[ScheduleListDescription] | None:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> bytes | None:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: int | None = None) -> None:
        """Fetch the next page if any.

        Args:
            page_size: Override the page size this iterator was originally
                created with.
        """
        resp = await self._client.workflow_service.list_schedules(
            temporalio.api.workflowservice.v1.ListSchedulesRequest(
                namespace=self._client.namespace,
                maximum_page_size=page_size or self._input.page_size,
                next_page_token=self._next_page_token or b"",
                query=self._input.query or "",
            ),
            retry=True,
            metadata=self._input.rpc_metadata,
            timeout=self._input.rpc_timeout,
        )
        self._current_page = [
            ScheduleListDescription._from_proto(v, self._client.data_converter)
            for v in resp.schedules
        ]
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> ScheduleAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> ScheduleListDescription:
        """Get the next execution on this iterator, fetching next page if
        necessary.
        """
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
            return ret
