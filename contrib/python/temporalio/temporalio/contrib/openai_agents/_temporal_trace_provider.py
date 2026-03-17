"""Provides support for integration with OpenAI Agents SDK tracing across workflows"""

import uuid
from types import TracebackType
from typing import Any, Optional, cast

from agents import SpanData, Trace, TracingProcessor
from agents.tracing import (
    get_trace_provider,
)
from agents.tracing.provider import (
    DefaultTraceProvider,
    SynchronousMultiTracingProcessor,
)
from agents.tracing.spans import Span

from temporalio import workflow
from temporalio.contrib.openai_agents._trace_interceptor import RunIdRandom
from temporalio.workflow import ReadOnlyContextError


class ActivitySpanData(SpanData):
    """Captures fields from ActivityTaskScheduledEventAttributes for tracing."""

    def __init__(
        self,
        activity_id: str,
        activity_type: str,
        task_queue: str,
        schedule_to_close_timeout: Optional[float] = None,
        schedule_to_start_timeout: Optional[float] = None,
        start_to_close_timeout: Optional[float] = None,
        heartbeat_timeout: Optional[float] = None,
    ):
        """Initialize an ActivitySpanData instance."""
        self.activity_id = activity_id
        self.activity_type = activity_type
        self.task_queue = task_queue
        self.schedule_to_close_timeout = schedule_to_close_timeout
        self.schedule_to_start_timeout = schedule_to_start_timeout
        self.start_to_close_timeout = start_to_close_timeout
        self.heartbeat_timeout = heartbeat_timeout

    @property
    def type(self) -> str:
        """Return the type of this span data."""
        return "temporal-activity"

    def export(self) -> dict[str, Any]:
        """Export the span data as a dictionary."""
        return {
            "type": self.type,
            "activity_id": self.activity_id,
            "activity_type": self.activity_type,
            "task_queue": self.task_queue,
            "schedule_to_close_timeout": self.schedule_to_close_timeout,
            "schedule_to_start_timeout": self.schedule_to_start_timeout,
            "start_to_close_timeout": self.start_to_close_timeout,
            "heartbeat_timeout": self.heartbeat_timeout,
        }


def activity_span(
    activity_id: str,
    activity_type: str,
    task_queue: str,
    start_to_close_timeout: float,
) -> Span[ActivitySpanData]:
    """Create a trace span for a Temporal activity."""
    return get_trace_provider().create_span(
        span_data=ActivitySpanData(
            activity_id=activity_id,
            activity_type=activity_type,
            task_queue=task_queue,
            start_to_close_timeout=start_to_close_timeout,
        ),
    )


class _TemporalTracingProcessor(SynchronousMultiTracingProcessor):
    def __init__(
        self, impl: SynchronousMultiTracingProcessor, auto_close_in_workflows: bool
    ):
        super().__init__()
        self._impl = impl
        self._auto_close_in_workflows = auto_close_in_workflows

    def add_tracing_processor(self, tracing_processor: TracingProcessor):
        self._impl.add_tracing_processor(tracing_processor)

    def set_processors(self, processors: list[TracingProcessor]):
        self._impl.set_processors(processors)

    def on_trace_start(self, trace: Trace) -> None:
        if workflow.in_workflow() and workflow.unsafe.is_replaying():
            # In replay mode, don't report
            return

        self._impl.on_trace_start(trace)
        if self._auto_close_in_workflows and workflow.in_workflow():
            self._impl.on_trace_end(trace)

    def on_trace_end(self, trace: Trace) -> None:
        if workflow.in_workflow() and workflow.unsafe.is_replaying():
            # In replay mode, don't report
            return
        if self._auto_close_in_workflows and workflow.in_workflow():
            return

        self._impl.on_trace_end(trace)

    def on_span_start(self, span: Span[Any]) -> None:
        if workflow.in_workflow() and workflow.unsafe.is_replaying():
            # In replay mode, don't report
            return

        self._impl.on_span_start(span)
        if self._auto_close_in_workflows and workflow.in_workflow():
            self._impl.on_span_end(span)

    def on_span_end(self, span: Span[Any]) -> None:
        if workflow.in_workflow() and workflow.unsafe.is_replaying():
            # In replay mode, don't report
            return
        if self._auto_close_in_workflows and workflow.in_workflow():
            return

        self._impl.on_span_end(span)

    def shutdown(self) -> None:
        self._impl.shutdown()

    def force_flush(self) -> None:
        self._impl.force_flush()


def _workflow_uuid() -> str:
    random = cast(
        RunIdRandom, getattr(workflow.instance(), "__temporal_openai_tracing_random")
    )
    return random.uuid4()


class TemporalTraceProvider(DefaultTraceProvider):
    """A trace provider that integrates with Temporal workflows."""

    def __init__(self, auto_close_in_workflows: bool = False):
        """Initialize the TemporalTraceProvider."""
        super().__init__()
        self._original_provider = cast(DefaultTraceProvider, get_trace_provider())
        self._multi_processor = _TemporalTracingProcessor(
            self._original_provider._multi_processor,
            auto_close_in_workflows,
        )

    def time_iso(self) -> str:
        """Return the current deterministic time in ISO 8601 format."""
        if workflow.in_workflow():
            return workflow.now().isoformat()
        return super().time_iso()

    def gen_trace_id(self) -> str:
        """Generate a new trace ID."""
        if workflow.in_workflow():
            try:
                """Generate a new trace ID."""
                return f"trace_{_workflow_uuid()}"
            except ReadOnlyContextError:
                return f"trace_{uuid.uuid4().hex}"
        return super().gen_trace_id()

    def gen_span_id(self) -> str:
        """Generate a span ID."""
        if workflow.in_workflow():
            try:
                """Generate a deterministic span ID."""
                return f"span_{_workflow_uuid()}"
            except ReadOnlyContextError:
                return f"span_{uuid.uuid4().hex[:24]}"
        return super().gen_span_id()

    def gen_group_id(self) -> str:
        """Generate a group ID."""
        if workflow.in_workflow():
            try:
                """Generate a deterministic group ID."""
                return f"group_{_workflow_uuid()}"
            except ReadOnlyContextError:
                return f"group_{uuid.uuid4().hex[:24]}"
        return super().gen_group_id()

    def __enter__(self):
        """Enter the context of the Temporal trace provider."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ):
        """Exit the context of the Temporal trace provider."""
        self._multi_processor.shutdown()
