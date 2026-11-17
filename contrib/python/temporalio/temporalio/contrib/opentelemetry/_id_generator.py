import random
from contextvars import ContextVar

from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace import (
    INVALID_SPAN_ID,
    INVALID_TRACE_ID,
)

import temporalio.workflow


def _get_workflow_random() -> random.Random | None:
    if (
        temporalio.workflow.in_workflow()
        and not temporalio.workflow.unsafe.is_read_only()
    ):
        if (
            getattr(temporalio.workflow.instance(), "__temporal_otel_id_random", None)
            is None
        ):
            setattr(
                temporalio.workflow.instance(),
                "__temporal_otel_id_random",
                temporalio.workflow.new_random(),
            )
        return getattr(temporalio.workflow.instance(), "__temporal_otel_id_random")

    return None


class TemporalIdGenerator(IdGenerator):
    """OpenTelemetry ID generator that uses Temporal's deterministic random generator.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This generator uses Temporal's workflow-safe random number generator when
    inside a workflow execution, ensuring deterministic span and trace IDs
    across workflow replays. Falls back to standard random generation outside
    of workflows.

    Can be seeded with OpenTelemetry span IDs from client context to maintain
    proper span parenting across the client-workflow boundary.
    """

    def __init__(self, id_generator: IdGenerator):
        """Initialize a TemporalIdGenerator."""
        self._id_generator = id_generator
        self._traces: ContextVar[tuple[int, ...]] = ContextVar(
            "temporalio_otel_trace_id_seeds", default=()
        )
        self._spans: ContextVar[tuple[int, ...]] = ContextVar(
            "temporalio_otel_span_id_seeds", default=()
        )

    def seed_span_id(self, span_id: int) -> None:
        """Seed the generator with a span ID to use as the first result.

        This is typically used to maintain OpenTelemetry span parenting
        when crossing the client-workflow boundary.

        Args:
            span_id: The span ID to use as the first generated span ID.
        """
        self._spans.set((*self._spans.get(), span_id))

    def seed_trace_id(self, trace_id: int) -> None:
        """Seed the generator with a trace ID to use as the first result.

        Args:
            trace_id: The trace ID to use as the first generated trace ID.
        """
        self._traces.set((*self._traces.get(), trace_id))

    def generate_span_id(self) -> int:
        """Generate a span ID using Temporal's deterministic random when in workflow.

        Returns:
            A 64-bit span ID.
        """
        spans = self._spans.get()
        if spans:
            self._spans.set(spans[:-1])
            return spans[-1]

        if workflow_random := _get_workflow_random():
            span_id = workflow_random.getrandbits(64)
            while span_id == INVALID_SPAN_ID:
                span_id = workflow_random.getrandbits(64)
            return span_id
        return self._id_generator.generate_span_id()

    def generate_trace_id(self) -> int:
        """Generate a trace ID using Temporal's deterministic random when in workflow.

        Returns:
            A 128-bit trace ID.
        """
        traces = self._traces.get()
        if traces:
            self._traces.set(traces[:-1])
            return traces[-1]

        if workflow_random := _get_workflow_random():
            trace_id = workflow_random.getrandbits(128)
            while trace_id == INVALID_TRACE_ID:
                trace_id = workflow_random.getrandbits(128)
            return trace_id
        return self._id_generator.generate_trace_id()
