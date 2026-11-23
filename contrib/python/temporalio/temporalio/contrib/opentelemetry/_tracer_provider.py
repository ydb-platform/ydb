from collections.abc import Iterator, Mapping, Sequence

import opentelemetry.sdk.trace as trace_sdk
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import (
    ConcurrentMultiSpanProcessor,
    SpanLimits,
    SynchronousMultiSpanProcessor,
    sampling,
)
from opentelemetry.sdk.trace.id_generator import IdGenerator, RandomIdGenerator
from opentelemetry.trace import (
    Link,
    Span,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    Tracer,
    TracerProvider,
    use_span,
)
from opentelemetry.util import types
from opentelemetry.util._decorator import _agnosticcontextmanager

from temporalio import workflow
from temporalio.contrib.opentelemetry._id_generator import TemporalIdGenerator


class _ReplaySafeSpan(Span):
    def __init__(self, span: Span):
        self._exception: BaseException | None = None
        self._span = span

    def __getattr__(self, name: str) -> object:
        return getattr(self._span, name)

    def end(self, end_time: int | None = None) -> None:
        if workflow.in_workflow() and workflow.unsafe.is_replaying_history_events():
            # Skip ending spans during workflow replay to avoid duplicate telemetry
            return

        if (
            workflow.in_workflow()
            and self._exception is not None
            and not workflow.is_failure_exception(self._exception)
        ):
            # Skip ending spans with workflow task failures. Otherwise, each failure will create its own span
            # This may still occur for spans which were completed during failed workflow tasks.
            return

        self._span.end(end_time=end_time)

    def get_span_context(self) -> SpanContext:
        return self._span.get_span_context()

    def set_attributes(self, attributes: Mapping[str, types.AttributeValue]) -> None:
        self._span.set_attributes(attributes)

    def set_attribute(self, key: str, value: types.AttributeValue) -> None:
        self._span.set_attribute(key, value)

    def add_event(
        self,
        name: str,
        attributes: types.Attributes = None,
        timestamp: int | None = None,
    ) -> None:
        self._span.add_event(name, attributes, timestamp)

    def add_link(
        self,
        context: SpanContext,
        attributes: types.Attributes = None,
    ) -> None:
        # Must be overridden explicitly: the Span ABC ships add_link as a
        # non-abstract warn-and-no-op default that __getattr__ cannot
        # intercept, which would silently drop links added after creation.
        self._span.add_link(context, attributes)

    def update_name(self, name: str) -> None:
        self._span.update_name(name)

    def is_recording(self) -> bool:
        return self._span.is_recording()

    def set_status(
        self, status: Status | StatusCode, description: str | None = None
    ) -> None:
        self._span.set_status(status, description)

    def record_exception(
        self,
        exception: BaseException,
        attributes: types.Attributes = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:
        self._exception = exception
        self._span.record_exception(exception, attributes, timestamp, escaped)


class _ReplaySafeTracer(Tracer):  # type: ignore[reportUnusedClass] # Used outside file
    def __init__(self, tracer: Tracer):
        self._tracer = tracer

    def start_span(
        self,
        name: str,
        context: Context | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: types.Attributes = None,
        links: Sequence[Link] | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> "Span":
        if workflow.in_workflow() and workflow.unsafe.is_replaying_history_events():
            start_time = start_time or workflow.time_ns()
        span = self._tracer.start_span(
            name,
            context,
            kind,
            attributes,
            links,
            start_time,
            record_exception,
            set_status_on_exception,
        )
        return _ReplaySafeSpan(span)

    @_agnosticcontextmanager
    def start_as_current_span(
        self,
        name: str,
        context: Context | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: types.Attributes = None,
        links: Sequence[Link] | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        end_on_exit: bool = True,
    ) -> Iterator["Span"]:
        if workflow.in_workflow() and workflow.unsafe.is_replaying_history_events():
            start_time = start_time or workflow.time_ns()
        span = self._tracer.start_span(
            name,
            context,
            kind,
            attributes,
            links,
            start_time,
            record_exception,
            set_status_on_exception,
        )
        span = _ReplaySafeSpan(span)
        with use_span(
            span,
            end_on_exit=end_on_exit,
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
        ) as span:
            yield span


class ReplaySafeTracerProvider(TracerProvider):
    """A tracer provider that is safe for use during workflow replay.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This tracer provider wraps an OpenTelemetry TracerProvider and ensures
    that telemetry operations are safe during workflow replay by using
    replay-safe spans and tracers.
    """

    def __init__(
        self,
        tracer_provider: trace_sdk.TracerProvider,
        id_generator: TemporalIdGenerator,
    ):
        """Initialize the replay-safe tracer provider.

        Prefer :py:func:`create_tracer_provider`, which constructs a provider
        that satisfies these requirements.

        Args:
            tracer_provider: The underlying OpenTelemetry TracerProvider to wrap.
                Must use a ``TemporalIdGenerator`` for replay safety.
            id_generator: The ``TemporalIdGenerator`` used by ``tracer_provider``.

        Raises:
            ValueError: If the tracer provider doesn't use a ``TemporalIdGenerator``.
        """
        if not isinstance(tracer_provider.id_generator, TemporalIdGenerator):
            raise ValueError(
                "ReplaySafeTracerProvider should only be used with a TemporalIdGenerator for replay safety. The given TracerProvider doesn't use one."
            )
        self._id_generator = id_generator
        self._tracer_provider = tracer_provider

    def add_span_processor(self, span_processor: trace_sdk.SpanProcessor) -> None:
        """Add a span processor to the underlying tracer provider.

        Args:
            span_processor: The span processor to add.
        """
        self._tracer_provider.add_span_processor(span_processor)

    def shutdown(self) -> None:
        """Shutdown the underlying tracer provider."""
        self._tracer_provider.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        """Force flush the underlying tracer provider.

        Args:
            timeout_millis: Timeout in milliseconds.

        Returns:
            True if flush was successful, False otherwise.
        """
        return self._tracer_provider.force_flush(timeout_millis)

    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: str | None = None,
        schema_url: str | None = None,
        attributes: types.Attributes | None = None,
    ) -> Tracer:
        """Get a replay-safe tracer from the underlying provider.

        Args:
            instrumenting_module_name: The name of the instrumenting module.
            instrumenting_library_version: The version of the instrumenting library.
            schema_url: The schema URL for the tracer.
            attributes: Additional attributes for the tracer.

        Returns:
            A replay-safe tracer instance.
        """
        tracer = self._tracer_provider.get_tracer(
            instrumenting_module_name,
            instrumenting_library_version,
            schema_url,
            attributes,
        )
        return _ReplaySafeTracer(tracer)

    def id_generator(self) -> TemporalIdGenerator:
        """Gets the temporal id generator associated with this provider."""
        return self._id_generator


def create_tracer_provider(
    sampler: sampling.Sampler | None = None,
    resource: Resource | None = None,
    shutdown_on_exit: bool = True,
    active_span_processor: SynchronousMultiSpanProcessor
    | ConcurrentMultiSpanProcessor
    | None = None,
    id_generator: IdGenerator | None = None,
    span_limits: SpanLimits | None = None,
) -> ReplaySafeTracerProvider:
    """Initialize a replay-safe tracer provider.

    .. warning::
        This function is experimental and may change in future versions.
        Use with caution in production environments.

    Creates a new TracerProvider with a TemporalIdGenerator for replay safety
    and wraps it in a ReplaySafeTracerProvider.

    Args:
        sampler: The sampler to use for sampling spans.
        resource: The resource to associate with the tracer provider.
        shutdown_on_exit: Whether to shutdown the provider on exit.
        active_span_processor: The active span processor to use.
        id_generator: The ID generator to wrap with TemporalIdGenerator.
        span_limits: The span limits to apply.

    Returns:
        A replay-safe tracer provider instance.
    """
    generator = TemporalIdGenerator(id_generator or RandomIdGenerator())
    provider = trace_sdk.TracerProvider(
        sampler=sampler,
        resource=resource,
        shutdown_on_exit=shutdown_on_exit,
        active_span_processor=active_span_processor,
        span_limits=span_limits,
        id_generator=generator,
    )
    return ReplaySafeTracerProvider(provider, generator)
