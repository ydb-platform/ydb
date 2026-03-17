from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable, cast
from weakref import WeakKeyDictionary, WeakValueDictionary

import opentelemetry.trace as trace_api
from opentelemetry import context as context_api
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import (
    ReadableSpan,
    Span as SDKSpan,
    SpanProcessor,
    Tracer as SDKTracer,
    TracerProvider as SDKTracerProvider,
)
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace import Link, NonRecordingSpan, Span, SpanContext, SpanKind, Tracer, TracerProvider
from opentelemetry.trace.propagation import get_current_span
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util import types as otel_types

from .constants import (
    ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY,
    ATTRIBUTES_MESSAGE_KEY,
    ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY,
    ATTRIBUTES_SAMPLE_RATE_KEY,
    ATTRIBUTES_SPAN_TYPE_KEY,
    ATTRIBUTES_VALIDATION_ERROR_KEY,
    log_level_attributes,
)
from .utils import handle_internal_errors, sha256_string

if TYPE_CHECKING:
    from ..types import ExceptionCallback
    from .config import LogfireConfig

try:
    from pydantic import ValidationError
except ImportError:  # pragma: no cover
    ValidationError = None


OPEN_SPANS: WeakValueDictionary[tuple[int, int], _LogfireWrappedSpan] = WeakValueDictionary()


@dataclass
class ProxyTracerProvider(TracerProvider):
    """A tracer provider that wraps another internal tracer provider allowing it to be re-assigned."""

    provider: TracerProvider
    config: LogfireConfig
    tracers: WeakKeyDictionary[_ProxyTracer, Callable[[], Tracer]] = field(default_factory=WeakKeyDictionary)  # type: ignore[reportUnknownVariableType]
    lock: Lock = field(default_factory=Lock)
    suppressed_scopes: set[str] = field(default_factory=set)  # type: ignore[reportUnknownVariableType]

    def set_provider(self, provider: SDKTracerProvider) -> None:
        with self.lock:
            self.provider = provider
            for tracer, factory in self.tracers.items():
                tracer.set_tracer(factory())

    def suppress_scopes(self, *scopes: str) -> None:
        with self.lock:
            self.suppressed_scopes.update(scopes)
            for tracer, factory in self.tracers.items():
                if tracer.instrumenting_module_name in scopes:
                    tracer.set_tracer(factory())

    def get_tracer(
        self,
        instrumenting_module_name: str,
        *args: Any,
        is_span_tracer: bool = True,
        **kwargs: Any,
    ) -> _ProxyTracer:
        with self.lock:

            def make() -> Tracer:
                if instrumenting_module_name in self.suppressed_scopes:
                    return SuppressedTracer()
                else:
                    return self.provider.get_tracer(instrumenting_module_name, *args, **kwargs)

            tracer = _ProxyTracer(instrumenting_module_name, make(), self, is_span_tracer)
            self.tracers[tracer] = make
            return tracer

    def add_span_processor(self, span_processor: Any) -> None:  # pragma: no cover
        with self.lock:
            if isinstance(self.provider, SDKTracerProvider):
                self.provider.add_span_processor(span_processor)

    def shutdown(self) -> None:
        with self.lock:
            if isinstance(self.provider, SDKTracerProvider):
                self.provider.shutdown()

    @property
    def resource(self) -> Resource:  # pragma: no cover
        with self.lock:
            if isinstance(self.provider, SDKTracerProvider):
                return self.provider.resource
            return Resource.create({'service.name': self.config.service_name})

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        with self.lock:
            if isinstance(self.provider, SDKTracerProvider):  # pragma: no branch
                return self.provider.force_flush(timeout_millis)
            return True  # pragma: no cover


@dataclass
class SpanMetric:
    details: dict[tuple[tuple[str, otel_types.AttributeValue], ...], float] = field(
        default_factory=lambda: defaultdict(int)
    )

    def dump(self):
        return {
            'details': [{'attributes': dict(attributes), 'total': total} for attributes, total in self.details.items()],
            'total': sum(total for total in self.details.values()),
        }

    def increment(self, attributes: Mapping[str, otel_types.AttributeValue], value: float):
        key = tuple(sorted(attributes.items()))
        self.details[key] += value


@dataclass(eq=False)
class _LogfireWrappedSpan(trace_api.Span, ReadableSpan):
    """A span that wraps another span and overrides some behaviors in a logfire-specific way.

    In particular:
    * Stores a reference to itself in `OPEN_SPANS`, used to close open spans when the program exits
    * Adds some logfire-specific tweaks to the exception recording behavior
    * Overrides end() to use a timestamp generator if one was provided
    """

    span: Span
    ns_timestamp_generator: Callable[[], int]
    record_metrics: bool
    metrics: dict[str, SpanMetric] = field(default_factory=lambda: defaultdict(SpanMetric))
    exception_callback: ExceptionCallback | None = None

    def __post_init__(self):
        OPEN_SPANS[self._open_spans_key()] = self

    def end(self, end_time: int | None = None) -> None:
        with handle_internal_errors:
            OPEN_SPANS.pop(self._open_spans_key(), None)
            if self.metrics:
                self.span.set_attribute(
                    'logfire.metrics', json.dumps({name: metric.dump() for name, metric in self.metrics.items()})
                )
        self.span.end(end_time or self.ns_timestamp_generator())

    def _open_spans_key(self):
        return _open_spans_key(self.span.get_span_context())

    def get_span_context(self) -> SpanContext:
        return self.span.get_span_context()

    def set_attributes(self, attributes: Mapping[str, otel_types.AttributeValue]) -> None:
        self.span.set_attributes(attributes)

    def set_attribute(self, key: str, value: otel_types.AttributeValue) -> None:
        self.span.set_attribute(key, value)

    def add_link(self, context: SpanContext, attributes: otel_types.Attributes = None) -> None:
        return self.span.add_link(context, attributes)

    def add_event(
        self,
        name: str,
        attributes: otel_types.Attributes = None,
        timestamp: int | None = None,
    ) -> None:
        self.span.add_event(name, attributes, timestamp or self.ns_timestamp_generator())

    def update_name(self, name: str) -> None:  # pragma: no cover
        self.span.update_name(name)

    def is_recording(self) -> bool:
        return self.span.is_recording()

    def set_status(
        self,
        status: Status | StatusCode,
        description: str | None = None,
    ) -> None:
        self.span.set_status(status, description)

    def record_exception(
        self,
        exception: BaseException,
        attributes: otel_types.Attributes = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:
        timestamp = timestamp or self.ns_timestamp_generator()
        record_exception(
            self.span,
            exception,
            attributes=attributes,
            timestamp=timestamp,
            escaped=escaped,
            callback=self.exception_callback,
        )

    def increment_metric(self, name: str, attributes: Mapping[str, otel_types.AttributeValue], value: float) -> None:
        if not (self.is_recording() and (self.record_metrics or name == 'operation.cost')):
            return

        self.metrics[name].increment(attributes, value)
        if parent := get_parent_span(self):
            parent.increment_metric(name, attributes, value)

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: Any) -> None:
        if self.is_recording():
            if isinstance(exc_value, BaseException):
                self.record_exception(exc_value, escaped=True)
            self.end()

    if not TYPE_CHECKING:  # pragma: no branch
        # for ReadableSpan
        def __getattr__(self, name: str) -> Any:
            return getattr(self.span, name)


def get_parent_span(span: ReadableSpan) -> _LogfireWrappedSpan | None:
    return span.parent and OPEN_SPANS.get(_open_spans_key(span.parent))


def _open_spans_key(ctx: SpanContext) -> tuple[int, int]:
    return ctx.trace_id, ctx.span_id


@dataclass
class _ProxyTracer(Tracer):
    """A tracer that wraps another internal tracer allowing it to be re-assigned."""

    instrumenting_module_name: str
    tracer: Tracer
    provider: ProxyTracerProvider
    is_span_tracer: bool

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: object) -> bool:  # pragma: no cover
        return other is self

    def set_tracer(self, tracer: Tracer) -> None:
        self.tracer = tracer

    def start_span(
        self,
        name: str,
        context: Context | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: otel_types.Attributes = None,
        links: Sequence[Link] | None = None,
        start_time: int | None = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> _LogfireWrappedSpan:
        config = self.provider.config
        ns_timestamp_generator = config.advanced.ns_timestamp_generator
        record_metrics: bool = not isinstance(config.metrics, (bool, type(None))) and config.metrics.collect_in_spans
        exception_callback = config.advanced.exception_callback

        start_time = start_time or ns_timestamp_generator()

        # Make a copy of the attributes since this method can be called by arbitrary external code,
        # e.g. third party instrumentation.
        attributes = {**(attributes or {})}
        if self.is_span_tracer:
            attributes[ATTRIBUTES_SPAN_TYPE_KEY] = 'span'
        attributes.setdefault(ATTRIBUTES_MESSAGE_KEY, name)

        span = self.tracer.start_span(
            name, context, kind, attributes, links, start_time, record_exception, set_status_on_exception
        )
        if not should_sample(span.get_span_context(), attributes):  # pragma: no cover
            span = trace_api.NonRecordingSpan(
                SpanContext(
                    trace_id=span.get_span_context().trace_id,
                    span_id=span.get_span_context().span_id,
                    is_remote=False,
                    trace_flags=trace_api.TraceFlags(
                        span.get_span_context().trace_flags & ~trace_api.TraceFlags.SAMPLED
                    ),
                )
            )
        return _LogfireWrappedSpan(
            span,
            ns_timestamp_generator=ns_timestamp_generator,
            record_metrics=record_metrics,
            exception_callback=exception_callback,
        )

    # This means that `with start_as_current_span(...):`
    # is roughly equivalent to `with use_span(start_span(...)):`
    start_as_current_span = SDKTracer.start_as_current_span


class SuppressedTracer(Tracer):
    def start_span(self, name: str, context: Context | None = None, *args: Any, **kwargs: Any) -> Span:
        # Create a no-op span with the same SpanContext as the current span.
        # This means that any spans created within will have the current span as their parent,
        # as if this span didn't exist at all.
        return NonRecordingSpan(get_current_span(context).get_span_context())

    # This means that `with start_as_current_span(...):`
    # is roughly equivalent to `with use_span(start_span(...)):`
    start_as_current_span = SDKTracer.start_as_current_span


@dataclass
class PendingSpanProcessor(SpanProcessor):
    """Span processor that emits an extra pending span for each span as it starts.

    The pending span is emitted by calling `on_end` on the inner `processor`.
    This is intentionally not a `WrapperSpanProcessor` to avoid the default implementations of `on_end`
    and `shutdown`. This processor is expected to contain processors which are already included
    elsewhere in the pipeline where `on_end` and `shutdown` are called normally.
    """

    id_generator: IdGenerator
    processor: SpanProcessor

    def on_start(
        self,
        span: Span,
        parent_context: context_api.Context | None = None,
    ) -> None:
        assert isinstance(span, ReadableSpan) and isinstance(span, Span)
        if not span.is_recording():  # pragma: no cover
            # Span was sampled out, or has finished already (happens with tail sampling)
            return

        attributes = span.attributes
        if not attributes or attributes.get(ATTRIBUTES_SPAN_TYPE_KEY) not in (None, 'span'):
            return

        real_span_context = span.get_span_context()
        if not should_sample(real_span_context, attributes):  # pragma: no cover
            # Currently our own sampling is only checked after the span has started,
            # so we have to repeat that check here.
            # This might change in the future, see
            # https://linear.app/pydantic/issue/PYD-552/sampling-behaves-very-differently-depending-on-how-its-configured
            return

        span_context = SpanContext(
            trace_id=real_span_context.trace_id,
            span_id=self.id_generator.generate_span_id(),
            is_remote=False,
            trace_flags=real_span_context.trace_flags,
        )
        attributes = {
            **attributes,
            ATTRIBUTES_SPAN_TYPE_KEY: 'pending_span',
            # use str here since protobuf can't encode ints above 2^64,
            # see https://github.com/pydantic/platform/pull/388
            ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY: trace_api.format_span_id(
                span.parent.span_id if span.parent else 0
            ),
        }
        start_and_end_time = span.start_time
        pending_span = ReadableSpan(
            name=span.name,
            context=span_context,
            parent=real_span_context,
            resource=span.resource,
            attributes=attributes,
            events=span.events,
            links=span.links,
            status=span.status,
            kind=span.kind,
            start_time=start_and_end_time,
            end_time=start_and_end_time,
            instrumentation_scope=span.instrumentation_scope,
        )
        self.processor.on_end(pending_span)


def should_sample(span_context: SpanContext, attributes: Mapping[str, otel_types.AttributeValue]) -> bool:
    """Determine if a span should be sampled.

    This is used to sample spans that are not sampled by the OTEL sampler.
    """
    sample_rate = get_sample_rate_from_attributes(attributes)
    return sample_rate is None or span_context.span_id <= round(sample_rate * 2**64)


def get_sample_rate_from_attributes(attributes: otel_types.Attributes) -> float | None:
    if not attributes:  # pragma: no cover
        return None
    return cast('float | None', attributes.get(ATTRIBUTES_SAMPLE_RATE_KEY))


@handle_internal_errors
def record_exception(
    span: trace_api.Span,
    exception: BaseException,
    *,
    attributes: otel_types.Attributes = None,
    timestamp: int | None = None,
    escaped: bool = False,
    callback: ExceptionCallback | None = None,
) -> None:
    """Similar to the OTEL SDK Span.record_exception method, with our own additions."""
    from ..types import ExceptionCallbackHelper

    if not span.is_recording():
        return

    # From https://opentelemetry.io/docs/specs/semconv/attributes-registry/exception/
    # `escaped=True` means that the exception is escaping the scope of the span.
    # This means we know that the exception hasn't been handled,
    # so we can set the OTEL status and the log level to error.
    if escaped:
        set_exception_status(span, exception)
        span.set_attributes(log_level_attributes('error'))

    helper = ExceptionCallbackHelper(
        span=cast(SDKSpan, span),
        exception=exception,
        event_attributes={**(attributes or {})},
    )

    if callback is not None:
        with handle_internal_errors:
            callback(helper)

    if not helper._record_exception:  # type: ignore
        return

    attributes = helper.event_attributes
    if ValidationError is not None and isinstance(exception, ValidationError):
        # insert a more detailed breakdown of pydantic errors
        try:
            err_json = exception.json(include_url=False)
        except TypeError:  # pragma: no cover
            # pydantic v1
            err_json = exception.json()
        span.set_attribute(ATTRIBUTES_VALIDATION_ERROR_KEY, err_json)
        attributes[ATTRIBUTES_VALIDATION_ERROR_KEY] = err_json

    if helper.create_issue:
        fingerprint = sha256_string(helper.issue_fingerprint_source)
        if attributes.get('recorded_by_logfire_fastapi'):
            # Put the fingerprint in the event instead of the span.
            # `_tweak_fastapi_span` will copy it to the span if the span ends up having level error,
            # which it should if the HTTP status code is 5xx.
            # At this point we have none of that info and we don't know if the exception is going to be handled.
            # If it is handled, the handler may or may not return a status code of 5xx,
            # which is what will determine whether an issue should be created.
            # If it's not handled, the same exception will be recorded on the span again by the OTel instrumentation,
            # so this record_exception function will be called again and the decision can be made then.
            # In that case there won't be recorded_by_logfire_fastapi.
            attributes[ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY] = fingerprint
        else:
            span.set_attribute(ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY, fingerprint)

    span.record_exception(exception, attributes=attributes, timestamp=timestamp, escaped=escaped)


def set_exception_status(span: trace_api.Span, exception: BaseException):
    span.set_status(
        trace_api.Status(
            status_code=StatusCode.ERROR,
            description=f'{exception.__class__.__name__}: {exception}',
        )
    )
