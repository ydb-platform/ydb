import opentelemetry.trace as trace_api
from ..types import ExceptionCallback as ExceptionCallback
from .config import LogfireConfig as LogfireConfig
from .constants import ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY as ATTRIBUTES_EXCEPTION_FINGERPRINT_KEY, ATTRIBUTES_MESSAGE_KEY as ATTRIBUTES_MESSAGE_KEY, ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY as ATTRIBUTES_PENDING_SPAN_REAL_PARENT_KEY, ATTRIBUTES_SAMPLE_RATE_KEY as ATTRIBUTES_SAMPLE_RATE_KEY, ATTRIBUTES_SPAN_TYPE_KEY as ATTRIBUTES_SPAN_TYPE_KEY, ATTRIBUTES_VALIDATION_ERROR_KEY as ATTRIBUTES_VALIDATION_ERROR_KEY, log_level_attributes as log_level_attributes
from .utils import handle_internal_errors as handle_internal_errors, sha256_string as sha256_string
from _typeshed import Incomplete
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from opentelemetry import context as context_api
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider as SDKTracerProvider
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace import Link as Link, Span, SpanContext, SpanKind, Tracer, TracerProvider
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util import types as otel_types
from threading import Lock
from typing import Any, Callable
from weakref import WeakKeyDictionary, WeakValueDictionary

OPEN_SPANS: WeakValueDictionary[tuple[int, int], _LogfireWrappedSpan]

@dataclass
class ProxyTracerProvider(TracerProvider):
    """A tracer provider that wraps another internal tracer provider allowing it to be re-assigned."""
    provider: TracerProvider
    config: LogfireConfig
    tracers: WeakKeyDictionary[_ProxyTracer, Callable[[], Tracer]] = field(default_factory=WeakKeyDictionary)
    lock: Lock = field(default_factory=Lock)
    suppressed_scopes: set[str] = field(default_factory=set)
    def set_provider(self, provider: SDKTracerProvider) -> None: ...
    def suppress_scopes(self, *scopes: str) -> None: ...
    def get_tracer(self, instrumenting_module_name: str, *args: Any, is_span_tracer: bool = True, **kwargs: Any) -> _ProxyTracer: ...
    def add_span_processor(self, span_processor: Any) -> None: ...
    def shutdown(self) -> None: ...
    @property
    def resource(self) -> Resource: ...
    def force_flush(self, timeout_millis: int = 30000) -> bool: ...

@dataclass
class SpanMetric:
    details: dict[tuple[tuple[str, otel_types.AttributeValue], ...], float] = field(default_factory=Incomplete)
    def dump(self): ...
    def increment(self, attributes: Mapping[str, otel_types.AttributeValue], value: float): ...

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
    metrics: dict[str, SpanMetric] = field(default_factory=Incomplete)
    exception_callback: ExceptionCallback | None = ...
    def __post_init__(self) -> None: ...
    def end(self, end_time: int | None = None) -> None: ...
    def get_span_context(self) -> SpanContext: ...
    def set_attributes(self, attributes: Mapping[str, otel_types.AttributeValue]) -> None: ...
    def set_attribute(self, key: str, value: otel_types.AttributeValue) -> None: ...
    def add_link(self, context: SpanContext, attributes: otel_types.Attributes = None) -> None: ...
    def add_event(self, name: str, attributes: otel_types.Attributes = None, timestamp: int | None = None) -> None: ...
    def update_name(self, name: str) -> None: ...
    def is_recording(self) -> bool: ...
    def set_status(self, status: Status | StatusCode, description: str | None = None) -> None: ...
    def record_exception(self, exception: BaseException, attributes: otel_types.Attributes = None, timestamp: int | None = None, escaped: bool = False) -> None: ...
    def increment_metric(self, name: str, attributes: Mapping[str, otel_types.AttributeValue], value: float) -> None: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: Any) -> None: ...
    def __getattr__(self, name: str) -> Any: ...

def get_parent_span(span: ReadableSpan) -> _LogfireWrappedSpan | None: ...

@dataclass
class _ProxyTracer(Tracer):
    """A tracer that wraps another internal tracer allowing it to be re-assigned."""
    instrumenting_module_name: str
    tracer: Tracer
    provider: ProxyTracerProvider
    is_span_tracer: bool
    def __hash__(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def set_tracer(self, tracer: Tracer) -> None: ...
    def start_span(self, name: str, context: Context | None = None, kind: SpanKind = ..., attributes: otel_types.Attributes = None, links: Sequence[Link] | None = None, start_time: int | None = None, record_exception: bool = True, set_status_on_exception: bool = True) -> _LogfireWrappedSpan: ...
    start_as_current_span = ...

class SuppressedTracer(Tracer):
    def start_span(self, name: str, context: Context | None = None, *args: Any, **kwargs: Any) -> Span: ...
    start_as_current_span: Incomplete

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
    def on_start(self, span: Span, parent_context: context_api.Context | None = None) -> None: ...

def should_sample(span_context: SpanContext, attributes: Mapping[str, otel_types.AttributeValue]) -> bool:
    """Determine if a span should be sampled.

    This is used to sample spans that are not sampled by the OTEL sampler.
    """
def get_sample_rate_from_attributes(attributes: otel_types.Attributes) -> float | None: ...
@handle_internal_errors
def record_exception(span: trace_api.Span, exception: BaseException, *, attributes: otel_types.Attributes = None, timestamp: int | None = None, escaped: bool = False, callback: ExceptionCallback | None = None) -> None:
    """Similar to the OTEL SDK Span.record_exception method, with our own additions."""
def set_exception_status(span: trace_api.Span, exception: BaseException): ...
