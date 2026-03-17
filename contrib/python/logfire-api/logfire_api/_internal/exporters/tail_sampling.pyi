from _typeshed import Incomplete
from dataclasses import dataclass
from functools import cached_property
from logfire._internal.constants import ATTRIBUTES_LOG_LEVEL_NUM_KEY as ATTRIBUTES_LOG_LEVEL_NUM_KEY, LEVEL_NUMBERS as LEVEL_NUMBERS, LevelName as LevelName, ONE_SECOND_IN_NANOSECONDS as ONE_SECOND_IN_NANOSECONDS
from logfire._internal.exporters.wrapper import WrapperSpanProcessor as WrapperSpanProcessor
from opentelemetry import context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor

@dataclass
class TailSamplingOptions:
    level: LevelName | None = ...
    duration: float | None = ...

@dataclass
class TraceBuffer:
    """Arguments of `on_start` and `on_end` for spans in a single trace."""
    started: list[tuple[Span, context.Context | None]]
    ended: list[ReadableSpan]
    @cached_property
    def first_span(self) -> Span: ...

class TailSamplingProcessor(WrapperSpanProcessor):
    """Passes spans to the wrapped processor if any span in a trace meets the sampling criteria."""
    duration: Incomplete
    level: Incomplete
    random_rate: Incomplete
    traces: Incomplete
    lock: Incomplete
    def __init__(self, processor: SpanProcessor, options: TailSamplingOptions, random_rate: float) -> None: ...
    def on_start(self, span: Span, parent_context: context.Context | None = None) -> None: ...
    def on_end(self, span: ReadableSpan) -> None: ...
    def check_span(self, span: ReadableSpan, buffer: TraceBuffer) -> bool:
        """If the span meets the sampling criteria, drop the buffer and return True. Otherwise, return False."""
    def drop_buffer(self, buffer: TraceBuffer) -> None: ...
    def push_buffer(self, buffer: TraceBuffer) -> None: ...
