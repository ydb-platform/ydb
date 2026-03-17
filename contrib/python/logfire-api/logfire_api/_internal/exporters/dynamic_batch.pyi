from _typeshed import Incomplete
from logfire._internal.exporters.wrapper import WrapperSpanProcessor as WrapperSpanProcessor
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter

class DynamicBatchSpanProcessor(WrapperSpanProcessor):
    """A wrapper around a BatchSpanProcessor that dynamically adjusts the schedule delay.

    The initial schedule delay is set to 100ms, and after processing 10 spans, it is set to the value of
    the `OTEL_BSP_SCHEDULE_DELAY` environment variable (default: 500ms).
    This makes the initial experience of the SDK more responsive.
    """
    processor: BatchSpanProcessor
    final_delay: Incomplete
    num_processed: int
    def __init__(self, exporter: SpanExporter) -> None: ...
    def on_end(self, span: ReadableSpan) -> None: ...
    @property
    def batch_processor(self): ...
    @property
    def span_exporter(self) -> SpanExporter: ...
    @property
    def schedule_delay_millis(self) -> float: ...
    @schedule_delay_millis.setter
    def schedule_delay_millis(self, value: float): ...
