from ..constants import ATTRIBUTES_SPAN_TYPE_KEY as ATTRIBUTES_SPAN_TYPE_KEY
from .wrapper import WrapperSpanExporter as WrapperSpanExporter
from collections.abc import Sequence
from opentelemetry.sdk.trace import ReadableSpan as ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult

class RemovePendingSpansExporter(WrapperSpanExporter):
    """An exporter that filters out pending spans if the corresponding final span is already in the same batch."""
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult: ...
