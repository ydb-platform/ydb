from __future__ import annotations

from collections.abc import Sequence

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult

from ..constants import ATTRIBUTES_SPAN_TYPE_KEY
from .wrapper import WrapperSpanExporter


class RemovePendingSpansExporter(WrapperSpanExporter):
    """An exporter that filters out pending spans if the corresponding final span is already in the same batch."""

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        result: list[ReadableSpan] = []

        # Mapping of (trace_id, span_id) to either a pending or final span, whichever is found first.
        # We avoid assuming that pending spans appear first in the `spans` list.
        # After all, `result` is likely to be in a different order than `spans`.
        spans_by_id: dict[tuple[int, int], ReadableSpan] = {}

        for span in spans:
            attributes = span.attributes or {}
            span_type = attributes.get(ATTRIBUTES_SPAN_TYPE_KEY)

            if span_type == 'pending_span':
                context = span.parent
                if context:  # pragma: no branch
                    key = (context.trace_id, context.span_id)
                    spans_by_id.setdefault(key, span)
                    continue

            elif span_type == 'span':
                context = span.context  # note that this context is different from the pending span case
                if context:  # pragma: no branch
                    key = (context.trace_id, context.span_id)
                    spans_by_id[key] = span
                    continue

            # In particular this includes logs.
            result.append(span)

        result.extend(spans_by_id.values())
        return super().export(result)
