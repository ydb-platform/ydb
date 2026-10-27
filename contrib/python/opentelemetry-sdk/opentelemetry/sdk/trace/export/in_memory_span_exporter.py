# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import collections.abc
import threading
from collections import deque

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class InMemorySpanExporter(SpanExporter):
    """Implementation of :class:`.SpanExporter` that stores spans in memory.

    This class can be used for testing purposes. It stores the exported spans
    in a deque in memory that can be retrieved using the
    :func:`.get_finished_spans` method.

    Args:
        max_spans: Maximum number of spans to store in memory. When the limit
            is reached, the oldest spans are dropped automatically. If ``None``
            (default), there is no limit.
    """

    def __init__(self, max_spans: int | None = None) -> None:
        self._finished_spans: deque[ReadableSpan] = deque(maxlen=max_spans)
        self._stopped = False
        self._lock = threading.Lock()

    def clear(self) -> None:
        """Clear list of collected spans."""
        with self._lock:
            self._finished_spans.clear()

    def get_finished_spans(self) -> tuple[ReadableSpan, ...]:
        """Get list of collected spans."""
        with self._lock:
            return tuple(self._finished_spans)

    def export(
        self, spans: collections.abc.Sequence[ReadableSpan]
    ) -> SpanExportResult:
        """Stores a list of spans in memory."""
        if self._stopped:
            return SpanExportResult.FAILURE
        with self._lock:
            self._finished_spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        """Shut downs the exporter.

        Calls to export after the exporter has been shut down will fail.
        """
        self._stopped = True

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True
