import requests
from ..utils import logger as logger, platform_is_emscripten as platform_is_emscripten
from .wrapper import WrapperLogExporter as WrapperLogExporter, WrapperSpanExporter as WrapperSpanExporter
from _typeshed import Incomplete
from collections import deque
from collections.abc import Mapping, Sequence
from functools import cached_property
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import ReadableLogRecord as ReadableLogRecord
from opentelemetry.sdk.trace import ReadableSpan as ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult
from pathlib import Path
from requests import Session
from threading import Thread
from typing import Any

class BodySizeCheckingOTLPSpanExporter(OTLPSpanExporter):
    max_body_size: Incomplete
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def export(self, spans: Sequence[ReadableSpan]): ...

class OTLPExporterHttpSession(Session):
    """A requests.Session subclass that defers failed requests to a DiskRetryer."""
    def post(self, url: str, data: bytes, **kwargs: Any): ...
    @cached_property
    def retryer(self) -> DiskRetryer: ...

def raise_for_retryable_status(response: requests.Response): ...

class DiskRetryer:
    """Retries requests failed by OTLPExporterHttpSession, saving the request body to disk to save memory."""
    MAX_DELAY: int
    MAX_TASK_SIZE: Incomplete
    LOG_INTERVAL: int
    lock: Incomplete
    thread: Thread | None
    tasks: deque[tuple[Path, dict[str, Any]]]
    total_size: int
    session: Incomplete
    dir: Incomplete
    last_log_time: Incomplete
    def __init__(self, headers: Mapping[str, str | bytes]) -> None: ...
    def add_task(self, data: bytes, kwargs: dict[str, Any]): ...

class RetryFewerSpansSpanExporter(WrapperSpanExporter):
    """A SpanExporter that retries exporting spans in smaller batches if BodyTooLargeError is raised.

    This wraps another exporter, typically an OTLPSpanExporter using an OTLPExporterHttpSession.
    """
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult: ...

class BodyTooLargeError(Exception):
    size: Incomplete
    max_size: Incomplete
    def __init__(self, size: int, max_size: int) -> None: ...

class QuietSpanExporter(WrapperSpanExporter):
    """A SpanExporter that catches request exceptions to prevent OTEL from logging a huge traceback."""
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult: ...

class QuietLogExporter(WrapperLogExporter):
    """A LogExporter that catches request exceptions to prevent OTEL from logging a huge traceback."""
    def export(self, batch: Sequence[ReadableLogRecord]): ...
