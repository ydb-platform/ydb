from __future__ import annotations

import random
import time
import uuid
from collections import deque
from collections.abc import Mapping, Sequence
from functools import cached_property
from pathlib import Path
from tempfile import mkdtemp
from threading import Lock, Thread
from typing import Any

import requests.exceptions
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs._internal.export import LogRecordExportResult
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExportResult
from requests import Session

import logfire

from ..utils import logger, platform_is_emscripten
from .wrapper import WrapperLogExporter, WrapperSpanExporter


class BodySizeCheckingOTLPSpanExporter(OTLPSpanExporter):
    # 5MB is significantly less than what our backend currently accepts,
    # but smaller requests are faster and more reliable.
    # This won't prevent bigger spans/payloads from being exported,
    # it just tries to make each request smaller.
    # This also helps in case the backend limit is reduced in the future.
    max_body_size = 5 * 1024 * 1024

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._current_num_spans = 0

    def export(self, spans: Sequence[ReadableSpan]):
        self._current_num_spans = len(spans)
        return super().export(spans)

    def _export(self, serialized_data: bytes, *args: Any, **kwargs: Any):
        # If there are multiple spans, check the body size first.
        if self._current_num_spans > 1 and len(serialized_data) > self.max_body_size:
            # Tell outer RetryFewerSpansSpanExporter to split in half
            raise BodyTooLargeError(len(serialized_data), self.max_body_size)

        return super()._export(serialized_data, *args, **kwargs)


class OTLPExporterHttpSession(Session):
    """A requests.Session subclass that defers failed requests to a DiskRetryer."""

    def post(self, url: str, data: bytes, **kwargs: Any):  # type: ignore
        start_time = time.time()
        try:
            return self._post(url, data, **kwargs)
        except requests.exceptions.RequestException:
            # Wait a little before trying again normally, before resorting to disk retrying.
            # This has several advantages:
            #  - Writing to disk might be impossible.
            #  - It adds backpressure to the exporter, leading to fewer, more efficient batches and requests
            #  - It's better when the process is currently shutting down, as DiskRetryer uses a daemon thread.
            # However, the longer we block here instead of in the disk retryer thread,
            # the more we risk filling up the BatchSpanProcessor queue in memory, or passing the shutdown deadline.
            # So only do this if the first attempt took less than 10 seconds.
            end_time = time.time()
            if end_time - start_time > 10:  # pragma: no cover
                self._add_task(data, url, kwargs)
                raise

            time.sleep(1)
            try:
                return self._post(url, data, **kwargs)
            except requests.exceptions.RequestException:
                self._add_task(data, url, kwargs)
                raise

    def _add_task(self, data: bytes, url: str, kwargs: dict[str, Any]):
        # No threads in Emscripten, we can't add a task to try later
        if not platform_is_emscripten():  # pragma: no branch
            self.retryer.add_task(data, {'url': url, **kwargs})

    def _post(self, url: str, data: bytes, **kwargs: Any):
        response = super().post(url, data=data, **kwargs)
        raise_for_retryable_status(response)
        return response

    @cached_property
    def retryer(self) -> DiskRetryer:
        # Only create this when needed to save resources,
        # and because the full set of headers are only set some time after this session is created.
        return DiskRetryer(self.headers)


def raise_for_retryable_status(response: requests.Response):
    # These are status codes that OTEL should retry.
    # We want to do the retrying ourselves, so we raise an exception instead of returning.
    if response.status_code in (408, 429) or response.status_code >= 500:
        response.raise_for_status()


class DiskRetryer:
    """Retries requests failed by OTLPExporterHttpSession, saving the request body to disk to save memory."""

    # The maximum delay between retries, in seconds
    MAX_DELAY = 128

    # Maximum number of bytes of failed exports to keep on disk before dropping new ones.
    # For most users this should be plenty, and for very high volume users it should last about 20 minutes.
    MAX_TASK_SIZE = 512 * 1024 * 1024  # 512MB

    # Log about problems at most once a minute.
    LOG_INTERVAL = 60

    def __init__(self, headers: Mapping[str, str | bytes]):
        # Reading/writing `thread`, `tasks`, and `total_size` should generally be protected by `lock`.
        self.lock = Lock()
        self.thread: Thread | None = None
        self.tasks: deque[tuple[Path, dict[str, Any]]] = deque()
        self.total_size = 0

        # Make a new session rather than using the OTLPExporterHttpSession directly
        # because thread safety of Session is questionable.
        # This assumes that the only important state is the headers.
        self.session = Session()
        self.session.headers.update(headers)

        # The directory where the export files are stored.
        self.dir = Path(mkdtemp(prefix='logfire-retryer-'))

        self.last_log_time = -float('inf')

    def add_task(self, data: bytes, kwargs: dict[str, Any]):
        try:
            with self.lock:
                if self.total_size >= self.MAX_TASK_SIZE:  # pragma: no cover
                    if self._should_log():
                        logger.error(
                            'Already retrying %s failed exports (%s bytes), dropping an export',
                            len(self.tasks),
                            self.total_size,
                        )
                    return
                self.total_size += len(data)

            # TODO consider keeping the first few tasks in memory to avoid disk I/O and possible errors.
            try:
                path = self.dir / uuid.uuid4().hex
                path.write_bytes(data)
            except Exception:  # pragma: no cover
                with self.lock:
                    self.total_size -= len(data)
                raise

            with self.lock:
                self.tasks.append((path, kwargs))
                if not (self.thread and self.thread.is_alive()):
                    # daemon=True to avoid hanging the program on exit, since this might never finish.
                    # See caveat about this where add_task is called.
                    self.thread = Thread(target=self._run, daemon=True)
                    self.thread.start()
                num_tasks = len(self.tasks)
                num_bytes = self.total_size

            if self._should_log():
                logger.warning('Currently retrying %s failed export(s) (%s bytes)', num_tasks, num_bytes)
        except Exception as e:  # pragma: no cover
            if self._should_log():
                logger.error('Export and retry failed: %s', e)

    def _should_log(self) -> bool:
        result = time.monotonic() - self.last_log_time >= self.LOG_INTERVAL
        if result:
            self.last_log_time = time.monotonic()
        return result

    def _run(self):
        delay = 1
        while True:
            with self.lock:
                if not self.tasks:
                    # All done, end the thread.
                    self.thread = None
                    break

                # Keep this outside the try block below so that if somehow this part fails
                # the queue still gets smaller, and we don't get stuck in a hot infinite loop.
                task = self.tasks.popleft()

            try:
                path, kwargs = task
                data = path.read_bytes()
                while True:
                    # Exponential backoff with jitter.
                    # The jitter is proportional to the delay, in particular so that if we go down for a while
                    # and then come back up then retry requests will be spread out over a time of MAX_DELAY.
                    time.sleep(delay * (1 + random.random()))
                    try:
                        with logfire.suppress_instrumentation():
                            response = self.session.post(**kwargs, data=data)
                        raise_for_retryable_status(response)
                    except requests.exceptions.RequestException:
                        # Failed, increase delay exponentially up to MAX_DELAY.
                        delay = min(delay * 2, self.MAX_DELAY)
                        # Make it at least 2 seconds, this is for when it was decreased to 0.2 in the block below.
                        delay = max(delay, 2)
                    else:
                        # Success, set the delay to a small value (so that remaining tasks can be done quickly),
                        # remove the file, and move on to the next task.
                        delay = 0.2
                        path.unlink()
                        with self.lock:
                            self.total_size -= len(data)
                        break

            except Exception:  # pragma: no cover
                if self._should_log():
                    logger.exception('Error retrying export')


class RetryFewerSpansSpanExporter(WrapperSpanExporter):
    """A SpanExporter that retries exporting spans in smaller batches if BodyTooLargeError is raised.

    This wraps another exporter, typically an OTLPSpanExporter using an OTLPExporterHttpSession.
    """

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            return super().export(spans)
        except BodyTooLargeError:
            half = len(spans) // 2
            # BodySizeCheckingOTLPSpanExporter should only raise BodyTooLargeError for >1 span,
            # otherwise it should just try exporting it.
            assert half > 0
            res1 = self.export(spans[:half])
            res2 = self.export(spans[half:])
            if res1 is not SpanExportResult.SUCCESS or res2 is not SpanExportResult.SUCCESS:
                return SpanExportResult.FAILURE
            return SpanExportResult.SUCCESS


class BodyTooLargeError(Exception):
    def __init__(self, size: int, max_size: int) -> None:
        super().__init__(f'Request body is too large ({size} bytes), must be less than {max_size} bytes.')
        self.size = size
        self.max_size = max_size


class QuietSpanExporter(WrapperSpanExporter):
    """A SpanExporter that catches request exceptions to prevent OTEL from logging a huge traceback."""

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            return super().export(spans)
        except requests.exceptions.RequestException:
            # Rely on OTLPExporterHttpSession/DiskRetryer to log this kind of error periodically.
            return SpanExportResult.FAILURE


class QuietLogExporter(WrapperLogExporter):
    """A LogExporter that catches request exceptions to prevent OTEL from logging a huge traceback."""

    def export(self, batch: Sequence[ReadableLogRecord]):
        try:
            return super().export(batch)
        except requests.exceptions.RequestException:
            # Rely on OTLPExporterHttpSession/DiskRetryer to log this kind of error periodically.
            return LogRecordExportResult.FAILURE
