# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import collections.abc
import threading

from typing_extensions import deprecated

from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk._logs.export import (
    LogRecordExporter,
    LogRecordExportResult,
)


class InMemoryLogRecordExporter(LogRecordExporter):
    """Implementation of :class:`.LogRecordExporter` that stores logs in memory.

    This class can be used for testing purposes. It stores the exported logs
    in a list in memory that can be retrieved using the
    :meth:`.get_finished_logs` method.
    """

    def __init__(self):
        self._logs = []
        self._lock = threading.Lock()
        self._stopped = False

    def clear(self) -> None:
        with self._lock:
            self._logs.clear()

    def get_finished_logs(self) -> tuple[ReadableLogRecord, ...]:
        with self._lock:
            return tuple(self._logs)

    def export(
        self, batch: collections.abc.Sequence[ReadableLogRecord]
    ) -> LogRecordExportResult:
        if self._stopped:
            return LogRecordExportResult.FAILURE
        with self._lock:
            self._logs.extend(batch)
        return LogRecordExportResult.SUCCESS

    def shutdown(self) -> None:
        self._stopped = True

    def force_flush(self, timeout_millis: int = 10_000) -> bool:
        return True


@deprecated(
    "Use InMemoryLogRecordExporter. Since logs are not stable yet this WILL be removed in future releases."
)
class InMemoryLogExporter(InMemoryLogRecordExporter):
    pass
