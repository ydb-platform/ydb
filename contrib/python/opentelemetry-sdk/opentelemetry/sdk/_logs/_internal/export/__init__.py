# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import abc
import copy
import enum
import logging
import sys
from collections.abc import Callable, Sequence
from os import environ, linesep
from typing import IO

from typing_extensions import deprecated

from opentelemetry.context import (
    _ON_EMIT_RECURSION_COUNT_KEY,
    _SUPPRESS_INSTRUMENTATION_KEY,
    Context,
    attach,
    detach,
    get_value,
    set_value,
)
from opentelemetry.metrics import MeterProvider, get_meter_provider
from opentelemetry.sdk._logs import (
    LogRecordProcessor,
    ReadableLogRecord,
    ReadWriteLogRecord,
)
from opentelemetry.sdk._shared_internal import (
    BatchProcessor,
    DuplicateFilter,
)
from opentelemetry.sdk._shared_internal._processor_metrics import (
    create_processor_metrics,
)
from opentelemetry.sdk.environment_variables import (
    OTEL_BLRP_EXPORT_TIMEOUT,
    OTEL_BLRP_MAX_EXPORT_BATCH_SIZE,
    OTEL_BLRP_MAX_QUEUE_SIZE,
    OTEL_BLRP_SCHEDULE_DELAY,
    OTEL_PYTHON_SDK_INTERNAL_METRICS_ENABLED,
)
from opentelemetry.sdk.environment_variables._internal import (
    parse_boolean_environment_variable,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv._incubating.attributes.otel_attributes import (
    OtelComponentTypeValues,
)

_DEFAULT_SCHEDULE_DELAY_MILLIS = 1000
_DEFAULT_MAX_EXPORT_BATCH_SIZE = 512
_DEFAULT_EXPORT_TIMEOUT_MILLIS = 30000
_DEFAULT_MAX_QUEUE_SIZE = 2048
_ENV_VAR_INT_VALUE_ERROR_MESSAGE = (
    "Unable to parse value for %s as integer. Defaulting to %s."
)
_logger = logging.getLogger(__name__)
_logger.addFilter(DuplicateFilter())

_propagate_false_logger = logging.getLogger(__name__ + ".propagate.false")
_propagate_false_logger.propagate = False


class LogRecordExportResult(enum.Enum):
    SUCCESS = 0
    FAILURE = 1


@deprecated(
    "Use LogRecordExportResult. Since logs are not stable yet this WILL be removed in future releases."
)
class LogExportResult(enum.Enum):
    SUCCESS = 0
    FAILURE = 1


class LogRecordExporter(abc.ABC):
    """Interface for exporting logs.

    Interface to be implemented by services that want to export logs received
    in their own format.

    To export data this MUST be registered to the :class:`opentelemetry.sdk._logs.Logger`
    using a log processor.

    Important
    ---------
    The ``export()`` method may raise exceptions (e.g., network errors,
    timeouts, serialization errors). It is the responsibility of the
    ``LogRecordProcessor`` calling this exporter to handle these exceptions
    appropriately to prevent application crashes. See ``LogRecordProcessor``
    for guidance on implementing proper error handling.
    """

    @abc.abstractmethod
    def export(
        self, batch: Sequence[ReadableLogRecord]
    ) -> LogRecordExportResult:
        """Exports a batch of logs.

        Args:
            batch: The list of ``ReadableLogRecord`` objects to be exported.

        Returns:
            The result of the export.

        Raises:
            Exception: This method may raise exceptions on network errors,
                timeouts, or other failures. Callers (i.e., log processors)
                should handle these exceptions to comply with OpenTelemetry
                error handling principles.
        """

    @abc.abstractmethod
    def shutdown(self):
        """Shuts down the exporter.

        Called when the SDK is shut down.
        """

    @abc.abstractmethod
    def force_flush(self, timeout_millis: int = 10_000) -> bool:
        """Hint to ensure that the export of any ``ReadableLogRecord`` objects
        the exporter has received prior to the call to ``force_flush`` SHOULD be
        completed as soon as possible, preferably before returning from this method.

        Args:
            timeout_millis: The maximum amount of time to wait for the flush to
                complete, in milliseconds.

        Returns:
            ``True`` if the flush completed successfully within the timeout,
            ``False`` otherwise.
        """


@deprecated(
    "Use LogRecordExporter. Since logs are not stable yet this WILL be removed in future releases."
)
class LogExporter(LogRecordExporter):
    pass


class ConsoleLogRecordExporter(LogRecordExporter):
    """Implementation of :class:`LogRecordExporter` that prints log records to the
    console.

    This class can be used for diagnostic purposes. It prints the exported
    log records to the console STDOUT.
    """

    def __init__(
        self,
        out: IO = sys.stdout,
        formatter: Callable[[ReadableLogRecord], str] = lambda record: (
            record.to_json() + linesep
        ),
    ):
        self.out = out
        self.formatter = formatter

    def export(self, batch: Sequence[ReadableLogRecord]):
        for log_record in batch:
            self.out.write(self.formatter(log_record))
        self.out.flush()
        return LogRecordExportResult.SUCCESS

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis: int = 10_000) -> bool:
        return True


@deprecated(
    "Use ConsoleLogRecordExporter. Since logs are not stable yet this WILL be removed in future releases."
)
class ConsoleLogExporter(ConsoleLogRecordExporter):
    pass


class SimpleLogRecordProcessor(LogRecordProcessor):
    """Implementation of LogRecordProcessor that exports logs synchronously.

    This processor passes received logs directly to the configured
    ``LogRecordExporter`` as soon as they are emitted.

    This class serves as a reference implementation for custom log processors,
    demonstrating proper error handling. Note how the ``on_emit`` method wraps
    the exporter call in a try/except block to prevent exceptions from
    propagating to the application.
    """

    def __init__(
        self,
        exporter: LogRecordExporter,
        *,
        meter_provider: MeterProvider | None = None,
    ):
        self._exporter = exporter
        self._shutdown = False
        self._metrics = create_processor_metrics(
            "logs",
            OtelComponentTypeValues.SIMPLE_LOG_PROCESSOR,
            meter_provider or get_meter_provider(),
            enabled=parse_boolean_environment_variable(
                OTEL_PYTHON_SDK_INTERNAL_METRICS_ENABLED
            ),
        )

    def on_emit(self, log_record: ReadWriteLogRecord):
        # Prevent entering a recursive loop.
        cnt = get_value(_ON_EMIT_RECURSION_COUNT_KEY) or 0
        # Recursive depth of 3 is sort of arbitrary. It's possible that an Exporter.export call
        # emits a log which returns us to this function, but when we call Exporter.export again the log
        # is no longer emitted and we exit this recursive loop naturally, a depth of >3 allows 3
        # recursive log calls but exits after because it's likely endless.
        if cnt > 3:  # pyright: ignore[reportOperatorIssue]
            _propagate_false_logger.warning(
                "SimpleLogRecordProcessor.on_emit has entered a recursive loop. Dropping log and exiting the loop."
            )
            return
        token = attach(
            set_value(
                _SUPPRESS_INSTRUMENTATION_KEY,
                True,
                set_value(_ON_EMIT_RECURSION_COUNT_KEY, cnt + 1),  # pyright: ignore[reportOperatorIssue]
            )
        )
        error: Exception | None = None
        try:
            if self._shutdown:
                _logger.warning("Processor is already shutdown, ignoring call")
                return
            # Convert ReadWriteLogRecord to ReadableLogRecord before exporting
            # Note: resource should not be None at this point as it's set during Logger.emit()
            resource = (
                log_record.resource
                if log_record.resource is not None
                else Resource.create({})
            )
            readable_log_record = ReadableLogRecord(
                log_record=log_record.log_record,
                resource=resource,
                instrumentation_scope=log_record.instrumentation_scope,
                limits=log_record.limits,
            )
            self._exporter.export((readable_log_record,))
        except Exception as err:  # pylint: disable=broad-exception-caught
            error = err
            _logger.exception("Exception while exporting logs.")
        finally:
            self._metrics.finish_items(1, error)
            detach(token)

    def shutdown(self):
        self._shutdown = True
        self._exporter.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # pylint: disable=no-self-use
        return True


class BatchLogRecordProcessor(LogRecordProcessor):
    """This is an implementation of LogRecordProcessor which creates batches of
    received logs and sends them to the configured LogRecordExporter.

    `BatchLogRecordProcessor` is configurable with the following environment
    variables which correspond to constructor parameters:

    - :envvar:`OTEL_BLRP_SCHEDULE_DELAY`
    - :envvar:`OTEL_BLRP_MAX_QUEUE_SIZE`
    - :envvar:`OTEL_BLRP_MAX_EXPORT_BATCH_SIZE`
    - :envvar:`OTEL_BLRP_EXPORT_TIMEOUT`

    All the logic for emitting logs, shutting down etc. resides in the BatchProcessor class.
    """

    def __init__(
        self,
        exporter: LogRecordExporter,
        schedule_delay_millis: float | None = None,
        max_export_batch_size: int | None = None,
        export_timeout_millis: float | None = None,
        max_queue_size: int | None = None,
        *,
        meter_provider: MeterProvider | None = None,
    ):
        if max_queue_size is None:
            max_queue_size = BatchLogRecordProcessor._default_max_queue_size()

        if schedule_delay_millis is None:
            schedule_delay_millis = (
                BatchLogRecordProcessor._default_schedule_delay_millis()
            )

        if max_export_batch_size is None:
            max_export_batch_size = (
                BatchLogRecordProcessor._default_max_export_batch_size()
            )
        # Not used. No way currently to pass timeout to export.
        if export_timeout_millis is None:
            export_timeout_millis = (
                BatchLogRecordProcessor._default_export_timeout_millis()
            )

        BatchLogRecordProcessor._validate_arguments(
            max_queue_size, schedule_delay_millis, max_export_batch_size
        )
        # Initializes BatchProcessor
        self._batch_processor = BatchProcessor(
            exporter,
            schedule_delay_millis,
            max_export_batch_size,
            export_timeout_millis,
            max_queue_size,
            "Log",
            create_processor_metrics(
                "logs",
                OtelComponentTypeValues.BATCHING_LOG_PROCESSOR,
                meter_provider or get_meter_provider(),
                capacity=max_queue_size,
                enabled=parse_boolean_environment_variable(
                    OTEL_PYTHON_SDK_INTERNAL_METRICS_ENABLED
                ),
            ),
        )

    def on_emit(self, log_record: ReadWriteLogRecord) -> None:
        # Convert ReadWriteLogRecord to ReadableLogRecord before passing to BatchProcessor
        # Note: resource should not be None at this point as it's set during Logger.emit()
        resource = (
            log_record.resource
            if log_record.resource is not None
            else Resource.create({})
        )
        # Shallow copy the API log record to break the reference to the potentially large context
        # while keeping the original context intact for other processors.
        api_log_record = copy.copy(log_record.log_record)
        api_log_record.context = Context()

        readable_log_record = ReadableLogRecord(
            log_record=api_log_record,
            resource=resource,
            instrumentation_scope=log_record.instrumentation_scope,
            limits=log_record.limits,
        )
        return self._batch_processor.emit(readable_log_record)

    def shutdown(self):
        return self._batch_processor.shutdown()

    def force_flush(self, timeout_millis: int | None = None) -> bool:
        return self._batch_processor.force_flush(timeout_millis)

    @staticmethod
    def _default_max_queue_size():
        try:
            return int(
                environ.get(OTEL_BLRP_MAX_QUEUE_SIZE, _DEFAULT_MAX_QUEUE_SIZE)
            )
        except ValueError:
            _logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BLRP_MAX_QUEUE_SIZE,
                _DEFAULT_MAX_QUEUE_SIZE,
            )
            return _DEFAULT_MAX_QUEUE_SIZE

    @staticmethod
    def _default_schedule_delay_millis():
        try:
            return int(
                environ.get(
                    OTEL_BLRP_SCHEDULE_DELAY, _DEFAULT_SCHEDULE_DELAY_MILLIS
                )
            )
        except ValueError:
            _logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BLRP_SCHEDULE_DELAY,
                _DEFAULT_SCHEDULE_DELAY_MILLIS,
            )
            return _DEFAULT_SCHEDULE_DELAY_MILLIS

    @staticmethod
    def _default_max_export_batch_size():
        try:
            return int(
                environ.get(
                    OTEL_BLRP_MAX_EXPORT_BATCH_SIZE,
                    _DEFAULT_MAX_EXPORT_BATCH_SIZE,
                )
            )
        except ValueError:
            _logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BLRP_MAX_EXPORT_BATCH_SIZE,
                _DEFAULT_MAX_EXPORT_BATCH_SIZE,
            )
            return _DEFAULT_MAX_EXPORT_BATCH_SIZE

    @staticmethod
    def _default_export_timeout_millis():
        try:
            return int(
                environ.get(
                    OTEL_BLRP_EXPORT_TIMEOUT, _DEFAULT_EXPORT_TIMEOUT_MILLIS
                )
            )
        except ValueError:
            _logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BLRP_EXPORT_TIMEOUT,
                _DEFAULT_EXPORT_TIMEOUT_MILLIS,
            )
            return _DEFAULT_EXPORT_TIMEOUT_MILLIS

    @staticmethod
    def _validate_arguments(
        max_queue_size, schedule_delay_millis, max_export_batch_size
    ):
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")

        if schedule_delay_millis <= 0:
            raise ValueError("schedule_delay_millis must be positive.")

        if max_export_batch_size <= 0:
            raise ValueError(
                "max_export_batch_size must be a positive integer."
            )

        if max_export_batch_size > max_queue_size:
            raise ValueError(
                "max_export_batch_size must be less than or equal to max_queue_size."
            )
