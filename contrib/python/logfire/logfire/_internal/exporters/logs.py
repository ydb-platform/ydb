from dataclasses import dataclass

from opentelemetry.sdk._logs import ReadWriteLogRecord

import logfire
from logfire._internal.exporters.wrapper import WrapperLogProcessor
from logfire._internal.scrubbing import BaseScrubber
from logfire._internal.utils import is_instrumentation_suppressed


class CheckSuppressInstrumentationLogProcessorWrapper(WrapperLogProcessor):
    """Checks if instrumentation is suppressed, then suppresses instrumentation itself.

    Placed at the root of the tree of processors.
    """

    def on_emit(self, log_record: ReadWriteLogRecord):
        if is_instrumentation_suppressed():
            return None
        with logfire.suppress_instrumentation():
            return super().on_emit(log_record)


@dataclass
class MainLogProcessorWrapper(WrapperLogProcessor):
    scrubber: BaseScrubber

    def on_emit(self, log_record: ReadWriteLogRecord):
        log_record.log_record = self.scrubber.scrub_log(log_record.log_record)
        return super().on_emit(log_record)
