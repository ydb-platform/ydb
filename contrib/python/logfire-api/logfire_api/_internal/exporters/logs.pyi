from dataclasses import dataclass
from logfire._internal.exporters.wrapper import WrapperLogProcessor as WrapperLogProcessor
from logfire._internal.scrubbing import BaseScrubber as BaseScrubber
from logfire._internal.utils import is_instrumentation_suppressed as is_instrumentation_suppressed
from opentelemetry.sdk._logs import ReadWriteLogRecord

class CheckSuppressInstrumentationLogProcessorWrapper(WrapperLogProcessor):
    """Checks if instrumentation is suppressed, then suppresses instrumentation itself.

    Placed at the root of the tree of processors.
    """
    def on_emit(self, log_record: ReadWriteLogRecord): ...

@dataclass
class MainLogProcessorWrapper(WrapperLogProcessor):
    scrubber: BaseScrubber
    def on_emit(self, log_record: ReadWriteLogRecord): ...
