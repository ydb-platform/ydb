from .. import Logfire as Logfire
from .._internal.constants import ATTRIBUTES_LOGGING_ARGS_KEY as ATTRIBUTES_LOGGING_ARGS_KEY, ATTRIBUTES_LOGGING_NAME as ATTRIBUTES_LOGGING_NAME, ATTRIBUTES_MESSAGE_KEY as ATTRIBUTES_MESSAGE_KEY, ATTRIBUTES_MESSAGE_TEMPLATE_KEY as ATTRIBUTES_MESSAGE_TEMPLATE_KEY, LOGGING_TO_OTEL_LEVEL_NUMBERS as LOGGING_TO_OTEL_LEVEL_NUMBERS
from .._internal.utils import is_instrumentation_suppressed as is_instrumentation_suppressed
from _typeshed import Incomplete
from collections.abc import Mapping as Mapping
from logging import Handler as LoggingHandler, LogRecord
from typing import Any, ClassVar

RESERVED_ATTRS: frozenset[str]

class LogfireLoggingHandler(LoggingHandler):
    """A [logging](../../integrations/logging.md) handler that sends logs to **Logfire**.

    Args:
        level: The threshold level for this handler. Logging messages which are less severe than *level* will be ignored.
        fallback: A fallback handler to use when [instrumentation is suppressed](../../how-to-guides/suppress.md#suppress-instrumentation).
        logfire_instance: The Logfire instance to use when emitting logs. Defaults to the default global instance.
    """
    custom_scope_suffix: ClassVar[str]
    fallback: Incomplete
    logfire_instance: Incomplete
    def __init__(self, level: int | str = ..., fallback: LoggingHandler = ..., logfire_instance: Logfire | None = None) -> None: ...
    def emit(self, record: LogRecord) -> None:
        """Send the log to Logfire.

        Args:
            record: The log record to send.
        """
    def fill_attributes(self, record: LogRecord) -> dict[str, Any]:
        """Fill the attributes to send to Logfire.

        This method can be overridden to add more attributes.

        Args:
            record: The log record.

        Returns:
            The attributes for the log record.
        """
