from .._internal.constants import ATTRIBUTES_LOGGING_ARGS_KEY as ATTRIBUTES_LOGGING_ARGS_KEY, ATTRIBUTES_MESSAGE_KEY as ATTRIBUTES_MESSAGE_KEY, ATTRIBUTES_MESSAGE_TEMPLATE_KEY as ATTRIBUTES_MESSAGE_TEMPLATE_KEY
from .._internal.stack_info import warn_at_user_stacklevel as warn_at_user_stacklevel
from .logging import LogfireLoggingHandler as LogfireLoggingHandler
from _typeshed import Incomplete
from logging import LogRecord
from typing import Any

LOGURU_PATH: Incomplete

class LoguruInspectionFailed(RuntimeWarning):
    """Warning raised when magic introspection of loguru stack frames fails.

    This may happen if the loguru library changes in a way that breaks the introspection.
    """

class LogfireHandler(LogfireLoggingHandler):
    """A loguru handler that sends logs to **Logfire**."""
    custom_scope_suffix: str
    def fill_attributes(self, record: LogRecord) -> dict[str, Any]:
        """Fill attributes from a log record.

        It filters out the 'extra' attribute and adds it's content to the attributes.

        Args:
            record: The log record.

        Returns:
            The attributes for the log record.
        """
