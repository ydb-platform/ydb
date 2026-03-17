"""Integration with [Loguru](https://github.com/Delgan/loguru)."""

from __future__ import annotations

import inspect
from logging import LogRecord
from pathlib import Path
from typing import Any

import loguru

from .._internal.constants import ATTRIBUTES_LOGGING_ARGS_KEY, ATTRIBUTES_MESSAGE_KEY, ATTRIBUTES_MESSAGE_TEMPLATE_KEY
from .._internal.stack_info import warn_at_user_stacklevel
from .logging import LogfireLoggingHandler

LOGURU_PATH = Path(loguru.__file__).parent


class LoguruInspectionFailed(RuntimeWarning):
    """Warning raised when magic introspection of loguru stack frames fails.

    This may happen if the loguru library changes in a way that breaks the introspection.
    """


class LogfireHandler(LogfireLoggingHandler):
    """A loguru handler that sends logs to **Logfire**."""

    custom_scope_suffix = 'loguru'

    def fill_attributes(self, record: LogRecord) -> dict[str, Any]:
        """Fill attributes from a log record.

        It filters out the 'extra' attribute and adds it's content to the attributes.

        Args:
            record: The log record.

        Returns:
            The attributes for the log record.
        """
        attributes = super().fill_attributes(record)
        attributes.update(attributes.pop('extra', {}))
        assert not record.args

        if _LOG_METHOD_CODE:  # pragma: no branch
            frame = inspect.currentframe()
            while frame:  # pragma: no branch
                if frame.f_code is _LOG_METHOD_CODE:
                    frame_locals = frame.f_locals
                    if 'message' in frame_locals:  # pragma: no branch
                        attributes[ATTRIBUTES_MESSAGE_TEMPLATE_KEY] = frame_locals['message']
                    else:  # pragma: no cover
                        warn_at_user_stacklevel(
                            'Failed to extract message template (span name) for loguru log.', LoguruInspectionFailed
                        )

                    args = frame_locals.get('args')
                    if isinstance(args, (tuple, list)):  # pragma: no branch
                        if args:
                            attributes[ATTRIBUTES_LOGGING_ARGS_KEY] = args
                    else:  # pragma: no cover
                        warn_at_user_stacklevel('Failed to extract args for loguru log.', LoguruInspectionFailed)

                    original_record: dict[str, Any] | None = frame_locals.get('log_record')
                    if (
                        isinstance(original_record, dict)
                        and isinstance(message := original_record.get('message'), str)
                        and message in record.msg
                    ):  # pragma: no branch
                        # `record.msg` may include a traceback added by Loguru,
                        # replace it with the original message.
                        attributes[ATTRIBUTES_MESSAGE_KEY] = message
                    else:  # pragma: no cover
                        warn_at_user_stacklevel(
                            'Failed to extract original message for loguru log.', LoguruInspectionFailed
                        )

                    break

                frame = frame.f_back
            else:  # pragma: no cover
                warn_at_user_stacklevel(
                    'Failed to find loguru log frame to extract detailed information', LoguruInspectionFailed
                )

        return attributes


try:
    _LOG_METHOD_CODE = inspect.unwrap(type(loguru.logger)._log).__code__  # type: ignore
except Exception:  # pragma: no cover
    _LOG_METHOD_CODE = None  # type: ignore
    warn_at_user_stacklevel(
        'Failed to find loguru log method code to extract detailed information', LoguruInspectionFailed
    )
