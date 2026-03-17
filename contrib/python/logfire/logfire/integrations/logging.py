"""Integration with the standard library logging module."""

from __future__ import annotations

from collections.abc import Mapping
from logging import NOTSET, Handler as LoggingHandler, LogRecord, StreamHandler
from typing import TYPE_CHECKING, Any, ClassVar, cast

import logfire

from .._internal.constants import (
    ATTRIBUTES_LOGGING_ARGS_KEY,
    ATTRIBUTES_LOGGING_NAME,
    ATTRIBUTES_MESSAGE_KEY,
    ATTRIBUTES_MESSAGE_TEMPLATE_KEY,
    LOGGING_TO_OTEL_LEVEL_NUMBERS,
)
from .._internal.utils import is_instrumentation_suppressed

# skip natural LogRecord attributes
# http://docs.python.org/library/logging.html#logrecord-attributes
RESERVED_ATTRS: frozenset[str] = frozenset(
    [
        'args',
        'asctime',
        'created',
        'exc_info',
        'exc_text',
        'filename',
        'funcName',
        'levelname',
        'levelno',
        'lineno',
        'module',
        'msecs',
        'message',
        'msg',
        'name',
        'pathname',
        'process',
        'processName',
        'relativeCreated',
        'stack_info',
        'thread',
        'threadName',
        'taskName',  # 3.12+
    ]
)

if TYPE_CHECKING:
    from .. import Logfire


class LogfireLoggingHandler(LoggingHandler):
    """A [logging](../../integrations/logging.md) handler that sends logs to **Logfire**.

    Args:
        level: The threshold level for this handler. Logging messages which are less severe than *level* will be ignored.
        fallback: A fallback handler to use when [instrumentation is suppressed](../../how-to-guides/suppress.md#suppress-instrumentation).
        logfire_instance: The Logfire instance to use when emitting logs. Defaults to the default global instance.
    """

    custom_scope_suffix: ClassVar[str] = 'stdlib.logging'

    def __init__(
        self,
        level: int | str = NOTSET,
        fallback: LoggingHandler = StreamHandler(),
        logfire_instance: Logfire | None = None,
    ) -> None:
        super().__init__(level=level)
        self.fallback = fallback
        self.logfire_instance = (logfire_instance or logfire.DEFAULT_LOGFIRE_INSTANCE).with_settings(
            custom_scope_suffix=self.custom_scope_suffix
        )

    def emit(self, record: LogRecord) -> None:
        """Send the log to Logfire.

        Args:
            record: The log record to send.
        """
        if is_instrumentation_suppressed():
            self.fallback.handle(record)
            return

        attributes = self.fill_attributes(record)

        self.logfire_instance.log(
            msg_template=attributes.pop(ATTRIBUTES_MESSAGE_TEMPLATE_KEY, record.msg),
            level=LOGGING_TO_OTEL_LEVEL_NUMBERS.get(record.levelno, record.levelno),
            attributes=attributes,
            exc_info=record.exc_info,
        )

    def fill_attributes(self, record: LogRecord) -> dict[str, Any]:
        """Fill the attributes to send to Logfire.

        This method can be overridden to add more attributes.

        Args:
            record: The log record.

        Returns:
            The attributes for the log record.
        """
        attributes = {k: v for k, v in record.__dict__.items() if k not in RESERVED_ATTRS}
        attributes['code.filepath'] = record.pathname
        attributes['code.lineno'] = record.lineno
        attributes['code.function'] = record.funcName
        attributes[ATTRIBUTES_LOGGING_NAME] = record.name

        attributes[ATTRIBUTES_MESSAGE_KEY], args = _format_message(record)
        attributes.update(args)

        return attributes


def _format_message(record: LogRecord) -> tuple[str, Mapping[str, Any]]:
    args = record.args
    msg = record.msg

    if not args:
        return msg, {}

    if type(args) is tuple:
        return msg % args, {ATTRIBUTES_LOGGING_ARGS_KEY: args}

    try:
        # args is a Mapping. Python extracted it from a tuple here:
        # https://github.com/python/cpython/blob/4c71d51a4b7989fc8754ba512c40e21666f9db0d/Lib/logging/__init__.py#L324-L326
        # Whether it should be treated as one positional argument or a mapping of keyword arguments
        # depends on the format string.
        # First check if the user wrote something like:
        #   log('Hello %s', {'name': 'Alice'})
        # in which case we should treat {'name': 'Alice'} as a single positional argument.
        formatted = msg % (args,)
    except TypeError:
        # This means the user wrote something like:
        #   log('Hello %(name)s', {'name': 'Alice'})
        # so `name` should be treated as a keyword argument, i.e. its own attribute.
        return msg % args, cast('Mapping[str, Any]', args)
    else:
        # We have to wrap the single positional argument in a tuple.
        # Otherwise this:
        #   log('Hello %s', x)
        # would result in the shape of the data depending on whether x is a Mapping or not.
        return formatted, {ATTRIBUTES_LOGGING_ARGS_KEY: (args,)}
