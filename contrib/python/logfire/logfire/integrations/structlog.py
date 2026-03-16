"""Logfire processor for [structlog](https://www.structlog.org/en/stable/)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import logfire

from .._internal.constants import ATTRIBUTES_MESSAGE_KEY
from .logging import RESERVED_ATTRS as LOGGING_RESERVED_ATTRS

# This file is currently imported eagerly from __init__.py, so it shouldn't import structlog directly
# since that's not a required dependency.
if TYPE_CHECKING:
    from structlog.types import EventDict, WrappedLogger

    from .. import Logfire

RESERVED_ATTRS = LOGGING_RESERVED_ATTRS | {'level', 'event', 'timestamp'}
"""Attributes to strip from the event before sending to Logfire."""


class LogfireProcessor:
    """Logfire processor for [structlog](../../integrations/structlog.md)."""

    def __init__(
        self,
        *,
        console_log: bool = False,
        logfire_instance: Logfire | None = None,
    ) -> None:
        self.console_log = console_log
        self.logfire_instance = (logfire_instance or logfire.DEFAULT_LOGFIRE_INSTANCE).with_settings(
            custom_scope_suffix='structlog'
        )

    def __call__(self, logger: WrappedLogger, name: str, event_dict: EventDict) -> EventDict:
        """A middleware to process structlog event, and send it to **Logfire**."""
        attributes = {k: v for k, v in event_dict.items() if k not in RESERVED_ATTRS}
        level = event_dict.get('level', 'info').lower()
        # NOTE: An event can be `None` in structlog. We may want to create a default msg in those cases.
        msg_template = event_dict.get('event') or 'structlog event'
        attributes.setdefault(ATTRIBUTES_MESSAGE_KEY, msg_template)
        self.logfire_instance.log(
            level=level,
            msg_template=msg_template,
            attributes=attributes,
            console_log=self.console_log,
            exc_info=event_dict.get('exc_info', False),
        )
        return event_dict
