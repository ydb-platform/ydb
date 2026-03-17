import weakref

from sentry_sdk.hub import Hub
from sentry_sdk.utils import ContextVar
from sentry_sdk.integrations import Integration
from sentry_sdk.scope import add_global_event_processor

from sentry_sdk._types import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Optional

    from sentry_sdk._types import Event, Hint


class DedupeIntegration(Integration):
    identifier = "dedupe"

    def __init__(self):
        # type: () -> None
        self._last_seen = ContextVar("last-seen")

    @staticmethod
    def setup_once():
        # type: () -> None
        @add_global_event_processor
        def processor(event, hint):
            # type: (Event, Optional[Hint]) -> Optional[Event]
            if hint is None:
                return event

            integration = Hub.current.get_integration(DedupeIntegration)

            if integration is None:
                return event

            exc_info = hint.get("exc_info", None)
            if exc_info is None:
                return event

            last_seen = integration._last_seen.get(None)
            if last_seen is not None:
                # last_seen is either a weakref or the original instance
                last_seen = (
                    last_seen() if isinstance(last_seen, weakref.ref) else last_seen
                )

            exc = exc_info[1]
            if last_seen is exc:
                return None
            # we can only weakref non builtin types
            try:
                integration._last_seen.set(weakref.ref(exc))
            except TypeError:
                integration._last_seen.set(exc)

            return event
