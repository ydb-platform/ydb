from opentelemetry.sdk.trace import Span as Span
from pymongo.monitoring import CommandFailedEvent as CommandFailedEvent, CommandStartedEvent as CommandStartedEvent, CommandSucceededEvent as CommandSucceededEvent
from typing import Any, Callable

def instrument_pymongo(*, capture_statement: bool, request_hook: Callable[[Span, CommandStartedEvent], None] | None = None, response_hook: Callable[[Span, CommandSucceededEvent], None] | None = None, failed_hook: Callable[[Span, CommandFailedEvent], None] | None = None, **kwargs: Any) -> None:
    """Instrument the `pymongo` module so that spans are automatically created for each operation.

    See the `Logfire.instrument_pymongo` method for details.
    """
