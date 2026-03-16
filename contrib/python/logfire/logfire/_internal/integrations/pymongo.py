from __future__ import annotations

from typing import Any, Callable

from opentelemetry.sdk.trace import Span
from pymongo.monitoring import CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent

try:
    from opentelemetry.instrumentation.pymongo import (
        PymongoInstrumentor,
        dummy_callback,
    )
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_pymongo()` requires the `opentelemetry-instrumentation-pymongo` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[pymongo]'"
    )


def instrument_pymongo(
    *,
    capture_statement: bool,
    request_hook: Callable[[Span, CommandStartedEvent], None] | None = None,
    response_hook: Callable[[Span, CommandSucceededEvent], None] | None = None,
    failed_hook: Callable[[Span, CommandFailedEvent], None] | None = None,
    **kwargs: Any,
) -> None:
    """Instrument the `pymongo` module so that spans are automatically created for each operation.

    See the `Logfire.instrument_pymongo` method for details.
    """
    PymongoInstrumentor().instrument(
        request_hook=request_hook or dummy_callback,
        response_hook=response_hook or dummy_callback,
        failed_hook=failed_hook or dummy_callback,
        capture_statement=capture_statement,
        **kwargs,
    )
