from __future__ import annotations

from typing import Any

try:
    from opentelemetry.instrumentation.celery import CeleryInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_celery()` requires the `opentelemetry-instrumentation-celery` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[celery]'"
    )


def instrument_celery(**kwargs: Any) -> None:
    """Instrument the `celery` module so that spans are automatically created for each task.

    See the `Logfire.instrument_celery` method for details.
    """
    return CeleryInstrumentor().instrument(**kwargs)
