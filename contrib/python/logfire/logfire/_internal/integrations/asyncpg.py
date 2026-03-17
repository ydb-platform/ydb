from __future__ import annotations

from typing import Any

try:
    from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_asyncpg()` requires the `opentelemetry-instrumentation-asyncpg` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[asyncpg]'"
    )


def instrument_asyncpg(**kwargs: Any) -> None:
    """Instrument the `asyncpg` module so that spans are automatically created for each query.

    See the `Logfire.instrument_asyncpg` method for details.
    """
    AsyncPGInstrumentor().instrument(**kwargs)
