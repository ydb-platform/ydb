from __future__ import annotations

import sqlite3
from typing import Any, TypeVar

from opentelemetry.trace import TracerProvider

try:
    from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_sqlite3()` requires the `opentelemetry-instrumentation-sqlite3` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[sqlite3]'"
    )


SQLite3Connection = TypeVar('SQLite3Connection', sqlite3.Connection, None)


def instrument_sqlite3(*, conn: SQLite3Connection, tracer_provider: TracerProvider, **kwargs: Any) -> SQLite3Connection:
    """Instrument the `sqlite3` module so that spans are automatically created for each query.

    See the `Logfire.instrument_sqlite3` method for details.
    """
    if conn is not None:
        return SQLite3Instrumentor().instrument_connection(conn, tracer_provider=tracer_provider)
    else:
        return SQLite3Instrumentor().instrument(tracer_provider=tracer_provider, **kwargs)  # type: ignore
