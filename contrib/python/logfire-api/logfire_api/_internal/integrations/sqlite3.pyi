import sqlite3
from opentelemetry.trace import TracerProvider
from typing import Any, TypeVar

SQLite3Connection = TypeVar('SQLite3Connection', sqlite3.Connection, None)

def instrument_sqlite3(*, conn: SQLite3Connection, tracer_provider: TracerProvider, **kwargs: Any) -> SQLite3Connection:
    """Instrument the `sqlite3` module so that spans are automatically created for each query.

    See the `Logfire.instrument_sqlite3` method for details.
    """
