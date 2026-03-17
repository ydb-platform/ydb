from __future__ import annotations

from typing import Any, TypeVar

from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
from opentelemetry.trace import TracerProvider

try:
    from opentelemetry.instrumentation.mysql import MySQLInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_mysql()` requires the `opentelemetry-instrumentation-mysql` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[mysql]'"
    )


MySQLConnection = TypeVar('MySQLConnection', 'PooledMySQLConnection | MySQLConnectionAbstract', None)


def instrument_mysql(
    *,
    conn: MySQLConnection = None,
    tracer_provider: TracerProvider,
    **kwargs: Any,
) -> MySQLConnection:
    """Instrument the `mysql` module or a specific MySQL connection so that spans are automatically created for each operation.

    See the `Logfire.instrument_mysql` method for details.
    """
    if conn is not None:
        return MySQLInstrumentor().instrument_connection(conn, tracer_provider=tracer_provider)  # type: ignore[reportUnknownMemberType]
    return MySQLInstrumentor().instrument(**kwargs, tracer_provider=tracer_provider)  # type: ignore[reportUnknownMemberType]
