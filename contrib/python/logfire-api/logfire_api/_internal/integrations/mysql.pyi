from mysql.connector.abstracts import MySQLConnectionAbstract as MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection as PooledMySQLConnection
from opentelemetry.trace import TracerProvider
from typing import Any, TypeVar

MySQLConnection = TypeVar('MySQLConnection', 'PooledMySQLConnection | MySQLConnectionAbstract', None)

def instrument_mysql(*, conn: MySQLConnection = None, tracer_provider: TracerProvider, **kwargs: Any) -> MySQLConnection:
    """Instrument the `mysql` module or a specific MySQL connection so that spans are automatically created for each operation.

    See the `Logfire.instrument_mysql` method for details.
    """
