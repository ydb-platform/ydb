from clickhouse_driver import defines
from clickhouse_driver.dbapi import __all__, apilevel, paramstyle, threadsafety  # NOQA
from clickhouse_driver.dbapi.errors import (  # NOQA
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

from .connection import Connection

# Binary is compatible for django's BinaryField.
from .types import JSON, Binary  # NOQA


def connect(
    dsn=None,
    host=None,
    user=defines.DEFAULT_USER,
    password=defines.DEFAULT_PASSWORD,
    port=defines.DEFAULT_PORT,
    database=defines.DEFAULT_DATABASE,
    **kwargs,
):
    """
    Support dict type params in INSERT query and support connection pool.
    """

    if dsn is None and host is None:
        raise ValueError("host or dsn is required")

    return Connection(
        dsn=dsn,
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        **kwargs,
    )
