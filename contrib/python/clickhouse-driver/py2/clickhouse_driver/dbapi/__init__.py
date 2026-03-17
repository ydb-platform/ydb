from .connection import Connection
from .errors import (
    Warning, Error, DataError, DatabaseError, ProgrammingError, IntegrityError,
    InterfaceError, InternalError, NotSupportedError, OperationalError
)

apilevel = '2.0'

threadsafety = 2

paramstyle = 'pyformat'


def connect(dsn=None, user=None, password=None, host=None, port=None,
            database=None, **kwargs):
    """
    Create a new database connection.

    The connection can be specified via DSN:

        ``conn = connect("clickhouse://localhost/test?param1=value1&...")``

    or using database and credentials arguments:

        ``conn = connect(database="test", user="default", password="default",
        host="localhost", **kwargs)``

    The basic connection parameters are:

    - *host*: host with running ClickHouse server.
    - *port*: port ClickHouse server is bound to.
    - *database*: database connect to.
    - *user*: database user.
    - *password*: user's password.

    See defaults in :data:`~clickhouse_driver.connection.Connection`
    constructor.

    DSN or host is required.

    Any other keyword parameter will be passed to the underlying Connection
    class.

    :return: a new connection.
    """

    if dsn is None and host is None:
        raise ValueError('host or dsn is required')

    return Connection(dsn=dsn, user=user, password=password, host=host,
                      port=port, database=database, **kwargs)


__all__ = [
    'connect',
    'Warning', 'Error', 'DataError', 'DatabaseError', 'ProgrammingError',
    'IntegrityError', 'InterfaceError', 'InternalError', 'NotSupportedError',
    'OperationalError'
]
