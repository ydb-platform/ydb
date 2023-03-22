from .connection import Connection
from .errors import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)

apilevel = "1.0"

threadsafety = 0

paramstyle = "pyformat"

errors = (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
