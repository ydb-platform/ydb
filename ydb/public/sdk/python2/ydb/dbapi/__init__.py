from __future__ import absolute_import
from __future__ import unicode_literals

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

version = "0.0.31"

version_info = (
    1,
    0,
    0,
)

apilevel = "1.0"

threadsafety = 0

paramstyle = "qmark"

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
