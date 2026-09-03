from datetime import date, datetime, time
from time import localtime
from typing import Any

from clickhouse_connect.dbapi.connection import Connection

apilevel = "2.0"  # PEP 249  DB API level
threadsafety = 2  # PEP 249  Threads may share the module and connections.
paramstyle = "pyformat"  # PEP 249  Python extended format codes, e.g. ...WHERE name=%(name)s

# PEP 249 type constructors.
Date = date
Time = time
Timestamp = datetime
Binary = bytes


def DateFromTicks(ticks: float) -> date:  # noqa: N802
    return date(*localtime(ticks)[:3])


def TimeFromTicks(ticks: float) -> time:  # noqa: N802
    return time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float) -> datetime:  # noqa: N802
    return datetime(*localtime(ticks)[:6])


class Error(Exception):
    pass


def connect(
    host: str | None = None,
    database: str | None = None,
    username: str = "",
    password: str = "",
    port: int | None = None,
    **kwargs: Any,
) -> Connection:
    secure = kwargs.pop("secure", False)
    return Connection(
        host=host,
        database=database,
        username=username,
        password=password,
        port=port,
        secure=secure,
        **kwargs,
    )
