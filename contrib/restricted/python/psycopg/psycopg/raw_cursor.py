"""
psycopg raw queries cursors
"""

# Copyright (C) 2023 The Psycopg Team

from __future__ import annotations

from typing import TYPE_CHECKING

from .abc import ConnectionType
from .rows import Row
from .cursor import Cursor
from ._queries import PostgresRawQuery
from ._cursor_base import BaseCursor
from .cursor_async import AsyncCursor
from ._server_cursor import ServerCursor
from ._server_cursor_async import AsyncServerCursor

if TYPE_CHECKING:
    from typing import Any  # noqa: F401

    from .connection import Connection  # noqa: F401
    from .connection_async import AsyncConnection  # noqa: F401


class RawCursorMixin(BaseCursor[ConnectionType, Row]):
    _query_cls = PostgresRawQuery


class RawCursor(RawCursorMixin["Connection[Any]", Row], Cursor[Row]):
    __module__ = "psycopg"


class AsyncRawCursor(RawCursorMixin["AsyncConnection[Any]", Row], AsyncCursor[Row]):
    __module__ = "psycopg"


class RawServerCursor(RawCursorMixin["Connection[Any]", Row], ServerCursor[Row]):
    __module__ = "psycopg"


class AsyncRawServerCursor(
    RawCursorMixin["AsyncConnection[Any]", Row], AsyncServerCursor[Row]
):
    __module__ = "psycopg"
