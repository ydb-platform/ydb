"""
Types used in the psycopg_pool package
"""

# Copyright (C) 2023 The Psycopg Team

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias, Union
from collections.abc import Awaitable, Callable

from ._compat import TypeVar

if TYPE_CHECKING:
    from psycopg import AsyncConnection, Connection  # noqa: F401
    from psycopg.rows import TupleRow  # noqa: F401

    from .pool import ConnectionPool  # noqa: F401
    from .pool_async import AsyncConnectionPool  # noqa: F401

# Connection types to make the pool generic
CT = TypeVar("CT", bound="Connection[Any]", default="Connection[TupleRow]")
ACT = TypeVar("ACT", bound="AsyncConnection[Any]", default="AsyncConnection[TupleRow]")

# Callbacks taking a connection from the pool
ConnectionCB: TypeAlias = Callable[[CT], None]
AsyncConnectionCB: TypeAlias = Callable[[ACT], Awaitable[None]]

# Callbacks to pass the pool to on connection failure
ConnectFailedCB: TypeAlias = Callable[["ConnectionPool[Any]"], None]
AsyncConnectFailedCB: TypeAlias = Union[
    Callable[["AsyncConnectionPool[Any]"], None],
    Callable[["AsyncConnectionPool[Any]"], Awaitable[None]],
]

# Types of the connection parameters
ConninfoParam: TypeAlias = Union[str, Callable[[], str]]
AsyncConninfoParam: TypeAlias = Union[
    str,
    Callable[[], str],
    Callable[[], Awaitable[str]],
]
KwargsParam: TypeAlias = Union[dict[str, Any], Callable[[], dict[str, Any]]]
AsyncKwargsParam: TypeAlias = Union[
    dict[str, Any],
    Callable[[], dict[str, Any]],
    Callable[[], Awaitable[dict[str, Any]]],
]
