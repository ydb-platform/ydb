"""
compatibility functions for different Python versions
"""

# Copyright (C) 2021 The Psycopg Team

from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

import psycopg
import psycopg.errors as e

PSYCOPG_VERSION = tuple(map(int, psycopg.__version__.split(".", 2)[:2]))

if PSYCOPG_VERSION >= (3, 3):
    AsyncPoolConnection = psycopg.AsyncConnection
    PoolConnection = psycopg.Connection

else:

    class AsyncPoolConnection(psycopg.AsyncConnection):  # type: ignore[no-redef]
        """
        Thin wrapper around the psycopg async connection to improve pool integration.
        """

        async def close(self) -> None:
            if pool := getattr(self, "_pool", None):
                # Connection currently checked out from the pool.
                # Instead of closing it, return it to the pool.
                await pool.putconn(self)
            else:
                # Connection not part of any pool, or currently into the pool.
                # Close the connection for real.
                await super().close()

    class PoolConnection(psycopg.Connection):  # type: ignore[no-redef]
        """
        Thin wrapper around the psycopg connection to improve pool integration.
        """

        def close(self) -> None:
            if pool := getattr(self, "_pool", None):
                # Connection currently checked out from the pool.
                # Instead of closing it, return it to the pool.
                pool.putconn(self)
            else:
                # Connection not part of any pool, or currently into the pool.
                # Close the connection for real.
                super().close()


__all__ = [
    "AsyncPoolConnection",
    "PoolConnection",
    "Self",
    "TypeVar",
]

# Workaround for psycopg < 3.0.8.
# Timeout on NullPool connection mignt not work correctly.
try:
    ConnectionTimeout: type[e.OperationalError] = e.ConnectionTimeout
except AttributeError:

    class DummyConnectionTimeout(e.OperationalError):
        pass

    ConnectionTimeout = DummyConnectionTimeout
