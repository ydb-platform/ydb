"""
Connection pool errors.
"""

# Copyright (C) 2021 The Psycopg Team

from psycopg import errors as e


class PoolClosed(e.OperationalError):
    """Attempt to get a connection from a closed pool."""

    __module__ = "psycopg_pool"


class PoolTimeout(e.OperationalError):
    """The pool couldn't provide a connection in acceptable time."""

    __module__ = "psycopg_pool"


class TooManyRequests(e.OperationalError):
    """Too many requests in the queue waiting for a connection from the pool."""

    __module__ = "psycopg_pool"
