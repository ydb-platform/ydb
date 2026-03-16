"""PEP 249 (DB API 2.0) interface for Logfire query data.

This module provides a DB API 2.0 compatible interface wrapping the
`LogfireQueryClient`, enabling Logfire query data to work with any tool that
supports DB API 2.0 connections — e.g. marimo SQL cells, pandas `read_sql()`,
and Jupyter `%%sql` magic.

Usage:
    import logfire.db_api

    conn = logfire.db_api.connect(read_token='pylf_v1_us_...')
    cursor = conn.cursor()
    cursor.execute('SELECT start_timestamp, message FROM records LIMIT 10')
    rows = cursor.fetchall()
    conn.close()
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from logfire.experimental.query_client import LogfireQueryClient

if TYPE_CHECKING:
    from logfire.experimental.query_client import ColumnDetails

# TODO: make use of PEP 661 sentinels once accepted.
_UNSET = Enum('_UNSET', 'UNSET').UNSET
"""Sentinel to distinguish 'not set' from an explicit `None`."""

# ---------------------------------------------------------------------------
# PEP 249 module-level attributes
# ---------------------------------------------------------------------------

apilevel = '2.0'
threadsafety = 1  # threads may share the module but not connections
paramstyle = 'pyformat'  # %(name)s style parameters

# ---------------------------------------------------------------------------
# PEP 249 exception hierarchy
# ---------------------------------------------------------------------------


class Warning(Exception):
    """Exception raised for important warnings, e.g. data truncation."""


class Error(Exception):
    """Base class for all DB API errors."""


class InterfaceError(Error):
    """Exception raised for errors related to the database interface."""


class DatabaseError(Error):
    """Exception raised for errors related to the database."""


class OperationalError(DatabaseError):
    """Exception raised for errors related to the database's operation."""


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. bad SQL or using a closed cursor."""


class NotSupportedError(DatabaseError):
    """Exception raised when an unsupported operation is attempted."""


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_LIMIT = 10_000  # server-side maximum
DEFAULT_MIN_TIMESTAMP_AGE = timedelta(days=1)


# ---------------------------------------------------------------------------
# Parameter substitution helpers
# ---------------------------------------------------------------------------


def _escape_value(value: Any) -> str:
    """Escape a Python value for safe interpolation into SQL.

    This is client-side escaping. The Logfire query API is read-only so
    injection risk is limited to the caller's own data.
    """
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        # Escape single quotes by doubling them
        return "'" + value.replace("'", "''") + "'"
    # Fallback: convert to string and quote
    return "'" + str(value).replace("'", "''") + "'"


def _substitute_params(operation: str, parameters: dict[str, Any] | Sequence[Any]) -> str:
    """Substitute parameters into the SQL operation string.

    For `pyformat` paramstyle, `parameters` should be a dict and the SQL
    should contain `%(name)s` placeholders.  Sequence parameters are also
    supported for positional `%s` placeholders.
    """
    if isinstance(parameters, dict):
        escaped = {key: _escape_value(val) for key, val in parameters.items()}
        return operation % escaped
    if isinstance(parameters, (str, bytes, bytearray)):
        raise ProgrammingError('parameters must be a sequence (e.g. list or tuple) or a dict, not a string/bytes')
    # Sequence of positional parameters
    escaped_seq = tuple(_escape_value(val) for val in parameters)
    return operation % escaped_seq


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


class Connection:
    """PEP 249 Connection wrapping a `LogfireQueryClient`."""

    def __init__(
        self,
        client: LogfireQueryClient,
        *,
        min_timestamp: datetime | timedelta | None = DEFAULT_MIN_TIMESTAMP_AGE,
        max_timestamp: datetime | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> None:
        self.client = client
        self.client.__enter__()
        self.closed = False
        if isinstance(min_timestamp, timedelta):
            min_timestamp = datetime.now(timezone.utc) - min_timestamp
        self.min_timestamp = min_timestamp
        self.max_timestamp = max_timestamp
        self.limit = limit

    # -- PEP 249 methods ---------------------------------------------------

    def close(self) -> None:
        """Close the connection and the underlying HTTP client."""
        if not self.closed:  # pragma: no branch
            self.client.__exit__(None, None, None)
            self.closed = True

    def commit(self) -> None:
        """No-op (read-only connection)."""

    def rollback(self) -> None:
        """No-op (read-only connection)."""

    def cursor(self) -> Cursor:
        """Create a new cursor associated with this connection."""
        if self.closed:
            raise ProgrammingError('Connection is closed')
        return Cursor(self)

    # -- context manager ---------------------------------------------------

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


class Cursor:
    """PEP 249 Cursor that executes queries via `LogfireQueryClient.query_json_rows()`."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._closed = False
        self._rows: list[tuple[Any, ...]] = []
        self._columns: Sequence[ColumnDetails] = []
        self._row_index = 0
        self._description: list[tuple[Any, ...]] | None = None
        self.rowcount = -1
        self.arraysize = 1

        # Per-cursor overrides (_UNSET means inherit from connection, None means no filter)
        self.min_timestamp: datetime | None = _UNSET  # type: ignore[assignment]
        self.max_timestamp: datetime | None = _UNSET  # type: ignore[assignment]
        self.limit: int = _UNSET  # type: ignore[assignment]

    # -- PEP 249 properties ------------------------------------------------

    @property
    def description(self) -> list[tuple[Any, ...]] | None:
        """Column description as a list of 7-tuples per PEP 249.

        Each tuple: (name, type_code, display_size, internal_size,
        precision, scale, null_ok).
        """
        return self._description

    # -- PEP 249 methods ---------------------------------------------------

    def execute(self, operation: str, parameters: dict[str, Any] | Sequence[Any] | None = None) -> None:
        """Execute a SQL query.

        Args:
            operation: SQL query string, optionally with `%(name)s` placeholders.
            parameters: Parameter dict (or sequence) for substitution.
        """
        if self._closed:
            raise ProgrammingError('Cursor is closed')
        if self._connection.closed:
            raise ProgrammingError('Connection is closed')

        sql = operation
        if parameters is not None:
            sql = _substitute_params(operation, parameters)

        result = self._connection.client.query_json_rows(
            sql=sql,
            min_timestamp=self.min_timestamp if self.min_timestamp is not _UNSET else self._connection.min_timestamp,
            max_timestamp=self.max_timestamp if self.max_timestamp is not _UNSET else self._connection.max_timestamp,
            limit=self.limit if self.limit is not _UNSET else self._connection.limit,
        )

        self._columns = result['columns']
        self._rows = [tuple(row[col['name']] for col in self._columns) for row in result['rows']]
        self._row_index = 0
        self.rowcount = len(self._rows)
        self._description = [
            (col['name'], col['datatype'], None, None, None, None, col.get('nullable')) for col in self._columns
        ]

        effective_limit = self.limit if self.limit is not _UNSET else self._connection.limit
        if self.rowcount == effective_limit:
            warnings.warn(
                f'Query returned {effective_limit} rows which is the limit. '
                'There may be more results. Use LIMIT/OFFSET in your SQL to paginate.',
                stacklevel=2,
            )

    def executemany(self, operation: str, seq_of_parameters: Sequence[dict[str, Any] | Sequence[Any]]) -> None:
        """Execute the same query with each set of parameters.

        Note: for a read-only API this is of limited utility, but is included
        for PEP 249 compliance.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or `None` if no more rows are available."""
        if self._closed:
            raise ProgrammingError('Cursor is closed')
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next `size` rows (default: `arraysize`)."""
        if self._closed:
            raise ProgrammingError('Cursor is closed')
        if size is None:
            size = self.arraysize
        end = self._row_index + size
        rows = self._rows[self._row_index : end]
        self._row_index = end
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows."""
        if self._closed:
            raise ProgrammingError('Cursor is closed')
        rows = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return rows

    def close(self) -> None:
        """Mark the cursor as closed."""
        self._closed = True

    def setinputsizes(self, _sizes: Any) -> None:
        """No-op (PEP 249 optional)."""

    def setoutputsize(self, _size: Any, _column: Any = None) -> None:
        """No-op (PEP 249 optional)."""

    # -- context manager ---------------------------------------------------

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Module-level connect() factory (PEP 249)
# ---------------------------------------------------------------------------


def connect(
    read_token: str,
    base_url: str | None = None,
    timeout: float = 30.0,
    *,
    min_timestamp: datetime | timedelta | None = DEFAULT_MIN_TIMESTAMP_AGE,
    max_timestamp: datetime | None = None,
    limit: int = DEFAULT_LIMIT,
    **kwargs: Any,
) -> Connection:
    """Create a PEP 249 connection to the Logfire query API.

    Args:
        read_token: A Logfire read token for authentication.
        base_url: Override the default API base URL (inferred from token region).
        timeout: HTTP request timeout in seconds.
        min_timestamp: Default lower bound for `start_timestamp` filtering.
            Accepts a `datetime` for an exact bound, a `timedelta` for a
            relative window (computed as `now - timedelta`), or `None` to
            disable the filter.  Defaults to 1 day ago.
        max_timestamp: Default upper bound for `start_timestamp` filtering.
        limit: Default row limit per query (max 10,000). When the number of
            returned rows equals the limit a warning is emitted.
        **kwargs: Additional keyword arguments forwarded to the underlying
            `httpx.Client`.

    Returns:
        A PEP 249 `Connection` object.
    """
    from httpx import Timeout as HttpxTimeout

    client = LogfireQueryClient(
        read_token=read_token,
        base_url=base_url,
        timeout=HttpxTimeout(timeout),
        **kwargs,
    )
    return Connection(
        client,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
        limit=limit,
    )
