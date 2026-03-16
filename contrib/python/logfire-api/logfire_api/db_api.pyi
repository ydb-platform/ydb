from _typeshed import Incomplete
from collections.abc import Sequence
from datetime import datetime, timedelta
from logfire.experimental.query_client import ColumnDetails as ColumnDetails, LogfireQueryClient as LogfireQueryClient
from typing import Any

apilevel: str
threadsafety: int
paramstyle: str

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

DEFAULT_LIMIT: int
DEFAULT_MIN_TIMESTAMP_AGE: Incomplete

class Connection:
    """PEP 249 Connection wrapping a `LogfireQueryClient`."""
    client: Incomplete
    closed: bool
    min_timestamp: Incomplete
    max_timestamp: Incomplete
    limit: Incomplete
    def __init__(self, client: LogfireQueryClient, *, min_timestamp: datetime | timedelta | None = ..., max_timestamp: datetime | None = None, limit: int = ...) -> None: ...
    def close(self) -> None:
        """Close the connection and the underlying HTTP client."""
    def commit(self) -> None:
        """No-op (read-only connection)."""
    def rollback(self) -> None:
        """No-op (read-only connection)."""
    def cursor(self) -> Cursor:
        """Create a new cursor associated with this connection."""
    def __enter__(self) -> Connection: ...
    def __exit__(self, *args: Any) -> None: ...

class Cursor:
    """PEP 249 Cursor that executes queries via `LogfireQueryClient.query_json_rows()`."""
    rowcount: int
    arraysize: int
    min_timestamp: datetime | None
    max_timestamp: datetime | None
    limit: int
    def __init__(self, connection: Connection) -> None: ...
    @property
    def description(self) -> list[tuple[Any, ...]] | None:
        """Column description as a list of 7-tuples per PEP 249.

        Each tuple: (name, type_code, display_size, internal_size,
        precision, scale, null_ok).
        """
    def execute(self, operation: str, parameters: dict[str, Any] | Sequence[Any] | None = None) -> None:
        """Execute a SQL query.

        Args:
            operation: SQL query string, optionally with `%(name)s` placeholders.
            parameters: Parameter dict (or sequence) for substitution.
        """
    def executemany(self, operation: str, seq_of_parameters: Sequence[dict[str, Any] | Sequence[Any]]) -> None:
        """Execute the same query with each set of parameters.

        Note: for a read-only API this is of limited utility, but is included
        for PEP 249 compliance.
        """
    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or `None` if no more rows are available."""
    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next `size` rows (default: `arraysize`)."""
    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows."""
    def close(self) -> None:
        """Mark the cursor as closed."""
    def setinputsizes(self, _sizes: Any) -> None:
        """No-op (PEP 249 optional)."""
    def setoutputsize(self, _size: Any, _column: Any = None) -> None:
        """No-op (PEP 249 optional)."""
    def __enter__(self) -> Cursor: ...
    def __exit__(self, *args: Any) -> None: ...

def connect(read_token: str, base_url: str | None = None, timeout: float = 30.0, *, min_timestamp: datetime | timedelta | None = ..., max_timestamp: datetime | None = None, limit: int = ..., **kwargs: Any) -> Connection:
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
