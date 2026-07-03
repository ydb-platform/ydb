"""
The driver exception classes here include all named exceptions required by th DB API 2.0 specification. It's not clear
how useful that naming convention is, but the convention is used for potential improved compatibility with other
libraries.  In most cases docstring are taken from the DBIApi 2.0 documentation
"""

import re

_error_name_re = re.compile(r"\(([A-Z][A-Z0-9_]+)\)")


def error_code_from_header(header_value: str | None) -> int | None:
    """Parse the numeric ClickHouse error code from the exception header value."""
    if not header_value:
        return None
    try:
        return int(header_value)
    except (TypeError, ValueError):
        return None


def error_name_from_body(body: str | None) -> str | None:
    """Extract the symbolic ClickHouse error name (e.g. UNKNOWN_TABLE) from a response body."""
    matches = _error_name_re.findall(body or "")
    return matches[-1] if matches else None


class ClickHouseError(Exception):
    """Exception related to operation with ClickHouse."""


class Warning(Warning, ClickHouseError):  # noqa: N818
    """Exception raised for important warnings like data truncations
    while inserting, etc."""


class Error(ClickHouseError):
    """Exception that is the base class of all other error exceptions
    (not Warning).

    `code` is the numeric ClickHouse server error code when known, `name` the symbolic
    name. Both are None when unavailable, e.g. transport errors or suppressed error detail.
    """

    def __init__(self, *args, code: int | None = None, name: str | None = None):
        super().__init__(*args)
        self.code = code
        self.name = name


class InterfaceError(Error):
    """Exception raised for errors that are related to the database
    interface rather than the database itself."""


class DatabaseError(Error):
    """Exception raised for errors that are related to the
    database."""


class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc."""


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc."""


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails, duplicate key,
    etc."""


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the transaction is
    out of sync, etc."""


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors, e.g. table not found
    or already exists, syntax error in the SQL statement, wrong number
    of parameters specified, etc."""


class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used
    which is not supported by the database, e.g. requesting a
    .rollback() on a connection that does not support transaction or
    has transactions turned off."""


class StreamClosedError(ProgrammingError):
    """Exception raised when a stream operation is executed on a closed stream."""

    def __init__(self):
        super().__init__("Executing a streaming operation on a closed stream")


class StreamCompleteException(Exception):  # noqa: N818
    """Internal exception used to indicate the end of a ClickHouse query result stream."""


class StreamFailureError(Exception):
    """Stream failed unexpectedly"""
