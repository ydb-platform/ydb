# ported from: https://github.com/aio-libs/aiopg/blob/master/aiopg/sa/exc.py


class Error(Exception):
    """Generic error class."""


class ArgumentError(Error):
    """Raised when an invalid or conflicting function argument is supplied.

    This error generally corresponds to construction time state errors.
    """


class InvalidRequestError(ArgumentError):
    """aiomysql.sa was asked to do something it can't do.

    This error generally corresponds to runtime state errors.
    """


class NoSuchColumnError(KeyError, InvalidRequestError):
    """A nonexistent column is requested from a ``RowProxy``."""


class ResourceClosedError(InvalidRequestError):
    """An operation was requested from a connection, cursor, or other
    object that's in a closed state."""
