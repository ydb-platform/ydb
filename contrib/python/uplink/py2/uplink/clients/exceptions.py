class _UnmappedClientException(BaseException):
    pass


class Exceptions(object):
    """Enum of standard HTTP client exceptions."""

    BaseClientException = _UnmappedClientException
    """Base class for client errors."""

    ConnectionError = _UnmappedClientException
    """A connection error occurred."""

    ConnectionTimeout = _UnmappedClientException
    """The request timed out while trying to connect to the remote server.

    Requests that produce this error are typically safe to retry.
    """

    ServerTimeout = _UnmappedClientException
    """The server did not send any data in the allotted amount of time."""

    SSLError = _UnmappedClientException
    """An SSL error occurred."""

    InvalidURL = _UnmappedClientException
    """The URL provided was somehow invalid."""
