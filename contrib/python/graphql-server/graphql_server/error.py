"""Error classes provided by graphql_server"""

__all__ = ["HttpQueryError"]


class HttpQueryError(Exception):
    """The error class for HTTP errors produced by running GraphQL queries."""

    def __init__(self, status_code, message=None, is_graphql_error=False, headers=None):
        """Create a HTTP query error.

        You need to pass the HTTP status code, the message that shall be shown,
        whether this is a GraphQL error, and the HTTP headers that shall be sent.
        """
        self.status_code = status_code
        self.message = message
        self.is_graphql_error = is_graphql_error
        self.headers = headers
        super(HttpQueryError, self).__init__(message)

    def __eq__(self, other):
        """Check whether this HTTP query error is equal to another one."""
        return (
            isinstance(other, HttpQueryError)
            and other.status_code == self.status_code
            and other.message == self.message
            and other.headers == self.headers
        )

    def __hash__(self):
        """Create a hash value for this HTTP query error."""
        headers_hash = tuple(self.headers.items()) if self.headers else None
        return hash((self.status_code, self.message, headers_hash))
