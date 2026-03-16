"""All exceptions for the github3 library."""


class GitHubException(Exception):
    """The base exception class.

    .. versionadded:: 1.0.0

        Necessary to handle pre-response exceptions

    """

    pass


class GeneratedTokenExpired(GitHubException):
    """This exception is used to prevent an expired token from being used.

    .. versionadded:: 1.2.0
    """

    pass


class AppInstallationTokenExpired(GeneratedTokenExpired):
    """This exception is used to prevent an expired token from being used.

    .. versionadded:: 1.2.0
    """

    pass


class AppTokenExpired(GeneratedTokenExpired):
    """This exception is used to prevent an expired token from being used.

    .. versionadded:: 1.2.0
    """

    pass


class MissingAppAuthentication(GitHubException):
    """Raised when user tries to call a method that requires app auth.

    .. versionadded:: 1.2.0
    """

    pass


class MissingAppBearerAuthentication(MissingAppAuthentication):
    """Raised when user tries to call a method that requires app auth.

    .. versionadded:: 1.2.0
    """

    pass


class MissingAppInstallationAuthentication(MissingAppAuthentication):
    """Raised when user tries to call a method that requires app installation.

    .. versionadded:: 1.2.0
    """

    pass


class CardHasNoContentUrl(GitHubException):
    """Raised when attempting a card has no ``content_url``.

    We use this in methods to retrieve the underlying issue or pull request
    based on the ``content_url``.

    .. versionadded:: 1.3.0
    """

    pass


class GitHubError(GitHubException):
    """The base exception class for all response-related exceptions.

    .. versionchanged:: 1.0.0

        This now inherits from :class:`~github3.exceptions.GitHubException`

    .. attribute:: response

        The response object that triggered the exception

    .. attribute:: code

        The response's status code

    .. attribute:: errors

        The list of errors (if present) returned by GitHub's API
    """

    def __init__(self, resp):
        """Initialize our exception class."""
        super().__init__(resp)
        #: Response code that triggered the error
        self.response = resp
        self.code = resp.status_code
        self.errors = []
        try:
            error = resp.json()
            #: Message associated with the error
            self.msg = error.get("message")
            #: List of errors provided by GitHub
            if error.get("errors"):
                self.errors = error.get("errors")
        except Exception:  # Amazon S3 error
            self.msg = resp.content or "[No message]"

    def __repr__(self):
        return "<{} [{}]>".format(
            self.__class__.__name__, self.msg or self.code
        )

    def __str__(self):
        return f"{self.code} {self.msg}"

    @property
    def message(self):
        """The actual message returned by the API."""
        return self.msg


class IncompleteResponse(GitHubError):
    """Exception for a response that doesn't have everything it should.

    This has the same attributes as :class:`~github3.exceptions.GitHubError`
    as well as

    .. attribute:: exception

        The original exception causing the IncompleteResponse exception
    """

    def __init__(self, json, exception):
        """Initialize our IncompleteResponse."""
        self.response = None
        self.code = None
        self.json = json
        self.errors = []
        self.exception = exception
        self.msg = (
            "The library was expecting more data in the response (%r)."
            " Either GitHub modified it's response body, or your token"
            " is not properly scoped to retrieve this information."
        ) % (exception,)


class NotRefreshable(GitHubException):
    """Exception to indicate that an object is not refreshable."""

    message_format = (
        '"{}" is not refreshable because the GitHub API does '
        "not provide a URL to retrieve its contents from."
    )

    def __init__(self, object_name):
        """Initialize our NotRefreshable exception."""
        super().__init__(self.message_format.format(object_name))


class ResponseError(GitHubError):
    """The base exception for errors stemming from GitHub responses."""

    pass


class TransportError(GitHubException):
    """Catch-all exception for errors coming from Requests.

    .. versionchanged:: 1.0.0

        Now inherits from :class:`~github3.exceptions.GitHubException`.
    """

    msg_format = "An error occurred while making a request to GitHub: {0}"

    def __init__(self, exception):
        """Initialize TransportError exception."""
        self.msg = self.msg_format.format(str(exception))
        super().__init__(self, self.msg, exception)
        self.exception = exception

    def __str__(self):
        return f"{type(self.exception)}: {self.msg}"


class ConnectionError(TransportError):
    """Exception for errors in connecting to or reading data from GitHub."""

    msg_format = "A connection-level exception occurred: {0}"


class UnexpectedResponse(ResponseError):
    """Exception class for responses that were unexpected."""

    pass


class UnprocessableResponseBody(ResponseError):
    """Exception class for response objects that cannot be handled."""

    def __init__(self, message, body):
        """Initialize UnprocessableResponseBody."""
        Exception.__init__(self, message)
        self.body = body
        self.msg = message

    def __repr__(self):
        return "<{} [{}]>".format("UnprocessableResponseBody", self.body)

    def __str__(self):
        return self.message


class BadRequest(ResponseError):
    """Exception class for 400 responses."""

    pass


class AuthenticationFailed(ResponseError):
    """Exception class for 401 responses.

    Possible reasons:

    - Need one time password (for two-factor authentication)
    - You are not authorized to access the resource
    """

    pass


class ForbiddenError(ResponseError):
    """Exception class for 403 responses.

    Possible reasons:

    - Too many requests (you've exceeded the ratelimit)
    - Too many login failures
    """

    pass


class NotFoundError(ResponseError):
    """Exception class for 404 responses."""

    pass


class MethodNotAllowed(ResponseError):
    """Exception class for 405 responses."""

    pass


class NotAcceptable(ResponseError):
    """Exception class for 406 responses."""

    pass


class Conflict(ResponseError):
    """Exception class for 409 responses.

    Possible reasons:

    - Head branch was modified (SHA sums do not match)
    """

    pass


class UnprocessableEntity(ResponseError):
    """Exception class for 422 responses."""

    pass


class ClientError(ResponseError):
    """Catch-all for 400 responses that aren't specific errors."""

    pass


class ServerError(ResponseError):
    """Exception class for 5xx responses."""

    pass


class UnavailableForLegalReasons(ResponseError):
    """Exception class for 451 responses."""

    pass


error_classes = {
    400: BadRequest,
    401: AuthenticationFailed,
    403: ForbiddenError,
    404: NotFoundError,
    405: MethodNotAllowed,
    406: NotAcceptable,
    409: Conflict,
    422: UnprocessableEntity,
    451: UnavailableForLegalReasons,
}


def error_for(response):
    """Return the appropriate initialized exception class for a response."""
    klass = error_classes.get(response.status_code)
    if klass is None:
        if 400 <= response.status_code < 500:
            klass = ClientError
        if 500 <= response.status_code < 600:
            klass = ServerError
    return klass(response)
