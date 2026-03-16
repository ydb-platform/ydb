# -*- coding: utf-8 -*-
import typing

from bravado_core.response import IncomingResponse
from six import with_metaclass

if getattr(typing, 'TYPE_CHECKING', False):
    T = typing.TypeVar('T')


# Dictionary of HTTP status codes to exception classes
status_map = {}  # type: typing.MutableMapping[int, typing.Type['HTTPError']]


def _register_exception(exception_class):
    # type: (typing.Type['HTTPError']) -> None
    """Store an HTTP exception class with a status code into a mapping
    of status codes to exception classes.
    :param exception_class: A subclass of HTTPError
    :type exception_class: :class:`HTTPError`
    """
    status_map[exception_class.status_code] = exception_class


if getattr(typing, 'TYPE_CHECKING', False):
    T2 = typing.TypeVar('T2', bound=type)


class HTTPErrorType(type):
    """A metaclass for registering HTTPError subclasses."""

    def __new__(cls, *args, **kwargs):
        # type: (typing.Type[T2], typing.Any, typing.Any) -> T2
        new_class = typing.cast('T2', type.__new__(cls, *args, **kwargs))
        if hasattr(new_class, 'status_code'):
            _register_exception(new_class)
        return new_class


class HTTPError(with_metaclass(HTTPErrorType, IOError)):
    """Unified HTTPError used across all http_client implementations.
    """
    status_code = None  # type: int

    def __init__(
        self,
        response,  # type: IncomingResponse
        message=None,   # type: typing.Optional[typing.Text]
        swagger_result=None,  # type: typing.Optional[T]
    ):
        # type: (...) -> None
        """
        :type response: :class:`bravado_core.response.IncomingResponse`
        :param message: Optional string message
        :param swagger_result: If the response for this HTTPError is
            documented in the swagger spec, then this should be the result
            value of the response.
        """
        self.response = response
        self.message = message
        self.swagger_result = swagger_result
        self.status_code = self.response.status_code

    def __str__(self):
        # type: (...) -> str
        # Try to surface the most useful/relevant information available
        # since this is the first thing a developer sees when bad things
        # happen.
        status_and_reason = str(self.response)
        message = ': ' + self.message if self.message else ''
        result = ': {0}'.format(self.swagger_result) \
            if self.swagger_result is not None else ''
        return '{0}{1}{2}'.format(status_and_reason, message, result)


def make_http_exception(
    response,  # type: IncomingResponse
    message=None,  # type: typing.Optional[typing.Text]
    swagger_result=None,  # type: typing.Optional[T]
):
    # type: (...) -> HTTPError
    """
    Return an HTTP exception class  based on the response. If a specific
    class doesn't exist for a particular HTTP status code, a more
    general :class:`HTTPError` class will be returned.
    :type response: :class:`bravado_core.response.IncomingResponse`
    :param message: Optional string message
    :param swagger_result: If the response for this HTTPError is
        documented in the swagger spec, then this should be the result
        value of the response.
    :return: An HTTP exception class that can be raised
    """
    status_code = response.status_code
    exc_class = status_map.get(status_code)

    if exc_class is None:
        exception_families = {
            300: HTTPRedirection,
            400: HTTPClientError,
            500: HTTPServerError,
        }
        exc_class = exception_families.get(
            (status_code // 100) * 100,
        ) or HTTPError

    return exc_class(response, message=message, swagger_result=swagger_result)


class HTTPRedirection(HTTPError):
    """3xx responses."""


class HTTPClientError(HTTPError):
    """4xx responses."""


class HTTPServerError(HTTPError):
    """5xx responses."""

    def __str__(self):
        # type: (...) -> str
        # Try to surface the most useful/relevant information available
        # since this is the first thing a developer sees when bad things
        # happen.
        status_and_reason = str(self.response)
        message = ': ' + self.message if self.message else ''
        text = ': ' + self.response.text if self.response.text else ''
        result = ': {0}'.format(self.swagger_result) \
            if self.swagger_result is not None else ''
        return '{0}{1}{2}{3}'.format(status_and_reason, message, text, result)


# The follow are based on the HTTP Status Code Registry at
# http://www.iana.org/assignments/http-status-codes/http-status-codes.xhtml
class HTTPMultipleChoices(HTTPRedirection):
    """HTTP/300 - Multiple Choices"""
    status_code = 300


class HTTPMovedPermanently(HTTPRedirection):
    """HTTP/301 - Moved Permanently"""
    status_code = 301


class HTTPFound(HTTPRedirection):
    """HTTP/302 - Found"""
    status_code = 302


class HTTPSeeOther(HTTPRedirection):
    """HTTP/303 - See Other"""
    status_code = 303


class HTTPNotModified(HTTPRedirection):
    """HTTP/304 - Not Modified"""
    status_code = 304


class HTTPUseProxy(HTTPRedirection):
    """HTTP/305 - Use Proxy"""
    status_code = 305


class HTTPTemporaryRedirect(HTTPRedirection):
    """HTTP/307 - Temporary Redirect"""
    status_code = 307


class HTTPPermanentRedirect(HTTPRedirection):
    """HTTP/308 - Permanent Redirect"""
    status_code = 308


class HTTPBadRequest(HTTPClientError):
    """HTTP/400 - Bad Request"""
    status_code = 400


class HTTPUnauthorized(HTTPClientError):
    """HTTP/401 - Unauthorized"""
    status_code = 401


class HTTPPaymentRequired(HTTPClientError):
    """HTTP/402 - Payment Required"""
    status_code = 402


class HTTPForbidden(HTTPClientError):
    """HTTP/403 - Forbidden"""
    status_code = 403


class HTTPNotFound(HTTPClientError):
    """HTTP/404 - Not Found"""
    status_code = 404


class HTTPMethodNotAllowed(HTTPClientError):
    """HTTP/405 - Method Not Allowed"""
    status_code = 405


class HTTPNotAcceptable(HTTPClientError):
    """HTTP/406 - Not Acceptable"""
    status_code = 406


class HTTPProxyAuthenticationRequired(HTTPClientError):
    """HTTP/407 - Proxy Authentication Required"""
    status_code = 407


class HTTPRequestTimeout(HTTPClientError):
    """HTTP/408 - Request Timeout"""
    status_code = 408


class HTTPConflict(HTTPClientError):
    """HTTP/409 - Conflict"""
    status_code = 409


class HTTPGone(HTTPClientError):
    """HTTP/410 - Gone"""
    status_code = 410


class HTTPLengthRequired(HTTPClientError):
    """HTTP/411 - Length Required"""
    status_code = 411


class HTTPPreconditionFailed(HTTPClientError):
    """HTTP/412 - Precondition Failed"""
    status_code = 412


class HTTPPayloadTooLarge(HTTPClientError):
    """HTTP/413 - Payload Too Large"""
    status_code = 413


class HTTPURITooLong(HTTPClientError):
    """HTTP/414 - URI Too Long"""
    status_code = 414


class HTTPUnsupportedMediaType(HTTPClientError):
    """HTTP/415 - Unsupported Media Type"""
    status_code = 415


class HTTPRangeNotSatisfiable(HTTPClientError):
    """HTTP/416 - Range Not Satisfiable"""
    status_code = 416


class HTTPExpectationFailed(HTTPClientError):
    """HTTP/417 - Expectation Failed"""
    status_code = 417


class HTTPMisdirectedRequest(HTTPClientError):
    """HTTP/421 - Misdirected Request"""
    status_code = 421


class HTTPUnprocessableEntity(HTTPClientError):
    """HTTP/422 - Unprocessable Entity"""
    status_code = 422


class HTTPLocked(HTTPClientError):
    """HTTP/423 - Locked"""
    status_code = 423


class HTTPFailedDependency(HTTPClientError):
    """HTTP/424 - Failed Dependency"""
    status_code = 424


class HTTPUpgradeRequired(HTTPClientError):
    """HTTP/426 - Upgrade Required"""
    status_code = 426


class HTTPPreconditionRequired(HTTPClientError):
    """HTTP/428 - Precondition Required"""
    status_code = 428


class HTTPTooManyRequests(HTTPClientError):
    """HTTP/429 - Too Many Requests"""
    status_code = 429


class HTTPRequestHeaderFieldsTooLarge(HTTPClientError):
    """HTTP/431 - Request Header Fields Too Large"""
    status_code = 431


class HTTPUnavailableForLegalReasons(HTTPClientError):
    """HTTP/451 - Unavailable For Legal Reasons"""
    status_code = 451


class HTTPInternalServerError(HTTPServerError):
    """HTTP/500 - Internal Server Error"""
    status_code = 500


class HTTPNotImplemented(HTTPServerError):
    """HTTP/501 - Not Implemented"""
    status_code = 501


class HTTPBadGateway(HTTPServerError):
    """HTTP/502 - Bad Gateway"""
    status_code = 502


class HTTPServiceUnavailable(HTTPServerError):
    """HTTP/503 - Service Unavailable"""
    status_code = 503


class HTTPGatewayTimeout(HTTPServerError):
    """HTTP/504 - Gateway Timeout"""
    status_code = 504


class HTTPHTTPVersionNotSupported(HTTPServerError):
    """HTTP/505 - HTTP Version Not Supported"""
    status_code = 505


class HTTPVariantAlsoNegotiates(HTTPServerError):
    """HTTP/506 - Variant Also Negotiates"""
    status_code = 506


class HTTPInsufficientStorage(HTTPServerError):
    """HTTP/507 - Insufficient Storage"""
    status_code = 507


class HTTPLoopDetected(HTTPServerError):
    """HTTP/508 - Loop Detected"""
    status_code = 508


class HTTPNotExtended(HTTPServerError):
    """HTTP/510 - Not Extended"""
    status_code = 510


class HTTPNetworkAuthenticationRequired(HTTPServerError):
    """HTTP/511 - Network Authentication Required"""
    status_code = 511


class BravadoTimeoutError(TimeoutError):
    pass


class BravadoConnectionError(ConnectionError):
    pass


class ForcedFallbackResultError(Exception):
    """This exception will be handled if the option to force returning a fallback result
     is used."""
