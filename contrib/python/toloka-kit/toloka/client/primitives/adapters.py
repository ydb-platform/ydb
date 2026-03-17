__all__ = [
    'httpx_exception_to_urllib3_exception',
    'map_urllib3_exception_for_retrying',
]


import socket
from functools import singledispatch
from http.client import HTTPException
from typing import Optional

import httpx
import urllib3.exceptions


def _get_url(error: httpx.HTTPError) -> Optional[str]:
    try:
        return str(error.request.url)
    except RuntimeError:
        return None


@singledispatch
def _map_httpx_exception_to_urllib3_exception(exception: BaseException):
    return urllib3.exceptions.HTTPError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.RequestError):
    # urllib3.exceptions.RequestError does have different meaning here: it is not a base class for all errors that
    # happen during request
    return urllib3.exceptions.HTTPError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.TransportError):
    return urllib3.exceptions.HTTPError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.TimeoutException):
    return urllib3.exceptions.TimeoutError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.ConnectTimeout):
    return urllib3.exceptions.ConnectTimeoutError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.ReadTimeout):
    url = _get_url(exception)
    return urllib3.exceptions.ReadTimeoutError(
        pool=None, url=url, message=str(exception)
    )


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.WriteTimeout):
    return socket.timeout()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.PoolTimeout):
    return urllib3.exceptions.EmptyPoolError(
        pool=None, message=str(exception)
    )


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.NetworkError):
    return urllib3.exceptions.HTTPError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.ConnectError):
    return urllib3.exceptions.NewConnectionError(None, str(exception))  # type: ignore


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.ReadError):
    return socket.error()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.WriteError):
    return socket.error()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.CloseError):
    return socket.error()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.ProtocolError):
    return urllib3.exceptions.ProtocolError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.LocalProtocolError):
    return urllib3.exceptions.ProtocolError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.RemoteProtocolError):
    return urllib3.exceptions.ProtocolError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.ProxyError):
    return urllib3.exceptions.ProxyError(error=exception, message=str(exception))


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.UnsupportedProtocol):
    return urllib3.exceptions.URLSchemeUnknown(scheme=exception.request.url.scheme)


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.DecodingError):
    return urllib3.exceptions.DecodeError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.TooManyRedirects):
    url = _get_url(exception)
    return urllib3.exceptions.MaxRetryError(
        pool=None, url=url
    )


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.HTTPStatusError):
    return urllib3.exceptions.ResponseError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.InvalidURL):
    return urllib3.exceptions.LocationValueError()


@_map_httpx_exception_to_urllib3_exception.register
def _(exception: httpx.CookieConflict):
    return urllib3.exceptions.ResponseError()


def httpx_exception_to_urllib3_exception(exception: httpx.HTTPError) -> BaseException:
    """Maps the httpx exception to the corresponding urllib3 exception."""
    mapped_exception = _map_httpx_exception_to_urllib3_exception(exception)
    mapped_exception.__cause__ = exception
    return mapped_exception


@singledispatch
def _map_urllib3_exception_for_retrying(exception: BaseException) -> BaseException:
    raise RuntimeError(
        'Unknown urllib3 exception is found. Unable to determine if this exception should be retried'
    ) from exception


@_map_urllib3_exception_for_retrying.register
def _(exception: urllib3.exceptions.TimeoutError):
    return exception


@_map_urllib3_exception_for_retrying.register
def _(exception: socket.error):
    return urllib3.exceptions.ProtocolError("Connection aborted.", exception)


@_map_urllib3_exception_for_retrying.register
def _(exception: HTTPException):
    return urllib3.exceptions.ProtocolError("Connection aborted.", exception)


@_map_urllib3_exception_for_retrying.register
def _(exception: urllib3.exceptions.ProtocolError):
    return exception


def map_urllib3_exception_for_retrying(exception: BaseException) -> BaseException:
    """Follows the exception mapping logic from the urllib3.connectionpool.HTTPConnectionPool.urlopen as of
    urllib3==1.26.15

    SSL-related exceptions are not supported.
    """

    return _map_urllib3_exception_for_retrying(exception)
