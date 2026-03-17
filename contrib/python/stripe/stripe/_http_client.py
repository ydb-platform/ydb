from io import BytesIO
import textwrap
import email
import time
import random
import threading
import json
import asyncio
import ssl
from http.client import HTTPResponse

# Used for global variables
import stripe  # noqa: IMP101
from stripe import _util
from stripe._request_metrics import RequestMetrics
from stripe._error import APIConnectionError

from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    ClassVar,
    Union,
    cast,
    overload,
    AsyncIterable,
)
from typing_extensions import (
    TYPE_CHECKING,
    Literal,
    NoReturn,
    TypedDict,
    Awaitable,
    Never,
)

if TYPE_CHECKING:
    from urllib.parse import ParseResult

    try:
        from requests import Session as RequestsSession
    except ImportError:
        pass

    try:
        from httpx import Timeout as HTTPXTimeout
        from httpx import Client as HTTPXClientType
    except ImportError:
        pass

    try:
        from aiohttp import ClientTimeout as AIOHTTPTimeout
        from aiohttp import StreamReader as AIOHTTPStreamReader
    except ImportError:
        pass


def _now_ms():
    return int(round(time.time() * 1000))


def new_default_http_client(*args: Any, **kwargs: Any) -> "HTTPClient":
    """
    This method creates and returns a new HTTPClient based on what libraries are available. It uses the following precedence rules:

    1. Urlfetch (this is provided by Google App Engine, so if it's present you probably want it)
    2. Requests (popular library, the top priority for all environments outside Google App Engine, but not always present)
    3. Pycurl (another library, not always present, not as preferred as Requests but at least it verifies SSL certs)
    4. urllib with a warning (basically always present, a reasonable final default)

    For performance, it only imports what it's actually going to use. But, it re-calculates every time its called, so probably save its result instead of calling it multiple times.
    """
    try:
        from google.appengine.api import urlfetch  # type: ignore # noqa: F401
    except ImportError:
        pass
    else:
        return UrlFetchClient(*args, **kwargs)

    try:
        import requests  # noqa: F401
    except ImportError:
        pass
    else:
        return RequestsClient(*args, **kwargs)

    try:
        import pycurl  # type: ignore # noqa: F401
    except ImportError:
        pass
    else:
        return PycurlClient(*args, **kwargs)

    return UrllibClient(*args, **kwargs)


def new_http_client_async_fallback(*args: Any, **kwargs: Any) -> "HTTPClient":
    """
    Similar to `new_default_http_client` above, this returns a client that can handle async HTTP requests, if available.
    """

    try:
        import httpx  # noqa: F401
        import anyio  # noqa: F401
    except ImportError:
        pass
    else:
        return HTTPXClient(*args, **kwargs)

    try:
        import aiohttp  # noqa: F401
    except ImportError:
        pass
    else:
        return AIOHTTPClient(*args, **kwargs)

    return NoImportFoundAsyncClient(*args, **kwargs)


class HTTPClient(object):
    """
    Base HTTP client that custom clients can inherit from.
    """

    name: ClassVar[str]

    class _Proxy(TypedDict):
        http: Optional[str]
        https: Optional[str]

    MAX_DELAY = 5
    INITIAL_DELAY = 0.5
    MAX_RETRY_AFTER = 60
    _proxy: Optional[_Proxy]
    _verify_ssl_certs: bool

    def __init__(
        self,
        verify_ssl_certs: bool = True,
        proxy: Optional[Union[str, _Proxy]] = None,
        async_fallback_client: Optional["HTTPClient"] = None,
        _lib=None,  # used for internal unit testing
    ):
        self._verify_ssl_certs = verify_ssl_certs
        if proxy:
            if isinstance(proxy, str):
                proxy = {"http": proxy, "https": proxy}
            if not isinstance(proxy, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise ValueError(
                    "Proxy(ies) must be specified as either a string "
                    "URL or a dict() with string URL under the"
                    " "
                    "https"
                    " and/or "
                    "http"
                    " keys."
                )
        self._proxy = proxy.copy() if proxy else None
        self._async_fallback_client = async_fallback_client

        self._thread_local = threading.local()

    def _should_retry(
        self,
        response: Optional[Tuple[Any, int, Optional[Mapping[str, str]]]],
        api_connection_error: Optional[APIConnectionError],
        num_retries: int,
        max_network_retries: Optional[int],
    ):
        max_network_retries = (
            max_network_retries if max_network_retries is not None else 0
        )
        if num_retries >= max_network_retries:
            return False

        if response is None:
            # We generally want to retry on timeout and connection
            # exceptions, but defer this decision to underlying subclass
            # implementations. They should evaluate the driver-specific
            # errors worthy of retries, and set flag on the error returned.
            assert api_connection_error is not None
            return api_connection_error.should_retry

        _, status_code, rheaders = response

        # The API may ask us not to retry (eg; if doing so would be a no-op)
        # or advise us to retry (eg; in cases of lock timeouts); we defer to that.
        #
        # Note that we expect the headers object to be a CaseInsensitiveDict, as is the case with the requests library.
        if rheaders is not None and "stripe-should-retry" in rheaders:
            if rheaders["stripe-should-retry"] == "false":
                return False
            if rheaders["stripe-should-retry"] == "true":
                return True

        # Retry on conflict errors.
        if status_code == 409:
            return True

        # Retry on 500, 503, and other internal errors.
        #
        # Note that we expect the stripe-should-retry header to be false
        # in most cases when a 500 is returned, since our idempotency framework
        # would typically replay it anyway.
        if status_code >= 500:
            return True

        return False

    def _retry_after_header(
        self, response: Optional[Tuple[Any, Any, Mapping[str, str]]] = None
    ):
        if response is None:
            return None
        _, _, rheaders = response

        try:
            return int(rheaders["retry-after"])
        except (KeyError, ValueError):
            return None

    def _sleep_time_seconds(
        self,
        num_retries: int,
        response: Optional[Tuple[Any, Any, Mapping[str, str]]] = None,
    ) -> float:
        """
        Apply exponential backoff with initial_network_retry_delay on the number of num_retries so far as inputs.
        Do not allow the number to exceed `max_network_retry_delay`.
        """
        sleep_seconds = min(
            HTTPClient.INITIAL_DELAY * (2 ** (num_retries - 1)),
            HTTPClient.MAX_DELAY,
        )

        sleep_seconds = self._add_jitter_time(sleep_seconds)

        # But never sleep less than the base sleep seconds.
        sleep_seconds = max(HTTPClient.INITIAL_DELAY, sleep_seconds)

        # And never sleep less than the time the API asks us to wait, assuming it's a reasonable ask.
        retry_after = self._retry_after_header(response) or 0
        if retry_after <= HTTPClient.MAX_RETRY_AFTER:
            sleep_seconds = max(retry_after, sleep_seconds)

        return sleep_seconds

    def _add_jitter_time(self, sleep_seconds: float) -> float:
        """
        Randomize the value in `[(sleep_seconds/ 2) to (sleep_seconds)]`.
        Also separated method here to isolate randomness for tests
        """
        sleep_seconds *= 0.5 * (1 + random.uniform(0, 1))
        return sleep_seconds

    def _add_telemetry_header(
        self, headers: Mapping[str, str]
    ) -> Mapping[str, str]:
        last_request_metrics = getattr(
            self._thread_local, "last_request_metrics", None
        )
        if stripe.enable_telemetry and last_request_metrics:
            telemetry = {
                "last_request_metrics": last_request_metrics.payload()
            }
            ret = dict(headers)
            ret["X-Stripe-Client-Telemetry"] = json.dumps(telemetry)
            return ret
        return headers

    def _record_request_metrics(self, response, request_start, usage):
        _, _, rheaders = response
        if "Request-Id" in rheaders and stripe.enable_telemetry:
            request_id = rheaders["Request-Id"]
            request_duration_ms = _now_ms() - request_start
            self._thread_local.last_request_metrics = RequestMetrics(
                request_id, request_duration_ms, usage=usage
            )

    def request_with_retries(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data: Any = None,
        max_network_retries: Optional[int] = None,
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[str, int, Mapping[str, str]]:
        return self._request_with_retries_internal(
            method,
            url,
            headers,
            post_data,
            is_streaming=False,
            max_network_retries=max_network_retries,
            _usage=_usage,
        )

    def request_stream_with_retries(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data=None,
        max_network_retries=None,
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[Any, int, Mapping[str, str]]:
        return self._request_with_retries_internal(
            method,
            url,
            headers,
            post_data,
            is_streaming=True,
            max_network_retries=max_network_retries,
            _usage=_usage,
        )

    def _request_with_retries_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data: Any,
        is_streaming: bool,
        max_network_retries: Optional[int],
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[Any, int, Mapping[str, str]]:
        headers = self._add_telemetry_header(headers)

        num_retries = 0

        while True:
            request_start = _now_ms()

            try:
                if is_streaming:
                    response = self.request_stream(
                        method, url, headers, post_data
                    )
                else:
                    response = self.request(method, url, headers, post_data)
                connection_error = None
            except APIConnectionError as e:
                connection_error = e
                response = None

            if self._should_retry(
                response, connection_error, num_retries, max_network_retries
            ):
                if connection_error:
                    _util.log_info(
                        "Encountered a retryable error %s"
                        % connection_error.user_message
                    )
                num_retries += 1
                sleep_time = self._sleep_time_seconds(num_retries, response)
                _util.log_info(
                    (
                        "Initiating retry %i for request %s %s after "
                        "sleeping %.2f seconds."
                        % (num_retries, method, url, sleep_time)
                    )
                )
                time.sleep(sleep_time)
            else:
                if response is not None:
                    self._record_request_metrics(
                        response, request_start, usage=_usage
                    )

                    return response
                else:
                    assert connection_error is not None
                    raise connection_error

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        post_data: Any = None,
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[str, int, Mapping[str, str]]:
        raise NotImplementedError(
            "HTTPClient subclasses must implement `request`"
        )

    def request_stream(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        post_data: Any = None,
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[Any, int, Mapping[str, str]]:
        raise NotImplementedError(
            "HTTPClient subclasses must implement `request_stream`"
        )

    def close(self):
        raise NotImplementedError(
            "HTTPClient subclasses must implement `close`"
        )

    async def request_with_retries_async(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data=None,
        max_network_retries: Optional[int] = None,
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[Any, int, Any]:
        return await self._request_with_retries_internal_async(
            method,
            url,
            headers,
            post_data,
            is_streaming=False,
            max_network_retries=max_network_retries,
            _usage=_usage,
        )

    async def request_stream_with_retries_async(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data=None,
        max_network_retries=None,
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[AsyncIterable[bytes], int, Any]:
        return await self._request_with_retries_internal_async(
            method,
            url,
            headers,
            post_data,
            is_streaming=True,
            max_network_retries=max_network_retries,
            _usage=_usage,
        )

    @overload
    async def _request_with_retries_internal_async(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[False],
        max_network_retries: Optional[int],
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[Any, int, Mapping[str, str]]: ...

    @overload
    async def _request_with_retries_internal_async(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[True],
        max_network_retries: Optional[int],
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[AsyncIterable[bytes], int, Mapping[str, str]]: ...

    async def _request_with_retries_internal_async(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: bool,
        max_network_retries: Optional[int],
        *,
        _usage: Optional[List[str]] = None,
    ) -> Tuple[Any, int, Mapping[str, str]]:
        headers = self._add_telemetry_header(headers)

        num_retries = 0

        while True:
            request_start = _now_ms()

            try:
                if is_streaming:
                    response = await self.request_stream_async(
                        method, url, headers, post_data
                    )
                else:
                    response = await self.request_async(
                        method, url, headers, post_data
                    )
                connection_error = None
            except APIConnectionError as e:
                connection_error = e
                response = None

            if self._should_retry(
                response, connection_error, num_retries, max_network_retries
            ):
                if connection_error:
                    _util.log_info(
                        "Encountered a retryable error %s"
                        % connection_error.user_message
                    )
                num_retries += 1
                sleep_time = self._sleep_time_seconds(num_retries, response)
                _util.log_info(
                    (
                        "Initiating retry %i for request %s %s after "
                        "sleeping %.2f seconds."
                        % (num_retries, method, url, sleep_time)
                    )
                )
                await self.sleep_async(sleep_time)
            else:
                if response is not None:
                    self._record_request_metrics(
                        response, request_start, usage=_usage
                    )

                    return response
                else:
                    assert connection_error is not None
                    raise connection_error

    async def request_async(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[bytes, int, Mapping[str, str]]:
        if self._async_fallback_client is not None:
            return await self._async_fallback_client.request_async(
                method, url, headers, post_data
            )
        raise NotImplementedError(
            "HTTPClient subclasses must implement `request_async`"
        )

    async def request_stream_async(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[AsyncIterable[bytes], int, Mapping[str, str]]:
        if self._async_fallback_client is not None:
            return await self._async_fallback_client.request_stream_async(
                method, url, headers, post_data
            )
        raise NotImplementedError(
            "HTTPClient subclasses must implement `request_stream_async`"
        )

    async def close_async(self):
        if self._async_fallback_client is not None:
            return await self._async_fallback_client.close_async()
        raise NotImplementedError(
            "HTTPClient subclasses must implement `close_async`"
        )

    def sleep_async(self, secs: float) -> Awaitable[None]:
        if self._async_fallback_client is not None:
            return self._async_fallback_client.sleep_async(secs)
        raise NotImplementedError(
            "HTTPClient subclasses must implement `sleep`"
        )


class RequestsClient(HTTPClient):
    name = "requests"

    def __init__(
        self,
        timeout: Union[float, Tuple[float, float]] = 80,
        session: Optional["RequestsSession"] = None,
        verify_ssl_certs: bool = True,
        proxy: Optional[Union[str, HTTPClient._Proxy]] = None,
        async_fallback_client: Optional[HTTPClient] = None,
        _lib=None,  # used for internal unit testing
        **kwargs,
    ):
        super(RequestsClient, self).__init__(
            verify_ssl_certs=verify_ssl_certs,
            proxy=proxy,
            async_fallback_client=async_fallback_client,
        )
        self._session = session
        self._timeout = timeout

        if _lib is None:
            import requests

            _lib = requests

        self.requests = _lib

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        post_data=None,
    ) -> Tuple[bytes, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=False
        )

    def request_stream(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        post_data=None,
    ) -> Tuple[Any, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=True
        )

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        post_data,
        is_streaming: Literal[True],
    ) -> Tuple[Any, int, Mapping[str, str]]: ...

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        post_data,
        is_streaming: Literal[False],
    ) -> Tuple[bytes, int, Mapping[str, str]]: ...

    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Optional[Mapping[str, str]],
        post_data,
        is_streaming: bool,
    ) -> Tuple[Union[bytes, Any], int, Mapping[str, str]]:
        kwargs = {}
        if self._verify_ssl_certs:
            kwargs["verify"] = stripe.ca_bundle_path
        else:
            kwargs["verify"] = False

        if self._proxy:
            kwargs["proxies"] = self._proxy

        if is_streaming:
            kwargs["stream"] = True

        if getattr(self._thread_local, "session", None) is None:
            self._thread_local.session = (
                self._session or self.requests.Session()
            )

        try:
            try:
                result = cast(
                    "RequestsSession", self._thread_local.session
                ).request(
                    method,
                    url,
                    headers=headers,
                    data=post_data,
                    timeout=self._timeout,
                    **kwargs,
                )
            except TypeError as e:
                raise TypeError(
                    "Warning: It looks like your installed version of the "
                    '"requests" library is not compatible with Stripe\'s '
                    "usage thereof. (HINT: The most likely cause is that "
                    'your "requests" library is out of date. You can fix '
                    'that by running "pip install -U requests".) The '
                    "underlying error was: %s" % (e,)
                )

            if is_streaming:
                content = result.raw
            else:
                # This causes the content to actually be read, which could cause
                # e.g. a socket timeout. TODO: The other fetch methods probably
                # are susceptible to the same and should be updated.
                content = result.content

            status_code = result.status_code
        except Exception as e:
            # Would catch just requests.exceptions.RequestException, but can
            # also raise ValueError, RuntimeError, etc.
            self._handle_request_error(e)

        return content, status_code, result.headers

    def _handle_request_error(self, e: Exception) -> NoReturn:
        # Catch SSL error first as it belongs to ConnectionError,
        # but we don't want to retry
        if isinstance(e, self.requests.exceptions.SSLError):
            msg = (
                "Could not verify Stripe's SSL certificate.  Please make "
                "sure that your network is not intercepting certificates.  "
                "If this problem persists, let us know at "
                "support@stripe.com."
            )
            err = "%s: %s" % (type(e).__name__, str(e))
            should_retry = False
        # Retry only timeout and connect errors; similar to urllib3 Retry
        elif isinstance(
            e,
            (
                self.requests.exceptions.Timeout,
                self.requests.exceptions.ConnectionError,
            ),
        ):
            msg = (
                "Unexpected error communicating with Stripe.  "
                "If this problem persists, let us know at "
                "support@stripe.com."
            )
            err = "%s: %s" % (type(e).__name__, str(e))
            should_retry = True
        # Catch remaining request exceptions
        elif isinstance(e, self.requests.exceptions.RequestException):
            msg = (
                "Unexpected error communicating with Stripe.  "
                "If this problem persists, let us know at "
                "support@stripe.com."
            )
            err = "%s: %s" % (type(e).__name__, str(e))
            should_retry = False
        else:
            msg = (
                "Unexpected error communicating with Stripe. "
                "It looks like there's probably a configuration "
                "issue locally.  If this problem persists, let us "
                "know at support@stripe.com."
            )
            err = "A %s was raised" % (type(e).__name__,)
            if str(e):
                err += " with error message %s" % (str(e),)
            else:
                err += " with no error message"
            should_retry = False

        msg = textwrap.fill(msg) + "\n\n(Network error: %s)" % (err,)
        raise APIConnectionError(msg, should_retry=should_retry) from e

    def close(self):
        if getattr(self._thread_local, "session", None) is not None:
            self._thread_local.session.close()


class UrlFetchClient(HTTPClient):
    name = "urlfetch"

    def __init__(
        self,
        verify_ssl_certs: bool = True,
        proxy: Optional[HTTPClient._Proxy] = None,
        deadline: int = 55,
        async_fallback_client: Optional[HTTPClient] = None,
        _lib=None,  # used for internal unit testing
    ):
        super(UrlFetchClient, self).__init__(
            verify_ssl_certs=verify_ssl_certs,
            proxy=proxy,
            async_fallback_client=async_fallback_client,
        )

        # no proxy support in urlfetch. for a patch, see:
        # https://code.google.com/p/googleappengine/issues/detail?id=544
        if proxy:
            raise ValueError(
                "No proxy support in urlfetch library. "
                "Set stripe.default_http_client to either RequestsClient, "
                "PycurlClient, or UrllibClient instance to use a proxy."
            )

        self._verify_ssl_certs = verify_ssl_certs
        # GAE requests time out after 60 seconds, so make sure to default
        # to 55 seconds to allow for a slow Stripe
        self._deadline = deadline

        if _lib is None:
            from google.appengine.api import urlfetch  # pyright: ignore

            _lib = urlfetch

        self.urlfetch = _lib

    def request(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[str, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=False
        )

    def request_stream(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[BytesIO, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=True
        )

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[True],
    ) -> Tuple[BytesIO, int, Any]: ...

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[False],
    ) -> Tuple[str, int, Any]: ...

    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming,
    ):
        try:
            result = self.urlfetch.fetch(
                url=url,
                method=method,
                headers=headers,
                # Google App Engine doesn't let us specify our own cert bundle.
                # However, that's ok because the CA bundle they use recognizes
                # api.stripe.com.
                validate_certificate=self._verify_ssl_certs,
                deadline=self._deadline,
                payload=post_data,
            )
        except self.urlfetch.Error as e:
            self._handle_request_error(e, url)

        if is_streaming:
            # This doesn't really stream.
            content = BytesIO(str.encode(result.content))
        else:
            content = result.content

        return content, result.status_code, result.headers

    def _handle_request_error(self, e: Exception, url: str) -> NoReturn:
        if isinstance(e, self.urlfetch.InvalidURLError):
            msg = (
                "The Stripe library attempted to fetch an "
                "invalid URL (%r). This is likely due to a bug "
                "in the Stripe Python bindings. Please let us know "
                "at support@stripe.com." % (url,)
            )
        elif isinstance(e, self.urlfetch.DownloadError):
            msg = "There was a problem retrieving data from Stripe."
        elif isinstance(e, self.urlfetch.ResponseTooLargeError):
            msg = (
                "There was a problem receiving all of your data from "
                "Stripe.  This is likely due to a bug in Stripe. "
                "Please let us know at support@stripe.com."
            )
        else:
            msg = (
                "Unexpected error communicating with Stripe. If this "
                "problem persists, let us know at support@stripe.com."
            )

        msg = textwrap.fill(msg) + "\n\n(Network error: " + str(e) + ")"
        raise APIConnectionError(msg) from e

    def close(self):
        pass


class _Proxy(TypedDict):
    http: Optional["ParseResult"]
    https: Optional["ParseResult"]


class PycurlClient(HTTPClient):
    class _ParsedProxy(TypedDict, total=False):
        http: Optional["ParseResult"]
        https: Optional["ParseResult"]

    name = "pycurl"
    _parsed_proxy: Optional[_ParsedProxy]

    def __init__(
        self,
        verify_ssl_certs: bool = True,
        proxy: Optional[HTTPClient._Proxy] = None,
        async_fallback_client: Optional[HTTPClient] = None,
        _lib=None,  # used for internal unit testing
    ):
        super(PycurlClient, self).__init__(
            verify_ssl_certs=verify_ssl_certs,
            proxy=proxy,
            async_fallback_client=async_fallback_client,
        )

        if _lib is None:
            import pycurl  # pyright: ignore[reportMissingModuleSource]

            _lib = pycurl

        self.pycurl = _lib
        # Initialize this within the object so that we can reuse connections.
        self._curl = _lib.Curl()

        self._parsed_proxy = {}
        # need to urlparse the proxy, since PyCurl
        # consumes the proxy url in small pieces
        if self._proxy:
            from urllib.parse import urlparse

            proxy_ = self._proxy
            for scheme, value in proxy_.items():
                # In general, TypedDict.items() gives you (key: str, value: object)
                # but we know value to be a string because all the value types on Proxy_ are strings.
                self._parsed_proxy[scheme] = urlparse(cast(str, value))

    def parse_headers(self, data):
        if "\r\n" not in data:
            return {}
        raw_headers = data.split("\r\n", 1)[1]
        headers = email.message_from_string(raw_headers)
        return dict((k.lower(), v) for k, v in dict(headers).items())

    def request(
        self, method, url, headers: Mapping[str, str], post_data=None
    ) -> Tuple[str, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=False
        )

    def request_stream(
        self, method, url, headers: Mapping[str, str], post_data=None
    ) -> Tuple[BytesIO, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=True
        )

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[True],
    ) -> Tuple[BytesIO, int, Any]: ...

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[False],
    ) -> Tuple[str, int, Mapping[str, str]]: ...

    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming,
    ) -> Tuple[Union[str, BytesIO], int, Mapping[str, str]]:
        b = BytesIO()
        rheaders = BytesIO()

        # Pycurl's design is a little weird: although we set per-request
        # options on this object, it's also capable of maintaining established
        # connections. Here we call reset() between uses to make sure it's in a
        # pristine state, but notably reset() doesn't reset connections, so we
        # still get to take advantage of those by virtue of re-using the same
        # object.
        self._curl.reset()

        proxy = self._get_proxy(url)
        if proxy:
            if proxy.hostname:
                self._curl.setopt(self.pycurl.PROXY, proxy.hostname)
            if proxy.port:
                self._curl.setopt(self.pycurl.PROXYPORT, proxy.port)
            if proxy.username or proxy.password:
                self._curl.setopt(
                    self.pycurl.PROXYUSERPWD,
                    "%s:%s" % (proxy.username, proxy.password),
                )

        if method == "get":
            self._curl.setopt(self.pycurl.HTTPGET, 1)
        elif method == "post":
            self._curl.setopt(self.pycurl.POST, 1)
            self._curl.setopt(self.pycurl.POSTFIELDS, post_data)
        else:
            self._curl.setopt(self.pycurl.CUSTOMREQUEST, method.upper())

        # pycurl doesn't like unicode URLs
        self._curl.setopt(self.pycurl.URL, url)

        self._curl.setopt(self.pycurl.WRITEFUNCTION, b.write)
        self._curl.setopt(self.pycurl.HEADERFUNCTION, rheaders.write)
        self._curl.setopt(self.pycurl.NOSIGNAL, 1)
        self._curl.setopt(self.pycurl.CONNECTTIMEOUT, 30)
        self._curl.setopt(self.pycurl.TIMEOUT, 80)
        self._curl.setopt(
            self.pycurl.HTTPHEADER,
            ["%s: %s" % (k, v) for k, v in dict(headers).items()],
        )
        if self._verify_ssl_certs:
            self._curl.setopt(self.pycurl.CAINFO, stripe.ca_bundle_path)
        else:
            self._curl.setopt(self.pycurl.SSL_VERIFYHOST, False)

        try:
            self._curl.perform()
        except self.pycurl.error as e:
            self._handle_request_error(e)

        if is_streaming:
            b.seek(0)
            rcontent = b
        else:
            rcontent = b.getvalue().decode("utf-8")

        rcode = self._curl.getinfo(self.pycurl.RESPONSE_CODE)
        headers = self.parse_headers(rheaders.getvalue().decode("utf-8"))

        return rcontent, rcode, headers

    def _handle_request_error(self, e: Exception) -> NoReturn:
        if e.args[0] in [
            self.pycurl.E_COULDNT_CONNECT,
            self.pycurl.E_COULDNT_RESOLVE_HOST,
            self.pycurl.E_OPERATION_TIMEOUTED,
        ]:
            msg = (
                "Could not connect to Stripe.  Please check your "
                "internet connection and try again.  If this problem "
                "persists, you should check Stripe's service status at "
                "https://twitter.com/stripestatus, or let us know at "
                "support@stripe.com."
            )
            should_retry = True
        elif e.args[0] in [
            self.pycurl.E_SSL_CACERT,
            self.pycurl.E_SSL_PEER_CERTIFICATE,
        ]:
            msg = (
                "Could not verify Stripe's SSL certificate.  Please make "
                "sure that your network is not intercepting certificates.  "
                "If this problem persists, let us know at "
                "support@stripe.com."
            )
            should_retry = False
        else:
            msg = (
                "Unexpected error communicating with Stripe. If this "
                "problem persists, let us know at support@stripe.com."
            )
            should_retry = False

        msg = textwrap.fill(msg) + "\n\n(Network error: " + e.args[1] + ")"
        raise APIConnectionError(msg, should_retry=should_retry) from e

    def _get_proxy(self, url) -> Optional["ParseResult"]:
        if self._parsed_proxy:
            proxy = self._parsed_proxy
            scheme = url.split(":")[0] if url else None
            if scheme:
                return proxy.get(scheme, proxy.get(scheme[0:-1]))
        return None

    def close(self):
        pass


class UrllibClient(HTTPClient):
    name = "urllib.request"

    def __init__(
        self,
        verify_ssl_certs: bool = True,
        proxy: Optional[HTTPClient._Proxy] = None,
        async_fallback_client: Optional[HTTPClient] = None,
        _lib=None,  # used for internal unit testing
    ):
        super(UrllibClient, self).__init__(
            verify_ssl_certs=verify_ssl_certs,
            proxy=proxy,
            async_fallback_client=async_fallback_client,
        )

        if _lib is None:
            import urllib.request as urllibrequest

            _lib = urllibrequest
        self.urllibrequest = _lib

        import urllib.error as urlliberror

        self.urlliberror = urlliberror

        # prepare and cache proxy tied opener here
        self._opener = None
        if self._proxy:
            # We have to cast _Proxy to Dict[str, str] because pyright is not smart enough to
            # realize that all the value types are str.
            proxy_handler = self.urllibrequest.ProxyHandler(
                cast(Dict[str, str], self._proxy)
            )
            self._opener = self.urllibrequest.build_opener(proxy_handler)

    def request(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[str, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=False
        )

    def request_stream(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[HTTPResponse, int, Mapping[str, str]]:
        return self._request_internal(
            method, url, headers, post_data, is_streaming=True
        )

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[False],
    ) -> Tuple[str, int, Any]: ...

    @overload
    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming: Literal[True],
    ) -> Tuple[HTTPResponse, int, Any]: ...

    def _request_internal(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data,
        is_streaming,
    ):
        if isinstance(post_data, str):
            post_data = post_data.encode("utf-8")

        req = self.urllibrequest.Request(
            url, post_data, cast(MutableMapping[str, str], headers)
        )

        if method not in ("get", "post"):
            req.get_method = lambda: method.upper()

        try:
            # use the custom proxy tied opener, if any.
            # otherwise, fall to the default urllib opener.
            response = (
                self._opener.open(req)
                if self._opener
                else self.urllibrequest.urlopen(req)
            )

            if is_streaming:
                rcontent = response
            else:
                rcontent = response.read()

            rcode = response.code
            headers = dict(response.info())
        except self.urlliberror.HTTPError as e:
            rcode = e.code
            rcontent = e.read()
            headers = dict(e.info())
        except (self.urlliberror.URLError, ValueError) as e:
            self._handle_request_error(e)
        lh = dict((k.lower(), v) for k, v in iter(dict(headers).items()))
        return rcontent, rcode, lh

    def _handle_request_error(self, e: Exception) -> NoReturn:
        msg = (
            "Unexpected error communicating with Stripe. "
            "If this problem persists, let us know at support@stripe.com."
        )
        msg = textwrap.fill(msg) + "\n\n(Network error: " + str(e) + ")"
        raise APIConnectionError(msg) from e

    def close(self):
        pass


class HTTPXClient(HTTPClient):
    name = "httpx"

    _client: Optional["HTTPXClientType"]

    def __init__(
        self,
        timeout: Optional[Union[float, "HTTPXTimeout"]] = 80,
        allow_sync_methods=False,
        _lib=None,  # used for internal unit testing
        **kwargs,
    ):
        super(HTTPXClient, self).__init__(**kwargs)

        if _lib is None:
            import httpx

            _lib = httpx
        self.httpx = _lib

        import anyio

        self.anyio = anyio

        kwargs = {}
        if self._verify_ssl_certs:
            kwargs["verify"] = ssl.create_default_context(
                cafile=stripe.ca_bundle_path
            )
        else:
            kwargs["verify"] = False

        self._client_async = self.httpx.AsyncClient(**kwargs)
        self._client = None
        if allow_sync_methods:
            self._client = self.httpx.Client(**kwargs)
        self._timeout = timeout

    def sleep_async(self, secs):
        return self.anyio.sleep(secs)

    def _get_request_args_kwargs(
        self, method: str, url: str, headers: Mapping[str, str], post_data
    ):
        kwargs = {}

        if self._proxy:
            kwargs["proxies"] = self._proxy

        if self._timeout:
            kwargs["timeout"] = self._timeout
        return [
            (method, url),
            {"headers": headers, "data": post_data or {}, **kwargs},
        ]

    def request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data=None,
    ) -> Tuple[bytes, int, Mapping[str, str]]:
        if self._client is None:
            raise RuntimeError(
                "Stripe: HTTPXClient was initialized with allow_sync_methods=False, "
                "so it cannot be used for synchronous requests."
            )
        args, kwargs = self._get_request_args_kwargs(
            method, url, headers, post_data
        )
        try:
            response = self._client.request(*args, **kwargs)
        except Exception as e:
            self._handle_request_error(e)

        content = response.content
        status_code = response.status_code
        response_headers = response.headers
        return content, status_code, response_headers

    async def request_async(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data=None,
    ) -> Tuple[bytes, int, Mapping[str, str]]:
        args, kwargs = self._get_request_args_kwargs(
            method, url, headers, post_data
        )
        try:
            response = await self._client_async.request(*args, **kwargs)
        except Exception as e:
            self._handle_request_error(e)

        content = response.content
        status_code = response.status_code
        response_headers = response.headers
        return content, status_code, response_headers

    def _handle_request_error(self, e: Exception) -> NoReturn:
        msg = (
            "Unexpected error communicating with Stripe. If this "
            "problem persists, let us know at support@stripe.com."
        )
        err = "A %s was raised" % (type(e).__name__,)
        should_retry = True

        msg = textwrap.fill(msg) + "\n\n(Network error: %s)" % (err,)
        raise APIConnectionError(msg, should_retry=should_retry) from e

    def request_stream(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[Iterable[bytes], int, Mapping[str, str]]:
        if self._client is None:
            raise RuntimeError(
                "Stripe: HTTPXClient was not initialized with allow_sync_methods=True, "
                "so it cannot be used for synchronous requests."
            )
        args, kwargs = self._get_request_args_kwargs(
            method, url, headers, post_data
        )
        try:
            response = self._client.send(
                request=self._client_async.build_request(*args, **kwargs),
                stream=True,
            )
        except Exception as e:
            self._handle_request_error(e)
        content = response.iter_bytes()
        status_code = response.status_code
        headers = response.headers

        return content, status_code, headers

    async def request_stream_async(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[AsyncIterable[bytes], int, Mapping[str, str]]:
        args, kwargs = self._get_request_args_kwargs(
            method, url, headers, post_data
        )
        try:
            response = await self._client_async.send(
                request=self._client_async.build_request(*args, **kwargs),
                stream=True,
            )
        except Exception as e:
            self._handle_request_error(e)
        content = response.aiter_bytes()
        status_code = response.status_code
        headers = response.headers

        return content, status_code, headers

    def close(self):
        if self._client is not None:
            self._client.close()

    async def close_async(self):
        await self._client_async.aclose()


class AIOHTTPClient(HTTPClient):
    name = "aiohttp"

    def __init__(
        self,
        timeout: Optional[Union[float, "AIOHTTPTimeout"]] = 80,
        _lib=None,  # used for internal unit testing
        session=None,
        connector=None,
        **kwargs,
    ):
        super(AIOHTTPClient, self).__init__(**kwargs)

        if _lib is None:
            import aiohttp

            _lib = aiohttp

        self.aiohttp = _lib

        self._timeout = timeout
        self._user_session = session
        self._user_connector = connector
        self._internally_managed_session = session is None
        self._cached_session = None

    @property
    def _session(self):
        if self._cached_session is None:
            if self._user_session:
                self._cached_session = self._user_session
            else:
                kwargs = {}
                if self._user_connector:
                    kwargs["connector"] = self._user_connector
                elif self._verify_ssl_certs:
                    ssl_context = ssl.create_default_context(
                        cafile=stripe.ca_bundle_path
                    )
                    kwargs["connector"] = self.aiohttp.TCPConnector(
                        ssl=ssl_context
                    )
                else:
                    kwargs["connector"] = self.aiohttp.TCPConnector(
                        verify_ssl=False
                    )

                self._cached_session = self.aiohttp.ClientSession(**kwargs)

        return self._cached_session

    def sleep_async(self, secs):
        return asyncio.sleep(secs)

    def request(self) -> Tuple[bytes, int, Mapping[str, str]]:
        raise NotImplementedError(
            "AIOHTTPClient does not support synchronous requests."
        )

    def _get_request_args_kwargs(
        self, method: str, url: str, headers: Mapping[str, str], post_data
    ):
        args = (method, url)
        kwargs = {}
        if self._proxy:
            if self._proxy["http"] != self._proxy["https"]:
                raise ValueError(
                    "AIOHTTPClient does not support different proxies for HTTP and HTTPS."
                )
            kwargs["proxy"] = self._proxy["https"]
        if self._timeout:
            kwargs["timeout"] = self._timeout

        kwargs["headers"] = headers
        kwargs["data"] = post_data
        return args, kwargs

    async def request_async(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        post_data=None,
    ) -> Tuple[bytes, int, Mapping[str, str]]:
        (
            content,
            status_code,
            response_headers,
        ) = await self.request_stream_async(
            method, url, headers, post_data=post_data
        )

        return (await content.read()), status_code, response_headers

    def _handle_request_error(self, e: Exception) -> NoReturn:
        msg = (
            "Unexpected error communicating with Stripe. If this "
            "problem persists, let us know at support@stripe.com."
        )
        err = "A %s was raised" % (type(e).__name__,)
        should_retry = True

        msg = textwrap.fill(msg) + "\n\n(Network error: %s)" % (err,)
        raise APIConnectionError(msg, should_retry=should_retry) from e

    def request_stream(self) -> Tuple[Iterable[bytes], int, Mapping[str, str]]:
        raise NotImplementedError(
            "AIOHTTPClient does not support synchronous requests."
        )

    async def request_stream_async(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple["AIOHTTPStreamReader", int, Mapping[str, str]]:
        args, kwargs = self._get_request_args_kwargs(
            method, url, headers, post_data
        )
        try:
            response = await self._session.request(*args, **kwargs)
        except Exception as e:
            self._handle_request_error(e)

        content = response.content
        status_code = response.status
        response_headers = response.headers
        return content, status_code, response_headers

    def close(self):
        pass

    async def close_async(self):
        if self._internally_managed_session:
            await self._session.close()


class NoImportFoundAsyncClient(HTTPClient):
    def __init__(self, **kwargs):
        super(NoImportFoundAsyncClient, self).__init__(**kwargs)

    @staticmethod
    def raise_async_client_import_error() -> Never:
        raise ImportError(
            (
                "Import httpx not found. To make async http requests,"
                "You must either install httpx or define your own"
                "async http client by subclassing stripe.HTTPClient"
                "and setting stripe.default_http_client to an instance of it."
            )
        )

    async def request_async(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ) -> Tuple[bytes, int, Mapping[str, str]]:
        self.raise_async_client_import_error()

    async def request_stream_async(
        self, method: str, url: str, headers: Mapping[str, str], post_data=None
    ):
        self.raise_async_client_import_error()

    async def close_async(self):
        self.raise_async_client_import_error()
