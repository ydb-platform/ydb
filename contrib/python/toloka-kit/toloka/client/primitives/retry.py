__all__ = [
    'TolokaRetry', 'SyncRetryingOverURLLibRetry', 'AsyncRetryingOverURLLibRetry', 'STATUSES_TO_RETRY',
]

import json
import logging
import sys
from functools import wraps
from inspect import signature
from typing import Callable, List, Optional, Tuple, Type, Union

import httpx
import urllib3
import urllib3.exceptions
from httpx import URL
from tenacity import AsyncRetrying, BaseRetrying, RetryCallState, Retrying, WrappedFn
from tenacity.retry import retry_base
from tenacity.stop import stop_base
from tenacity.wait import wait_base
from typing_extensions import Protocol, runtime_checkable
from urllib3.response import HTTPResponse  # type: ignore
from urllib3.util.retry import Retry  # type: ignore

from .adapters import httpx_exception_to_urllib3_exception, map_urllib3_exception_for_retrying

logger = logging.getLogger(__name__)

STATUSES_TO_RETRY = {408, 429, 500, 503, 504}


class TolokaRetry(Retry):
    """Retry toloka quotas. By default, only minutes quotas.

    Args:
        retry_quotas (Union[List[str], str, None]): List of quotas that will be retried.
            None or empty list for not retrying quotas.
            You can specify quotas:
            * MIN - Retry minutes quotas.
            * HOUR - Retry hourly quotas. This is means that the program just sleeps for an hour! Be careful.
            * DAY - Retry daily quotas. We strongly not recommended retrying these quotas.
    """

    class Unit:
        MIN = 'MIN'
        HOUR = 'HOUR'
        DAY = 'DAY'

    seconds_to_wait = {
        Unit.MIN: 60,
        Unit.HOUR: 60 * 60,
        Unit.DAY: 60 * 60 * 24,
    }

    _retry_quotas: Union[List[str], str, None] = None

    def __init__(self, *args, retry_quotas: Union[List[str], str, None] = Unit.MIN, **kwargs):
        if isinstance(retry_quotas, str):
            self._retry_quotas = [retry_quotas]
        else:
            self._retry_quotas = retry_quotas

        self._last_response = kwargs.pop('last_response', None)
        super(TolokaRetry, self).__init__(*args, **kwargs)

    def new(self, **kwargs):
        kwargs['last_response'] = self._last_response
        return super(TolokaRetry, self).new(retry_quotas=self._retry_quotas, **kwargs)

    def get_retry_after(self, response: HTTPResponse) -> Optional[float]:
        seconds = super(TolokaRetry, self).get_retry_after(response)
        if seconds is not None:
            return seconds

        if response.status != 429 or self._retry_quotas is None or self._last_response is None:
            return None
        payload = self._last_response.get('payload', None)
        if payload is None or 'interval' not in payload:
            return None

        interval = payload['interval']
        if interval not in self._retry_quotas:
            return None

        if interval == TolokaRetry.Unit.HOUR:
            logger.warning('The limit on hourly quotas worked. The program "falls asleep" for an hour.')
        if interval == TolokaRetry.Unit.DAY:
            logger.warning('The daily quota limit worked. The program "falls asleep" for the day.')
        return TolokaRetry.seconds_to_wait.get(interval, None)

    def increment(self, *args, **kwargs) -> Retry:
        self._last_response = None
        response = kwargs.get('response', None)
        try:
            if response is not None:
                data = response.data
                if data:
                    self._last_response = json.loads(response.data.decode("utf-8"))
        except json.JSONDecodeError:
            pass
        return super(TolokaRetry, self).increment(*args, **kwargs)


@runtime_checkable
class HTTPXRequestFn(Protocol):
    def __call__(self, method: str, url: Union[URL, str], **kwargs) -> HTTPResponse:
        ...


class RetryingOverURLLibRetry(BaseRetrying):
    """Adapter class that allows usage of the urllib3 Retry class in httpx using the tenacity retrying mechanism.

    Wrapped function should make a single request using HTTPX library and either return httpx.Response or raise an
    exception.

    This class tries to follow the behavior of urllib3.connectionpool.HTTPConnectionPool.urlopen as close as possible:
    * If an exception is raised during the request:
        * urllib3 catches a subset of all possible exceptions, calls `Retry.increment` with the raised error and
            recursively calls urlopen;
        * this class matches the raised exception against exception_to_retry field in retry callback: if the exception
            is not matched the retry callback returns False and the exception is raised in user code. Otherwise, retry
            callback returns True and the `Retry.increment` is called inside the `after` callback with the
            corresponding urllib3 exception.
    * If request returned a response:
        * urllib3 checks if retry should happen using `Retry.is_retry` method, calls `Retry.increment` with the
            received urllib3.Response object and makes recursive call with the new `Retry` instance. If not retry
            should happen the response object is returned;
        * this class calls `Retry.is_retry` method in `retry` callback. If no retry should happen `retry` callback
            returns False and the execution control is returned to the user code. If retry should happen, this class
            increments `Retry` instance using the `urllib3.Response` object constructed from the `httpx.Response`.
    * Retrying is stopped when `Retry.is_exhausted` returns False:
        * urllib3 achieves this by raising an exception inside `Retry.increment`;
        * this class catches this exception when calling `Retry.increment` inside the `after` callback and explicitly
            calls `is_exhausted` inside the `stop` callback.

    Usage of proxies is currently not supported
    """

    def __init__(self, base_url: str, retry: Retry, exception_to_retry: Tuple[Type[Exception], ...] = (), **kwargs):
        self.base_url = base_url
        self.urllib3_retry = retry

        # default exceptions are based on:
        #   * exceptions that are caught in urllib3.connectionpool.HTTPConnectionPool.urlopen
        #   * toloka.client.primitives.adapters.httpx_exception_to_urllib3_exception mapping
        self.exception_to_retry = (
            httpx.TimeoutException,  # urllib3.exceptions.TimeoutError
            httpx.NetworkError,  # SocketError or NewConnectionError
            httpx.ProtocolError,  # urllib3.exceptions.ProtocolError
            *exception_to_retry
        )

        super().__init__(
            stop=self._get_stop_callback(),
            wait=self._get_wait_callback(),
            after=self._get_after_callback(),
            retry=self._get_retry_callback(),
            **kwargs,
        )

    def copy(self, *args, **kwargs):
        return self.__class__(
            base_url=self.base_url,
            retry=self.urllib3_retry,
            exception_to_retry=self.exception_to_retry,
            *args,
            **kwargs,
        )

    def wraps(self, f: WrappedFn) -> WrappedFn:
        # This check contradicts Liskov substitution principle, so it is not enforced by type hints. However, it is
        # handy to use inheritance here to define SyncRetryingOverURLLibRetry and AsyncRetryingOverURLLibRetry despite
        # the fact that they are not substitutable for the BaseRetrying.
        assert isinstance(f, HTTPXRequestFn), 'Wrapped function should be callable with (method, url, **kwargs)'
        return super().wraps(f)

    def __getstate__(self):
        return {
            'base_url': self.base_url, 'urllib3_retry': self.urllib3_retry,
            'exception_to_retry': self.exception_to_retry
        }

    def __setstate__(self, state):
        self.__init__(
            base_url=state['base_url'], retry=state['urllib3_retry'], exception_to_retry=state['exception_to_retry']
        )

    def _patch_with_urllib3_retry(self, func: Callable):
        """Ensures that retry_state contains current urllib3 Retry instance before function call."""

        @wraps(func)
        def wrapped(*args, **kwargs):
            bound_args = signature(func).bind(*args, **kwargs)
            retry_state = bound_args.arguments['retry_state']
            if getattr(retry_state, 'urllib3_retry', None) is None:
                retry_state.urllib3_retry = self.urllib3_retry
            return func(*args, **kwargs)

        return wrapped

    @staticmethod
    def _get_urllib_response(retry_state: RetryCallState) -> Optional[urllib3.HTTPResponse]:
        """Constructs urllib3 response from httpx response."""

        httpx_response: Optional[httpx.Response]
        if retry_state.outcome.failed:
            httpx_response = getattr(retry_state.outcome.exception(), 'response', None)
        else:
            httpx_response = retry_state.outcome.result()

        if httpx_response is None:
            return None

        _httpx_to_urllib_http_version = {'HTTP/2': 20, 'HTTP/1.1': 11}
        return urllib3.HTTPResponse(
            body=httpx_response.content,
            headers=httpx_response.headers,
            status=httpx_response.status_code,
            version=_httpx_to_urllib_http_version[httpx_response.http_version],
            reason=httpx_response.reason_phrase,
            preload_content=True,
            decode_content=False,
            request_method=httpx_response.request.method,
            request_url=httpx_response.request.url,
        )

    def _get_stop_callback(self) -> stop_base:

        class IsExhausted(stop_base):
            """Callback wrapped in callable class to match BaseRetrying signature."""

            @self._patch_with_urllib3_retry
            def __call__(self, retry_state):
                retry: Retry = retry_state.urllib3_retry  # noqa:
                return retry.is_exhausted()

        return IsExhausted()

    def _get_wait_callback(self) -> wait_base:
        outer_self = self

        class GetBackoffTime(wait_base):
            """Callback wrapped in callable class to match BaseRetrying signature."""

            @self._patch_with_urllib3_retry
            def __call__(self, retry_state):
                response = outer_self._get_urllib_response(retry_state)
                retry: Retry = retry_state.urllib3_retry  # noqa:
                if response and retry.respect_retry_after_header:
                    retry_after = retry.get_retry_after(response)
                    if retry_after:
                        return retry_state
                return retry.get_backoff_time()

        return GetBackoffTime()

    def _get_retry_callback(self) -> retry_base:
        class IsRetry(retry_base):
            """Callback wrapped in callable class to match BaseRetrying signature."""

            def _should_retry_exception(self, retry_state) -> bool:
                """Returns False if either can't map the exception"""

                exception = retry_state.outcome.exception()
                urllib3_exception = httpx_exception_to_urllib3_exception(exception)

                try:
                    map_urllib3_exception_for_retrying(urllib3_exception)
                except RuntimeError:
                    # exception will be reraised
                    return False

                return isinstance(exception, retry_state.retry_object.exception_to_retry)

            @self._patch_with_urllib3_retry
            def __call__(self, retry_state):
                httpx_response: Optional[httpx.Response]
                if retry_state.outcome.failed:
                    httpx_response = getattr(retry_state.outcome.exception(), 'response', None)
                else:
                    httpx_response = retry_state.outcome.result()

                if httpx_response is None:
                    return self._should_retry_exception(retry_state)

                has_retry_after = bool(httpx_response.headers.get("Retry-After", False))
                retry: Retry = retry_state.urllib3_retry  # noqa:
                return retry.is_retry(
                    method=httpx_response.request.method, status_code=httpx_response.status_code,
                    has_retry_after=has_retry_after
                )

        return IsRetry()

    def _get_after_callback(self):

        @self._patch_with_urllib3_retry
        def increment(retry_state: RetryCallState):
            """This function:
            * tries to extract response from retry_state: either from the retry_state outcome or the raised exception
                `response` field
            * if response is found maps it to urllib3.HTTPReponse object and passes it to the wrapped urllib3.Retry
                increment method
            * otherwise maps the httpx exception to the urllib3 exception and passes it to the wrapped urllib3.Retry
                increment method
            """

            retry: Retry = retry_state.urllib3_retry  # noqa:
            bound_args = signature(retry_state.fn).bind(*retry_state.args, **retry_state.kwargs)

            response = self._get_urllib_response(retry_state)
            exception = None
            if response is None:
                exception = httpx_exception_to_urllib3_exception(retry_state.outcome.exception())
                # should not raise RuntimeError as it is checked inside the retry callback
                exception = map_urllib3_exception_for_retrying(exception)

            try:
                retry_state.urllib3_retry = retry.increment(
                    method=bound_args.arguments['method'],
                    url=f'{self.base_url}{bound_args.arguments["url"]}',
                    response=response,
                    error=exception,
                    _stacktrace=sys.exc_info()[2]
                )
            except urllib3.exceptions.MaxRetryError:
                # retry.increment raises MaxRetryError when exhausted. We want to delegate checking if the process
                # should be stopped to the stop callback. MaxRetryError is raised if and only if retry.is_exhausted of
                # the resulting exception is True.
                retry_state.urllib3_retry = Retry(total=-1)

        return increment


class SyncRetryingOverURLLibRetry(RetryingOverURLLibRetry, Retrying):
    pass


class AsyncRetryingOverURLLibRetry(RetryingOverURLLibRetry, AsyncRetrying):
    pass
