__all__ = [
    'TolokaRetry',
    'SyncRetryingOverURLLibRetry',
    'AsyncRetryingOverURLLibRetry',
    'STATUSES_TO_RETRY',
]
import abc
import tenacity
import typing
import urllib3.response
import urllib3.util.retry


STATUSES_TO_RETRY = ...

class TolokaRetry(urllib3.util.retry.Retry):
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
        ...

    def __init__(
        self,
        *args,
        retry_quotas: typing.Union[typing.List[str], str, None] = 'MIN',
        **kwargs
    ): ...

    def new(self, **kwargs): ...

    def get_retry_after(self, response: urllib3.response.HTTPResponse) -> typing.Optional[float]: ...

    def increment(
        self,
        *args,
        **kwargs
    ) -> urllib3.util.retry.Retry: ...

    DEFAULT: typing.Any
    _retry_quotas: typing.Union[typing.List[str], str, None]


class RetryingOverURLLibRetry(tenacity.BaseRetrying, metaclass=abc.ABCMeta):
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

    def __init__(
        self,
        base_url: str,
        retry: urllib3.util.retry.Retry,
        exception_to_retry: typing.Tuple[typing.Type[Exception], ...] = ...,
        **kwargs
    ): ...

    def wraps(self, f: tenacity.WrappedFn) -> tenacity.WrappedFn: ...

    def __getstate__(self): ...

    def __setstate__(self, state): ...


class SyncRetryingOverURLLibRetry(RetryingOverURLLibRetry, tenacity.Retrying):
    ...


class AsyncRetryingOverURLLibRetry(RetryingOverURLLibRetry, tenacity.AsyncRetrying):
    sleep: typing.Callable[[float], typing.Awaitable[typing.Any]]
