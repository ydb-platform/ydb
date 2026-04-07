import asyncio
import functools
import inspect
import random
import time
from typing import Any, Callable, Generator, Optional, Union

from . import issues
from ._errors import check_retriable_error


class BackoffSettings:
    def __init__(
        self,
        ceiling: int = 6,
        slot_duration: float = 0.001,
        uncertain_ratio: float = 0.5,
    ) -> None:
        self.ceiling = ceiling
        self.slot_duration = slot_duration
        self.uncertain_ratio = uncertain_ratio

    def calc_timeout(self, retry_number: int) -> float:
        slots_count = 1 << min(retry_number, self.ceiling)
        max_duration_ms = slots_count * self.slot_duration * 1000.0
        # duration_ms = random.random() * max_duration_ms * uncertain_ratio) + max_duration_ms * (1 - uncertain_ratio)
        duration_ms = max_duration_ms * (random.random() * self.uncertain_ratio + 1.0 - self.uncertain_ratio)
        return duration_ms / 1000.0


class RetrySettings:
    def __init__(
        self,
        max_retries: int = 10,
        max_session_acquire_timeout: Optional[float] = None,
        on_ydb_error_callback: Optional[Callable[[issues.Error], None]] = None,
        backoff_ceiling: int = 6,
        backoff_slot_duration: float = 1,
        get_session_client_timeout: float = 5,
        fast_backoff_settings: Optional[BackoffSettings] = None,
        slow_backoff_settings: Optional[BackoffSettings] = None,
        idempotent: bool = False,
        retry_cancelled: bool = False,
    ) -> None:
        self.max_retries = max_retries
        self.max_session_acquire_timeout = max_session_acquire_timeout
        self.on_ydb_error_callback: Callable[[issues.Error], None] = (
            (lambda e: None) if on_ydb_error_callback is None else on_ydb_error_callback
        )
        self.fast_backoff = BackoffSettings(10, 0.005) if fast_backoff_settings is None else fast_backoff_settings
        self.slow_backoff = (
            BackoffSettings(backoff_ceiling, backoff_slot_duration)
            if slow_backoff_settings is None
            else slow_backoff_settings
        )
        self.retry_not_found = True
        self.idempotent = idempotent
        self.retry_internal_error = True
        self.retry_cancelled = retry_cancelled
        self.unknown_error_handler: Callable[[Exception], None] = lambda e: None
        self.get_session_client_timeout: float = get_session_client_timeout
        if max_session_acquire_timeout is not None:
            self.get_session_client_timeout = min(max_session_acquire_timeout, self.get_session_client_timeout)

    def with_fast_backoff(self, backoff_settings: BackoffSettings) -> "RetrySettings":
        self.fast_backoff = backoff_settings
        return self

    def with_slow_backoff(self, backoff_settings: BackoffSettings) -> "RetrySettings":
        self.slow_backoff = backoff_settings
        return self


class YdbRetryOperationSleepOpt:
    def __init__(self, timeout: float) -> None:
        self.timeout = timeout

    def __eq__(self, other: object) -> bool:
        return (
            type(self) is type(other) and isinstance(other, YdbRetryOperationSleepOpt) and self.timeout == other.timeout
        )

    def __repr__(self) -> str:
        return "YdbRetryOperationSleepOpt(%s)" % self.timeout


class YdbRetryOperationFinalResult:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.exc: Optional[BaseException] = None

    def __eq__(self, other: object) -> bool:
        return (
            type(self) is type(other)
            and isinstance(other, YdbRetryOperationFinalResult)
            and self.result == other.result
            and self.exc == other.exc
        )

    def __repr__(self) -> str:
        return "YdbRetryOperationFinalResult(%s, exc=%s)" % (self.result, self.exc)

    def set_exception(self, exc: BaseException) -> None:
        self.exc = exc


def retry_operation_impl(
    callee: Callable[..., Any],
    retry_settings: Optional[RetrySettings] = None,
    *args: Any,
    **kwargs: Any,
) -> Generator[Union[YdbRetryOperationSleepOpt, YdbRetryOperationFinalResult], None, None]:
    retry_settings = RetrySettings() if retry_settings is None else retry_settings
    status: Optional[issues.Error] = None

    for attempt in range(retry_settings.max_retries + 1):
        try:
            result = YdbRetryOperationFinalResult(callee(*args, **kwargs))
            yield result

            if result.exc is not None:
                raise result.exc

        except issues.Error as e:
            status = e
            retry_settings.on_ydb_error_callback(e)

            retriable_info = check_retriable_error(e, retry_settings, attempt)
            if not retriable_info.is_retriable:
                raise

            skip_yield_error_types = [
                issues.Aborted,
                issues.BadSession,
                issues.NotFound,
                issues.InternalError,
            ]

            yield_sleep = True
            for t in skip_yield_error_types:
                if isinstance(e, t):
                    yield_sleep = False

            if yield_sleep:
                yield YdbRetryOperationSleepOpt(retriable_info.sleep_timeout_seconds)

        except Exception as e:
            # you should provide your own handler you want
            retry_settings.unknown_error_handler(e)
            raise

    if status is not None:
        raise status


def retry_operation_sync(
    callee: Callable[..., Any],
    retry_settings: Optional[RetrySettings] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    opt_generator = retry_operation_impl(callee, retry_settings, *args, **kwargs)
    for next_opt in opt_generator:
        if isinstance(next_opt, YdbRetryOperationSleepOpt):
            time.sleep(next_opt.timeout)
        else:
            return next_opt.result
    return None


async def retry_operation_async(  # pylint: disable=W1113
    callee: Callable[..., Any],
    retry_settings: Optional[RetrySettings] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    The retry operation helper can be used to retry a coroutine that raises YDB specific
    exceptions.

    :param callee: A coroutine to retry.
    :param retry_settings: An instance of ydb.RetrySettings that describes how the coroutine
    should be retried. If None, default instance of retry settings will be used.
    :param args: A tuple with positional arguments to be passed into the coroutine.
    :param kwargs: A dictionary with keyword arguments to be passed into the coroutine.

    Returns awaitable result of coroutine. If retries are not succussful exception is raised.
    """
    opt_generator = retry_operation_impl(callee, retry_settings, *args, **kwargs)
    for next_opt in opt_generator:
        if isinstance(next_opt, YdbRetryOperationSleepOpt):
            await asyncio.sleep(next_opt.timeout)
        else:
            try:
                return await next_opt.result
            except BaseException as e:  # pylint: disable=W0703
                next_opt.set_exception(e)
    return None


def ydb_retry(
    max_retries: int = 10,
    max_session_acquire_timeout: Optional[float] = None,
    on_ydb_error_callback: Optional[Callable[[issues.Error], None]] = None,
    backoff_ceiling: int = 6,
    backoff_slot_duration: float = 1,
    get_session_client_timeout: float = 5,
    fast_backoff_settings: Optional[BackoffSettings] = None,
    slow_backoff_settings: Optional[BackoffSettings] = None,
    idempotent: bool = False,
    retry_cancelled: bool = False,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator for automatic function retry in case of YDB errors.

    Supports both synchronous and asynchronous functions.

    :param max_retries: Maximum number of retries (default: 10)
    :param max_session_acquire_timeout: Maximum session acquisition timeout (default: None)
    :param on_ydb_error_callback: Callback for handling YDB errors (default: None)
    :param backoff_ceiling: Ceiling for backoff algorithm (default: 6)
    :param backoff_slot_duration: Slot duration for backoff (default: 1)
    :param get_session_client_timeout: Session client timeout (default: 5)
    :param fast_backoff_settings: Fast backoff settings (default: None)
    :param slow_backoff_settings: Slow backoff settings (default: None)
    :param idempotent: Whether the operation is idempotent (default: False)
    :param retry_cancelled: Whether to retry cancelled operations (default: False)
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        retry_settings = RetrySettings(
            max_retries=max_retries,
            max_session_acquire_timeout=max_session_acquire_timeout,
            on_ydb_error_callback=on_ydb_error_callback,
            backoff_ceiling=backoff_ceiling,
            backoff_slot_duration=backoff_slot_duration,
            get_session_client_timeout=get_session_client_timeout,
            fast_backoff_settings=fast_backoff_settings,
            slow_backoff_settings=slow_backoff_settings,
            idempotent=idempotent,
            retry_cancelled=retry_cancelled,
        )

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await retry_operation_async(func, retry_settings, *args, **kwargs)

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return retry_operation_sync(func, retry_settings, *args, **kwargs)

            return sync_wrapper

    return decorator
