import asyncio
import random
import time

from . import issues
from ._errors import check_retriable_error


class BackoffSettings(object):
    def __init__(self, ceiling=6, slot_duration=0.001, uncertain_ratio=0.5):
        self.ceiling = ceiling
        self.slot_duration = slot_duration
        self.uncertain_ratio = uncertain_ratio

    def calc_timeout(self, retry_number):
        slots_count = 1 << min(retry_number, self.ceiling)
        max_duration_ms = slots_count * self.slot_duration * 1000.0
        # duration_ms = random.random() * max_duration_ms * uncertain_ratio) + max_duration_ms * (1 - uncertain_ratio)
        duration_ms = max_duration_ms * (random.random() * self.uncertain_ratio + 1.0 - self.uncertain_ratio)
        return duration_ms / 1000.0


class RetrySettings(object):
    def __init__(
        self,
        max_retries=10,
        max_session_acquire_timeout=None,
        on_ydb_error_callback=None,
        backoff_ceiling=6,
        backoff_slot_duration=1,
        get_session_client_timeout=5,
        fast_backoff_settings=None,
        slow_backoff_settings=None,
        idempotent=False,
    ):
        self.max_retries = max_retries
        self.max_session_acquire_timeout = max_session_acquire_timeout
        self.on_ydb_error_callback = (lambda e: None) if on_ydb_error_callback is None else on_ydb_error_callback
        self.fast_backoff = BackoffSettings(10, 0.005) if fast_backoff_settings is None else fast_backoff_settings
        self.slow_backoff = (
            BackoffSettings(backoff_ceiling, backoff_slot_duration)
            if slow_backoff_settings is None
            else slow_backoff_settings
        )
        self.retry_not_found = True
        self.idempotent = idempotent
        self.retry_internal_error = True
        self.unknown_error_handler = lambda e: None
        self.get_session_client_timeout = get_session_client_timeout
        if max_session_acquire_timeout is not None:
            self.get_session_client_timeout = min(self.max_session_acquire_timeout, self.get_session_client_timeout)

    def with_fast_backoff(self, backoff_settings):
        self.fast_backoff = backoff_settings
        return self

    def with_slow_backoff(self, backoff_settings):
        self.slow_backoff = backoff_settings
        return self


class YdbRetryOperationSleepOpt(object):
    def __init__(self, timeout):
        self.timeout = timeout

    def __eq__(self, other):
        return type(self) == type(other) and self.timeout == other.timeout

    def __repr__(self):
        return "YdbRetryOperationSleepOpt(%s)" % self.timeout


class YdbRetryOperationFinalResult(object):
    def __init__(self, result):
        self.result = result
        self.exc = None

    def __eq__(self, other):
        return type(self) == type(other) and self.result == other.result and self.exc == other.exc

    def __repr__(self):
        return "YdbRetryOperationFinalResult(%s, exc=%s)" % (self.result, self.exc)

    def set_exception(self, exc):
        self.exc = exc


def retry_operation_impl(callee, retry_settings=None, *args, **kwargs):
    retry_settings = RetrySettings() if retry_settings is None else retry_settings
    status = None

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

    raise status


def retry_operation_sync(callee, retry_settings=None, *args, **kwargs):
    opt_generator = retry_operation_impl(callee, retry_settings, *args, **kwargs)
    for next_opt in opt_generator:
        if isinstance(next_opt, YdbRetryOperationSleepOpt):
            time.sleep(next_opt.timeout)
        else:
            return next_opt.result


async def retry_operation_async(callee, retry_settings=None, *args, **kwargs):  # pylint: disable=W1113
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
