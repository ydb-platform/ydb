import functools
import logging
from datetime import timedelta
from random import random, uniform
from typing import Callable, Optional, Sequence, Tuple, Type, TypeVar

from .clock import Clock, RealClock

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retried(
    *,
    on: Optional[Sequence[Type[BaseException]]] = None,
    is_retryable: Optional[Callable[[BaseException], Optional[str]]] = None,
    timeout=timedelta(minutes=20),
    clock: Optional[Clock] = None,
    before_retry: Optional[Callable] = None,
    max_attempts: Optional[int] = None,
):
    has_allowlist = on is not None
    has_callback = is_retryable is not None
    if not (has_allowlist or has_callback) or (has_allowlist and has_callback):
        raise SyntaxError("either on=[Exception] or callback=lambda x: .. is required")
    if clock is None:
        clock = RealClock()

    def decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            deadline = clock.time() + timeout.total_seconds()
            attempt = 1
            last_err = None
            while clock.time() < deadline and (max_attempts is None or attempt <= max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    last_err = err
                    retry_reason = None
                    # sleep 10s max per attempt, unless it's HTTP 429 or 503
                    sleep = min(10, attempt)
                    retry_after_secs = getattr(err, "retry_after_secs", None)
                    if retry_after_secs is not None:
                        # cannot depend on DatabricksError directly because of circular dependency
                        sleep = retry_after_secs
                        retry_reason = "throttled by platform"
                    elif is_retryable is not None:
                        retry_reason = is_retryable(err)
                    elif on is not None:
                        for err_type in on:
                            if not isinstance(err, err_type):
                                continue
                            retry_reason = f"{type(err).__name__} is allowed to retry"

                    if retry_reason is None:
                        # raise if exception is not retryable
                        raise err

                    logger.debug(f"Retrying: {retry_reason} (sleeping ~{sleep}s)")
                    if before_retry:
                        before_retry()

                    clock.sleep(sleep + random())
                    attempt += 1

            # Determine which limit was hit
            if max_attempts is not None and attempt > max_attempts:
                raise RuntimeError(f"Exceeded max retry attempts ({max_attempts})") from last_err
            raise TimeoutError(f"Timed out after {timeout}") from last_err

        return wrapper

    return decorator


class RetryError(Exception):
    """Error that can be returned from poll functions to control retry behavior."""

    def __init__(self, err: Exception, halt: bool = False):
        self.err = err
        self.halt = halt
        super().__init__(str(err))

    @staticmethod
    def continues(msg: str) -> "RetryError":
        """Create a non-halting retry error with a message."""
        return RetryError(Exception(msg), halt=False)

    @staticmethod
    def halt(err: Exception) -> "RetryError":
        """Create a halting retry error."""
        return RetryError(err, halt=True)


def _backoff(attempt: int) -> float:
    """Calculate backoff time with jitter.

    Linear backoff: attempt * 1 second, capped at 10 seconds
    Plus random jitter between 50ms and 750ms.
    """
    wait = min(10, attempt)
    jitter = uniform(0.05, 0.75)
    return wait + jitter


def poll(
    fn: Callable[[], Tuple[Optional[T], Optional[RetryError]]],
    timeout: Optional[timedelta] = None,
    clock: Optional[Clock] = None,
) -> T:
    """Poll a function until it succeeds or times out.

    The backoff is linear backoff and jitter.

    This function is not meant to be used directly by users.
    It is used internally by the SDK to poll for the result of an operation.
    It can be changed in the future without any notice.

    :param fn: Function that returns (result, error).
               Return (None, RetryError.continues("msg")) to continue polling.
               Return (None, RetryError.halt(err)) to stop with error.
               Return (result, None) on success.
    :param timeout: Maximum time to poll. If None, polls indefinitely.
    :param clock: Clock implementation for testing (default: RealClock)
    :returns: The result of the successful function call
    :raises TimeoutError: If the timeout is reached
    :raises Exception: If a halting error is encountered

    Example:
        def check_operation():
            op = get_operation()
            if not op.done:
                return None, RetryError.continues("operation still in progress")
            if op.error:
                return None, RetryError.halt(Exception(f"operation failed: {op.error}"))
            return op.result, None

        result = poll(check_operation, timeout=timedelta(minutes=5))
    """
    if clock is None:
        clock = RealClock()

    deadline = float("inf") if timeout is None else clock.time() + timeout.total_seconds()
    attempt = 0
    last_err = None

    while clock.time() < deadline:
        attempt += 1

        try:
            result, err = fn()

            if err is None:
                return result

            if err.halt:
                raise err.err

            # Continue polling.
            last_err = err.err
            wait = _backoff(attempt)
            logger.debug(f"{str(err.err).rstrip('.')}. Sleeping {wait:.3f}s")
            clock.sleep(wait)

        except RetryError:
            raise
        except Exception as e:
            # Unexpected error, halt immediately.
            raise e

    raise TimeoutError(f"Timed out after {timeout}") from last_err
