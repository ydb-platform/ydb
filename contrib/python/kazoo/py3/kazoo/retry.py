import logging
import random
import time

from kazoo.exceptions import (
    ConnectionClosedError,
    ConnectionLoss,
    KazooException,
    OperationTimeoutError,
    SessionExpiredError,
)


log = logging.getLogger(__name__)


class ForceRetryError(Exception):
    """Raised when some recipe logic wants to force a retry."""


class RetryFailedError(KazooException):
    """Raised when retrying an operation ultimately failed, after
    retrying the maximum number of attempts.
    """


class InterruptedError(RetryFailedError):
    """Raised when the retry is forcibly interrupted by the interrupt
    function"""


class KazooRetry(object):
    """Helper for retrying a method in the face of retry-able
    exceptions"""

    RETRY_EXCEPTIONS = (ConnectionLoss, OperationTimeoutError, ForceRetryError)

    EXPIRED_EXCEPTIONS = (SessionExpiredError,)

    def __init__(
        self,
        max_tries=1,
        delay=0.1,
        backoff=2,
        max_jitter=0.4,
        max_delay=60.0,
        ignore_expire=True,
        sleep_func=time.sleep,
        deadline=None,
        interrupt=None,
    ):
        """Create a :class:`KazooRetry` instance for retrying function
        calls.

        :param max_tries: How many times to retry the command. -1 means
                          infinite tries.
        :param delay: Initial delay between retry attempts.
        :param backoff: Backoff multiplier between retry attempts.
                        Defaults to 2 for exponential backoff.
        :param max_jitter: Percentage of jitter to apply to each retry's delay
                           to ensure all clients to do not hammer the server
                           at the same time. Between 0.0 and 1.0.
        :param max_delay: Maximum delay in seconds, regardless of other
                          backoff settings. Defaults to one minute.
        :param ignore_expire:
            Whether a session expiration should be ignored and treated
            as a retry-able command.
        :param interrupt:
            Function that will be called with no args that may return
            True if the retry should be ceased immediately. This will
            be called no more than every 0.1 seconds during a wait
            between retries.

        """
        self.max_tries = max_tries
        self.delay = delay
        self.backoff = backoff
        # Ensure max_jitter is in (0, 1)
        self.max_jitter = max(min(max_jitter, 1.0), 0.0)
        self.max_delay = float(max_delay)
        self._attempts = 0
        self._cur_delay = delay
        self.deadline = deadline
        self._cur_stoptime = None
        self.sleep_func = sleep_func
        self.retry_exceptions = self.RETRY_EXCEPTIONS
        self.interrupt = interrupt
        if ignore_expire:
            self.retry_exceptions += self.EXPIRED_EXCEPTIONS

    def reset(self):
        """Reset the attempt counter"""
        self._attempts = 0
        self._cur_delay = self.delay
        self._cur_stoptime = None

    def copy(self):
        """Return a clone of this retry manager"""
        obj = KazooRetry(
            max_tries=self.max_tries,
            delay=self.delay,
            backoff=self.backoff,
            max_jitter=self.max_jitter,
            max_delay=self.max_delay,
            sleep_func=self.sleep_func,
            deadline=self.deadline,
            interrupt=self.interrupt,
        )
        obj.retry_exceptions = self.retry_exceptions
        return obj

    def __call__(self, func, *args, **kwargs):
        """Call a function with arguments until it completes without
        throwing a Kazoo exception

        :param func: Function to call
        :param args: Positional arguments to call the function with
        :params kwargs: Keyword arguments to call the function with

        The function will be called until it doesn't throw one of the
        retryable exceptions (ConnectionLoss, OperationTimeout, or
        ForceRetryError), and optionally retrying on session
        expiration.

        """
        self.reset()

        while True:
            try:
                if self.deadline is not None and self._cur_stoptime is None:
                    self._cur_stoptime = time.time() + self.deadline
                return func(*args, **kwargs)
            except ConnectionClosedError:
                raise
            except self.retry_exceptions:
                # Note: max_tries == -1 means infinite tries.
                if self._attempts == self.max_tries:
                    raise RetryFailedError("Too many retry attempts")
                self._attempts += 1
                jitter = random.uniform(
                    1.0 - self.max_jitter, 1.0 + self.max_jitter
                )
                sleeptime = self._cur_delay * jitter

                if (
                    self._cur_stoptime is not None
                    and time.time() + sleeptime >= self._cur_stoptime
                ):
                    raise RetryFailedError("Exceeded retry deadline")

                if self.interrupt:
                    remain_time = sleeptime
                    while remain_time > 0:
                        # Break the time period down and sleep for no
                        # longer than 0.1 before calling the interrupt
                        self.sleep_func(min(0.1, remain_time))
                        remain_time -= 0.1
                        if self.interrupt():
                            raise InterruptedError()
                else:
                    self.sleep_func(sleeptime)
                self._cur_delay = min(sleeptime * self.backoff, self.max_delay)
