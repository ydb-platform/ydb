# Standard imports
import random
import sys

# Constants
MAX_VALUE = sys.maxsize / 2

__all__ = ["jittered", "exponential", "fixed"]


def from_iterable(iterable):
    """Creates a retry strategy from an iterable of timeouts"""

    class IterableRetryBackoff(_IterableBackoff):
        def __iter__(self):
            return iter(iterable)

    return IterableRetryBackoff()


def from_iterable_factory(iterable_factory):
    """
    Creates a retry strategy from a function that returns an iterable
    of timeouts.
    """

    class IterableRetryBackoff(_IterableBackoff):
        def __iter__(self):
            return iter(iterable_factory())

    return IterableRetryBackoff()


class RetryBackoff(object):
    """
    Base class for a strategy that calculates the timeout between
    retry attempts.

    You can compose two ``RetryBackoff`` instances by using the ``|``
    operator::

        CustomBackoffA() | CustomBackoffB()

    The resulting backoff strategy will first compute the timeout using
    the left-hand instance. If that timeout is ``None``, the strategy
    will try to compute a fallback using the right-hand instance. If
    both instances return ``None``, the resulting strategy will also
    return ``None``.
    """

    def get_timeout_after_response(self, request, response):
        """
        Returns the number of seconds to wait before retrying the
        request, or ``None`` to indicate that the given response should
        be returned.
        """
        raise NotImplementedError  # pragma: no cover

    def get_timeout_after_exception(self, request, exc_type, exc_val, exc_tb):
        """
        Returns the number of seconds to wait before retrying the
        request, or ``None`` to indicate that the given exception
        should be raised.
        """
        raise NotImplementedError  # pragma: no cover

    def handle_after_final_retry(self):
        """
        Handles any clean-up necessary following the final retry
        attempt.
        """
        pass  # pragma: no cover

    def __or__(self, other):
        """Composes the current strategy with another."""
        assert isinstance(
            other, RetryBackoff
        ), "Both objects should be backoffs."
        return _Or(self, other)


class _Or(RetryBackoff):
    def __init__(self, left, right):
        self._left = left
        self._right = right

    def get_timeout_after_response(self, request, response):
        timeout = self._left.get_timeout_after_response(request, response)
        if timeout is None:
            return self._right.get_timeout_after_response(request, response)
        return timeout

    def get_timeout_after_exception(self, request, exc_type, exc_val, exc_tb):
        timeout = self._left.get_timeout_after_exception(
            request, exc_type, exc_val, exc_tb
        )
        if timeout is None:
            return self._right.get_timeout_after_exception(
                request, exc_type, exc_val, exc_tb
            )
        return timeout

    def handle_after_final_retry(self):
        self._left.handle_after_final_retry()
        self._right.handle_after_final_retry()


class _IterableBackoff(RetryBackoff):
    __iterator = None

    def __iter__(self):
        raise NotImplementedError  # pragma: no cover

    def __call__(self):
        return iter(self)

    def __next(self):
        if self.__iterator is None:
            self.__iterator = iter(self)

        try:
            return next(self.__iterator)
        except StopIteration:
            return None

    def get_timeout_after_response(self, request, response):
        return self.__next()

    def get_timeout_after_exception(self, request, exc_type, exc_val, exc_tb):
        return self.__next()

    def handle_after_final_retry(self):
        self.__iterator = None


class jittered(_IterableBackoff):
    """
    Waits using capped exponential backoff and full jitter.

    The implementation is discussed in `this AWS Architecture Blog
    post <https://amzn.to/2xc2nK2>`_, which recommends this approach
    for any remote clients, as it minimizes the total completion
    time of competing clients in a distributed system experiencing
    high contention.
    """

    def __init__(self, base=2, multiplier=1, minimum=0, maximum=MAX_VALUE):
        self._exp_backoff = exponential(base, multiplier, minimum, maximum)

    def __iter__(self):
        for delay in self._exp_backoff():  # pragma: no branch
            yield random.uniform(0, 1) * delay


class exponential(_IterableBackoff):
    """
    Waits using capped exponential backoff, meaning that the delay
    is multiplied by a constant ``base`` after each attempt, up to
    an optional ``maximum`` value.
    """

    def __init__(self, base=2, multiplier=1, minimum=0, maximum=MAX_VALUE):
        self._base = base
        self._multiplier = multiplier
        self._minimum = minimum
        self._maximum = maximum

    def __iter__(self):
        delay = self._multiplier
        while self._minimum > delay:
            delay *= self._base
        while True:
            yield min(delay, self._maximum)
            delay *= self._base


class fixed(_IterableBackoff):
    """Waits for a fixed number of ``seconds`` before each retry."""

    def __init__(self, seconds):
        self._seconds = seconds

    def __iter__(self):
        while True:
            yield self._seconds
