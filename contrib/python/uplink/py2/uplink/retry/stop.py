
__all__ = ["after_attempt", "after_delay"]


class RetryBreaker(object):
    def __or__(self, other):
        if other is not None:
            assert isinstance(
                other, RetryBreaker
            ), "Both objects should be retry breakers."
            return _Or(self, other)
        return self

    def __call__(self):  # pragma: no cover
        raise NotImplementedError


class _Or(RetryBreaker):
    def __init__(self, left, right):
        self._left = left
        self._right = right

    def __call__(self):
        left = self._left()
        right = self._right()
        while True:
            delay = yield
            next(left)
            next(right)
            stop_left = left.send(delay)
            stop_right = right.send(delay)
            yield stop_left or stop_right


# noinspection PyPep8Naming
class after_attempt(RetryBreaker):
    """Stops retrying after the specified number of ``attempts``."""

    def __init__(self, attempt):
        self._max_attempt = attempt
        self._attempt = 0

    def __call__(self):
        attempt = 0
        while True:
            yield
            attempt += 1
            yield self._max_attempt <= attempt


# noinspection PyPep8Naming
class after_delay(RetryBreaker):
    """
    Stops retrying after the backoff exceeds the specified ``delay``
    in seconds.
    """

    def __init__(self, delay):
        self._max_delay = delay

    def __call__(self):
        while True:
            delay = yield
            yield self._max_delay < delay


class _NeverStop(RetryBreaker):
    def __call__(self):
        while True:
            yield
            yield False


#: Continuously retry until the server returns a successful response
NEVER = _NeverStop()

# Keep for backwards compatibility with v0.8.0
# TODO: Remove in v1.0.0
DISABLE = NEVER
