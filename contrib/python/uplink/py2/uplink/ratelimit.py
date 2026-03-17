# Standard library imports
import contextlib
import math
import threading
import time
import sys

# Local imports
from uplink import decorators, utils
from uplink.clients.io import RequestTemplate, transitions

__all__ = ["ratelimit", "RateLimitExceeded"]

# Use monotonic time if available, otherwise fall back to the system clock.
now = time.monotonic if hasattr(time, "monotonic") else time.time


def _get_host_and_port(base_url):
    parsed_url = utils.urlparse.urlparse(base_url)
    return parsed_url.hostname, parsed_url.port


class RateLimitExceeded(RuntimeError):
    """A request failed because it exceeded the client-side rate limit."""

    def __init__(self, calls, period):
        super(RateLimitExceeded, self).__init__(
            "Exceeded rate limit of [%s] calls every [%s] seconds."
            % (calls, period)
        )


class Limiter(object):
    _last_reset = _num_calls = None

    def __init__(self, max_calls, period, clock):
        self._max_calls = max_calls
        self._period = period
        self._clock = clock
        self._lock = threading.RLock()
        self._reset()

    @property
    def period_remaining(self):
        return self._period - (self._clock() - self._last_reset)

    @contextlib.contextmanager
    def check(self):
        with self._lock:
            if self.period_remaining <= 0:
                self._reset()
            yield self._max_calls > self._num_calls
            self._num_calls += 1

    def _reset(self):
        self._num_calls = 0
        self._last_reset = self._clock()


class RateLimiterTemplate(RequestTemplate):
    def __init__(self, limiter, create_limit_reached_exception):
        self._limiter = limiter
        self._create_limit_reached_exception = create_limit_reached_exception

    def before_request(self, request):
        with self._limiter.check() as ok:
            if ok:
                return  # Fallback to default behavior
            elif self._create_limit_reached_exception is not None:
                raise self._create_limit_reached_exception()
            else:
                return transitions.sleep(self._limiter.period_remaining)


# noinspection PyPep8Naming
class ratelimit(decorators.MethodAnnotation):
    """
    A decorator that constrains a consumer method or an entire
    consumer to making a specified maximum number of requests within a
    defined time period (e.g., 15 calls every 15 minutes).

    Note:
        The rate limit is enforced separately for each host-port
        combination. Logically, requests are grouped by host and port,
        and the number of requests within a time period are counted and
        capped separately for each group.

    By default, when the limit is reached, the client will wait until
    the current period is over before executing any subsequent
    requests. If you'd prefer the client to raise an exception when the
    limit is exceeded, set the ``raise_on_limit`` argument.

    Args:
        calls (int): The maximum number of allowed calls that the
            consumer can make within the time period.
        period (float): The duration of each time period in seconds.
        raise_on_limit (:class:`Exception` or bool, optional): Either an
            exception to raise when the client exceeds the rate limit or
            a :class:`bool`. If :obj:`True`, a
            :class:`~uplink.ratelimit.RateLimitExceeded` exception is
            raised.
    """

    BY_HOST_AND_PORT = _get_host_and_port

    def __init__(
        self,
        calls=15,
        period=900,
        raise_on_limit=False,
        group_by=BY_HOST_AND_PORT,
        clock=now,
    ):
        self._max_calls = max(1, min(sys.maxsize, math.floor(calls)))
        self._period = period
        self._clock = clock
        self._limiter_cache = {}
        self._group_by = utils.no_op if group_by is None else group_by

        if utils.is_subclass(raise_on_limit, Exception) or isinstance(
            raise_on_limit, Exception
        ):
            self._create_limit_reached_exception = raise_on_limit
        elif raise_on_limit:
            self._create_limit_reached_exception = (
                self._create_rate_limit_exceeded
            )
        else:
            self._create_limit_reached_exception = None

    def _get_limiter_for_request(self, request_builder):
        key = self._group_by(request_builder.base_url)
        try:
            return self._limiter_cache[key]
        except KeyError:
            return self._limiter_cache.setdefault(
                key, Limiter(self._max_calls, self._period, self._clock)
            )

    def modify_request(self, request_builder):
        limiter = self._get_limiter_for_request(request_builder)
        request_builder.add_request_template(
            RateLimiterTemplate(limiter, self._create_limit_reached_exception)
        )

    def _create_rate_limit_exceeded(self):
        return RateLimitExceeded(self._max_calls, self._period)
