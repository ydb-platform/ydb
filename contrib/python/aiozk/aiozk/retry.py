import asyncio
import collections
import logging
import time

from aiozk import exc


log = logging.getLogger(__name__)


class RetryPolicy:
    """
    The class of Retry policy enforcer for Zookeper calls.

    Class methods of this class implement several different retry policies.

    Custom implementation can be provided using constructor directly
    with following parameters:

    :param int try_limit: Retry attempts limit

    :param func sleep_func: function that calculates sleep time. Accepts
                            one parameter, list of timestamps of each attempt
    """

    def __init__(self, try_limit, sleep_func):
        self.try_limit = try_limit
        self.sleep_func = sleep_func

        self.timings = collections.defaultdict(list)

    async def enforce(self, request=None):
        """
        Execute policy, perform sleep according to *sleep_func* result

        :param request: Request object
        :raises aiozk.exc.FailedRetry: Raised if try limit is exceeded
                                       or wait time returned by *sleep_func*
                                       is below 0
        :return:
        """
        self.timings[id(request)].append(time.time())

        tries = len(self.timings[id(request)])
        if tries == 1:
            return

        if self.try_limit is not None and tries >= self.try_limit:
            raise exc.FailedRetry

        wait_time = self.sleep_func(self.timings[id(request)])
        if wait_time is None or wait_time == 0:
            return
        elif wait_time < 0:
            raise exc.FailedRetry

        log.debug('Waiting %d seconds until next try.', wait_time)
        await asyncio.sleep(wait_time)

    def clear(self, request):
        """
        Dereference timings for request

        :param request: Request object
        """
        self.timings.pop(id(request), None)

    @classmethod
    def once(cls):
        """
        One retry policy

        :return: Rolicy with one retry

        :rtype: aiozk.RetryPolicy
        """
        return cls.n_times(1)

    @classmethod
    def n_times(cls, n):
        """
        *n* times retry policy, **no delay between retries**

        :param n: retries limit

        :return: Policy with *n* retries
        :rtype: aiozk.RetryPolicy
        """

        def never_wait(_):
            return None

        return cls(try_limit=n, sleep_func=never_wait)

    @classmethod
    def forever(cls):
        """
        Forever retry policy, **no delay between retries**

        :return: Retry forever policy
        :rtype: aiozk.RetryPolicy
        """

        def never_wait(_):
            return None

        return cls(try_limit=None, sleep_func=never_wait)

    @classmethod
    def exponential_backoff(cls, base=2, maximum=None):
        """
        Exponential backoff retry policy.

        :param base: base of exponentiation
        :param maximum: optional timeout in seconds

        :return: Exponential backoff policy
        :rtype: aiozk.RetryPolicy
        """

        def exponential(timings):
            wait_time = base ** len(timings)
            if maximum is not None:
                wait_time = min(maximum, wait_time)

            return wait_time

        return cls(try_limit=None, sleep_func=exponential)

    @classmethod
    def until_elapsed(cls, timeout):
        """
        Retry until *timeout* elapsed policy

        :param timeout: retry time delay

        :return: Retry until elapsed policy.
        :rtype: aiozk.RetryPolicy
        """

        def elapsed_time(timings):
            if timings:
                first_timing = timings[0]
            else:
                first_timing = time.time()

            return (first_timing + timeout) - time.time()

        return cls(try_limit=None, sleep_func=elapsed_time)
