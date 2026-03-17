import functools
from inspect import iscoroutinefunction
from time import perf_counter as time_now


def safe_wraps(wrapper, *args, **kwargs):
    """Safely wraps partial functions."""
    while isinstance(wrapper, functools.partial):
        wrapper = wrapper.func
    return functools.wraps(wrapper, *args, **kwargs)


class Timer(object):
    """A context manager/decorator for statsd.timing()."""

    def __init__(self, client, stat, rate=1):
        self.client = client
        self.stat = stat
        self.rate = rate
        self.ms = None
        self._sent = False
        self._start_time = None

    def __call__(self, f):
        """Thread-safe timing function decorator."""
        if iscoroutinefunction(f):
            @safe_wraps(f)
            async def _async_wrapped(*args, **kwargs):
                start_time = time_now()
                try:
                    return await f(*args, **kwargs)
                finally:
                    elapsed_time_ms = 1000.0 * (time_now() - start_time)
                    self.client.timing(self.stat, elapsed_time_ms, self.rate)
            return _async_wrapped

        @safe_wraps(f)
        def _wrapped(*args, **kwargs):
            start_time = time_now()
            try:
                return f(*args, **kwargs)
            finally:
                elapsed_time_ms = 1000.0 * (time_now() - start_time)
                self.client.timing(self.stat, elapsed_time_ms, self.rate)
        return _wrapped

    def __enter__(self):
        return self.start()

    def __exit__(self, typ, value, tb):
        self.stop()

    def start(self):
        self.ms = None
        self._sent = False
        self._start_time = time_now()
        return self

    def stop(self, send=True):
        if self._start_time is None:
            raise RuntimeError('Timer has not started.')
        dt = time_now() - self._start_time
        self.ms = 1000.0 * dt  # Convert to milliseconds.
        if send:
            self.send()
        return self

    def send(self):
        if self.ms is None:
            raise RuntimeError('No data recorded.')
        if self._sent:
            raise RuntimeError('Already sent data.')
        self._sent = True
        self.client.timing(self.stat, self.ms, self.rate)
