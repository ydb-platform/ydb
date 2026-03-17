from datetime import timedelta
import time
import inspect
from collections import deque
from bisect import bisect

from .decorators import wraps


__all__ = ['memoize', 'make_lookuper', 'silent_lookuper', 'cache']



class SkipMemory(Exception):
    pass


# TODO: use pos-only arg once in Python 3.8+ only
def memoize(_func=None, *, key_func=None):
    """@memoize(key_func=None). Makes decorated function memoize its results.

    If key_func is specified uses key_func(*func_args, **func_kwargs) as memory key.
    Otherwise uses args + tuple(sorted(kwargs.items()))

    Exposes its memory via .memory attribute.
    """
    if _func is not None:
        return memoize()(_func)
    return _memory_decorator({}, key_func)

memoize.skip = SkipMemory


def cache(timeout, *, key_func=None):
    """Caches a function results for timeout seconds."""
    if isinstance(timeout, timedelta):
        timeout = timeout.total_seconds()

    return _memory_decorator(CacheMemory(timeout), key_func)

cache.skip = SkipMemory


def _memory_decorator(memory, key_func):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # We inline this here since @memoize also targets microoptimizations
            key = key_func(*args, **kwargs) if key_func else \
                  args + tuple(sorted(kwargs.items())) if kwargs else args
            try:
                return memory[key]
            except KeyError:
                try:
                    value = memory[key] = func(*args, **kwargs)
                    return value
                except SkipMemory as e:
                    return e.args[0] if e.args else None

        def invalidate(*args, **kwargs):
            key = key_func(*args, **kwargs) if key_func else \
                  args + tuple(sorted(kwargs.items())) if kwargs else args
            memory.pop(key, None)
        wrapper.invalidate = invalidate

        def invalidate_all():
            memory.clear()
        wrapper.invalidate_all = invalidate_all

        wrapper.memory = memory
        return wrapper
    return decorator

class CacheMemory(dict):
    def __init__(self, timeout):
        self.timeout = timeout
        self.clear()

    def __setitem__(self, key, value):
        expires_at = time.time() + self.timeout
        dict.__setitem__(self, key, (value, expires_at))
        self._keys.append(key)
        self._expires.append(expires_at)

    def __getitem__(self, key):
        value, expires_at = dict.__getitem__(self, key)
        if expires_at <= time.time():
            self.expire()
            raise KeyError(key)
        return value

    def expire(self):
        i = bisect(self._expires, time.time())
        for _ in range(i):
            self._expires.popleft()
            self.pop(self._keys.popleft(), None)

    def clear(self):
        dict.clear(self)
        self._keys = deque()
        self._expires = deque()


def _make_lookuper(silent):
    def make_lookuper(func):
        """
        Creates a single argument function looking up result in a memory.

        Decorated function is called once on first lookup and should return all available
        arg-value pairs.

        Resulting function will raise LookupError when using @make_lookuper
        or simply return None when using @silent_lookuper.
        """
        has_args, has_keys = has_arg_types(func)
        assert not has_keys, \
            'Lookup table building function should not have keyword arguments'

        if has_args:
            @memoize
            def wrapper(*args):
                f = lambda: func(*args)
                f.__name__ = '%s(%s)' % (func.__name__, ', '.join(map(str, args)))
                return make_lookuper(f)
        else:
            memory = {}

            def wrapper(arg):
                if not memory:
                    memory[object()] = None # prevent continuos memory refilling
                    memory.update(func())

                if silent:
                    return memory.get(arg)
                elif arg in memory:
                    return memory[arg]
                else:
                    raise LookupError("Failed to look up %s(%s)" % (func.__name__, arg))

        return wraps(func)(wrapper)
    return make_lookuper

make_lookuper = _make_lookuper(False)
silent_lookuper = _make_lookuper(True)
silent_lookuper.__name__ = 'silent_lookuper'


def has_arg_types(func):
    params = inspect.signature(func).parameters.values()
    return any(p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL)
               for p in params), \
           any(p.kind in (p.KEYWORD_ONLY, p.VAR_KEYWORD) for p in params)
