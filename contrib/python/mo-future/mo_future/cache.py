# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from collections import namedtuple
from functools import update_wrapper
from types import FunctionType

from mo_dots import Null
from mo_future import get_function_arguments, get_function_name, get_function_defaults
from mo_logs import Log
from mo_logs.exceptions import Except
from mo_math import randoms
from mo_threads import Lock
from mo_times.dates import Date
from mo_times.durations import DAY


class cache:

    """
    :param func: ASSUME FIRST PARAMETER OF `func` IS `self`
    :param duration: USE CACHE IF LAST CALL WAS LESS THAN duration AGO
    :param lock: True if you want multithreaded monitor (default False)
    :param ignore: Parameters to ignore while caching
    :return:
    """

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], FunctionType):
            func = args[0]
            return wrap_function(_SimpleCache(), func)
        else:
            return object.__new__(cls)

    def __init__(self, duration=DAY, lock=False, ignore=None):
        self.timeout = duration
        self.ignore = ignore
        if lock:
            self.locker = Lock()
        else:
            self.locker = _FakeLock()

    def __call__(self, func):
        return wrap_function(self, func)


class _SimpleCache:
    def __init__(self):
        self.timeout = Null
        self.locker = _FakeLock()


def wrap_function(cache_store, func_):
    attr_name = "_cache_for_" + func_.__name__

    func_name = get_function_name(func_)
    params = get_function_arguments(func_)
    if not get_function_defaults(func_):
        defaults = {}
    else:
        defaults = {k: v for k, v in zip(reversed(params), reversed(get_function_defaults(func_)))}

    func_args = get_function_arguments(func_)
    if len(func_args) > 0 and func_args[0] == "self":
        using_self = True
        func = lambda self, *args: func_(self, *args)
    else:
        using_self = False
        func = lambda self, *args: func_(*args)

    def output(*args, **kwargs):
        if kwargs:
            Log.error("Sorry, caching only works with ordered parameter, not keyword arguments")

        with cache_store.locker:
            if using_self:
                self = args[0]
                args = args[1:]
            else:
                self = cache_store

            now = Date.now()
            try:
                _cache = getattr(self, attr_name)
            except Exception:
                _cache = {}
                setattr(self, attr_name, _cache)

            if randoms.int(100) == 0:
                # REMOVE OLD CACHE
                _cache = {k: v for k, v in _cache.items() if v.timeout == None or v.timeout > now}
                setattr(self, attr_name, _cache)

            timeout, key, value, exception = _cache.get(args, (Null, Null, Null, Null))

        if now >= timeout:
            value = func(self, *args)
            with cache_store.locker:
                _cache[args] = CacheElement(now + cache_store.timeout, args, value, None)
            return value

        if value == None:
            if exception == None:
                try:
                    value = func(self, *args)
                    with cache_store.locker:
                        _cache[args] = CacheElement(now + cache_store.timeout, args, value, None)
                    return value
                except Exception as e:
                    e = Except.wrap(e)
                    with cache_store.locker:
                        _cache[args] = CacheElement(now + cache_store.timeout, args, None, e)
                    raise e
            else:
                raise exception
        else:
            return value

    return update_wrapper(output, func)


CacheElement = namedtuple("CacheElement", ("timeout", "key", "value", "exception"))


class _FakeLock:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
