# -*- coding: utf-8 -*-
import importlib.util
import threading
import codecs
from concurrent import futures
import functools
import hashlib
import collections
import urllib.parse
from . import ydb_version

try:
    from . import interceptor
except ImportError:
    interceptor = None


_grpcs_protocol = "grpcs://"
_grpc_protocol = "grpc://"


def wrap_result_in_future(result):
    f = futures.Future()
    f.set_result(result)
    return f


def wrap_exception_in_future(exc):
    f = futures.Future()
    f.set_exception(exc)
    return f


def future():
    return futures.Future()


def x_ydb_sdk_build_info_header():
    return ("x-ydb-sdk-build-info", "ydb-python-sdk/" + ydb_version.VERSION)


def is_secure_protocol(endpoint):
    return endpoint.startswith("grpcs://")


def wrap_endpoint(endpoint):
    if endpoint.startswith(_grpcs_protocol):
        return endpoint[len(_grpcs_protocol) :]
    if endpoint.startswith(_grpc_protocol):
        return endpoint[len(_grpc_protocol) :]
    return endpoint


def parse_connection_string(connection_string):
    cs = connection_string
    if not cs.startswith(_grpc_protocol) and not cs.startswith(_grpcs_protocol):
        # default is grpcs
        cs = _grpcs_protocol + cs

    p = urllib.parse.urlparse(connection_string)
    b = urllib.parse.parse_qs(p.query)
    database = b.get("database", [])
    assert len(database) > 0

    return p.scheme + "://" + p.netloc, database[0]


# Decorator that ensures no exceptions are leaked from decorated async call
def wrap_async_call_exceptions(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            return wrap_exception_in_future(e)

    return decorator


def check_module_exists(path: str) -> bool:
    try:
        if importlib.util.find_spec(path):
            return True
    except ModuleNotFoundError:
        pass
    return False


def get_query_hash(yql_text):
    try:
        return hashlib.sha256(str(yql_text, "utf-8").encode("utf-8")).hexdigest()
    except TypeError:
        return hashlib.sha256(str(yql_text).encode("utf-8")).hexdigest()


class LRUCache(object):
    def __init__(self, capacity=1000):
        self.items = collections.OrderedDict()
        self.capacity = capacity

    def put(self, key, value):
        self.items[key] = value
        while len(self.items) > self.capacity:
            self.items.popitem(last=False)

    def get(self, key, _default):
        if key not in self.items:
            return _default
        value = self.items.pop(key)
        self.items[key] = value
        return value

    def erase(self, key):
        self.items.pop(key)


def from_bytes(val):
    """
    Translates value into valid utf8 string
    :param val: A value to translate
    :return: A valid utf8 string
    """
    try:
        return codecs.decode(val, "utf8")
    except (UnicodeEncodeError, TypeError):
        return val


class AsyncResponseIterator(object):
    def __init__(self, it, wrapper):
        self.it = it
        self.wrapper = wrapper

    def cancel(self):
        self.it.cancel()
        return self

    def __iter__(self):
        return self

    def _next(self):
        return interceptor.operate_async_stream_call(self.it, self.wrapper)

    def next(self):
        return self._next()

    def __next__(self):
        return self._next()


class SyncResponseIterator(object):
    def __init__(self, it, wrapper):
        self.it = it
        self.wrapper = wrapper

    def cancel(self):
        self.it.cancel()
        return self

    def __iter__(self):
        return self

    def _next(self):
        return self.wrapper(next(self.it))

    def next(self):
        return self._next()

    def __next__(self):
        return self._next()


class AtomicCounter:
    _lock: threading.Lock
    _value: int

    def __init__(self, initial_value: int = 0):
        self._lock = threading.Lock()
        self._value = initial_value

    def inc_and_get(self) -> int:
        with self._lock:
            self._value += 1
            return self._value
