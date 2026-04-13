# -*- coding: utf-8 -*-
import atexit
import concurrent.futures
import importlib.util
import threading
import codecs
from concurrent import futures
import functools
import hashlib
import collections
import socket
import sys
import logging
import random
import time
import urllib.parse
from typing import Dict, List, Optional, TYPE_CHECKING
from . import ydb_version

import typing

if TYPE_CHECKING:
    from . import resolver

interceptor: typing.Any
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


def x_ydb_sdk_build_info_header(additional_sdk_headers):
    sdk_header_list = ["ydb-python-sdk/" + ydb_version.VERSION]
    sdk_header_list.extend(additional_sdk_headers)
    return ("x-ydb-sdk-build-info", ";".join(sdk_header_list))


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
        res = self.wrapper(next(self.it))
        if res is not None:
            return res
        return self._next()

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


def get_first_message_with_timeout(status_stream: SyncResponseIterator, timeout: int):
    waiter = future()

    def get_first_response(waiter):
        first_response = next(status_stream)
        waiter.set_result(first_response)

    thread = threading.Thread(
        target=get_first_response,
        args=(waiter,),
        name="first response attach stream thread",
        daemon=True,
    )
    thread.start()

    return waiter.result(timeout=timeout)


# ============================================================================
# Nearest DC detection utilities
# ============================================================================

logger = logging.getLogger(__name__)

# Module-level thread pool for TCP race (reused across discovery cycles)
_TCP_RACE_MAX_WORKERS = 30
_TCP_RACE_EXECUTOR: Optional[concurrent.futures.ThreadPoolExecutor] = None
_EXECUTOR_LOCK = threading.Lock()
_ATEXIT_REGISTERED = False


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    """
    Lazily create and return the thread pool executor.

    The executor is created on first use to avoid import-time side effects.
    The atexit hook is registered only when the executor is actually created.
    """
    global _TCP_RACE_EXECUTOR, _ATEXIT_REGISTERED

    if _TCP_RACE_EXECUTOR is None:
        with _EXECUTOR_LOCK:
            if _TCP_RACE_EXECUTOR is None:
                _TCP_RACE_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
                    max_workers=_TCP_RACE_MAX_WORKERS,
                    thread_name_prefix="ydb-tcp-race",
                )

                if not _ATEXIT_REGISTERED:
                    atexit.register(_shutdown_executor)
                    _ATEXIT_REGISTERED = True

    return _TCP_RACE_EXECUTOR


def _shutdown_executor():
    """Shutdown the executor if it was created."""
    if _TCP_RACE_EXECUTOR is not None:
        if sys.version_info >= (3, 9):
            _TCP_RACE_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        else:
            _TCP_RACE_EXECUTOR.shutdown(wait=False)


def _check_fastest_endpoint(
    endpoints: List["resolver.EndpointInfo"], timeout: float = 5.0
) -> Optional["resolver.EndpointInfo"]:
    """
    Perform TCP race using a bounded thread pool and return the fastest endpoint.

    Uses a module-level ThreadPoolExecutor to avoid creating new threads on every
    discovery cycle. Returns immediately when the first endpoint connects successfully.

    If there are more endpoints than the thread pool size, takes one random endpoint
    per location to ensure fair representation of all locations in the race. If there
    are still too many locations, randomly samples them to stay within the limit.

    :param endpoints: List of resolver.EndpointInfo objects
    :param timeout: Maximum time to wait for any connection (seconds)
    :return: Fastest endpoint that connected successfully, or None if all failed
    """
    if not endpoints:
        return None

    if len(endpoints) > _TCP_RACE_MAX_WORKERS:
        endpoints_by_location = _split_endpoints_by_location(endpoints)
        endpoints = [random.choice(location_eps) for location_eps in endpoints_by_location.values()]

        if len(endpoints) > _TCP_RACE_MAX_WORKERS:
            endpoints = random.sample(endpoints, _TCP_RACE_MAX_WORKERS)

    stop_event = threading.Event()
    winner_lock = threading.Lock()
    deadline = time.monotonic() + timeout

    def try_connect(endpoint: "resolver.EndpointInfo") -> Optional["resolver.EndpointInfo"]:
        """Try to connect to endpoint and return it if successful."""
        remaining = deadline - time.monotonic()
        if remaining <= 0 or stop_event.is_set():
            return None

        if endpoint.ipv6_addrs:
            target_host = endpoint.ipv6_addrs[0]
        elif endpoint.ipv4_addrs:
            target_host = endpoint.ipv4_addrs[0]
        else:
            target_host = endpoint.address

        try:
            sock = socket.create_connection((target_host, endpoint.port), timeout=remaining)
            try:
                with winner_lock:
                    if stop_event.is_set():
                        return None
                    stop_event.set()
                    return endpoint
            finally:
                sock.close()
        except (OSError, socket.timeout):
            # Ignore expected connection errors; endpoints that fail simply lose the TCP race.
            return None
        except Exception as e:
            logger.debug("Unexpected error connecting to %s: %s", endpoint.endpoint, e)
            return None

    executor = _get_executor()
    futures_list: List[concurrent.futures.Future] = [executor.submit(try_connect, ep) for ep in endpoints]

    try:
        for fut in concurrent.futures.as_completed(futures_list, timeout=timeout):
            result = fut.result()
            if result is not None:
                return result
    except concurrent.futures.TimeoutError:
        # Overall timeout expired
        pass
    finally:
        for f in futures_list:
            f.cancel()

    return None


def _split_endpoints_by_location(endpoints: List["resolver.EndpointInfo"]) -> Dict[str, List["resolver.EndpointInfo"]]:
    """
    Group endpoints by their location.

    :param endpoints: List of resolver.EndpointInfo objects
    :return: Dictionary mapping location -> list of resolver.EndpointInfo
    """
    result: Dict[str, List["resolver.EndpointInfo"]] = {}
    for endpoint in endpoints:
        location = endpoint.location
        if location not in result:
            result[location] = []
        result[location].append(endpoint)
    return result


def _get_random_endpoints(endpoints: List["resolver.EndpointInfo"], count: int) -> List["resolver.EndpointInfo"]:
    """
    Get random sample of endpoints.

    :param endpoints: List of resolver.EndpointInfo objects
    :param count: Maximum number of endpoints to return
    :return: Random sample of resolver.EndpointInfo
    """
    if len(endpoints) <= count:
        return endpoints
    return random.sample(endpoints, count)


def detect_local_dc(
    endpoints: List["resolver.EndpointInfo"], max_per_location: int = 3, timeout: float = 5.0
) -> Optional[str]:
    """
    Detect nearest datacenter by performing TCP race between endpoints.

    This function groups endpoints by location, selects random samples from each location,
    and performs parallel TCP connections to find the fastest one. The location of the
    fastest endpoint is considered the nearest datacenter.

    Algorithm:
    1. Group endpoints by location
    2. If only one location exists, return it immediately
    3. Select up to max_per_location random endpoints from each location
    4. Perform TCP race: connect to all selected endpoints simultaneously
    5. Return the location of the first endpoint that connects successfully
    6. If all connections fail, return None

    :param endpoints: List of resolver.EndpointInfo objects from discovery
    :param max_per_location: Maximum number of endpoints to test per location (default: 3, must be >= 1)
    :param timeout: TCP connection timeout in seconds (default: 5.0, must be > 0)
    :return: Location string of the nearest datacenter, or None if detection failed
    :raises ValueError: If endpoints list is empty, max_per_location < 1, or timeout <= 0
    """
    if not endpoints:
        raise ValueError("Empty endpoints list for local DC detection")
    if max_per_location < 1:
        raise ValueError(f"max_per_location must be >= 1, got {max_per_location}")
    if timeout <= 0:
        raise ValueError(f"timeout must be > 0, got {timeout}")

    endpoints_by_location = _split_endpoints_by_location(endpoints)

    logger.debug(
        "Detecting local DC from %d endpoints across %d locations",
        len(endpoints),
        len(endpoints_by_location),
    )

    if len(endpoints_by_location) == 1:
        location = list(endpoints_by_location.keys())[0]
        logger.debug("Only one location found: %s", location)
        return location

    endpoints_to_test = []
    for location, location_endpoints in endpoints_by_location.items():
        sample = _get_random_endpoints(location_endpoints, max_per_location)
        endpoints_to_test.extend(sample)
        logger.debug(
            "Selected %d/%d endpoints from location '%s' for testing",
            len(sample),
            len(location_endpoints),
            location,
        )

    fastest_endpoint = _check_fastest_endpoint(endpoints_to_test, timeout=timeout)

    if fastest_endpoint is None:
        logger.debug("Failed to detect local DC via TCP race: no endpoint connected in time")
        return None

    detected_location = fastest_endpoint.location
    logger.debug("Detected local DC: %s", detected_location)

    return detected_location
