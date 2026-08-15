"""Vendor-neutral client metrics interface, Noop backend and SDK helpers.

The SDK records metrics only after :func:`ydb.observability.enable_metrics` installs a
concrete :class:`MetricsProvider` (OpenTelemetry in :mod:`ydb.opentelemetry`, or any
custom one). Until then every helper is a cheap no-op, so metrics stay independent from
tracing and safe to call from hot paths — and the SDK never depends on ``opentelemetry``
being importable.

A provider handles three instrument kinds:

* ``record(name, value, attributes)`` — a value distribution (histogram);
* ``add(name, value, attributes)`` — an additive counter (may go negative, i.e. up/down);
* ``observe_gauge(name, callback)`` — an asynchronous gauge: the SDK owns the accumulated
  state (e.g. open sessions per pool) and the provider reads it via ``callback`` at
  collection time. This keeps the pool bookkeeping vendor-neutral and out of the backend.
"""

import time
import threading
import itertools
import functools
import inspect
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Tuple

from ydb.observability._endpoint import split_endpoint

CLIENT_OPERATION_DURATION = "db.client.operation.duration"
CLIENT_OPERATION_FAILED = "ydb.client.operation.failed"
QUERY_SESSION_COUNT = "ydb.query.session.count"
QUERY_SESSION_CREATE_TIME = "ydb.query.session.create_time"
QUERY_SESSION_PENDING_REQUESTS = "ydb.query.session.pending_requests"
QUERY_SESSION_TIMEOUTS = "ydb.query.session.timeouts"
QUERY_SESSION_MAX = "ydb.query.session.max"
QUERY_SESSION_MIN = "ydb.query.session.min"
RETRY_ATTEMPTS = "ydb.client.retry.attempts"
RETRY_DURATION = "ydb.client.retry.duration"

METRICS_SDK_BUILD_INFO = "ydb-sdk-metrics/0.1.0"

DURATION_BUCKETS_SECONDS = (
    0.001,
    0.005,
    0.01,
    0.05,
    0.1,
    0.5,
    1,
    5,
    10,
)
RETRY_DURATION_BUCKETS_SECONDS = (
    0.001,
    0.005,
    0.01,
    0.05,
    0.1,
    0.5,
    1,
    2,
    5,
    10,
    30,
)
ATTEMPT_BUCKETS = (1, 2, 3, 4, 5, 7, 10, 20)
_UNKNOWN_POOL = "unknown"
_pool_name_counter = itertools.count(1)
_pool_name_lock = threading.Lock()
_OPERATION_ATTR_KEYS = frozenset(
    {
        "database",
        "endpoint",
        "operation.name",
    }
)
_CLIENT_OPERATION_NAMES = frozenset(
    {
        "ExecuteQuery",
        "Commit",
        "Rollback",
        "CreateSession",
        "BeginTransaction",
    }
)
_CLIENT_OPERATION_NAME_BY_INPUT = {
    "ydb.ExecuteQuery": "ExecuteQuery",
    "ExecuteQuery": "ExecuteQuery",
    "ydb.Commit": "Commit",
    "Commit": "Commit",
    "ydb.Rollback": "Rollback",
    "Rollback": "Rollback",
    "ydb.CreateSession": "CreateSession",
    "CreateSession": "CreateSession",
    "ydb.BeginTransaction": "BeginTransaction",
    "BeginTransaction": "BeginTransaction",
}

# A gauge callback returns the current ``(value, attributes)`` observations.
GaugeCallback = Callable[[], Iterable[Tuple[float, Dict[str, Any]]]]


class MetricsProvider(Protocol):
    """Pluggable metrics backend.

    Implement this to wire the SDK into any metrics system. Three methods cover the
    three instrument kinds the SDK uses; see the module docstring for the semantics.
    """

    def record(self, name: str, value: float, attributes: Optional[Dict[str, Any]] = None) -> None: ...

    def add(self, name: str, value: int, attributes: Optional[Dict[str, Any]] = None) -> None: ...

    def observe_gauge(self, name: str, callback: GaugeCallback) -> None:
        """Register an asynchronous gauge whose current values *callback* returns."""
        ...


class NoopMetricsProvider:
    """Default backend used while metrics are disabled — drops everything."""

    def record(self, name, value, attributes=None):
        pass

    def add(self, name, value, attributes=None):
        pass

    def observe_gauge(self, name, callback):
        pass


_NOOP_PROVIDER = NoopMetricsProvider()
_provider: MetricsProvider = _NOOP_PROVIDER

# Accumulated state for the asynchronous gauges, owned by the SDK (vendor-neutral) and
# read by whatever provider is installed via ``observe_gauge``.
_gauge_lock = threading.Lock()
_session_count_state: Dict[Tuple, int] = {}
_session_max_state: Dict[Tuple, int] = {}


def is_metrics_enabled() -> bool:
    return _provider is not _NOOP_PROVIDER


def _observe_session_count() -> List[Tuple[float, Dict[str, Any]]]:
    with _gauge_lock:
        return [(value, dict(attrs)) for attrs, value in _session_count_state.items()]


def _observe_session_max() -> List[Tuple[float, Dict[str, Any]]]:
    with _gauge_lock:
        return [(value, dict(attrs)) for attrs, value in _session_max_state.items()]


def _observe_session_min() -> List[Tuple[float, Dict[str, Any]]]:
    # The SDK never configures a pool minimum, so this is always 0 for every known pool.
    with _gauge_lock:
        return [(0, dict(attrs)) for attrs in _session_max_state]


_OBSERVABLE_GAUGES = (
    (QUERY_SESSION_COUNT, _observe_session_count),
    (QUERY_SESSION_MAX, _observe_session_max),
    (QUERY_SESSION_MIN, _observe_session_min),
)


def _set_metrics_provider(provider: Optional[MetricsProvider]) -> None:
    global _provider

    _provider = provider if provider is not None else _NOOP_PROVIDER
    if _provider is not _NOOP_PROVIDER:
        for name, callback in _OBSERVABLE_GAUGES:
            _provider.observe_gauge(name, callback)


def _reset_metrics_provider() -> None:
    global _provider

    _provider = _NOOP_PROVIDER
    with _gauge_lock:
        _session_count_state.clear()
        _session_max_state.clear()


def is_metrics_operation_name(name: str) -> bool:
    return _operation_name(name) in _CLIENT_OPERATION_NAMES


def next_query_session_pool_name() -> str:
    """Return a process-unique default query session pool name for metric labels."""
    return "query-session-pool-%d" % next(_pool_name_counter)


def query_session_pool_name(
    name: Optional[str],
    endpoint: Optional[str] = None,
    database: Optional[str] = None,
) -> str:
    """Return a stable label for the ``ydb.query.session.pool.name`` metric attribute.

    If the user passed an explicit ``name`` to ``QuerySessionPool``, it wins. Otherwise
    the SDK builds a YDB connection string in the canonical ``<endpoint><database>``
    form (e.g. ``grpc://localhost:2136/local``) so that the pool is identifiable in
    dashboards without leaking driver-internal counters. When neither piece of the
    connection string is available, a process-unique counter name is used as a last
    resort.
    """
    if name:
        return name
    endpoint_part = endpoint or ""
    database_part = database or ""
    if database_part and not database_part.startswith("/"):
        database_part = "/" + database_part
    connection_string = endpoint_part + database_part
    if connection_string:
        return connection_string
    return next_query_session_pool_name()


def _metrics_build_info_tokens() -> List[str]:
    """Metrics' contribution to the ``x-ydb-sdk-build-info`` header.

    Returns ``["ydb-sdk-metrics/0.1.0"]`` once a metrics backend is installed,
    otherwise an empty list. Aggregated with other features by
    :func:`ydb.observability.sdk_build_info_tokens`.
    """
    return [METRICS_SDK_BUILD_INFO] if is_metrics_enabled() else []


def _pool_attrs(pool_name: Optional[str]) -> Dict[str, Any]:
    return {"ydb.query.session.pool.name": pool_name or _UNKNOWN_POOL}


def _build_ydb_metrics_attrs(driver_config) -> Dict[str, Any]:
    host, port = split_endpoint(getattr(driver_config, "endpoint", None))
    endpoint = "%s:%d" % (host, port) if port else host
    return {
        "database": getattr(driver_config, "database", None) or "",
        "endpoint": endpoint,
    }


def _operation_name(operation_name: str) -> str:
    return _CLIENT_OPERATION_NAME_BY_INPUT.get(operation_name, operation_name)


def _operation_attrs(operation_name: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
    name = _operation_name(operation_name)
    return {
        "database": attributes.get("database", ""),
        "endpoint": attributes.get("endpoint", ""),
        "operation.name": name,
    }


def _response_status_code(exception: BaseException) -> str:
    status = getattr(exception, "status", None)
    if status is not None:
        return getattr(status, "name", str(status))
    return type(exception).__qualname__


class MetricsOperation:
    """Metric lifecycle object for one user-visible YDB client operation.

    ``MetricsOperation`` mirrors the small span-like interface used by tracing
    so both can be composed by ``create_ydb_span``. It records operation
    duration once, records a failed-operation counter when an exception is
    attached, and accepts only stable operation labels.
    """

    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        self._name = name
        self._attributes = _operation_attrs(name, attributes or {})
        self._start_time = time.monotonic()
        self._exception: Optional[BaseException] = None
        self._ended = False
        self._end_lock = threading.Lock()

    def set_error(self, exception: BaseException) -> None:
        """Remember the operation exception for the failed-operation metric."""
        self._exception = exception

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a metric label only when it is part of the operation metric contract."""
        if key in _OPERATION_ATTR_KEYS:
            self._attributes[key] = value

    def attach_context(self, end_on_exit=True) -> "_MetricsOperationContext":
        return _MetricsOperationContext(self, end_on_exit)

    def end(self) -> None:
        with self._end_lock:
            if self._ended:
                return
            self._ended = True

        duration = time.monotonic() - self._start_time
        _provider.record(CLIENT_OPERATION_DURATION, duration, self._attributes)

        if self._exception is not None:
            attrs = dict(self._attributes)
            attrs["status_code"] = _response_status_code(self._exception)
            _provider.add(CLIENT_OPERATION_FAILED, 1, attrs)

    def __enter__(self) -> "MetricsOperation":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            self.set_error(exc_val)
        self.end()
        return False


class _NoopMetricsOperation:
    def set_error(self, exception: BaseException) -> None:
        pass

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def attach_context(self, end_on_exit=True) -> "_NoopMetricsOperationContext":
        return _NoopMetricsOperationContext(self)

    def end(self) -> None:
        pass

    def __enter__(self) -> "_NoopMetricsOperation":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _NoopMetricsOperationContext:
    def __init__(self, operation: _NoopMetricsOperation) -> None:
        self._operation = operation

    def __enter__(self) -> _NoopMetricsOperation:
        return self._operation

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _MetricsOperationContext:
    """Context manager that optionally leaves ``end()`` to a streaming result iterator."""

    def __init__(self, operation: MetricsOperation, end_on_exit: bool) -> None:
        self._operation = operation
        self._end_on_exit = end_on_exit

    def __enter__(self) -> MetricsOperation:
        return self._operation

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            self._operation.set_error(exc_val)
            self._operation.end()
        elif self._end_on_exit:
            self._operation.end()
        return False


_NOOP_METRICS_OPERATION = _NoopMetricsOperation()


def create_metrics_operation(name: str, attributes: Optional[Dict[str, Any]] = None):
    if _provider is _NOOP_PROVIDER or _operation_name(name) not in _CLIENT_OPERATION_NAMES:
        return _NOOP_METRICS_OPERATION
    return MetricsOperation(name, attributes)


def record_query_session_count(delta: int, pool_name: Optional[str] = None, state: str = "used") -> None:
    if not is_metrics_enabled():
        return
    attrs = _pool_attrs(pool_name)
    attrs["ydb.query.session.state"] = state
    key = tuple(sorted(attrs.items()))
    with _gauge_lock:
        _session_count_state[key] = _session_count_state.get(key, 0) + delta


def record_query_session_create_time(duration: float, pool_name: Optional[str]) -> None:
    if not is_metrics_enabled():
        return
    _provider.record(QUERY_SESSION_CREATE_TIME, duration, _pool_attrs(pool_name))


def record_query_session_pending_requests(delta: int, pool_name: Optional[str]) -> None:
    if not is_metrics_enabled():
        return
    _provider.add(QUERY_SESSION_PENDING_REQUESTS, delta, _pool_attrs(pool_name))


def record_query_session_timeout(pool_name: Optional[str]) -> None:
    if not is_metrics_enabled():
        return
    _provider.add(QUERY_SESSION_TIMEOUTS, 1, _pool_attrs(pool_name))


def record_query_session_max(value: int, pool_name: Optional[str]) -> None:
    if not is_metrics_enabled():
        return
    key = tuple(sorted(_pool_attrs(pool_name).items()))
    with _gauge_lock:
        _session_max_state[key] = value


def remove_query_session_pool_metrics(pool_name: Optional[str]) -> None:
    if not is_metrics_enabled():
        return
    base = list(_pool_attrs(pool_name).items())
    max_key = tuple(sorted(base))
    idle_key = tuple(sorted(base + [("ydb.query.session.state", "idle")]))
    used_key = tuple(sorted(base + [("ydb.query.session.state", "used")]))
    with _gauge_lock:
        _session_count_state.pop(idle_key, None)
        _session_count_state.pop(used_key, None)
        _session_max_state.pop(max_key, None)


def record_retry_metrics(duration: float, attempts: int) -> None:
    if not is_metrics_enabled():
        return
    _provider.record(RETRY_DURATION, duration)
    _provider.record(RETRY_ATTEMPTS, attempts)


class _NoopContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


_NOOP_CM = _NoopContext()


class SessionMetrics:
    """Per-session query-session-count bookkeeping, kept out of the session's own code.

    Every :class:`~ydb.query.session.BaseQuerySession` owns one. It counts the session
    as open exactly once and decrements the same bucket on close; the pool updates
    :attr:`state` and :attr:`pool_name` as the session moves between idle and used.
    """

    __slots__ = ("pool_name", "state", "_counted")

    def __init__(self) -> None:
        self.pool_name: Optional[str] = None
        self.state: str = "used"
        self._counted = False

    def count_open(self) -> None:
        if self._counted:
            return
        self._counted = True
        record_query_session_count(1, self.pool_name, self.state)

    def count_closed(self) -> None:
        if not self._counted:
            return
        self._counted = False
        record_query_session_count(-1, self.pool_name, self.state)


class _NoopSessionMetrics(SessionMetrics):
    """Class-level default so sessions built bypassing ``__init__`` (in tests) stay safe."""

    def count_open(self) -> None:
        pass

    def count_closed(self) -> None:
        pass


_NOOP_SESSION_METRICS = _NoopSessionMetrics()


class _CreateTimer:
    __slots__ = ("_pool_name", "_start")

    def __init__(self, pool_name: Optional[str]) -> None:
        self._pool_name = pool_name
        self._start = 0.0

    def __enter__(self):
        self._start = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        record_query_session_create_time(time.monotonic() - self._start, self._pool_name)
        return False


class _PendingTracker:
    __slots__ = ("_pool_name",)

    def __init__(self, pool_name: Optional[str]) -> None:
        self._pool_name = pool_name

    def __enter__(self):
        record_query_session_pending_requests(1, self._pool_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        record_query_session_pending_requests(-1, self._pool_name)
        return False


class QuerySessionPoolMetrics:
    """All metric bookkeeping for one query session pool, hidden from the pool code.

    The pool holds one instance and calls semantically named methods; timing and
    counters live here, and stay cheap no-ops while metrics are disabled.
    """

    def __init__(self, name: Optional[str], driver, size: int) -> None:
        driver_config = getattr(driver, "_driver_config", None)
        self._pool_name = query_session_pool_name(
            name,
            endpoint=getattr(driver_config, "endpoint", None),
            database=getattr(driver_config, "database", None),
        )
        record_query_session_max(size, self._pool_name)

    def attach(self, session) -> None:
        """Bind this pool's label to a freshly created session."""
        session._session_metrics.pool_name = self._pool_name
        session._session_metrics.state = "used"

    def measure_create(self):
        """Context manager recording query session creation time (no-op when disabled)."""
        return _CreateTimer(self._pool_name) if is_metrics_enabled() else _NOOP_CM

    def track_pending(self):
        """Context manager counting a request waiting for a session (no-op when disabled)."""
        return _PendingTracker(self._pool_name) if is_metrics_enabled() else _NOOP_CM

    def on_timeout(self) -> None:
        record_query_session_timeout(self._pool_name)

    def on_acquired(self, session) -> None:
        self._transition(session, "used")

    def on_released(self, session) -> None:
        self._transition(session, "idle")

    def _transition(self, session, new_state: str) -> None:
        session_metrics = session._session_metrics
        record_query_session_count(-1, self._pool_name, session_metrics.state)
        record_query_session_count(1, self._pool_name, new_state)
        session_metrics.state = new_state

    def close(self) -> None:
        remove_query_session_pool_metrics(self._pool_name)


class _RetryMetrics:
    __slots__ = ("_start", "_attempts")

    def __init__(self) -> None:
        self._start = time.monotonic()
        self._attempts = 0

    def count(self, callee: Callable) -> Callable:
        """Wrap *callee* so each invocation counts as one retry attempt."""
        if inspect.iscoroutinefunction(callee):

            @functools.wraps(callee)
            async def acounted(*args, **kwargs):
                self._attempts += 1
                return await callee(*args, **kwargs)

            return acounted

        @functools.wraps(callee)
        def counted(*args, **kwargs):
            self._attempts += 1
            return callee(*args, **kwargs)

        return counted

    def finish(self) -> None:
        record_retry_metrics(time.monotonic() - self._start, self._attempts)


def observe_retry_metrics(retry_func: Callable) -> Callable:
    """Decorator recording retry duration and attempt count around a retry helper.

    Wraps the retried callee to count attempts and times the whole operation — but only
    while metrics are enabled, so the retry hot path is otherwise left untouched (no
    ``time.monotonic`` calls, no wrappers).
    """
    if inspect.iscoroutinefunction(retry_func):

        @functools.wraps(retry_func)
        async def awrapper(callee, retry_settings=None, *args, **kwargs):
            if not is_metrics_enabled():
                return await retry_func(callee, retry_settings, *args, **kwargs)
            metrics = _RetryMetrics()
            try:
                return await retry_func(metrics.count(callee), retry_settings, *args, **kwargs)
            finally:
                metrics.finish()

        return awrapper

    @functools.wraps(retry_func)
    def wrapper(callee, retry_settings=None, *args, **kwargs):
        if not is_metrics_enabled():
            return retry_func(callee, retry_settings, *args, **kwargs)
        metrics = _RetryMetrics()
        try:
            return retry_func(metrics.count(callee), retry_settings, *args, **kwargs)
        finally:
            metrics.finish()

    return wrapper
