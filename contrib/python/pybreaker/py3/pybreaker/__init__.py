"""Threadsafe pure-Python implementation of the Circuit Breaker pattern, described
by Michael T. Nygard in his book 'Release It!'.

For more information on this and other patterns and best practices, buy the
book at https://pragprog.com/titles/mnee2/release-it-second-edition/
"""

from __future__ import annotations

import calendar
import contextlib
import logging
import sys
import threading
import time
import types
from abc import abstractmethod
from datetime import datetime, timedelta
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    NoReturn,
    TypeVar,
    Union,
    cast,
    overload,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Sequence

# For compatibility with Python 3.10 and earlier.
# Otherwise, `from datetime import UTC` would suffice.
try:
    from datetime import UTC  # type: ignore[attr-defined]
except ImportError:
    from datetime import timezone

    UTC = timezone.utc

try:
    from tornado import gen

    HAS_TORNADO_SUPPORT = True
except ImportError:
    HAS_TORNADO_SUPPORT = False

try:
    from redis import Redis
    from redis.client import Pipeline
    from redis.exceptions import RedisError

    HAS_REDIS_SUPPORT = True
except ImportError:
    HAS_REDIS_SUPPORT = False

__all__ = (
    "CircuitBreaker",
    "CircuitBreakerListener",
    "CircuitBreakerError",
    "CircuitMemoryStorage",
    "CircuitRedisStorage",
    "STATE_OPEN",
    "STATE_CLOSED",
    "STATE_HALF_OPEN",
)

STATE_OPEN = "open"
STATE_CLOSED = "closed"
STATE_HALF_OPEN = "half-open"

T = TypeVar("T")
ExceptionType = TypeVar("ExceptionType", bound=BaseException)
CBListenerType = TypeVar("CBListenerType", bound="CircuitBreakerListener")
CBStateType = Union["CircuitClosedState", "CircuitHalfOpenState", "CircuitOpenState"]


class CircuitBreaker:
    """More abstractly, circuit breakers exists to allow one subsystem to fail
    without destroying the entire system.

    This is done by wrapping dangerous operations (typically integration points)
    with a component that can circumvent calls when the system is not healthy.

    This pattern is described by Michael T. Nygard in his book 'Release It!'.
    """

    def __init__(
        self,
        fail_max: int = 5,
        reset_timeout: float = 60,
        success_threshold: int = 1,
        exclude: Iterable[type[ExceptionType] | Callable[[Any], bool]] | None = None,
        listeners: Sequence[CBListenerType] | None = None,
        state_storage: CircuitBreakerStorage | None = None,
        name: str | None = None,
        throw_new_error_on_trip: bool = True,
    ) -> None:
        """Create a new circuit breaker with the given parameters."""
        self._lock = threading.RLock()
        self._state_storage = state_storage or CircuitMemoryStorage(STATE_CLOSED)
        self._state = self._create_new_state(self.current_state)

        self._fail_max = fail_max
        self._reset_timeout = reset_timeout
        self._success_threshold = success_threshold

        self._excluded_exceptions = list(exclude or [])
        self._listeners = list(listeners or [])
        self._name = name

        self._throw_new_error_on_trip = throw_new_error_on_trip

    @property
    def fail_counter(self) -> int:
        """Return the current number of consecutive failures."""
        return self._state_storage.counter

    @property
    def success_counter(self) -> int:
        """Return the current number of consecutive successes in half-open state."""
        return self._state_storage.success_counter

    @property
    def fail_max(self) -> int:
        """Return the maximum number of failures tolerated before the circuit is opened."""
        return self._fail_max

    @fail_max.setter
    def fail_max(self, number: int) -> None:
        """Set the maximum `number` of failures tolerated before the circuit is opened."""
        self._fail_max = number

    @property
    def reset_timeout(self) -> float:
        """Once this circuit breaker is opened, it should remain opened until the
        timeout period, in seconds, elapses.
        """
        return self._reset_timeout

    @reset_timeout.setter
    def reset_timeout(self, timeout: float) -> None:
        """Set the `timeout` period, in seconds, this circuit breaker should be kept open."""
        self._reset_timeout = timeout

    @property
    def success_threshold(self) -> int:
        """Return the number of successful requests required before transitioning from half-open to closed state."""
        return self._success_threshold

    @success_threshold.setter
    def success_threshold(self, threshold: int) -> None:
        """Set the number of successful requests required before transitioning from half-open to closed state."""
        self._success_threshold = threshold

    def _create_new_state(
        self,
        new_state: str,
        prev_state: CircuitBreakerState | None = None,
        notify: bool = False,
    ) -> CBStateType:
        """Return state object from state string, i.e., 'closed' -> <CircuitClosedState>."""
        state_map: dict[str, type[CBStateType]] = {
            STATE_CLOSED: CircuitClosedState,
            STATE_OPEN: CircuitOpenState,
            STATE_HALF_OPEN: CircuitHalfOpenState,
        }
        try:
            cls = state_map[new_state]
            return cls(self, prev_state=prev_state, notify=notify)
        except KeyError as e:
            msg = "Unknown state {!r}, valid states: {}"
            raise ValueError(msg.format(new_state, ", ".join(state_map))) from e

    @property
    def state(self) -> CBStateType:
        """Update (if needed) and returns the cached state object."""
        # Ensure cached state is up-to-date
        if self.current_state != self._state.name:
            # If cached state is out-of-date, that means that it was likely
            # changed elsewhere (e.g. another process instance). We still send
            # out a notification, informing others that this particular circuit
            # breaker instance noticed the changed circuit.
            self.state = self.current_state  # type: ignore[assignment]
        return self._state

    @state.setter
    def state(self, state_str: str) -> None:
        """Set cached state and notify listeners of newly cached state."""
        with self._lock:
            self._state = self._create_new_state(state_str, prev_state=self._state, notify=True)

    @property
    def current_state(self) -> str:
        """Return a string that identifies the state of the circuit breaker as
        reported by the _state_storage. i.e., 'closed', 'open', 'half-open'.
        """
        return self._state_storage.state

    @property
    def excluded_exceptions(
        self,
    ) -> tuple[type[ExceptionType] | Callable[[Any], bool], ...]:
        """Return the list of excluded exceptions, e.g., exceptions that should
        not be considered system errors by this circuit breaker.
        """
        return tuple(self._excluded_exceptions)

    def add_excluded_exception(self, exception: type[ExceptionType]) -> None:
        """Add an exception to the list of excluded exceptions."""
        with self._lock:
            self._excluded_exceptions.append(exception)

    def add_excluded_exceptions(self, *exceptions: type[ExceptionType]) -> None:
        """Add exceptions to the list of excluded exceptions."""
        for exc in exceptions:
            self.add_excluded_exception(exc)

    def remove_excluded_exception(self, exception: type[ExceptionType]) -> None:
        """Remove an exception from the list of excluded exceptions."""
        with self._lock:
            self._excluded_exceptions.remove(exception)

    def _inc_counter(self) -> None:
        """Increment the counter of failed calls."""
        self._state_storage.increment_counter()

    def is_system_error(self, exception: ExceptionType) -> bool:
        """Return whether the exception `exception` is considered a signal of
        system malfunction. Business exceptions should not cause this circuit
        breaker to open.
        """
        exception_type = type(exception)
        for exclusion in self._excluded_exceptions:
            if type(exclusion) is type:
                if issubclass(exception_type, exclusion):
                    return False
            elif callable(exclusion):
                if exclusion(exception):
                    return False
        return True

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Call `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        """
        with self._lock:
            return self.state.call(func, *args, **kwargs)

    @contextlib.contextmanager
    def calling(self) -> Any:
        """Return a context manager, enabling the circuit breaker to be used with a
        `with` statement. The block of code inside the `with` statement will be
        executed according to the rules implemented by the current state of this
        circuit breaker.
        """

        def _wrapper() -> Generator:
            yield

        yield from self.call(_wrapper)

    def call_async(self, func, *args, **kwargs):  # type: ignore[no-untyped-def]
        """Call async `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.

        Return a closure to prevent import errors when using without tornado present
        """

        @gen.coroutine
        def wrapped():  # type: ignore[no-untyped-def]
            with self._lock:
                ret = yield self.state.call_async(func, *args, **kwargs)
                raise gen.Return(ret)

        return wrapped()

    def open(self) -> bool:
        """Open the circuit, e.g., the following calls will immediately fail until timeout elapses."""
        with self._lock:
            self._state_storage.opened_at = datetime.now(UTC)
            self.state = self._state_storage.state = STATE_OPEN  # type: ignore[assignment]

            return self._throw_new_error_on_trip

    def half_open(self) -> None:
        """Half-open the circuit, e.g. lets the following call pass through and
        opens the circuit if the call fails (or closes the circuit if the call
        succeeds).
        """
        with self._lock:
            self.state = self._state_storage.state = STATE_HALF_OPEN  # type: ignore[assignment]

    def close(self) -> None:
        """Close the circuit, e.g. lets the following calls execute as usual."""
        with self._lock:
            self._state_storage.reset_success_counter()  # Reset success counter when closing
            self.state = self._state_storage.state = STATE_CLOSED  # type: ignore[assignment]

    def __call__(self, *call_args: Any, **call_kwargs: bool) -> Callable:
        """Return a wrapper that calls the function `func` according to the rules
        implemented by the current state of this circuit breaker.

        Optionally takes the keyword argument `__pybreaker_call_coroutine`,
        which will will call `func` as a Tornado co-routine.
        """
        call_async = call_kwargs.pop("__pybreaker_call_async", False)

        if call_async and not HAS_TORNADO_SUPPORT:
            message = "No module named tornado"
            raise ImportError(message)

        def _outer_wrapper(func):  # type: ignore[no-untyped-def]
            @wraps(func)
            def _inner_wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
                if call_async:
                    return self.call_async(func, *args, **kwargs)
                return self.call(func, *args, **kwargs)

            return _inner_wrapper

        if call_args:
            return _outer_wrapper(*call_args)
        return _outer_wrapper

    @property
    def listeners(self) -> tuple[CBListenerType, ...]:
        """Return the registered listeners as a tuple."""
        return tuple(self._listeners)  # type: ignore[arg-type]

    def add_listener(self, listener: CBListenerType) -> None:
        """Register a listener for this circuit breaker."""
        with self._lock:
            self._listeners.append(listener)  # type: ignore[arg-type]

    def add_listeners(self, *listeners: CBListenerType) -> None:
        """Register listeners for this circuit breaker."""
        for listener in listeners:
            self.add_listener(listener)

    def remove_listener(self, listener: CBListenerType) -> None:
        """Unregister a listener of this circuit breaker."""
        with self._lock:
            self._listeners.remove(listener)  # type: ignore[arg-type]

    @property
    def name(self) -> str | None:
        """Return the name of this circuit breaker. Useful for logging."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """Set the name of this circuit breaker."""
        self._name = name


class CircuitBreakerStorage:
    """Define the underlying storage for a circuit breaker - the underlying
    implementation should be in a subclass that overrides the method this
    class defines.
    """

    def __init__(self, name: str) -> None:
        """Create a new instance identified by `name`."""
        self._name = name

    @property
    def name(self) -> str:
        """Return a human friendly name that identifies this state."""
        return self._name

    @property
    @abstractmethod
    def state(self) -> str:
        """Override this method to retrieve the current circuit breaker state."""

    @state.setter
    def state(self, state: str) -> None:
        """Override this method to set the current circuit breaker state."""

    def increment_counter(self) -> None:
        """Override this method to increase the failure counter by one."""

    def reset_counter(self) -> None:
        """Override this method to set the failure counter to zero."""

    def increment_success_counter(self) -> None:
        """Override this method to increase the success counter by one."""

    def reset_success_counter(self) -> None:
        """Override this method to set the success counter to zero."""

    @property
    @abstractmethod
    def counter(self) -> int:
        """Override this method to retrieve the current value of the failure counter."""

    @property
    @abstractmethod
    def success_counter(self) -> int:
        """Override this method to retrieve the current value of the success counter."""

    @property
    @abstractmethod
    def opened_at(self) -> datetime | None:
        """Override this method to retrieve the most recent value of when the circuit was opened."""

    @opened_at.setter
    def opened_at(self, datetime: datetime) -> None:
        """Override this method to set the most recent value of when the circuit was opened."""


class CircuitMemoryStorage(CircuitBreakerStorage):
    """Implement a `CircuitBreakerStorage` in local memory."""

    def __init__(self, state: str) -> None:
        """Create a new instance with the given `state`."""
        super().__init__("memory")
        self._fail_counter = 0
        self._success_counter = 0
        self._opened_at: datetime | None = None
        self._state = state

    @property
    def state(self) -> str:
        """Return the current circuit breaker state."""
        return self._state

    @state.setter
    def state(self, state: str) -> None:
        """Set the current circuit breaker state to `state`."""
        self._state = state

    def increment_counter(self) -> None:
        """Increase the failure counter by one."""
        self._fail_counter += 1

    def reset_counter(self) -> None:
        """Set the failure counter to zero."""
        self._fail_counter = 0

    def increment_success_counter(self) -> None:
        """Increase the success counter by one."""
        self._success_counter += 1

    def reset_success_counter(self) -> None:
        """Set the success counter to zero."""
        self._success_counter = 0

    @property
    def counter(self) -> int:
        """Return the current value of the failure counter."""
        return self._fail_counter

    @property
    def success_counter(self) -> int:
        """Return the current value of the success counter."""
        return self._success_counter

    @property
    def opened_at(self) -> datetime | None:
        """Return the most recent value of when the circuit was opened."""
        return self._opened_at

    @opened_at.setter
    def opened_at(self, datetime: datetime) -> None:
        """Set the most recent value of when the circuit was opened to `datetime`."""
        self._opened_at = datetime


class CircuitRedisStorage(CircuitBreakerStorage):
    """Implement a `CircuitBreakerStorage` using redis."""

    BASE_NAMESPACE = "pybreaker"

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        state: str,
        redis_object: Redis,
        namespace: str | None = None,
        fallback_circuit_state: str = STATE_CLOSED,
        cluster_mode: bool = False,
    ):
        """Create a new instance with the given `state` and `redis` object. The
        redis object should be similar to pyredis' StrictRedis class. If there
        are any connection issues with redis, the `fallback_circuit_state` is
        used to determine the state of the circuit.
        """
        # Module does not exist, so this feature is not available
        if not HAS_REDIS_SUPPORT:
            message = "CircuitRedisStorage can only be used if the required dependencies exist"
            raise ImportError(message)

        super().__init__("redis")

        self._redis = redis_object
        self._namespace_name = namespace
        self._fallback_circuit_state = fallback_circuit_state
        self._initial_state = str(state)
        self._cluster_mode = cluster_mode

        self._initialize_redis_state(self._initial_state)

    def _initialize_redis_state(self, state: str) -> None:
        self._redis.setnx(self._namespace("fail_counter"), 0)
        self._redis.setnx(self._namespace("success_counter"), 0)
        self._redis.setnx(self._namespace("state"), state)

    @property
    def state(self) -> str:
        """Return the current circuit breaker state.

        If the circuit breaker state on Redis is missing, re-initialize it
        with the fallback circuit state and reset the fail counter.
        """
        try:
            state_bytes: bytes | None = self._redis.get(self._namespace("state"))
        except RedisError:
            self.logger.exception("RedisError: falling back to default circuit state")
            return self._fallback_circuit_state

        state = self._fallback_circuit_state
        if state_bytes is not None:
            state = state_bytes.decode("utf-8")
        else:
            # state retrieved from redis was missing, so we re-initialize
            # the circuit breaker state on redis
            self._initialize_redis_state(self._fallback_circuit_state)

        return state

    @state.setter
    def state(self, state: str) -> None:
        """Set the current circuit breaker state to `state`."""
        try:
            self._redis.set(self._namespace("state"), str(state))
        except RedisError:
            self.logger.exception("RedisError")

    def increment_counter(self) -> None:
        """Increase the failure counter by one."""
        try:
            self._redis.incr(self._namespace("fail_counter"))
        except RedisError:
            self.logger.exception("RedisError")

    def reset_counter(self) -> None:
        """Set the failure counter to zero."""
        try:
            self._redis.set(self._namespace("fail_counter"), 0)
        except RedisError:
            self.logger.exception("RedisError")

    def increment_success_counter(self) -> None:
        """Increase the success counter by one."""
        try:
            self._redis.incr(self._namespace("success_counter"))
        except RedisError:
            self.logger.exception("RedisError")

    def reset_success_counter(self) -> None:
        """Set the success counter to zero."""
        try:
            self._redis.set(self._namespace("success_counter"), 0)
        except RedisError:
            self.logger.exception("RedisError")

    @property
    def counter(self) -> int:
        """Return the current value of the failure counter."""
        try:
            value = self._redis.get(self._namespace("fail_counter"))
            if value:
                return int(value)
            return 0
        except RedisError:
            self.logger.exception("RedisError: Assuming no errors")
            return 0

    @property
    def success_counter(self) -> int:
        """Return the current value of the success counter."""
        try:
            value = self._redis.get(self._namespace("success_counter"))
            if value:
                return int(value)
            return 0
        except RedisError:
            self.logger.exception("RedisError: Assuming no successes")
            return 0

    @property
    def opened_at(self) -> datetime | None:
        """Returns a datetime object of the most recent value of when the circuit was opened."""
        try:
            timestamp = self._redis.get(self._namespace("opened_at"))
            if timestamp:
                return datetime(*time.gmtime(int(timestamp))[:6], tzinfo=UTC)
        except RedisError:
            self.logger.exception("RedisError")
        return None

    @opened_at.setter
    def opened_at(self, now: datetime) -> None:
        """Atomically set the most recent value of when the circuit was opened
        to `now`. Stored in redis as a simple integer of unix epoch time.
        To avoid timezone issues between different systems, the passed in
        datetime should be in UTC.
        """
        try:
            key = self._namespace("opened_at")

            if self._cluster_mode:
                current_value = self._redis.get(key)
                next_value = int(calendar.timegm(now.timetuple()))

                if not current_value or next_value > int(current_value):
                    self._redis.set(key, next_value)

            else:

                def set_if_greater(pipe: Pipeline[bytes]) -> None:
                    current_value = cast(bytes, pipe.get(key))
                    next_value = int(calendar.timegm(now.timetuple()))
                    pipe.multi()
                    if not current_value or next_value > int(current_value):
                        pipe.set(key, next_value)

                self._redis.transaction(set_if_greater, key)

        except RedisError:
            self.logger.exception("RedisError")

    def _namespace(self, key: str) -> str:
        name_parts = [self.BASE_NAMESPACE, key]
        if self._namespace_name:
            name_parts.insert(0, self._namespace_name)

        return ":".join(name_parts)


class CircuitBreakerListener:
    """Listener class used to plug code to a ``CircuitBreaker`` instance when certain events happen."""

    def before_call(self, cb: CircuitBreaker, func: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        """This callback function is called before the circuit breaker `cb` calls `fn`."""

    def failure(self, cb: CircuitBreaker, exc: BaseException) -> None:
        """This callback function is called when a function called by the circuit breaker `cb` fails."""

    def success(self, cb: CircuitBreaker) -> None:
        """This callback function is called when a function called by the circuit breaker `cb` succeeds."""

    def state_change(
        self,
        cb: CircuitBreaker,
        old_state: CircuitBreakerState | None,
        new_state: CircuitBreakerState,
    ) -> None:
        """This callback function is called when the state of the circuit breaker `cb` state changes."""


class CircuitBreakerState:
    """Implement the behavior needed by all circuit breaker states."""

    def __init__(self, cb: CircuitBreaker, name: str) -> None:
        """Create a new instance associated with the circuit breaker `cb` and identified by `name`."""
        self._breaker: CircuitBreaker = cb
        self._name: str = name

    @property
    def name(self) -> str:
        """Return a human friendly name that identifies this state."""
        return self._name

    @overload
    def _handle_error(self, exc: BaseException, reraise: Literal[True] = ...) -> NoReturn:
        ...

    @overload
    def _handle_error(self, exc: BaseException, reraise: Literal[False] = ...) -> None:
        ...

    def _handle_error(self, exc: BaseException, reraise: bool = True) -> None:
        """Handle a failed call to the guarded operation."""
        if self._breaker.is_system_error(exc):
            self._breaker._inc_counter()
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exc)
            self.on_failure(exc)
        else:
            self._handle_success()

        if reraise:
            raise exc

    def _handle_success(self) -> None:
        """Handle a successful call to the guarded operation."""
        self._breaker._state_storage.reset_counter()
        self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Calls `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        """
        ret = None

        self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = func(*args, **kwargs)
            if isinstance(ret, types.GeneratorType):
                return self.generator_call(ret)

        except BaseException as e:
            self._handle_error(e)
        else:
            self._handle_success()
        return ret

    def call_async(self, func, *args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        """Call async `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.

        Return a closure to prevent import errors when using without tornado present
        """

        @gen.coroutine
        def wrapped():  # type: ignore[no-untyped-def]
            ret = None

            self.before_call(func, *args, **kwargs)
            for listener in self._breaker.listeners:
                listener.before_call(self._breaker, func, *args, **kwargs)

            try:
                ret = yield func(*args, **kwargs)
                if isinstance(ret, types.GeneratorType):
                    raise gen.Return(self.generator_call(ret))

            except BaseException as e:
                self._handle_error(e)
            else:
                self._handle_success()
            raise gen.Return(ret)

        return wrapped()

    def generator_call(self, wrapped_generator):  # type: ignore[no-untyped-def]
        try:
            value = yield next(wrapped_generator)
            while True:
                value = yield wrapped_generator.send(value)
        except StopIteration:
            self._handle_success()
            return
        except BaseException as e:
            self._handle_error(e, reraise=False)
            wrapped_generator.throw(e)

    def before_call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Override this method to be notified before a call to the guarded operation is attempted."""

    def on_success(self) -> None:
        """Override this method to be notified when a call to the guarded operation succeeds."""

    def on_failure(self, exc: BaseException) -> None:
        """Override this method to be notified when a call to the guarded operation fails."""


class CircuitClosedState(CircuitBreakerState):
    """In the normal "closed" state, the circuit breaker executes operations as
    usual. If the call succeeds, nothing happens. If it fails, however, the
    circuit breaker makes a note of the failure.

    Once the number of failures exceeds a threshold, the circuit breaker trips
    and "opens" the circuit.
    """

    def __init__(
        self,
        cb: CircuitBreaker,
        prev_state: CircuitBreakerState | None = None,
        notify: bool = False,
    ) -> None:
        """Move the given circuit breaker `cb` to the "closed" state."""
        super().__init__(cb, STATE_CLOSED)
        if notify:
            # We only reset the counter if notify is True, otherwise the CircuitBreaker
            # will lose it's failure count due to a second CircuitBreaker being created
            # using the same _state_storage object, or if the _state_storage objects
            # share a central source of truth (as would be the case with the redis
            # storage).
            self._breaker._state_storage.reset_counter()
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc: BaseException) -> None:
        """Move the circuit breaker to the "open" state once the failures threshold is reached."""
        if self._breaker._state_storage.counter >= self._breaker.fail_max:
            throw_new_error = self._breaker.open()

            if throw_new_error:
                error_msg = "Failures threshold reached, circuit breaker opened"
                raise CircuitBreakerError(error_msg).with_traceback(sys.exc_info()[2])
            raise exc


class CircuitOpenState(CircuitBreakerState):
    """When the circuit is "open", calls to the circuit breaker fail immediately,
    without any attempt to execute the real operation. This is indicated by the
    ``CircuitBreakerError`` exception.

    After a suitable amount of time, the circuit breaker decides that the
    operation has a chance of succeeding, so it goes into the "half-open" state.
    """

    def __init__(
        self,
        cb: CircuitBreaker,
        prev_state: CircuitBreakerState | None = None,
        notify: bool = False,
    ) -> None:
        """Move the given circuit breaker `cb` to the "open" state."""
        super().__init__(cb, STATE_OPEN)
        if notify:
            # Reset success counter when opening the circuit
            self._breaker._state_storage.reset_success_counter()
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def before_call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """After the timeout elapses, move the circuit breaker to the "half-open"
        state; otherwise, raises ``CircuitBreakerError`` without any attempt
        to execute the real operation.
        """
        timeout = timedelta(seconds=self._breaker.reset_timeout)
        opened_at = self._breaker._state_storage.opened_at
        if opened_at and datetime.now(UTC) < opened_at + timeout:
            error_msg = "Timeout not elapsed yet, circuit breaker still open"
            raise CircuitBreakerError(error_msg)
        self._breaker.half_open()
        return self._breaker.call(func, *args, **kwargs)

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Delegate the call to before_call, if the time out is not elapsed it will throw an exception, otherwise we get
        the results from the call performed after the state is switch to half-open.
        """
        return self.before_call(func, *args, **kwargs)


class CircuitHalfOpenState(CircuitBreakerState):
    """In the "half-open" state, the next call to the circuit breaker is allowed
    to execute the dangerous operation. Should the call succeed, the circuit
    breaker resets and returns to the "closed" state. If this trial call fails,
    however, the circuit breaker returns to the "open" state until another
    timeout elapses.
    """

    def __init__(
        self,
        cb: CircuitBreaker,
        prev_state: CircuitBreakerState | None,
        notify: bool = False,
    ) -> None:
        """Move the given circuit breaker `cb` to the "half-open" state."""
        super().__init__(cb, STATE_HALF_OPEN)
        if notify:
            # Reset success counter when entering half-open state
            self._breaker._state_storage.reset_success_counter()
            for listener in self._breaker._listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exc: BaseException) -> NoReturn:
        """Opens the circuit breaker."""
        throw_new_error = self._breaker.open()

        if throw_new_error:
            error_msg = "Trial call failed, circuit breaker opened"
            raise CircuitBreakerError(error_msg).with_traceback(sys.exc_info()[2])
        raise exc

    def on_success(self) -> None:
        """Increment success counter and close the circuit breaker if threshold is reached."""
        self._breaker._state_storage.increment_success_counter()

        if self._breaker._state_storage.success_counter >= self._breaker.success_threshold:
            self._breaker.close()


class CircuitBreakerError(Exception):
    """When calls to a service fails because the circuit is open, this error is
    raised to allow the caller to handle this type of exception differently.
    """
