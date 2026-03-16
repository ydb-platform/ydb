# SPDX-FileCopyrightText: 2022 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import contextlib
import datetime as dt
import random
import warnings

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from functools import wraps
from inspect import (
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
)
from types import TracebackType
from typing import (
    ParamSpec,
    TypeAlias,
    TypedDict,
    TypeVar,
    cast,
)

import tenacity as _t

from ._config import CONFIG, _Config, _Testing
from .instrumentation._data import RetryDetails, guess_name


try:
    from sniffio import current_async_library
except ImportError:  # pragma: no cover -- we always have sniffio in tests

    def current_async_library() -> str:
        return "asyncio"


async def _smart_sleep(delay: float) -> None:
    io = current_async_library()

    if io == "asyncio":
        import asyncio

        await asyncio.sleep(delay)
    elif io == "trio":
        import trio

        await trio.sleep(delay)
    else:  # pragma: no cover
        msg = f"Unknown async library: {io!r}"
        raise RuntimeError(msg)


T = TypeVar("T")
P = ParamSpec("P")
BackoffHook: TypeAlias = Callable[[Exception], bool | float | dt.timedelta]
ExcOrBackoffHook: TypeAlias = (
    type[Exception] | tuple[type[Exception], ...] | BackoffHook
)

# Attribute used to store custom backoff in RetryCallState
_CUSTOM_BACKOFF_ATTR = "_stamina_custom_backoff"


class _TenacityBackoffCallbackAdapter:
    """
    Custom Tenacity retry "predicate" that uses a hook to decide whether to
    retry and additionally allows storing custom backoff values by
    piggy-backing on top of Tenacity's RetryCallState.
    """

    __slots__ = ("_backoff_hook",)

    def __init__(self, backoff_hook: BackoffHook):
        self._backoff_hook = backoff_hook

    def __call__(self, retry_state: _t.RetryCallState) -> bool:
        """
        Evaluate the backoff hook and store the custom backoff if one is
        returned.
        """
        if (exc := retry_state.outcome.exception()) is None:
            return False

        result = self._backoff_hook(exc)

        if result is None:
            warnings.warn(
                (
                    f"Backoff hook {self._backoff_hook!r} returned None. "
                    "Backoff hooks must return a bool or a float or a timedelta. "
                    "This will be an error in a future version."
                ),
                # Since we get called from Tenacity and not from user code, the
                # stack is meaningless.
                stacklevel=1,
            )
            return False

        if isinstance(result, bool):
            return result

        if isinstance(result, dt.timedelta):
            result = result.total_seconds()

        # Naughty but better than global state. This gets picked up when
        # deciding the next backoff.
        setattr(retry_state, _CUSTOM_BACKOFF_ATTR, result)

        return True


def retry_context(
    on: ExcOrBackoffHook,
    attempts: int | None = 10,
    timeout: float | dt.timedelta | None = 45.0,
    wait_initial: float | dt.timedelta = 0.1,
    wait_max: float | dt.timedelta = 5.0,
    wait_jitter: float | dt.timedelta = 1.0,
    wait_exp_base: float = 2,
) -> _RetryContextIterator:
    """
    Iterator that yields context managers that can be used to retry code
    blocks.

    Arguments have the same meaning as for :func:`stamina.retry`.

    .. versionadded:: 23.1.0
    .. versionadded:: 23.3.0 `Trio <https://trio.readthedocs.io/>`_ support.
    """

    return _RetryContextIterator.from_params(
        on=on,
        attempts=attempts,
        timeout=timeout,
        wait_initial=wait_initial,
        wait_max=wait_max,
        wait_jitter=wait_jitter,
        wait_exp_base=wait_exp_base,
        name="<context block>",
        args=(),
        kw={},
    )


class Attempt:
    """
    A context manager that can be used to retry code blocks.

    Instances are yielded by the :func:`stamina.retry_context` iterator.

    .. versionadded:: 23.2.0
    """

    __slots__ = ("_next_wait_fn", "_t_attempt")

    _t_attempt: _t.AttemptManager

    def __init__(
        self,
        attempt: _t.AttemptManager,
        next_wait_fn: Callable[[int], float] | None,
    ):
        self._t_attempt = attempt
        self._next_wait_fn = next_wait_fn

    def __repr__(self) -> str:
        return f"<Attempt num={self.num}, next_wait={float(self.next_wait)}>"

    @property
    def num(self) -> int:
        """
        The number of the current attempt.
        """
        return self._t_attempt.retry_state.attempt_number  # type: ignore[no-any-return]

    @property
    def next_wait(self) -> float:
        """
        The number of seconds of backoff before the *next* attempt if *this*
        attempt fails.

        .. warning::
            This value does **not** include a possible random jitter and is
            therefore just a *lower bound* of the actual value.

        .. versionadded:: 24.3.0
        .. versionchanged:: 25.2.0
           Fixed off-by-one error when passing the attempt number to the
           next_wait_fn.
        """
        return (
            # The next wait time is computed based on the current attempt number.
            self._next_wait_fn(self._t_attempt.retry_state.attempt_number)
            if self._next_wait_fn
            else 0.0
        )

    def __enter__(self) -> None:
        return self._t_attempt.__enter__()  # type: ignore[no-any-return]

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._t_attempt.__exit__(  # type: ignore[no-any-return]
            exc_type, exc_value, traceback
        )


class RetryKWs(TypedDict):
    attempts: int | None
    timeout: float | dt.timedelta | None
    wait_initial: float | dt.timedelta
    wait_max: float | dt.timedelta
    wait_jitter: float | dt.timedelta
    wait_exp_base: float


class BaseRetryingCaller:
    """
    Simple base class that transforms retry parameters into a dictionary that
    can be `**`-passed into `retry_context`.
    """

    __slots__ = ("_context_kws",)

    _context_kws: RetryKWs

    def __init__(
        self,
        attempts: int | None = 10,
        timeout: float | dt.timedelta | None = 45.0,
        wait_initial: float | dt.timedelta = 0.1,
        wait_max: float | dt.timedelta = 5.0,
        wait_jitter: float | dt.timedelta = 1.0,
        wait_exp_base: float = 2,
    ):
        self._context_kws = {
            "attempts": attempts,
            "timeout": timeout,
            "wait_initial": wait_initial,
            "wait_max": wait_max,
            "wait_jitter": wait_jitter,
            "wait_exp_base": wait_exp_base,
        }

    def __repr__(self) -> str:
        kws = ", ".join(
            f"{k}={self._context_kws[k]!r}"  # type: ignore[literal-required]
            for k in sorted(self._context_kws)
            if k != "on"
        )
        return f"<{self.__class__.__name__}({kws})>"


class RetryingCaller(BaseRetryingCaller):
    """
    Call your callables with retries.

    Arguments have the same meaning as for :func:`stamina.retry`.

    Tip:
        Instances of ``RetryingCaller`` may be reused because they internally
        create a new :func:`retry_context` iterator on each call.

    .. versionadded:: 24.2.0
    """

    def __call__(
        self,
        on: ExcOrBackoffHook,
        callable_: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        r"""
        Call ``callable_(*args, **kw)`` with retries if *on* is raised.

        Args:
            on: Exception(s) to retry on.

            callable\_: Callable to call.

            args: Positional arguments to pass to *callable_*.

            kw: Keyword arguments to pass to *callable_*.
        """
        for attempt in retry_context(on, **self._context_kws):
            with attempt:
                return callable_(*args, **kwargs)

        raise SystemError("unreachable")  # noqa: EM101

    def on(self, on: ExcOrBackoffHook, /) -> BoundRetryingCaller:
        """
        Create a new instance of :class:`BoundRetryingCaller` with the same
        parameters, but bound to a specific exception type.

        .. versionadded:: 24.2.0
        """
        # This should be a `functools.partial`, but unfortunately it's
        # impossible to provide a nicely typed API with it, so we use a
        # separate class.
        return BoundRetryingCaller(self, on)


class BoundRetryingCaller:
    """
    Same as :class:`RetryingCaller`, but pre-bound to a specific exception
    type.

    Caution:
        Returned by :meth:`RetryingCaller.on` -- do not instantiate directly.

    .. versionadded:: 24.2.0
    """

    __slots__ = ("_caller", "_on")

    _caller: RetryingCaller
    _on: ExcOrBackoffHook

    def __init__(
        self,
        caller: RetryingCaller,
        on: ExcOrBackoffHook,
    ):
        self._caller = caller
        self._on = on

    def __repr__(self) -> str:
        return (
            f"<BoundRetryingCaller({guess_name(self._on)}, {self._caller!r})>"
        )

    def __call__(
        self, callable_: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
    ) -> T:
        """
        Same as :func:`RetryingCaller.__call__`, except retry on the exception
        that is bound to this instance.
        """
        return self._caller(self._on, callable_, *args, **kwargs)


class AsyncRetryingCaller(BaseRetryingCaller):
    """
    Same as :class:`RetryingCaller`, but for async callables.

    .. versionadded:: 24.2.0
    """

    async def __call__(
        self,
        on: ExcOrBackoffHook,
        callable_: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """
        Same as :meth:`RetryingCaller.__call__`, but *callable_* is awaited.
        """
        async for attempt in retry_context(on, **self._context_kws):
            with attempt:
                return await callable_(*args, **kwargs)

        raise SystemError("unreachable")  # noqa: EM101

    def on(self, on: ExcOrBackoffHook, /) -> BoundAsyncRetryingCaller:
        """
        Create a new instance of :class:`BoundAsyncRetryingCaller` with the
        same parameters, but bound to a specific exception type.

        .. versionadded:: 24.2.0
        """
        return BoundAsyncRetryingCaller(self, on)


class BoundAsyncRetryingCaller:
    """
    Same as :class:`BoundRetryingCaller`, but for async callables.

    Caution:
        Returned by :meth:`AsyncRetryingCaller.on` -- do not instantiate
        directly.

    .. versionadded:: 24.2.0
    """

    __slots__ = ("_caller", "_on")

    _caller: AsyncRetryingCaller
    _on: ExcOrBackoffHook

    def __init__(
        self,
        caller: AsyncRetryingCaller,
        on: ExcOrBackoffHook,
    ):
        self._caller = caller
        self._on = on

    def __repr__(self) -> str:
        return f"<BoundAsyncRetryingCaller({guess_name(self._on)}, {self._caller!r})>"

    async def __call__(
        self,
        callable_: Callable[P, Awaitable[T]],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """
        Same as :func:`AsyncRetryingCaller.__call__`, except retry on the
        exception that is bound to this instance.
        """
        return await self._caller(self._on, callable_, *args, **kwargs)


_STOP_NO_RETRY = _t.stop_after_attempt(1)


class _LazyNoAsyncRetry:
    """
    Allows us a free null object pattern using non-retries and avoid None.
    """

    __slots__ = ()

    def __aiter__(self) -> _t.AsyncRetrying:
        return _t.AsyncRetrying(
            reraise=True, stop=_STOP_NO_RETRY, sleep=_smart_sleep
        ).__aiter__()


_LAZY_NO_ASYNC_RETRY = _LazyNoAsyncRetry()


@dataclass
class _RetryContextIterator:
    __slots__ = (
        "_args",
        "_attempts",
        "_cms_to_exit",
        "_kw",
        "_name",
        "_t_a_retrying",
        "_t_kw",
        "_wait_exp_base",
        "_wait_initial",
        "_wait_jitter",
        "_wait_max",
    )
    _t_kw: dict[str, object]
    _t_a_retrying: _t.AsyncRetrying
    _name: str
    _args: tuple[object, ...]
    _kw: dict[str, object]

    _attempts: int | None
    _wait_jitter: float
    _wait_initial: float
    _wait_max: float
    _wait_exp_base: float

    _cms_to_exit: list[AbstractContextManager[None]]

    @classmethod
    def from_params(
        cls,
        on: ExcOrBackoffHook,
        attempts: int | None,
        timeout: float | dt.timedelta | None,
        wait_initial: float | dt.timedelta,
        wait_max: float | dt.timedelta,
        wait_jitter: float | dt.timedelta,
        wait_exp_base: float,
        name: str,
        args: tuple[object, ...],
        kw: dict[str, object],
    ) -> _RetryContextIterator:
        if (
            isinstance(on, type) and issubclass(on, BaseException)
        ) or isinstance(on, tuple):
            _retry = _t.retry_if_exception_type(on)
        else:
            _retry = _TenacityBackoffCallbackAdapter(cast(BackoffHook, on))

        if isinstance(wait_initial, dt.timedelta):
            wait_initial = wait_initial.total_seconds()

        if isinstance(wait_max, dt.timedelta):
            wait_max = wait_max.total_seconds()

        if isinstance(wait_jitter, dt.timedelta):
            wait_jitter = wait_jitter.total_seconds()

        inst = cls(
            _name=name,
            _args=args,
            _kw=kw,
            _attempts=attempts,
            _wait_jitter=wait_jitter,
            _wait_initial=wait_initial,
            _wait_max=wait_max,
            _wait_exp_base=wait_exp_base,
            _t_kw={
                "retry": _retry,
                "stop": _make_stop(
                    attempts=attempts,
                    timeout=(
                        timeout.total_seconds()
                        if isinstance(timeout, dt.timedelta)
                        else timeout
                    ),
                ),
                "reraise": True,
            },
            _t_a_retrying=_LAZY_NO_ASYNC_RETRY,
            _cms_to_exit=[],
        )

        inst._t_kw["wait"] = inst._jittered_backoff_for_rcs

        return inst

    def with_name(
        self, name: str, args: tuple[object, ...], kw: dict[str, object]
    ) -> _RetryContextIterator:
        """
        Recreate ourselves with a new name and arguments.
        """
        return replace(self, _name=name, _args=args, _kw=kw)

    def _apply_maybe_test_mode_to_tenacity_kw(
        self, testing: _Testing | None
    ) -> dict[str, object]:
        if testing is None:
            return self._t_kw

        t_kw = self._t_kw.copy()

        t_kw["stop"] = _t.stop_after_attempt(
            testing.get_attempts(self._attempts)
        )

        return t_kw

    def _exit_cms(self, _: _t.RetryCallState | None) -> None:
        for cm in reversed(self._cms_to_exit):
            cm.__exit__(None, None, None)

    def __iter__(self) -> Iterator[Attempt]:
        if not CONFIG.is_active:
            for r in _t.Retrying(reraise=True, stop=_STOP_NO_RETRY):
                yield Attempt(r, None)

            return

        before_sleep = _make_before_sleep(
            self._name, CONFIG, self._args, self._kw, self._cms_to_exit
        )

        for r in _t.Retrying(
            before=self._exit_cms,
            before_sleep=before_sleep,
            **self._apply_maybe_test_mode_to_tenacity_kw(CONFIG.testing),
        ):
            yield Attempt(r, self._backoff_for_attempt_number)

    def __aiter__(self) -> AsyncIterator[Attempt]:
        if CONFIG.is_active:
            self._t_a_retrying = _t.AsyncRetrying(
                sleep=_smart_sleep,
                before=self._exit_cms,
                before_sleep=_make_before_sleep(
                    self._name, CONFIG, self._args, self._kw, self._cms_to_exit
                ),
                **self._apply_maybe_test_mode_to_tenacity_kw(CONFIG.testing),
            )

        self._t_a_retrying = self._t_a_retrying.__aiter__()

        return self

    async def __anext__(self) -> Attempt:
        return Attempt(
            await self._t_a_retrying.__anext__(),
            self._backoff_for_attempt_number,
        )

    def _backoff_for_attempt_number(self, num: int) -> float:
        """
        Compute a jitter-less lower bound for backoff number *num*.

        *num* is 1-based.
        """
        return _compute_backoff(
            num, self._wait_max, self._wait_initial, self._wait_exp_base, 0
        )

    def _jittered_backoff_for_rcs(self, rcs: _t.RetryCallState) -> float:
        """
        Compute the backoff for *rcs*.

        If a custom backoff was provided by a backoff hook, use it. Otherwise,
        use exponential backoff.
        """
        if (
            custom_backoff := getattr(rcs, _CUSTOM_BACKOFF_ATTR, None)
        ) is not None:
            delattr(rcs, _CUSTOM_BACKOFF_ATTR)

            if CONFIG.testing is not None:
                return 0.0

            return cast(float, custom_backoff)

        return _compute_backoff(
            rcs.attempt_number,
            self._wait_max,
            self._wait_initial,
            self._wait_exp_base,
            self._wait_jitter,
        )


def _compute_backoff(
    num: int,
    max_backoff: float,
    initial: float,
    exp_base: float,
    max_jitter: float,
) -> float:
    """
    If not in testing mode, compute the backoff for attempt *num* with the
    given parameters and clamp it to *max_backoff*.
    """
    if CONFIG.testing is not None:
        return 0.0

    jitter = random.uniform(0, max_jitter) if max_jitter else 0  # noqa: S311

    return min(max_backoff, initial * (exp_base ** (num - 1)) + jitter)


def _make_before_sleep(
    name: str,
    config: _Config,
    args: tuple[object, ...],
    kw: dict[str, object],
    hook_cms: list[contextlib.AbstractContextManager[None]],
) -> Callable[[_t.RetryCallState], None]:
    """
    Create a `before_sleep` callback function that runs our `RetryHook`s with
    the necessary arguments.

    If a hook returns a context manager, it's entered before retries start and
    exited after they finish by keeping track of the context managers in
    *hook_cms*.
    """

    last_idle_for = 0.0

    def before_sleep(rcs: _t.RetryCallState) -> None:
        nonlocal last_idle_for

        wait_for = rcs.idle_for - last_idle_for

        details = RetryDetails(
            name=name,
            retry_num=rcs.attempt_number,
            wait_for=wait_for,
            waited_so_far=rcs.idle_for - wait_for,
            caused_by=rcs.outcome.exception(),
            args=args,
            kwargs=kw,
        )

        for hook in config.on_retry:
            maybe_cm = hook(details)

            if isinstance(maybe_cm, AbstractContextManager):
                maybe_cm.__enter__()
                hook_cms.append(maybe_cm)

        last_idle_for = rcs.idle_for

    return before_sleep


def _make_stop(*, attempts: int | None, timeout: float | None) -> _t.stop_base:
    """
    Combine *attempts* and *timeout* into one stop condition.
    """
    stops = []

    if attempts is not None:
        stops.append(_t.stop_after_attempt(attempts))

    if timeout is not None:
        stops.append(_t.stop_after_delay(timeout))

    if not stops:
        return _t.stop_never

    return _t.stop_any(*stops)


def retry(  # noqa: C901
    *,
    on: ExcOrBackoffHook,
    attempts: int | None = 10,
    timeout: float | dt.timedelta | None = 45.0,
    wait_initial: float | dt.timedelta = 0.1,
    wait_max: float | dt.timedelta = 5.0,
    wait_jitter: float | dt.timedelta = 1.0,
    wait_exp_base: float = 2,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    r"""
    Retry if one of configured exceptions are raised.

    The backoff delays between retries grow exponentially plus a random jitter.

    The backoff for retry attempt number *attempt* is computed as:

    .. keep in-sync with docs/motivation.md
    .. math::

       min(wait\_max, wait\_initial * wait\_exp\_base^{attempt - 1} + random(0,
       wait\_jitter))

    Since :math:`x^0` is always 1, the first backoff is within the interval
    :math:`[wait\_initial,wait\_initial+wait\_jitter]`. Thus, with default
    values between 0.1 and 1.1 seconds.

    If all retries fail, the *last* exception is let through.

    All float-based time parameters are in seconds.

    Generators are restarted from the beginning on each retry.

    .. note::
       Being able to :meth:`~agen.asend` and :meth:`~agen.athrow` into wrapped
       async generators introduced nontrivial complexity in the implementation
       and is therefore **provisional**. If supporting these features causes
       problems, they may be removed again in a future version.

    .. warning::
       It is possible to get unbounded retries by passing `None` for *attempts*
       (= infinite attempts) and *timeout* (= no timeout).

    Args:
        on:
            An Exception or a tuple of Exceptions on which the decorated
            callable will be retried.

            You can also pass a *backoff hook* in the form of a callable that
            takes an exception and returns a bool which decides whether the
            exception should be retried -- True meaning yes.

            This allows more fine-grained control over when to retry. For
            example, to only retry on HTTP errors in the 500s range that
            indicate server errors, but not those in the 400s which indicate a
            client error.

            For even more control, the hook may return a float or a
            :class:`datetime.timedelta` to specify a custom backoff that
            overrides the default backoff. This is useful when the error
            carries information like a `Retry-After
            <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Retry-After>`_
            header. A custom backoff is not part of the exponential backoff
            machinery so none of the other backoff parameters apply to it.

            There is no default for the *on* parameter -- you *must* pass *on*
            explicitly.

        attempts:
            Maximum total number of attempts. Can be combined with *timeout*.
            `None` means *no* limit to attempts.

        timeout:
            Maximum total time for all retries. Can be combined with
            *attempts*. `None` means *no* timeout.

        wait_initial: Minimum backoff before the *first* retry.

        wait_max: Maximum backoff time between retries at any time.

        wait_jitter:
            Maximum *jitter* that is added to retry back-off delays (the actual
            jitter added is a random number between 0 and *wait_jitter*)

        wait_exp_base: The exponential base used to compute the retry backoff.

    .. versionchanged:: 23.1.0
       All time-related parameters can now be specified as a
       :class:`datetime.timedelta`.

    .. versionadded:: 23.3.0 `Trio <https://trio.readthedocs.io/>`_ support.

    .. versionadded:: 24.3.0 *on* can be a callable now.

    .. versionadded:: 25.2.0
       Generator functions and async generator functions are now retried, too.

    .. versionadded:: 25.2.0
       An *on* backoff hook can now return a float or a `datetime.timedelta` to
       specify a custom backoff that overrides the default backoff.

    """
    retry_ctx = _RetryContextIterator.from_params(
        on=on,
        attempts=attempts,
        timeout=timeout,
        wait_initial=wait_initial,
        wait_max=wait_max,
        wait_jitter=wait_jitter,
        wait_exp_base=wait_exp_base,
        name="<unknown>",
        args=(),
        kw={},
    )

    def retry_decorator(wrapped: Callable[P, T]) -> Callable[P, T]:  # noqa: C901
        name = guess_name(wrapped)

        if isgeneratorfunction(wrapped):

            @wraps(wrapped)
            def sync_gen_inner(*args: P.args, **kw: P.kwargs) -> T:  # type: ignore[misc]
                for attempt in retry_ctx.with_name(name, args, kw):
                    with attempt:
                        return (yield from wrapped(*args, **kw))

            return sync_gen_inner

        if isasyncgenfunction(wrapped):

            @wraps(wrapped)
            async def async_gen_inner(*args: P.args, **kw: P.kwargs) -> T:  # type: ignore[misc]
                async for attempt in retry_ctx.with_name(name, args, kw):
                    with attempt:
                        agen = wrapped(*args, **kw)
                        try:
                            try:
                                item = await agen.__anext__()
                            except StopAsyncIteration:
                                return

                            while True:
                                try:
                                    to_send = yield item
                                except GeneratorExit:  # noqa: PERF203
                                    await agen.aclose()
                                    raise
                                except BaseException as thrown:  # noqa: BLE001
                                    try:
                                        item = await agen.athrow(thrown)
                                    except StopAsyncIteration:
                                        return
                                else:
                                    try:
                                        if to_send is None:
                                            item = await agen.__anext__()
                                        else:
                                            item = await agen.asend(to_send)
                                    except StopAsyncIteration:
                                        return
                        finally:
                            await agen.aclose()

            return async_gen_inner

        if not iscoroutinefunction(wrapped):

            @wraps(wrapped)  # noqa: RET503
            def sync_inner(*args: P.args, **kw: P.kwargs) -> T:  # type: ignore[return]
                for attempt in retry_ctx.with_name(name, args, kw):
                    with attempt:
                        return wrapped(*args, **kw)

            return sync_inner

        @wraps(wrapped)  # noqa: RET503
        async def async_inner(*args: P.args, **kw: P.kwargs) -> T:  # type: ignore[return]
            async for attempt in retry_ctx.with_name(name, args, kw):
                with attempt:
                    return await wrapped(*args, **kw)  # type: ignore[no-any-return]

        return async_inner  # type: ignore[return-value]

    return retry_decorator
