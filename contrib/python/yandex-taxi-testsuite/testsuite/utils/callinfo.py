import asyncio
import inspect
import typing

from testsuite.utils import cached_property, traceback


class BaseError(Exception):
    """Base exception class for this module."""


class CallQueueError(BaseError):
    pass


class CallQueueEmptyError(CallQueueError):
    """Call queue is empty error."""


class CallQueueTimeoutError(CallQueueError):
    """Timed out while waiting for call."""


CheckerType = typing.Callable[[str], None]

__tracebackhide__ = traceback.hide(BaseError)


class AsyncCallQueue:
    """Function wrapper that puts information about function call into async
    queue.

    This class provides methods to wait/check function underlying function
    calls.
    """

    def __init__(
        self,
        func: typing.Callable,
        *,
        name=None,
        checker: CheckerType | None = None,
    ):
        self._func = func
        self._name = name or func.__name__
        self._checker = checker

    @property
    def func(self):
        """Returns underlying function."""
        return self._func

    @cached_property
    def _is_coro(self):
        return inspect.iscoroutinefunction(self._func)

    @cached_property
    def _get_callinfo(self):
        return callinfo(self._func)

    @cached_property
    def _queue(self) -> asyncio.Queue:
        return asyncio.Queue()

    def __repr__(self):
        return f'<AsyncCallQueue: for {self._func!r}>'

    async def __call__(self, *args, **kwargs):
        """Call underlying function."""
        try:
            if self._is_coro:
                return await self._func(*args, **kwargs)
            return self._func(*args, **kwargs)
        finally:
            await self._queue.put((args, kwargs))

    def flush(self) -> None:
        """Clear call queue."""
        self._queue = asyncio.Queue()

    @property
    def has_calls(self) -> bool:
        """Returns ``True`` if call queue is not empty."""
        self._check_callqueue('has_calls')
        return self.times_called > 0

    @property
    def times_called(self) -> int:
        """Returns call queue length."""
        self._check_callqueue('times_called')
        return self._queue.qsize()

    def next_call(self) -> dict:
        """Pops call from queue and return its arguments dict.

        Raises ``CallQueueError`` if queue is empty
        """
        self._check_callqueue('next_call')
        try:
            return self._get_callinfo(*self._queue.get_nowait())
        except asyncio.queues.QueueEmpty:
            raise CallQueueEmptyError(
                f'No calls for {self._name}() left in the queue',
            ) from None

    async def wait_call(self, timeout=10.0) -> dict:
        """Wait for fucntion to be called. Pops call from queue. Blocks if
        it's empty.

        :param timeout: timeout in seconds

        Raises ``CallQueueTimeoutError`` if queue is empty for ``timeout``
        seconds.
        """
        self._check_callqueue('wait_call')
        try:
            item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            return self._get_callinfo(*item)
        except asyncio.TimeoutError:
            raise CallQueueTimeoutError(
                f'Timeout while waiting for {self._name}() to be called',
            ) from None

    def _check_callqueue(self, caller):
        if self._checker is not None:
            self._checker(caller)


def getfullargspec(func):
    if isinstance(func, staticmethod):
        func = func.__func__
    func = getattr(func, '__wrapped__', func)
    return inspect.getfullargspec(func)


def callinfo(func):
    func_spec = getfullargspec(func)
    func_varkw = func_spec.varkw
    func_kwonlyargs = func_spec.kwonlyargs
    func_kwonlydefaults = func_spec.kwonlydefaults

    func_args = func_spec.args
    func_varargs = func_spec.varargs
    defaults = func_spec.defaults or ()
    func_defaults = dict(zip(func_args[-len(defaults) :], defaults))

    def callinfo_getter(args, kwargs):
        dct = dict(zip(func_args, args))
        for argname in func_args[len(args) :]:
            if argname in kwargs:
                dct[argname] = kwargs[argname]
            else:
                dct[argname] = func_defaults.get(argname)
        if func_varargs is not None:
            dct[func_varargs] = args[len(dct) :]
        for argname in func_kwonlyargs:
            if argname in kwargs:
                dct[argname] = kwargs[argname]
            else:
                dct[argname] = func_kwonlydefaults[argname]
        if func_varkw is not None:
            dct[func_varkw] = {k: v for k, v in kwargs.items() if k not in dct}
        return dct

    return callinfo_getter


def acallqueue(
    func: typing.Callable,
    *,
    checker: CheckerType | None = None,
) -> AsyncCallQueue:
    """Turn function into async call queue.

    :param func: async or sync callable, can be decorated with @staticmethod
    :param checker: optional function to check whether or not operation on
        callqueue is possible
    """
    if isinstance(func, AsyncCallQueue):
        return func
    if isinstance(func, staticmethod):
        func = func.__func__
    name = None
    if hasattr(func, '__name__'):
        name = func.__name__
    elif hasattr(func, '__call__'):
        name = func.__class__.__name__
        func = func.__call__
    else:
        raise RuntimeError(f'Unsupported func {func!r} given')
    return AsyncCallQueue(func, name=name, checker=checker)
