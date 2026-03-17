__all__ = [
    'AsyncInterfaceWrapper',
    'AsyncMultithreadWrapper',
    'ComplexException',
    'ensure_async',
    'get_task_traceback',
    'Cooldown',
    'AsyncGenAdapter',
    'generate_async_methods_from',
    'isasyncgenadapterfunction',
]

import asyncio
import contextvars
import functools
import inspect
import linecache
import logging
import pickle
import re
import sys
import time
from concurrent import futures
from io import StringIO
from textwrap import dedent
from typing import (
    AsyncGenerator, AsyncIterable, Awaitable, Callable, Dict, Generator, Generic, List, Optional, Type, TypeVar,
)

import attr

from .stored import PICKLE_DEFAULT_PROTOCOL

logger = logging.Logger(__file__)


@attr.s
class ComplexException(Exception):
    """Exception to aggregate multiple exceptions occured.
    Unnderlying exceptions are stored in the `exceptions` attribute.

    Attributes:
        exceptions: List of underlying exceptions.
    """
    exceptions: List[Exception] = attr.ib()

    def __attrs_post_init__(self) -> None:
        aggregated_by_type: Dict[Type, ComplexException] = {}
        flat: List[Exception] = []

        self_class = type(self)
        for exc in self.exceptions:
            exc_class = type(exc)
            if exc_class is self_class:
                flat.extend(exc.exceptions)
            elif isinstance(exc, ComplexException):
                if exc_class in aggregated_by_type:
                    aggregated_by_type[exc_class].exceptions.extend(exc.exceptions)
                else:
                    aggregated_by_type[exc_class] = exc
            else:
                flat.append(exc)

        self.exceptions = list(aggregated_by_type.values()) + flat

        raise self


@attr.s
class _EnsureAsynced:
    __wrapped__: Callable = attr.ib()

    async def __call__(self, *args, **kwargs):
        return self.__wrapped__(*args, **kwargs)


def ensure_async(func: Callable) -> Callable[..., Awaitable]:
    """Ensure given callable is async.

    Note, that it doesn't provide concurrency by itself!
    It just allow to treat sync and async callables in the same way.

    Args:
        func: Any callable: synchronous or asynchronous.
    Returns:
        Wrapper that return awaitable object at call.
    """

    if asyncio.iscoroutinefunction(func) or asyncio.iscoroutinefunction(getattr(func, '__call__', None)):
        return func
    return functools.wraps(func)(_EnsureAsynced(func))


T = TypeVar('T')


class AsyncInterfaceWrapper(Generic[T]):
    """Wrap arbitrary object to be able to await any of it's methods even if it's sync.

    Note, that it doesn't provide concurrency by itself!
    It just allow to treat sync and async callables in the same way.
    """

    def __init__(self, wrapped: T):
        self.__wrapped__ = wrapped

    def __getstate__(self) -> bytes:
        return pickle.dumps(self.__wrapped__, protocol=PICKLE_DEFAULT_PROTOCOL)

    def __setstate__(self, state: bytes) -> None:
        self.__wrapped__ = pickle.loads(state)

    def __getattr__(self, name: str):
        attribute = getattr(self.__wrapped__, name)
        return ensure_async(attribute) if callable(attribute) else attribute


class AsyncMultithreadWrapper(Generic[T]):
    """Wrap arbitrary object to run each of it's methods in a separate thread.

    Examples:
        Simple usage example.

        >>> class SyncClassExample:
        >>>     def sync_method(self, sec):
        >>>         time.sleep(sec)  # Definitely not async.
        >>>         return sec
        >>>
        >>> obj = AsyncMultithreadWrapper(SyncClassExample())
        >>> await asyncio.gather(*[obj.sync_method(1) for _ in range(10)])
        ...
    """

    def __init__(self, wrapped: T, pool_size: int = 10, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.__wrapped__ = wrapped
        self.__loop = loop
        self.__pool_size = pool_size
        self.__executor = futures.ThreadPoolExecutor(max_workers=pool_size)
        logger.warning(
            'AsyncMultithreadWrapper is deprecated and will be removed in future versions. Please use AsyncTolokaClient:'
            ' this will increase performance in case of many concurrent connections.'
        )

    @property
    def _loop(self):
        if self.__loop is None:
            self.__loop = asyncio.get_event_loop()
        return self.__loop

    def __getstate__(self) -> bytes:
        return pickle.dumps((self.__wrapped__, self.__pool_size), protocol=PICKLE_DEFAULT_PROTOCOL)

    def __setstate__(self, state: bytes) -> None:
        self.__wrapped__, self.__pool_size = pickle.loads(state)
        self.__executor = futures.ThreadPoolExecutor(max_workers=self.__pool_size)

    def __getattr__(self, name: str):
        attribute = getattr(self.__wrapped__, name)
        if not callable(attribute):
            return attribute

        @functools.wraps(attribute)
        async def _wrapper(*args, **kwargs):

            def func_with_init_vars(*args, **kwargs):
                func, ctx_vars, *args_for_func = args
                for var, value in ctx_vars.items():
                    var.set(value)
                return func(*args_for_func, **kwargs)

            ctx = contextvars.copy_context()
            return await self._loop.run_in_executor(
                self.__executor,
                functools.partial(func_with_init_vars, attribute, ctx, *args, **kwargs)
            )

        return _wrapper


def get_task_traceback(task: asyncio.Task) -> Optional[str]:
    """Get traceback for given task as string.
    Return traceback as string if exists. Or None if there was no error.
    """
    if task.exception() is None:
        return None
    with StringIO() as stream:
        task.print_stack(file=stream)
        stream.flush()
        stream.seek(0)
        return stream.read()


class Cooldown:
    """Сontext manager that implements a delay between calls occurring inside the context

    Args:
        cooldown_time(int): seconds between calls

    Example:
        >>> coldown = toloka.util.Cooldown(5)
        >>> while True:
        >>>     async with coldown:
        >>>         await do_it()  # will be called no more than once every 5 seconds
        ...
    """
    _touch_time: float
    _cooldown_time: int

    def __init__(self, cooldown_time):
        self._touch_time = None
        self._cooldown_time = cooldown_time

    async def __aenter__(self):
        if self._touch_time:
            time_to_sleep = self._cooldown_time + self._touch_time - time.time()
            if time_to_sleep > 0:
                await asyncio.sleep(time_to_sleep)
        self._touch_time = time.time()

    async def __aexit__(self, *exc):
        pass


def generate_async_methods_from(cls):
    """Class decorator that generates asynchronous versions of methods using the provided class.

    This class assumes that every method of the resulting class is either an async gen or async function. In case of
    the naming collision (decorated class already has the method that would have been created by the decorator)
    the new method is not generated. This allows you to custom implement asynchronous versions of non-trivial methods
    while automatically generating boilerplate code.
    """

    def _substitute_for_loop(match):
        method_name = match.group(2)
        method = getattr(cls, method_name)
        if inspect.isgeneratorfunction(method):
            return match.expand(r'async for \1 in self.\2\3:')
        else:
            return match.expand(r'for \1 in await self.\2\3:')

    def _substitute_method(match):
        method_name = match.group(1)
        method = getattr(cls, method_name)
        if inspect.isgeneratorfunction(method):
            return match.expand(r'self.\1(')
        else:
            return match.expand(r'await self.\1(')

    def _generate_async_version_source(method):
        source = inspect.getsource(method)

        yield_from_regex = re.compile(r'yield from (\S+)')
        source = re.sub(
            yield_from_regex,
            r'async for _val in \1: yield _val',
            source,
        )

        # Generator[YieldType, SendType, ReturnType] -> AsyncGenAdapter[YieldType, SendType]
        generator_type_annotation_regex = re.compile(r'Generator\[(\S+, \S+), \S+]')
        source = re.sub(generator_type_annotation_regex, r'AsyncGenAdapter[\1]', source)

        # Generator.send -> AsyncGenerator.asend
        send_regex = re.compile(r'(\w+)\.send\(')
        source = re.sub(send_regex, r'await \1.asend(', source)

        # method calls
        method_call_regex = re.compile(r'(?<!in )self\.(\w+)\(')
        source = re.sub(method_call_regex, _substitute_method, source)

        # for loops
        for_loop_regex = re.compile(r'for (\w+) in self\.(\w+)([^:]+):')
        source = re.sub(for_loop_regex, _substitute_for_loop, source)

        # method signatures
        source = source.replace(
            f'def {method.__name__}', f'async def {method.__name__}'
        )

        # headers
        source = source.replace("add_headers('client')", "add_headers('async_client')")

        return dedent(source)

    def wrapper(target_cls):
        def _compile_function(member_name, source):
            file_name = f'async_client_{member_name}'

            bytecode = compile(source, file_name, 'exec')
            # use a modified target class module __dict__ as globals
            proxy_globals = dict(**sys.modules[cls.__module__].__dict__)
            proxy_globals['AsyncGenAdapter'] = AsyncGenAdapter
            proxy_locals = dict(**cls.__dict__)
            eval(bytecode, proxy_globals, proxy_locals)
            function = proxy_locals[member_name]
            function.__module__ = target_cls.__module__
            function.__qualname__ = f'{target_cls.__name__}.{function.__name__}'

            linecache.cache[file_name] = (
                len(source),
                None,
                source.splitlines(True),
                file_name
            )

            return function

        for member_name, member in cls.__dict__.items():
            if (
                inspect.isfunction(member)
                and not hasattr(target_cls, member_name)
                and not isinstance(member_name, property)
            ):
                source = _generate_async_version_source(member)

                function = _compile_function(member_name, source)
                if inspect.isasyncgenfunction(function):
                    function = async_gen_adapter(function)
                setattr(target_cls, member_name, function)
        return target_cls

    return wrapper


def async_gen_adapter(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return AsyncGenAdapter(func(*args, **kwargs))

    wrapper.__is_async_gen_adapter = True
    return wrapper


YieldType = TypeVar('YieldType')
SendType = TypeVar('SendType')


class SyncGenWrapper:
    def __init__(self, gen: AsyncGenerator[YieldType, SendType]):
        self.gen = gen

    def __iter__(self):
        # use new event loop in different thread not to interfere with the current one
        loop = asyncio.new_event_loop()
        try:
            # Nested event loops are prohibited in python, so we use threading to synchronously run such a loop.
            # Creating new loop inside thread each time is impossible since it will be destroyed on thread exit and
            # generator will be closed.
            with futures.ThreadPoolExecutor(max_workers=1) as executor:
                while True:
                    res = executor.submit(loop.run_until_complete, self.gen.__anext__())
                    yield res.result()
        except StopAsyncIteration:
            return
        finally:
            loop.close()


class AsyncGenAdapter(Generic[YieldType, SendType], AsyncIterable, Awaitable):
    """Adapter class that enables alternative syntax for iteration over async generator.

    This class is used for backwards compatibility. Please use "async for" syntax in new code.

    Examples:
        main syntax
         >>> async for value in AsyncGenAdapter(async_gen): ...
         ...

         alternative syntax (please do not use in new code):
         >>> for value in await AsyncGenAdapter(async_gen): ...
         ...
    """

    def __init__(self, gen: AsyncGenerator[YieldType, SendType]):
        self.gen = gen

    async def as_sync_gen(self):
        return SyncGenWrapper(self.gen)

    def __await__(self) -> Generator[SyncGenWrapper, None, None]:
        logger.warning(
            'Awaiting AsyncGenAdapter, which was returned by AsyncTolokaClient\'s method is deprecated and does not '
            'provide true asynchrony. Please use "async for" syntax to iterate over the results.'
        )
        return self.as_sync_gen().__await__()

    # defined explicitly due to the check in "async for"
    async def __aiter__(self):
        async for value in self.gen:
            yield value

    def __getattr__(self, item):
        return getattr(self.gen, item)


def isasyncgenadapterfunction(obj):
    return getattr(obj, '__is_async_gen_adapter', False)
