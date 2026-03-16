from __future__ import annotations

import asyncio
import inspect
import sys
from contextvars import copy_context
from functools import partial, wraps
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Generator,
    Iterable,
    List,
    Tuple,
    TYPE_CHECKING,
    Union,
)

from werkzeug.datastructures import Headers

from .typing import FilePath

if TYPE_CHECKING:
    from .wrappers.response import Response  # noqa: F401


def file_path_to_path(*paths: FilePath) -> Path:
    # Flask supports bytes paths
    safe_paths: List[Union[str, PathLike]] = []
    for path in paths:
        if isinstance(path, bytes):
            safe_paths.append(path.decode())
        else:
            safe_paths.append(path)
    return Path(*safe_paths)


def run_sync(func: Callable[..., Any]) -> Callable[..., Coroutine[None, None, Any]]:
    """Ensure that the sync function is run within the event loop.

    If the *func* is not a coroutine it will be wrapped such that
    it runs in the default executor (use loop.set_default_executor
    to change). This ensures that synchronous functions do not
    block the event loop.
    """

    @wraps(func)
    async def _wrapper(*args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None, copy_context().run, partial(func, *args, **kwargs)
        )
        if inspect.isgenerator(result):
            return run_sync_iterable(result)
        else:
            return result

    _wrapper._quart_async_wrapper = True  # type: ignore
    return _wrapper


def run_sync_iterable(iterable: Generator[Any, None, None]) -> AsyncGenerator[Any, None]:
    async def _gen_wrapper() -> AsyncGenerator[Any, None]:
        # Wrap the generator such that each iteration runs
        # in the executor. Then rationalise the raised
        # errors so that it ends.
        def _inner() -> Any:
            # https://bugs.python.org/issue26221
            # StopIteration errors are swallowed by the
            # run_in_exector method
            try:
                return next(iterable)
            except StopIteration:
                raise StopAsyncIteration()

        loop = asyncio.get_running_loop()
        while True:
            try:
                yield await loop.run_in_executor(None, copy_context().run, _inner)
            except StopAsyncIteration:
                return

    return _gen_wrapper()


def is_coroutine_function(func: Any) -> bool:
    # Python < 3.8 does not correctly determine partially wrapped
    # coroutine functions are coroutine functions, hence the need for
    # this to exist. Code taken from CPython.
    if sys.version_info >= (3, 8):
        return asyncio.iscoroutinefunction(func)
    else:
        # Note that there is something special about the AsyncMock
        # such that it isn't determined as a coroutine function
        # without an explicit check.
        try:
            from mock import AsyncMock

            if isinstance(func, AsyncMock):
                return True
        except ImportError:
            # Not testing, no asynctest to import
            pass

        while inspect.ismethod(func):
            func = func.__func__
        while isinstance(func, partial):
            func = func.func
        if not inspect.isfunction(func):
            return False
        result = bool(func.__code__.co_flags & inspect.CO_COROUTINE)
        return result or getattr(func, "_is_coroutine", None) is asyncio.coroutines._is_coroutine


def encode_headers(headers: Headers) -> List[Tuple[bytes, bytes]]:
    return [(key.lower().encode(), value.encode()) for key, value in headers.items()]


def decode_headers(headers: Iterable[Tuple[bytes, bytes]]) -> Headers:
    return Headers([(key.decode(), value.decode()) for key, value in headers])
