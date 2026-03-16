from __future__ import annotations

import asyncio
import asyncio.events
import asyncio.tasks
import inspect
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from types import CoroutineType
from typing import TYPE_CHECKING, Any

from .constants import ONE_SECOND_IN_NANOSECONDS
from .stack_info import StackInfo, get_code_object_info, get_stack_info_from_frame
from .utils import safe_repr

if TYPE_CHECKING:
    from .main import Logfire

ASYNCIO_PATH = str(Path(asyncio.__file__).parent.absolute())


def log_slow_callbacks(logfire: Logfire, slow_duration: float) -> AbstractContextManager[None]:
    """Log a warning whenever a function running in the asyncio event loop blocks for too long.

    See Logfire.log_slow_async_callbacks.
    Inspired by https://gitlab.com/quantlane/libs/aiodebug.
    """
    original_run = asyncio.events.Handle._run
    logfire = logfire.with_settings(custom_scope_suffix='asyncio')
    timer = logfire.config.advanced.ns_timestamp_generator
    slow_duration *= ONE_SECOND_IN_NANOSECONDS

    def patched_run(self: asyncio.events.Handle) -> Any:
        start_time = timer()
        # Handle._run currently doesn't actually return anything, but maybe it will in the future?
        return_value = original_run(self)
        duration = timer() - start_time
        if duration >= slow_duration:
            try:
                duration /= ONE_SECOND_IN_NANOSECONDS
                callback: Any = self._callback
                logfire.warn(
                    'Async {name} blocked for {duration:.3f} seconds',
                    duration=duration,
                    **_callback_attributes(callback),
                )
            except Exception:  # pragma: no cover
                # Don't crash the event loop for this.
                try:
                    logfire.exception('Error in log_slow_callbacks')
                except Exception:
                    pass
        return return_value

    asyncio.events.Handle._run = patched_run

    @contextmanager
    def patch_context():
        # The user isn't required (or even expected) to use this context manager,
        # which is why the patching has already happened before this point.
        # It exists mostly for tests, and just in case users want it.
        try:
            yield
        finally:
            asyncio.events.Handle._run = original_run

    return patch_context()


class _CallbackAttributes(StackInfo, total=False):
    name: str
    stack: list[StackInfo]


def stack_info_from_coroutine(coro: CoroutineType[Any, Any, Any]) -> StackInfo:
    if frame := coro.cr_frame:
        return get_stack_info_from_frame(frame)
    else:
        # This typically means that the coroutine has finished.
        # We can't get an exact line number, so we'll use the line number of the code object.
        return get_code_object_info(coro.cr_code)


def _callback_attributes(callback: Any) -> _CallbackAttributes:
    task = getattr(callback, '__self__', None)
    if isinstance(task, asyncio.tasks.Task):
        # `callback` is a bound method of a Task.
        # This is the common case for typical user code.
        # In particular this method is usually for advancing an async function (coroutine) to the next `await`.
        coro: Any = task.get_coro()  # type: ignore
        result: _CallbackAttributes = {'name': f'task {task.get_name()}'}
        if not isinstance(coro, CoroutineType):  # pragma: no cover
            return result
        stack_info = stack_info_from_coroutine(coro)  # type: ignore
        result = {**result, **stack_info}
        if function_name := stack_info.get('code.function'):  # pragma: no branch
            result['name'] += f' ({function_name})'

        # Walk through the coroutines being awaited to create an 'async stacktrace'
        stack = [stack_info]
        while isinstance(coro := coro.cr_await, CoroutineType):
            stack_info = stack_info_from_coroutine(coro)  # type: ignore
            # Ignore frames from the stdlib asyncio
            if not stack_info.get('code.filepath', '').startswith(ASYNCIO_PATH):
                stack.append(stack_info)
        result['stack'] = stack

        return result

    # `callback` is a callable passed to a low-level API like `call_soon`.
    # Hopefully it's a function, but maybe not.
    callback = inspect.unwrap(callback)
    result: _CallbackAttributes = {}
    code = getattr(callback, '__code__', None)
    if code:  # pragma: no branch
        result = {**get_code_object_info(code)}
    name: str = (
        getattr(callback, '__qualname__', '') or getattr(callback, '__name__', '') or result.get('code.function', '')
    )
    name = name or safe_repr(callback)
    result['name'] = f'callback {name}'
    return result
