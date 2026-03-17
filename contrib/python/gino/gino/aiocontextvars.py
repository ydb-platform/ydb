import asyncio
import contextvars
import sys
import types


def patch_asyncio():
    """Patches asyncio to support :mod:`contextvars`.

    This is automatically called when :mod:`gino` is imported. If Python version is 3.7
    or greater, this function is a no-op.
    """

    if not sys.version_info < (3, 7):
        return

    def _get_context():
        state = _get_state()
        ctx = getattr(state, "context", None)
        if ctx is None:
            ctx = contextvars.Context()
            state.context = ctx
        return ctx

    def _set_context(ctx):
        state = _get_state()
        state.context = ctx

    def _get_state():
        loop = asyncio._get_running_loop()
        if loop is None:
            return contextvars._state
        task = asyncio.Task.current_task(loop=loop)
        return contextvars._state if task is None else task

    contextvars._get_context = _get_context
    contextvars._set_context = _set_context

    def create_task(loop, coro):
        task = loop._orig_create_task(coro)
        if task._source_traceback:
            del task._source_traceback[-1]
        task.context = contextvars.copy_context()
        return task

    def _patch_loop(loop):
        if loop and not hasattr(loop, "_orig_create_task"):
            loop._orig_create_task = loop.create_task
            loop.create_task = types.MethodType(create_task, loop)
        return loop

    def get_event_loop():
        return _patch_loop(_get_event_loop())

    def set_event_loop(loop):
        return _set_event_loop(_patch_loop(loop))

    def new_event_loop():
        return _patch_loop(_new_event_loop())

    _get_event_loop = asyncio.get_event_loop
    _set_event_loop = asyncio.set_event_loop
    _new_event_loop = asyncio.new_event_loop

    asyncio.get_event_loop = asyncio.events.get_event_loop = get_event_loop
    asyncio.set_event_loop = asyncio.events.set_event_loop = set_event_loop
    asyncio.new_event_loop = asyncio.events.new_event_loop = new_event_loop
