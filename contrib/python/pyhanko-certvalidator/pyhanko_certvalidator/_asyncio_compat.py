import asyncio
import contextvars
import functools

# Used as an alternative for asyncio.to_thread in python <=3.8
# Repurposed from CPython, used under the terms of the PSL
# Upstream source: https://github.com/python/cpython/blob/4b4227b907a262446b9d276c274feda2590a4e6e/Lib/asyncio/threads.py
# License: https://github.com/python/cpython/blob/4b4227b907a262446b9d276c274feda2590a4e6e/LICENSE
# Copyright (c) 2021 Python Software Foundation


async def _to_thread(func, *args, **kwargs):
    """Asynchronously run function *func* in a separate thread.
    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propagated,
    allowing context variables from the main thread to be accessed in the
    separate thread.
    Return a coroutine that can be awaited to get the eventual result of *func*.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


to_thread = getattr(asyncio, 'to_thread', _to_thread)
