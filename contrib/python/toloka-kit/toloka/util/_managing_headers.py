__all__ = [
    'add_headers',
    'async_add_headers',
    'form_additional_headers',
    'set_variable',
    'top_level_method_var',
]

import asyncio
import contextvars
import inspect
from contextvars import ContextVar, copy_context
import functools
from contextlib import ExitStack, contextmanager
from typing import Dict
from ._codegen import universal_decorator

caller_context_var: ContextVar = ContextVar('caller_context')
top_level_method_var: ContextVar = ContextVar('top_level_method')
low_level_method_var: ContextVar = ContextVar('low_level_method')


@contextmanager
def set_variable(var, value):
    token = var.set(value)
    try:
        yield copy_context()
    finally:
        if token:
            var.reset(token)


class LocalContext:
    def __init__(self, ctx: contextvars.Context = None):
        if ctx is None:
            ctx = copy_context()
        self._cached_vars = dict(ctx.items())
        self.exit_stack = ExitStack()

    def __enter__(self):
        self.exit_stack.__enter__()
        for var, value in self._cached_vars.items():
            self.exit_stack.enter_context(set_variable(var, value))

    def __exit__(self, *args, **kwargs):
        self.exit_stack.__exit__(*args, **kwargs)


@universal_decorator(has_parameters=True)
def add_headers(client: str):
    """
    This decorator add 3 headers into resulting http request:
    1) X-Caller-Context: high-level abstraction like client, metrics, streaming
    2) X-Top-Level-Method: first function, that was called and then called other functions which provoked request
    3) X-Low-Level-Method: last function before calling TolokaClient _method (_raw_request for example)

    Args:
        client: name of high-level abstraction for X-Caller-Context
    """

    def wrapper(func):

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            ctx = copy_context()
            with ExitStack() as stack:
                stack.enter_context(set_variable(low_level_method_var, func.__name__))
                if caller_context_var not in ctx:
                    stack.enter_context(set_variable(caller_context_var, client))
                if top_level_method_var not in ctx:
                    stack.enter_context(set_variable(top_level_method_var, func.__name__))

                return run_in_current_context(func, *args, **kwargs)

        return wrapped

    return wrapper


# backwards compatibility
async_add_headers = add_headers


def form_additional_headers(ctx: contextvars.Context = None) -> Dict[str, str]:
    if ctx is None:
        ctx = copy_context()
    return {
        'X-Caller-Context': ctx.get(caller_context_var),
        'X-Top-Level-Method': ctx.get(top_level_method_var),
        'X-Low-Level-Method': ctx.get(low_level_method_var),
    }


def run_in_current_context(func, *args, **kwargs):
    """Runs the function using context state from the moment of calling run_in_current_context function.

    Unlike Context.run supports generators, async generators and functions that return an awaitable (e.g. coroutines).
    """

    result = func(*args, **kwargs)
    if inspect.isawaitable(result):
        # capture context by running inside task
        loop = asyncio.get_event_loop()
        return loop.create_task(result)
    elif inspect.isgenerator(result) or inspect.isasyncgen(result):
        local_vars = LocalContext()
        if inspect.isgenerator(result):
            def gen():
                while True:
                    try:
                        with local_vars:
                            item = result.__next__()
                        yield item
                    except StopIteration:
                        return
        else:
            async def gen():
                while True:
                    try:
                        with local_vars:
                            item = await result.__anext__()
                        yield item
                    except StopAsyncIteration:
                        return
        return gen()
    else:
        return result
