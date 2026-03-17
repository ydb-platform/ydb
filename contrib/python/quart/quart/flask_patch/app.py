# The aim is to replace the Quart class exception handling defaults to
# allow for Werkzeug HTTPExceptions to be considered in a special way
# (like the quart HTTPException). In addition a Flask reference is
# created.
from __future__ import annotations

from functools import wraps
from inspect import iscoroutine
from typing import Any, Awaitable, Callable, Optional, Union

from werkzeug.wrappers import Response as WerkzeugResponse

from quart import Response
from quart.app import Quart
from quart.ctx import _request_ctx_stack, RequestContext
from quart.utils import is_coroutine_function
from ._synchronise import sync_with_context

old_full_dispatch_request = Quart.full_dispatch_request


async def new_full_dispatch_request(
    self: Quart, request_context: Optional[RequestContext] = None
) -> Union[Response, WerkzeugResponse]:
    request_ = (request_context or _request_ctx_stack.top).request
    await request_.get_data()
    return await old_full_dispatch_request(self, request_context)


Quart.full_dispatch_request = new_full_dispatch_request  # type: ignore


def new_ensure_async(  # type: ignore
    self, func: Callable[..., Any]
) -> Callable[..., Awaitable[Any]]:

    if is_coroutine_function(func):
        return func
    else:

        @wraps(func)
        async def _wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            if iscoroutine(result):
                return await result
            else:
                return result

        return _wrapper


Quart.ensure_async = new_ensure_async  # type: ignore


def ensure_sync(self, func: Callable) -> Callable:  # type: ignore
    if is_coroutine_function(func):

        @wraps(func)
        def _wrapper(*args: Any, **kwargs: Any) -> Any:
            return sync_with_context(func(*args, **kwargs))

        return _wrapper
    else:
        return func


Quart.ensure_sync = ensure_sync  # type: ignore

Flask = Quart

__all__ = ("Quart",)
