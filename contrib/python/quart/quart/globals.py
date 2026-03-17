from __future__ import annotations

from contextvars import ContextVar
from functools import partial
from typing import Any, List, TYPE_CHECKING

from werkzeug.local import ContextVar as WerkzeugContextVar, LocalProxy, LocalStack

if TYPE_CHECKING:
    from .app import Quart
    from .ctx import _AppCtxGlobals
    from .sessions import SessionMixin
    from .wrappers import Request, Websocket

if WerkzeugContextVar != ContextVar:
    raise RuntimeError("Error with Werkzeug's locals, please open a Quart issue for help")


def _ctx_lookup(ctx_stacks: List[LocalStack], name: str) -> Any:
    top = None
    for ctx_stack in ctx_stacks:
        top = ctx_stack.top
        if top is not None:
            break
    if top is None:
        raise RuntimeError(f"Attempt to access {name} outside of a relevant context")
    return getattr(top, name)


_app_ctx_stack = LocalStack()
_request_ctx_stack = LocalStack()
_websocket_ctx_stack = LocalStack()

current_app: Quart = LocalProxy(partial(_ctx_lookup, [_app_ctx_stack], "app"))  # type: ignore
g: _AppCtxGlobals = LocalProxy(partial(_ctx_lookup, [_app_ctx_stack], "g"))  # type: ignore
request: Request = LocalProxy(partial(_ctx_lookup, [_request_ctx_stack], "request"))  # type: ignore
session: SessionMixin = LocalProxy(partial(_ctx_lookup, [_request_ctx_stack, _websocket_ctx_stack], "session"))  # type: ignore  # noqa: E501
websocket: Websocket = LocalProxy(partial(_ctx_lookup, [_websocket_ctx_stack], "websocket"))  # type: ignore  # noqa: E501
