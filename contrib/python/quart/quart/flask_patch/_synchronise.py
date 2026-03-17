from __future__ import annotations

import asyncio
from typing import Any, Awaitable

from quart.globals import _app_ctx_stack, _request_ctx_stack, _websocket_ctx_stack


def sync_with_context(future: Awaitable) -> Any:
    context = None
    if _request_ctx_stack.top is not None:
        context = _request_ctx_stack.top.copy()
    elif _websocket_ctx_stack.top is not None:
        context = _websocket_ctx_stack.top.copy()
    elif _app_ctx_stack.top is not None:
        context = _app_ctx_stack.top.copy()

    async def context_wrapper() -> Any:
        if context is not None:
            async with context:
                return await future
        else:
            return await future

    return asyncio.get_event_loop().sync_wait(context_wrapper())  # type: ignore
