"""Utility functions for Langfuse OpenTelemetry integration.

This module provides utility functions for working with OpenTelemetry spans,
including formatting and serialization of span data, and async execution helpers.
"""

import asyncio
import json
import threading
from hashlib import sha256
from typing import Any, Coroutine

from opentelemetry import trace as otel_trace_api
from opentelemetry.sdk import util
from opentelemetry.sdk.trace import ReadableSpan


def span_formatter(span: ReadableSpan) -> str:
    parent_id = (
        otel_trace_api.format_span_id(span.parent.span_id) if span.parent else None
    )
    start_time = util.ns_to_iso_str(span._start_time) if span._start_time else None
    end_time = util.ns_to_iso_str(span._end_time) if span._end_time else None
    status = {
        "status_code": str(span._status.status_code.name),
    }

    if span._status.description:
        status["description"] = span._status.description

    context = (
        {
            "trace_id": otel_trace_api.format_trace_id(span._context.trace_id),
            "span_id": otel_trace_api.format_span_id(span._context.span_id),
            "trace_state": repr(span._context.trace_state),
        }
        if span._context
        else None
    )

    instrumentationScope = json.loads(
        span._instrumentation_scope.to_json() if span._instrumentation_scope else "{}"
    )

    return (
        json.dumps(
            {
                "name": span._name,
                "context": context,
                "kind": str(span.kind),
                "parent_id": parent_id,
                "start_time": start_time,
                "end_time": end_time,
                "status": status,
                "attributes": span._format_attributes(span._attributes),
                "events": span._format_events(span._events),
                "links": span._format_links(span._links),
                "resource": json.loads(span.resource.to_json()),
                "instrumentationScope": instrumentationScope,
            },
            indent=2,
        )
        + "\n"
    )


class _RunAsyncThread(threading.Thread):
    """Helper thread class for running async coroutines in a separate thread."""

    def __init__(self, coro: Coroutine[Any, Any, Any]) -> None:
        self.coro = coro
        self.result: Any = None
        self.exception: Exception | None = None
        super().__init__()

    def run(self) -> None:
        try:
            self.result = asyncio.run(self.coro)
        except Exception as e:
            self.exception = e


def run_async_safely(coro: Coroutine[Any, Any, Any]) -> Any:
    """Safely run an async coroutine, handling existing event loops.

    This function detects if there's already a running event loop and uses
    a separate thread if needed to avoid the "asyncio.run() cannot be called
    from a running event loop" error. This is particularly useful in environments
    like Jupyter notebooks, FastAPI applications, or other async frameworks.

    Args:
        coro: The coroutine to run

    Returns:
        The result of the coroutine

    Raises:
        Any exception raised by the coroutine

    Example:
        ```python
        # Works in both sync and async contexts
        async def my_async_function():
            await asyncio.sleep(1)
            return "done"

        result = run_async_safely(my_async_function())
        ```
    """
    try:
        # Check if there's already a running event loop
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop, safe to use asyncio.run()
        return asyncio.run(coro)

    if loop and loop.is_running():
        # There's a running loop, use a separate thread
        thread = _RunAsyncThread(coro)
        thread.start()
        thread.join()

        if thread.exception:
            raise thread.exception
        return thread.result
    else:
        # Loop exists but not running, safe to use asyncio.run()
        return asyncio.run(coro)


def get_sha256_hash_hex(value: Any) -> str:
    return sha256(value.encode("utf-8")).digest().hex()
