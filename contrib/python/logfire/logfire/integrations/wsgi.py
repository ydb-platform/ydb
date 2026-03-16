from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from opentelemetry.trace import Span

if TYPE_CHECKING:
    from wsgiref.types import WSGIEnvironment

ResponseHook = Callable[[Span, 'WSGIEnvironment', str, 'list[tuple[str, str]]'], None]
"""A callback called when a response is sent by the server."""

RequestHook = Callable[[Span, 'WSGIEnvironment'], None]
"""A callback called when a request is received by the server."""
