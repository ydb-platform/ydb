from __future__ import annotations

from typing import TYPE_CHECKING, Callable, TypedDict

from opentelemetry.trace import Span

if TYPE_CHECKING:
    from wsgiref.types import WSGIEnvironment


RequestHook = Callable[[Span, 'WSGIEnvironment'], None]
"""A hook that is called before a request is processed."""
ResponseHook = Callable[[Span, str, 'list[tuple[str, str]]'], None]
"""A hook that is called after a response is processed."""


class CommenterOptions(TypedDict, total=False):
    """The `commenter_options` parameter for `instrument_flask`."""

    framework: bool
    """Include the framework name and version in the comment."""
    route: bool
    """Include the route name in the comment."""
    controller: bool
    """Include the controller name in the comment."""
