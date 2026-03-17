"""TraceRite extension for FastAPI/Starlette applications."""

from __future__ import annotations

from typing import Any

from .html import html_traceback
from .logging import logger
from .tty import load as load_tty

_original_debug_response = None


def patch_fastapi(*, tty: bool = True) -> None:
    """
    Load TraceRite extension for FastAPI by patching ServerErrorMiddleware.

    This patches Starlette's ServerErrorMiddleware.debug_response to return
    TraceRite HTML tracebacks instead of the default debug HTML when running
    in debug mode and the client accepts HTML.

    Additionally, we load the TTY formatting for console output.
    """
    global _original_debug_response
    if _original_debug_response is not None:
        return  # Already loaded
    try:
        import fastapi  # ty: ignore
        from starlette.middleware.errors import ServerErrorMiddleware  # ty: ignore
        from starlette.responses import HTMLResponse  # ty: ignore
    except ImportError:
        logger.info("TraceRite FastAPI cannot load: FastAPI/Starlette not found")
        return
    _original_debug_response = ServerErrorMiddleware.debug_response

    def tracerite_debug_response(self, request, exc) -> Any:
        """Return TraceRite HTML traceback instead of Starlette's debug response."""
        accept = request.headers.get("accept", "")
        if "text/html" not in accept:
            return _original_debug_response(self, request, exc)  # type: ignore
        try:
            html = str(html_traceback(exc=exc, include_js_css=True))
            return HTMLResponse(
                "<!DOCTYPE html><title>FastAPI TraceRite</title>" + html,
                status_code=500,
            )
        except Exception as e:
            logger.error(f"Failed to generate TraceRite response: {e}")
            return _original_debug_response(self, request, exc)  # type: ignore

    ServerErrorMiddleware.debug_response = tracerite_debug_response  # type: ignore
    fastapi.routing.__tracebackhide__ = "until"  # type: ignore

    if tty:
        load_tty()

    logger.info("TraceRite FastAPI extension loaded")
