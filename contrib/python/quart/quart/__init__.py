from __future__ import annotations

from markupsafe import escape, Markup
from werkzeug.exceptions import abort
from werkzeug.utils import redirect

from .app import Quart
from .blueprints import Blueprint
from .config import Config
from .ctx import (
    after_this_request,
    copy_current_app_context,
    copy_current_request_context,
    copy_current_websocket_context,
    has_app_context,
    has_request_context,
    has_websocket_context,
)
from .globals import (
    _app_ctx_stack,
    _request_ctx_stack,
    _websocket_ctx_stack,
    current_app,
    g,
    request,
    session,
    websocket,
)
from .helpers import (
    flash,
    get_flashed_messages,
    get_template_attribute,
    make_push_promise,
    make_response,
    safe_join,
    send_file,
    send_from_directory,
    stream_with_context,
    url_for,
)
from .json import jsonify
from .signals import (
    appcontext_popped,
    appcontext_pushed,
    appcontext_tearing_down,
    before_render_template,
    got_request_exception,
    got_websocket_exception,
    message_flashed,
    request_finished,
    request_started,
    request_tearing_down,
    signals_available,
    template_rendered,
    websocket_finished,
    websocket_started,
    websocket_tearing_down,
)
from .templating import render_template, render_template_string
from .typing import ResponseReturnValue
from .wrappers import Request, Response, Websocket

__all__ = (
    "_app_ctx_stack",
    "_request_ctx_stack",
    "_websocket_ctx_stack",
    "abort",
    "after_this_request",
    "appcontext_popped",
    "appcontext_pushed",
    "appcontext_tearing_down",
    "before_render_template",
    "Blueprint",
    "Config",
    "copy_current_app_context",
    "copy_current_request_context",
    "copy_current_websocket_context",
    "current_app",
    "escape",
    "flash",
    "g",
    "get_flashed_messages",
    "get_template_attribute",
    "got_request_exception",
    "got_websocket_exception",
    "has_app_context",
    "has_request_context",
    "has_websocket_context",
    "jsonify",
    "make_push_promise",
    "make_response",
    "Markup",
    "message_flashed",
    "Quart",
    "redirect",
    "render_template",
    "render_template_string",
    "request",
    "Request",
    "request_finished",
    "request_started",
    "request_tearing_down",
    "Response",
    "ResponseReturnValue",
    "safe_join",
    "send_file",
    "send_from_directory",
    "session",
    "signals_available",
    "stream_with_context",
    "template_rendered",
    "url_for",
    "websocket",
    "websocket_finished",
    "websocket_started",
    "websocket_tearing_down",
    "Websocket",
)
