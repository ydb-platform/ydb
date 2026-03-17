from __future__ import annotations  # isort:skip

import quart.flask_patch.app  # isort:skip
import quart.flask_patch.cli  # isort:skip # noqa: F401
import quart.flask_patch.globals  # isort:skip # noqa: F401
import quart.flask_patch.testing  # isort:skip # noqa: F401
import quart.views  # isort:skip # noqa: F401
from quart.flask_patch._patch import patch_all  # isort:skip

patch_all()

from flask.app import Flask  # noqa: E402, I100
from flask.blueprints import Blueprint  # noqa: E402
from flask.config import Config  # noqa: E402
from flask.ctx import (  # noqa: E402
    after_this_request,
    copy_current_request_context,
    has_app_context,
    has_request_context,
)
from flask.globals import (  # noqa: E402
    _app_ctx_stack,
    _request_ctx_stack,
    current_app,
    g,
    request,
    session,
)
from flask.helpers import (  # noqa: E402
    flash,
    get_flashed_messages,
    get_template_attribute,
    make_response,
    safe_join,
    send_file,
    send_from_directory,
    stream_with_context,
    url_for,
)
from flask.json import jsonify  # noqa: E402
from flask.signals import (  # noqa: E402
    appcontext_popped,
    appcontext_pushed,
    appcontext_tearing_down,
    before_render_template,
    got_request_exception,
    message_flashed,
    request_finished,
    request_started,
    request_tearing_down,
    signals_available,
    template_rendered,
)
from flask.templating import render_template, render_template_string  # noqa: E402
from flask.typing import ResponseReturnValue  # noqa: E402
from flask.wrappers import Request, Response  # noqa: E402
from markupsafe import escape, Markup  # noqa: E402
from werkzeug.exceptions import abort  # noqa: E402
from werkzeug.utils import redirect  # noqa: E402

__all__ = (
    "_app_ctx_stack",
    "_request_ctx_stack",
    "abort",
    "after_this_request",
    "appcontext_popped",
    "appcontext_pushed",
    "appcontext_tearing_down",
    "before_render_template",
    "Blueprint",
    "Config",
    "copy_current_request_context",
    "current_app",
    "escape",
    "flash",
    "Flask",
    "g",
    "get_flashed_messages",
    "get_template_attribute",
    "got_request_exception",
    "has_app_context",
    "has_request_context",
    "jsonify",
    "make_response",
    "Markup",
    "message_flashed",
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
)


import sys  # isort:skip # noqa: E402, I100

json = sys.modules["flask.json"]
sys.modules["flask"] = sys.modules[__name__]
