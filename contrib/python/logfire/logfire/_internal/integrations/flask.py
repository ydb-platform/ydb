from __future__ import annotations

from typing import Any

from flask.app import Flask

from logfire._internal.stack_info import warn_at_user_stacklevel

try:
    from opentelemetry.instrumentation.flask import FlaskInstrumentor
except ImportError:
    raise RuntimeError(
        '`logfire.instrument_flask()` requires the `opentelemetry-instrumentation-flask` package.\n'
        'You can install this with:\n'
        "    pip install 'logfire[flask]'"
    )

from logfire._internal.utils import maybe_capture_server_headers
from logfire.integrations.flask import CommenterOptions, RequestHook, ResponseHook


def instrument_flask(
    app: Flask,
    *,
    capture_headers: bool,
    enable_commenter: bool,
    commenter_options: CommenterOptions | None,
    excluded_urls: str | None = None,
    request_hook: RequestHook | None = None,
    response_hook: ResponseHook | None = None,
    **kwargs: Any,
):
    """Instrument `app` so that spans are automatically created for each request.

    See the `Logfire.instrument_flask` method for details.
    """
    maybe_capture_server_headers(capture_headers)

    # Previously the parameter was accidentally called exclude_urls, so we support both.
    if 'exclude_urls' in kwargs:  # pragma: no cover
        warn_at_user_stacklevel('exclude_urls is deprecated; use excluded_urls instead', DeprecationWarning)
    excluded_urls = excluded_urls or kwargs.pop('exclude_urls', None)

    FlaskInstrumentor().instrument_app(  # type: ignore[reportUnknownMemberType]
        app,
        enable_commenter=enable_commenter,
        commenter_options=commenter_options,
        excluded_urls=excluded_urls,
        request_hook=request_hook,
        response_hook=response_hook,
        **kwargs,
    )
