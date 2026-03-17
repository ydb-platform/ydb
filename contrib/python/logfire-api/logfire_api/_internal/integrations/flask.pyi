from flask.app import Flask
from logfire._internal.stack_info import warn_at_user_stacklevel as warn_at_user_stacklevel
from logfire._internal.utils import maybe_capture_server_headers as maybe_capture_server_headers
from logfire.integrations.flask import CommenterOptions as CommenterOptions, RequestHook as RequestHook, ResponseHook as ResponseHook
from typing import Any

def instrument_flask(app: Flask, *, capture_headers: bool, enable_commenter: bool, commenter_options: CommenterOptions | None, excluded_urls: str | None = None, request_hook: RequestHook | None = None, response_hook: ResponseHook | None = None, **kwargs: Any):
    """Instrument `app` so that spans are automatically created for each request.

    See the `Logfire.instrument_flask` method for details.
    """
