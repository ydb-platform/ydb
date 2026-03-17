from logfire._internal.utils import maybe_capture_server_headers as maybe_capture_server_headers
from logfire.integrations.wsgi import RequestHook as RequestHook, ResponseHook as ResponseHook
from typing import Any
from wsgiref.types import WSGIApplication

def instrument_wsgi(app: WSGIApplication, *, capture_headers: bool = False, request_hook: RequestHook | None = None, response_hook: ResponseHook | None = None, **kwargs: Any) -> WSGIApplication:
    """Instrument `app` so that spans are automatically created for each request.

    See the `Logfire.instrument_wsgi` method for details.
    """
