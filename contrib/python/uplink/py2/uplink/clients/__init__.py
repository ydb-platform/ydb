"""
This package defines an adapter layer for writing wrappers of existing
HTTP clients (Requests, aiohttp, etc.) so they can handle requests built by
Uplink's high-level, declarative API.

We refer to this layer as the backend, as these adapters handle the
actual HTTP client logic (i.e., making a request to a server).

Todo:
    At some point, we may want to expose this layer to the user, so
    they can create custom adapters.
"""
# Local imports
from uplink import utils
from uplink.clients import interfaces, register
from uplink.clients.register import DEFAULT_CLIENT, get_client
from uplink.clients.requests_ import RequestsClient
from uplink.clients.twisted_ import TwistedClient


@register.handler
def _client_class_handler(key):
    if utils.is_subclass(key, interfaces.HttpClientAdapter):
        return key()


try:
    from uplink.clients.aiohttp_ import AiohttpClient
except (ImportError, SyntaxError):  # pragma: no cover

    class AiohttpClient(interfaces.HttpClientAdapter):
        def __init__(self, *args, **kwargs):
            raise NotImplementedError(
                "Failed to load `aiohttp` client: you may be using a version "
                "of Python below 3.3. `aiohttp` requires Python 3.4+."
            )


__all__ = [
    "RequestsClient",
    "AiohttpClient",
    "TwistedClient",
    "DEFAULT_CLIENT",
    "get_client",
]

register.set_default_client(RequestsClient)
