from collections.abc import Sequence

from starlette.requests import HTTPConnection, Request
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from starlette_context import request_cycle_context
from starlette_context.errors import (
    ConfigurationError,
    MiddleWareValidationError,
)
from starlette_context.plugins import Plugin


class RawContextMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        plugins: Sequence[Plugin] | None = None,
        default_error_response: Response = Response(status_code=400),
    ) -> None:
        self.app = app
        for plugin in plugins or ():
            if not isinstance(plugin, Plugin):
                raise ConfigurationError(
                    f"Plugin {plugin} is not a valid instance"
                )

        self.plugins = plugins or ()
        self.error_response = default_error_response

    async def set_context(self, request: Request | HTTPConnection) -> dict:
        """
        You might want to override this method.

        The dict it returns will be saved in the scope of a context. You can
        always do that later.
        """
        return {
            plugin.key: await plugin.process_request(request)
            for plugin in self.plugins
        }

    @staticmethod
    def get_request_object(
        scope: Scope, receive: Receive, send: Send
    ) -> Request | HTTPConnection:
        # here we instantiate HTTPConnection instead of a Request object
        # because only headers are needed so that's sufficient.
        # If you need the payload etc for your plugin
        # instantiate Request(scope, receive, send)
        return HTTPConnection(scope)

    async def send_response(
        self, error_response: Response, send: Send
    ) -> None:
        message_head = {
            "type": "http.response.start",
            "status": error_response.status_code,
            "headers": error_response.raw_headers,
        }
        await send(message_head)

        message_body = {
            "type": "http.response.body",
            "body": error_response.body,
        }
        await send(message_body)

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        if scope["type"] not in ("http", "websocket"):  # pragma: no cover
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message: Message) -> None:
            for plugin in self.plugins:
                await plugin.enrich_response(message)
            await send(message)

        request = self.get_request_object(scope, receive, send)

        try:
            context = await self.set_context(request)
        except MiddleWareValidationError as e:
            error_response = e.error_response or self.error_response
            return await self.send_response(error_response, send)

        with request_cycle_context(context):
            await self.app(scope, receive, send_wrapper)
