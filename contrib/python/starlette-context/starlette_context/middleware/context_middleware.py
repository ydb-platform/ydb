from collections.abc import Sequence
from typing import Any

from starlette.middleware.base import (
    ASGIApp,
    BaseHTTPMiddleware,
    RequestResponseEndpoint,
)
from starlette.requests import Request
from starlette.responses import Response

from starlette_context import request_cycle_context
from starlette_context.errors import (
    ConfigurationError,
    MiddleWareValidationError,
)
from starlette_context.plugins import Plugin


class ContextMiddleware(BaseHTTPMiddleware):
    """
    Middleware that creates empty context for request it's used on.

    If not used, you won't be able to use context object.
    """

    def __init__(
        self,
        app: ASGIApp,
        plugins: Sequence[Plugin] | None = None,
        default_error_response: Response = Response(status_code=400),
        *args: Any,
        **kwargs: Any,
    ) -> None:

        super().__init__(app, *args, **kwargs)
        for plugin in plugins or ():
            if not isinstance(plugin, Plugin):
                raise ConfigurationError(
                    f"Plugin {plugin} is not a valid instance"
                )
        self.plugins = plugins or ()
        self.error_response = default_error_response

    async def set_context(self, request: Request) -> dict:
        """
        You might want to override this method.

        The dict it returns will be saved in the scope of a context. You can
        always do that later.
        """
        return {
            plugin.key: await plugin.process_request(request)
            for plugin in self.plugins
        }

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        try:
            context = await self.set_context(request)
        except MiddleWareValidationError as e:
            error_response = e.error_response or self.error_response
            return error_response

        # create request-scoped context
        with request_cycle_context(context):
            # process rest of response stack
            response = await call_next(request)
            # gets back to middleware, process response with plugins
            for plugin in self.plugins:
                await plugin.enrich_response(response)
            # return response before resetting context
            # allowing further middlewares to still use the context
            return response
        # context reset
