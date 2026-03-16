from typing import Union

from odmantic import AIOEngine, SyncEngine
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp


class EngineMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, engine: Union[AIOEngine, SyncEngine]) -> None:
        super().__init__(app)
        self.engine = engine

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if isinstance(self.engine, AIOEngine):
            async with self.engine.session() as session:
                request.state.session = session
                return await call_next(request)
        else:
            with self.engine.session() as session:
                request.state.session = session
                return await call_next(request)
