from fastapi import FastAPI
from starlette.middleware.base import (  # type: ignore[attr-defined]
    Request,
    Response,
    RequestResponseEndpoint,
)

from context_async_sqlalchemy.starlette_utils import (
    add_starlette_http_db_session_middleware,
    starlette_http_db_session_middleware,
)


def add_fastapi_http_db_session_middleware(app: FastAPI) -> None:
    """Adds middleware to the application"""
    add_starlette_http_db_session_middleware(app)


async def fastapi_http_db_session_middleware(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    """
    Database session lifecycle management.
    The session itself is created on demand in db_session().

    Transaction auto-commit is implemented if there is no exception and
        the response status is < 400. Otherwise, a rollback is performed.

    But you can commit or rollback manually in the handler.
    """
    return await starlette_http_db_session_middleware(request, call_next)
