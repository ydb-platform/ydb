from collections.abc import Awaitable, MutableMapping
from typing import Any, Callable

from http import HTTPStatus

from context_async_sqlalchemy import (
    init_db_session_ctx,
    is_context_initiated,
    reset_db_session_ctx,
    auto_commit_by_status_code,
    rollback_all_sessions,
)

Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Scope = MutableMapping[str, Any]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class ASGIHTTPDBSessionMiddleware:
    """Database session lifecycle management."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        """
        Database session lifecycle management.
        The session itself is created on demand in db_session().

        Transaction auto-commit is implemented if there is no exception and
            the response status is < 400. Otherwise, a rollback is performed.

        But you can commit or rollback manually in the handler.
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Tests have different session management rules
        # so if the context variable is already set, we do nothing
        if is_context_initiated():
            await self.app(scope, receive, send)
            return

        # We set the context here, meaning all child coroutines
        # will receive the same context.
        # And even if a child coroutine requests the session first,
        # the container itself is shared, and this coroutine will
        # add the session to container = shared context.
        token = init_db_session_ctx()

        status_code = HTTPStatus.INTERNAL_SERVER_ERROR

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
            # using the status code, we decide to commit or rollback
            # all sessions
            await auto_commit_by_status_code(status_code)
        except Exception:
            # If an exception occurs, we roll all sessions back
            await rollback_all_sessions()
            raise
        finally:
            # Close all sessions and clear the context
            await reset_db_session_ctx(token)
