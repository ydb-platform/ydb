from contextvars import ContextVar, Token
from typing import Any, Generator

from sqlalchemy.ext.asyncio import AsyncSession

from .connect import DBConnect


def init_db_session_ctx(
    force: bool = False,
) -> Token[dict[str, AsyncSession] | None]:
    """
    Initiates a context for storing sessions
    """
    if not force and is_context_initiated():
        raise Exception("Context already initiated")

    return _init_db_session_ctx()


def is_context_initiated() -> bool:
    """
    Checks whether the context is initiated
    """
    return _db_session_ctx.get() is not None


def pop_db_session_from_context(connect: DBConnect) -> AsyncSession | None:
    """
    Removes a session from the context
    """
    session_ctx = _db_session_ctx.get()
    if not session_ctx:
        return None

    session: AsyncSession | None = session_ctx.pop(connect.context_key, None)
    return session


async def reset_db_session_ctx(
    token: Token[dict[str, AsyncSession] | None], with_close: bool = True
) -> None:
    """
    Removes sessions from the context and also closes the session if it
        is open.
    """
    if with_close:
        for session in sessions_stream():
            await session.close()
    _db_session_ctx.reset(token)


def get_db_session_from_context(connect: DBConnect) -> AsyncSession | None:
    """
    Extracts the session from the context
    """
    session_ctx = _get_initiated_context()
    return session_ctx.get(connect.context_key)


def put_db_session_to_context(
    connection: DBConnect,
    session: AsyncSession,
) -> None:
    """
    Puts the session into context
    """
    session_ctx = _get_initiated_context()
    session_ctx[connection.context_key] = session


def sessions_stream() -> Generator[AsyncSession, Any, None]:
    """Read all open context sessions"""
    for session in _get_initiated_context().values():
        yield session


_db_session_ctx: ContextVar[dict[str, AsyncSession] | None] = ContextVar(
    "db_session_ctx", default=None
)


def _get_initiated_context() -> dict[str, AsyncSession]:
    session_ctx = _db_session_ctx.get()
    if session_ctx is None:
        raise Exception("Context is not initiated")
    return session_ctx


def _init_db_session_ctx() -> Token[dict[str, AsyncSession] | None]:
    session_ctx: dict[str, AsyncSession] | None = {}
    return _db_session_ctx.set(session_ctx)
