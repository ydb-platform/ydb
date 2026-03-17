"""Utilities to use during testing"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from context_async_sqlalchemy import (
    commit_all_sessions,
    DBConnect,
    init_db_session_ctx,
    put_db_session_to_context,
    reset_db_session_ctx,
)


@asynccontextmanager
async def rollback_session(
    connection: DBConnect,
) -> AsyncGenerator[AsyncSession, None]:
    """A session that always rolls back"""
    session_maker = await connection.session_maker()
    async with session_maker() as session:
        try:
            yield session
        finally:
            await session.rollback()


@asynccontextmanager
async def set_test_context(
    auto_close: bool = False,
) -> AsyncGenerator[None, None]:
    """
    Opens a context similar to middleware.

    Use auto_close=False if you’re using a test session and transaction
        that you close elsewhere in your code.

    Use auto_close=True if you want to call a function
        in a test that uses a context while the middleware is not
        active, and you want all sessions to be closed automatically.
    """
    token = init_db_session_ctx()
    try:
        yield
    finally:
        if auto_close:
            await commit_all_sessions()
        await reset_db_session_ctx(
            token,
            # Don't close the session here if you opened in fixture.
            with_close=auto_close,
        )


@asynccontextmanager
async def put_savepoint_session_in_ctx(
    connection: DBConnect,
    session: AsyncSession,
) -> AsyncGenerator[None, None]:
    """
    Sets the context to a session that uses a save point instead of creating
        a transaction. You need to pass the session you're using inside
        your tests to attach a new session to the same connection.

    It is important to use this function inside set_test_context.
    """
    session_maker = await connection.session_maker()
    async with session_maker(
        # Bind to the same connection
        bind=await session.connection(),
        # Instead of opening a transaction, it creates a save point
        join_transaction_mode="create_savepoint",
    ) as session:
        put_db_session_to_context(connection, session)
    yield
