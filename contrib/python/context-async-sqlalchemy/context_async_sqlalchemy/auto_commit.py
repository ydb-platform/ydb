from http import HTTPStatus

from .context import sessions_stream


async def auto_commit_by_status_code(status_code: int) -> None:
    """
    Implements automatic commit or rollback.
    It should be used in a middleware or anywhere else where you expect
        session lifecycle management.
    """
    if status_code < HTTPStatus.BAD_REQUEST:
        await commit_all_sessions()
    else:
        await rollback_all_sessions()


async def rollback_all_sessions() -> None:
    """
    Rolls back all open context sessions.

    It should be used in middleware or anywhere else where you expect
        lifecycle management and need to roll back all opened sessions.
    For example, inside an except block.
    """
    for session in sessions_stream():
        if session.in_transaction():
            await session.rollback()


async def commit_all_sessions() -> None:
    """
    Commits all open context sessions.

    It should be used in middleware or anywhere else where you expect
        lifecycle management and need to commit all opened sessions.
    """
    for session in sessions_stream():
        if session.in_transaction():
            await session.commit()


async def close_all_sessions() -> None:
    """
    Closes all open context sessions.

    It should be used in middleware or anywhere else where you expect
        lifecycle management and need to close all sessions.
    """
    for session in sessions_stream():
        await session.close()
