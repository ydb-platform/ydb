from contextvars import copy_context
from typing import Any, Awaitable, Callable, TypeVar

from .context import init_db_session_ctx, reset_db_session_ctx
from .auto_commit import commit_all_sessions, rollback_all_sessions


AsyncCallableResult = TypeVar("AsyncCallableResult")
AsyncCallable = Callable[..., Awaitable[AsyncCallableResult]]


async def run_in_new_ctx(
    callable_func: AsyncCallable[AsyncCallableResult],
    *args: Any,
    **kwargs: Any,
) -> AsyncCallableResult:
    """
    Runs a function in a new context with new sessions that have their
        own connection.

    It will commit the transaction automatically if callable_func does not
        raise exceptions. Otherwise, the transaction will be rolled back.

    The intended use is to run multiple database queries concurrently.

    example of use:
        await asyncio.gather(
            run_in_new_ctx(
                your_function_with_db_session, some_arg, some_kwarg=123,
            ),
            run_in_new_ctx(your_function_with_db_session, ...),
        )
    """
    new_ctx = copy_context()
    return await new_ctx.run(_new_ctx_wrapper, callable_func, *args, **kwargs)


async def _new_ctx_wrapper(
    callable_func: AsyncCallable[AsyncCallableResult],
    *args: Any,
    **kwargs: Any,
) -> AsyncCallableResult:
    token = init_db_session_ctx(force=True)
    try:
        result = await callable_func(*args, **kwargs)
        await commit_all_sessions()
        return result
    except Exception:
        await rollback_all_sessions()
        raise
    finally:
        await reset_db_session_ctx(token)
