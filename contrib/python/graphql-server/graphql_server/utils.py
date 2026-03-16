import sys
from typing import Awaitable, Callable, TypeVar

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:  # pragma: no cover
    from typing_extensions import ParamSpec


__all__ = ["wrap_in_async"]

P = ParamSpec("P")
R = TypeVar("R")


def wrap_in_async(f: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    """Convert a sync callable (normal def or lambda) to a coroutine (async def).

    This is similar to asyncio.coroutine which was deprecated in Python 3.8.
    """

    async def f_async(*args: P.args, **kwargs: P.kwargs) -> R:
        return f(*args, **kwargs)

    return f_async
