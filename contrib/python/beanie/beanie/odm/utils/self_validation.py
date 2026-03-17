from functools import wraps
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from beanie.odm.documents import AsyncDocMethod, DocType

P = ParamSpec("P")
R = TypeVar("R")


def validate_self_before(
    f: "AsyncDocMethod[DocType, P, R]",
) -> "AsyncDocMethod[DocType, P, R]":
    @wraps(f)
    async def wrapper(self: "DocType", *args: P.args, **kwargs: P.kwargs) -> R:
        await self.validate_self(*args, **kwargs)
        return await f(self, *args, **kwargs)

    return wrapper
