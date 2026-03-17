from __future__ import annotations

__all__ = [
    "IS_PYDANTIC_V2",
    "FastAPIPaginationWarning",
    "await_if_async",
    "await_if_coro",
    "check_installed_extensions",
    "create_pydantic_model",
    "disable_installed_extensions_check",
    "get_caller",
    "is_async_callable",
    "is_coro",
    "unwrap_annotated",
    "verify_params",
]

import functools
import inspect
import warnings
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeVar, cast, get_origin, overload

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from .bases import AbstractParams, BaseRawParams, CursorRawParams, RawParams
    from .pydantic import create_pydantic_model
    from .types import ParamsType

    TParams = TypeVar("TParams", bound=AbstractParams)

    from .pydantic import IS_PYDANTIC_V2


def __getattr__(name: str) -> Any:
    if name == "IS_PYDANTIC_V2":  # pragma: no cover
        from .pydantic import IS_PYDANTIC_V2

        warnings.warn(
            "Importing 'IS_PYDANTIC_V2' from 'fastapi_pagination.utils' is deprecated. "
            "Please import it from 'fastapi_pagination.pydantic' instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        return IS_PYDANTIC_V2

    if name == "create_pydantic_model":  # pragma: no cover
        from .pydantic import create_pydantic_model

        warnings.warn(
            "Importing 'create_pydantic_model' from 'fastapi_pagination.utils' is deprecated. "
            "Please import it from 'fastapi_pagination.pydantic' instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        return create_pydantic_model

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


@overload
def verify_params(params: TParams | None, *params_types: Literal["limit-offset"]) -> tuple[TParams, RawParams]:
    pass


@overload
def verify_params(params: TParams | None, *params_types: Literal["cursor"]) -> tuple[TParams, CursorRawParams]:
    pass


@overload
def verify_params(params: TParams | None, *params_types: ParamsType) -> tuple[TParams, BaseRawParams]:
    pass


def verify_params(params: TParams | None, *params_types: ParamsType) -> tuple[TParams, BaseRawParams]:
    from .api import resolve_params

    params = resolve_params(params)
    raw_params = params.to_raw_params()

    if raw_params.type not in params_types:
        raise ValueError(f"{raw_params.type!r} params not supported")

    return params, raw_params


def is_async_callable(obj: Any) -> bool:  # pragma: no cover
    # retrieve base function if embedded
    while isinstance(obj, functools.partial):
        obj = obj.func

    return inspect.iscoroutinefunction(obj) or (callable(obj) and inspect.iscoroutinefunction(obj.__call__))


P = ParamSpec("P")
R = TypeVar("R")


@overload
async def await_if_async(func: Callable[P, Awaitable[R]], /, *args: P.args, **kwargs: P.kwargs) -> R:
    pass


@overload
async def await_if_async(func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    pass


async def await_if_async(func: Callable[P, Any], /, *args: P.args, **kwargs: P.kwargs) -> Any:
    if is_async_callable(func):
        return await func(*args, **kwargs)

    return func(*args, **kwargs)


def is_coro(obj: Any) -> bool:
    return isinstance(obj, Awaitable)


async def await_if_coro(coro: Awaitable[R] | R, /) -> R:
    if isinstance(coro, Awaitable):
        return await coro

    return coro


_EXTENSIONS = [
    "databases",
    "django",
    "cassandra",
    "tortoise",
    "motor",
    "orm",
    "ormar",
    "pony",
    "piccolo",
    "gino",
    "beanie",
    "sqlmodel",
    "sqlalchemy",
    "asyncpg",
    "mongoengine",
    "pymongo",
]


def _check_installed(module: str) -> bool:
    try:
        __import__(module)
    except ImportError:
        return False
    else:
        return True


class FastAPIPaginationWarning(UserWarning):
    pass


_WARNING_MSG = """
Package "{ext}" is installed.

It's recommended to use extension "fastapi_pagination.ext.{ext}" instead of default 'paginate' implementation.

Otherwise, you can disable this warning by adding the following code to your code:
from fastapi_pagination.utils import disable_installed_extensions_check

disable_installed_extensions_check()
""".strip()

_CHECK_INSTALLED_EXTENSIONS = True


def disable_installed_extensions_check() -> None:
    global _CHECK_INSTALLED_EXTENSIONS  # noqa: PLW0603
    _CHECK_INSTALLED_EXTENSIONS = False


def check_installed_extensions() -> None:
    if not _CHECK_INSTALLED_EXTENSIONS:
        return

    for ext in _EXTENSIONS:
        if _check_installed(f"fastapi_pagination.ext.{ext}"):
            warnings.warn(
                _WARNING_MSG.format(ext=ext),
                FastAPIPaginationWarning,
                stacklevel=3,
            )
            break


def get_caller(depth: int = 1) -> str | None:
    frame = inspect.currentframe()

    for _ in range(depth + 1):
        if frame is None:
            return None

        frame = frame.f_back

    return cast(str | None, frame and frame.f_globals.get("__name__"))


def unwrap_annotated(ann: Any) -> Any:
    if get_origin(ann) is Annotated:
        return ann.__args__[0]

    return ann
