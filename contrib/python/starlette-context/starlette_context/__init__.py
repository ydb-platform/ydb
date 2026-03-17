from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any

_request_scope_context_storage: ContextVar[dict[Any, Any]] = ContextVar(
    "starlette_context"
)


@contextmanager
def request_cycle_context(
    initial_data: dict | None = None,
) -> Iterator[None]:
    """
    Creates and resets a starlette-context context.

    Used in the Context and Raw middlewares, but can also be used to create a
    context out of a proper request cycle, such as in unit tests.
    """
    if initial_data is None:
        initial_data = {}
    token: Token = _request_scope_context_storage.set(initial_data.copy())
    yield
    _request_scope_context_storage.reset(token)


from starlette_context.ctx import context  # noqa: E402

__all__ = ["context", "request_cycle_context"]
