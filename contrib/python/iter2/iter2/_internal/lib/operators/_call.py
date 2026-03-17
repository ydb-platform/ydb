from __future__ import annotations

import typing as tp


# ---

def call[**Parameters](
    *args: Parameters.args,
    **kwargs: Parameters.kwargs,
) -> _TypedCallable[Parameters]:
    def _call(fn):
        return fn(*args, **kwargs)
    return _call


# ---

class _TypedCallable[**Parameters](tp.Protocol):
    def __call__[ReturnValue](self, fn: tp.Callable[Parameters, ReturnValue]) -> ReturnValue: ...
