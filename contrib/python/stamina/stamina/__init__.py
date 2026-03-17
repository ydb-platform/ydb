# SPDX-FileCopyrightText: 2022 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from . import instrumentation
from ._config import is_active, is_testing, set_active, set_testing
from ._core import (
    AsyncRetryingCaller,
    Attempt,
    BoundAsyncRetryingCaller,
    BoundRetryingCaller,
    RetryingCaller,
    retry,
    retry_context,
)


__all__ = [
    "AsyncRetryingCaller",
    "Attempt",
    "BoundAsyncRetryingCaller",
    "BoundRetryingCaller",
    "RetryingCaller",
    "instrumentation",
    "is_active",
    "is_testing",
    "retry",
    "retry_context",
    "set_active",
    "set_testing",
]


def __getattr__(name: str) -> str:
    if name != "__version__":
        msg = f"module {__name__} has no attribute {name}"
        raise AttributeError(msg)

    from importlib.metadata import version

    return version("stamina")
