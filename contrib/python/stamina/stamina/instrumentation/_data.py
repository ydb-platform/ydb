# SPDX-FileCopyrightText: 2022 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol


def guess_name(obj: object) -> str:
    name = getattr(obj, "__qualname__", None) or "<unnamed object>"
    mod = getattr(obj, "__module__", None) or "<unknown module>"

    if mod == "builtins":
        return name

    return f"{mod}.{name}"


@dataclass(frozen=True)
class RetryDetails:
    r"""
    Details about a retry attempt that are passed into :class:`RetryHook`\ s.

    Attributes:
        name: Name of the callable that is being retried.

        args: Positional arguments that were passed to the callable.

        kwargs: Keyword arguments that were passed to the callable.

        retry_num:
            Number of the retry attempt. Starts at 1 after the first failure.

        wait_for:
            Time in seconds that *stamina* will wait before the next attempt.

        waited_so_far:
            Time in seconds that *stamina* has waited so far for the current
            callable.

        caused_by: Exception that caused the retry attempt.

    .. versionadded:: 23.2.0
    """

    __slots__ = (
        "args",
        "caused_by",
        "kwargs",
        "name",
        "retry_num",
        "wait_for",
        "waited_so_far",
    )

    name: str
    args: tuple[object, ...]
    kwargs: dict[str, object]
    retry_num: int
    wait_for: float
    waited_so_far: float
    caused_by: Exception


class RetryHook(Protocol):
    """
    A callable that gets called after an attempt has failed and a retry has
    been scheduled.

    This is a :class:`typing.Protocol` that can be implemented by any callable
    that takes one argument of type :class:`RetryDetails` and returns None.

    If the hook returns a context manager, it will be entered when the retry is
    scheduled and exited right before the retry is attempted.

    .. versionadded:: 23.2.0

    .. versionadded:: 25.1.0
       Added support for context managers.
    """

    def __call__(
        self, details: RetryDetails
    ) -> None | AbstractContextManager[None]: ...


@dataclass(frozen=True)
class RetryHookFactory:
    """
    Wraps a callable that returns a :class:`RetryHook`.

    They are called on the first scheduled retry and can be used to delay
    initialization. If you need to pass arguments, you can do that using
    :func:`functools.partial`.

    .. versionadded:: 23.2.0
    """

    hook_factory: Callable[[], RetryHook]
