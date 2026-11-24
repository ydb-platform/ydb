# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC, abstractmethod
from contextvars import Token


class Context(dict[str, object]):
    def __setitem__(self, key: str, value: object) -> None:
        raise ValueError

    def __delitem__(self, key: str) -> None:
        raise ValueError

    def setdefault(self, key: str, default: object = None) -> object:
        raise ValueError

    def pop(self, key: str, *args: object) -> object:
        raise ValueError

    def popitem(self) -> tuple[str, object]:
        raise ValueError

    def clear(self) -> None:
        raise ValueError

    def update(self, *args: object, **kwargs: object) -> None:
        raise ValueError

    def __ior__(self, other: object) -> Context:
        raise ValueError


class _RuntimeContext(ABC):
    """The RuntimeContext interface provides a wrapper for the different
    mechanisms that are used to propagate context in Python.
    Implementations can be made available via entry_points and
    selected through environment variables.
    """

    @abstractmethod
    def attach(self, context: Context) -> Token[Context]:
        """Sets the current `Context` object. Returns a
        token that can be used to reset to the previous `Context`.

        Args:
            context: The Context to set.
        """

    @abstractmethod
    def get_current(self) -> Context:
        """Returns the current `Context` object."""

    @abstractmethod
    def detach(self, token: Token[Context]) -> None:
        """Resets Context to a previous value

        Args:
            token: A reference to a previous Context.
        """


__all__ = ["Context"]
