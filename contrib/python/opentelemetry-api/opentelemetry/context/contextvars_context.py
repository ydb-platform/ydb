# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from contextvars import ContextVar, Token

from opentelemetry.context.context import Context, _RuntimeContext


class ContextVarsRuntimeContext(_RuntimeContext):
    """An implementation of the RuntimeContext interface which wraps ContextVar under
    the hood. This is the preferred implementation for usage with Python 3.5+
    """

    _CONTEXT_KEY = "current_context"

    def __init__(self) -> None:
        self._current_context = ContextVar(
            self._CONTEXT_KEY, default=Context()
        )

    def attach(self, context: Context) -> Token[Context]:
        """Sets the current `Context` object. Returns a
        token that can be used to reset to the previous `Context`.

        Args:
            context: The Context to set.
        """
        return self._current_context.set(context)

    def get_current(self) -> Context:
        """Returns the current `Context` object."""
        return self._current_context.get()

    def detach(self, token: Token[Context]) -> None:
        """Resets Context to a previous value

        Args:
            token: A reference to a previous Context.
        """
        self._current_context.reset(token)


__all__ = ["ContextVarsRuntimeContext"]
