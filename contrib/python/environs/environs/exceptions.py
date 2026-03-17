from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .types import ErrorMapping


class EnvError(ValueError):
    """Raised when an environment variable or
    if a required environment variable is unset.
    """


class EnvValidationError(EnvError):
    """Raised when validation fails against one or all of the parsed environment variables."""

    def __init__(self, message: str, error_messages: list[str] | ErrorMapping):
        self.error_messages = error_messages
        super().__init__(message)


class EnvSealedError(TypeError, EnvError):
    """Raised when trying to parse new values after an `Env` has already been sealed."""


class ParserConflictError(ValueError):
    """Raised when adding a custom parser that conflicts
    with a built-in parser method.
    """
