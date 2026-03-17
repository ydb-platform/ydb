from __future__ import annotations

from typing import Any


class Auth0Error(Exception):
    def __init__(
        self,
        status_code: int,
        error_code: str,
        message: str,
        content: Any | None = None,
        headers: Any | None = None,
    ) -> None:
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.content = content
        self.headers = headers

    def __str__(self) -> str:
        return f"{self.status_code}: {self.message}"


class RateLimitError(Auth0Error):
    def __init__(self, error_code: str, message: str, reset_at: int, headers: Any | None = None) -> None:
        super().__init__(status_code=429, error_code=error_code, message=message, headers=headers)
        self.reset_at = reset_at


class TokenValidationError(Exception):
    pass
