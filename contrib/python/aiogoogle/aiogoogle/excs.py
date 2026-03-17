from __future__ import annotations
from typing import TYPE_CHECKING

__all__ = ["AiogoogleError", "ValidationError", "HTTPError", "AuthError"]

if TYPE_CHECKING:
    from .models import Response


class AiogoogleError(Exception):
    pass


class ValidationError(AiogoogleError):
    """
    Raised when the validate flag is set true and a validation error occurs
    """

    pass


class HTTPError(AiogoogleError):
    def __init__(self, msg, req=None, res: Response | None = None):
        self.req = req
        self.res = res
        super().__init__(msg)


class AuthError(HTTPError):
    pass
