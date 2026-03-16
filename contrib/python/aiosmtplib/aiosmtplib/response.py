"""
SMTPResponse class, a simple namedtuple of (code, message).
"""

from typing import NamedTuple


__all__ = ("SMTPResponse",)


class SMTPResponse(NamedTuple):
    """
    NamedTuple of server response code and server response message.

    ``code`` and ``message`` can be accessed via attributes or indexes:

        >>> response = SMTPResponse(200, "OK")
        >>> response.message
        'OK'
        >>> response[0]
        200
        >>> response.code
        200

    """

    code: int
    message: str

    def __repr__(self) -> str:
        return f"({self.code}, {self.message})"

    def __str__(self) -> str:
        return f"{self.code} {self.message}"
