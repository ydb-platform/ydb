from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from django.http import HttpRequest


def nonce(request: HttpRequest) -> dict[Literal["CSP_NONCE"], str]:
    nonce = getattr(request, "csp_nonce", "")

    return {"CSP_NONCE": nonce}
