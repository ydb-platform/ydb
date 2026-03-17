from __future__ import annotations

from typing import Any

from .base import AuthenticationBase


class Delegated(AuthenticationBase):
    """Delegated authentication endpoints.

    Args:
        domain (str): Your auth0 domain (e.g: username.auth0.com)
    """

    def get_token(
        self,
        target: str,
        api_type: str,
        grant_type: str,
        id_token: str | None = None,
        refresh_token: str | None = None,
        scope: str = "openid",
    ) -> Any:
        """Obtain a delegation token."""

        if id_token and refresh_token:
            raise ValueError("Only one of id_token or refresh_token can be None")

        data = {
            "client_id": self.client_id,
            "grant_type": grant_type,
            "target": target,
            "scope": scope,
            "api_type": api_type,
        }

        if id_token:
            data.update({"id_token": id_token})
        elif refresh_token:
            data.update({"refresh_token": refresh_token})
        else:
            raise ValueError("Either id_token or refresh_token must have a value")

        return self.post(f"{self.protocol}://{self.domain}/delegation", data=data)
