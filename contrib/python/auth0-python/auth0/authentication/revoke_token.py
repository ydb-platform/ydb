from typing import Any

from .base import AuthenticationBase


class RevokeToken(AuthenticationBase):
    """Revoke Refresh Token endpoint

    Args:
        domain (str): Your auth0 domain (e.g: my-domain.us.auth0.com)
    """

    def revoke_refresh_token(self, token: str) -> Any:
        """Revokes a Refresh Token if it has been compromised

        Each revocation request invalidates not only the specific token, but all other tokens
        based on the same authorization grant. This means that all Refresh Tokens that have
        been issued for the same user, application, and audience will be revoked.

        Args:
             token (str): The Refresh Token you want to revoke

         See: https://auth0.com/docs/api/authentication#refresh-token
        """
        body = {
            "client_id": self.client_id,
            "token": token,
        }

        return self.authenticated_post(
            f"{self.protocol}://{self.domain}/oauth/revoke", data=body
        )
