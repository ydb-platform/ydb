from typing import Any

from .base import AuthenticationBase


class Social(AuthenticationBase):

    """Social provider's endpoints.

    Args:
        domain (str): Your auth0 domain (e.g: my-domain.us.auth0.com)
    """

    def login(self, access_token: str, connection: str, scope: str = "openid") -> Any:
        """Login using a social provider's access token

        Given the social provider's access_token and the connection specified,
        it will do the authentication on the provider and return a dict with
        the access_token and id_token. Currently, this endpoint only works for
        Facebook, Google, Twitter and Weibo.

        Args:
            access_token (str): social provider's access_token.

            connection (str): connection type (e.g: 'facebook')

        Returns:
            A dict with 'access_token' and 'id_token' keys.
        """

        return self.post(
            f"{self.protocol}://{self.domain}/oauth/access_token",
            data={
                "client_id": self.client_id,
                "access_token": access_token,
                "connection": connection,
                "scope": scope,
            },
        )
