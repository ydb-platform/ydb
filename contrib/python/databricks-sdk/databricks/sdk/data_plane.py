from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable, Optional
from urllib import parse

from databricks.sdk import oauth
from databricks.sdk.oauth import Token

URL_ENCODED_CONTENT_TYPE = "application/x-www-form-urlencoded"
JWT_BEARER_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
OIDC_TOKEN_PATH = "/oidc/v1/token"


class DataPlaneTokenSource:
    """
    EXPERIMENTAL Manages token sources for multiple DataPlane endpoints.
    """

    # TODO: Enable async once its stable. @oauth_credentials_provider must also have async enabled.
    def __init__(self, token_exchange_host: str, cpts: Callable[[], Token], disable_async: Optional[bool] = True):
        self._cpts = cpts
        self._token_exchange_host = token_exchange_host
        self._token_sources = {}
        self._disable_async = disable_async
        self._lock = threading.Lock()

    def token(self, endpoint, auth_details):
        key = f"{endpoint}:{auth_details}"

        # First, try to read without acquiring the lock to avoid contention.
        # Reads are atomic, so this is safe.
        token_source = self._token_sources.get(key)
        if token_source:
            return token_source.token()

        # If token_source is not found, acquire the lock and check again.
        with self._lock:
            # Another thread might have created it while we were waiting for the lock.
            token_source = self._token_sources.get(key)
            if not token_source:
                token_source = DataPlaneEndpointTokenSource(
                    self._token_exchange_host, self._cpts, auth_details, self._disable_async
                )
                self._token_sources[key] = token_source

        return token_source.token()


class DataPlaneEndpointTokenSource(oauth.Refreshable):
    """
    EXPERIMENTAL A token source for a specific DataPlane endpoint.
    """

    def __init__(self, token_exchange_host: str, cpts: Callable[[], Token], auth_details: str, disable_async: bool):
        super().__init__(disable_async=disable_async)
        self._auth_details = auth_details
        self._cpts = cpts
        self._token_exchange_host = token_exchange_host

    def refresh(self) -> Token:
        control_plane_token = self._cpts()
        headers = {"Content-Type": URL_ENCODED_CONTENT_TYPE}
        params = parse.urlencode(
            {
                "grant_type": JWT_BEARER_GRANT_TYPE,
                "authorization_details": self._auth_details,
                "assertion": control_plane_token.access_token,
            }
        )
        return oauth.retrieve_token(
            client_id="",
            client_secret="",
            token_url=self._token_exchange_host + OIDC_TOKEN_PATH,
            params=params,
            headers=headers,
        )


@dataclass
class DataPlaneDetails:
    """
    Contains details required to query a DataPlane endpoint.
    """

    endpoint_url: str
    """URL used to query the endpoint through the DataPlane."""
    token: Token
    """Token to query the DataPlane endpoint."""
