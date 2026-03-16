from typing import Any

import aiohttp

from kubernetes_asyncio.config.config_exception import ConfigException

GRANT_TYPE_REFRESH_TOKEN = "refresh_token"


class OpenIDRequestor:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        issuer_url: str,
        ssl_ca_cert: Any | None = None,
    ) -> None:
        """OpenIDRequestor implements a very limited subset of the oauth2 APIs that we
        require in order to refresh access tokens"""

        self._client_id = client_id
        self._client_secret = client_secret
        self._issuer_url = issuer_url
        self._ssl_ca_cert = ssl_ca_cert
        self._well_known = None

    def _get_connector(self) -> aiohttp.TCPConnector:
        return aiohttp.TCPConnector(
            verify_ssl=self._ssl_ca_cert is not None, ssl_context=self._ssl_ca_cert
        )

    def _client_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers=self._default_headers,
            connector=self._get_connector(),
            auth=aiohttp.BasicAuth(self._client_id, self._client_secret),
            raise_for_status=True,
            trust_env=True,
        )

    async def refresh_token(self, refresh_token: str) -> Any:
        """
        :param refresh_token: an openid refresh-token from a previous token request
        """
        async with self._client_session() as client:
            well_known = await self._get_well_known(client)

            try:
                return await self._post(
                    client,
                    well_known["token_endpoint"],
                    data={
                        "grant_type": GRANT_TYPE_REFRESH_TOKEN,
                        "refresh_token": refresh_token,
                    },
                )
            except aiohttp.ClientResponseError as e:
                raise ConfigException("oidc: failed to refresh access token") from e

    async def _get(
        self, client: aiohttp.ClientSession, *args: Any, **kwargs: Any
    ) -> Any:
        async with client.get(*args, **kwargs) as resp:
            return await resp.json()

    async def _post(
        self, client: aiohttp.ClientSession, *args: Any, **kwargs: Any
    ) -> Any:
        async with client.post(*args, **kwargs) as resp:
            return await resp.json()

    async def _get_well_known(self, client: aiohttp.ClientSession) -> Any:
        if self._well_known is None:
            try:
                self._well_known = await self._get(
                    client,
                    f"{self._issuer_url.rstrip('/')}/.well-known/openid-configuration",
                )
            except aiohttp.ClientResponseError as e:
                raise ConfigException(
                    "oidc: failed to query well-known metadata endpoint"
                ) from e

        return self._well_known

    @property
    def _default_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        }
