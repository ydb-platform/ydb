# -*- coding: utf-8 -*-
from .credentials import AbstractExpiringTokenCredentials
from ydb.oauth2_token_exchange.token_source import TokenSource
from ydb.oauth2_token_exchange.token_exchange import Oauth2TokenExchangeCredentialsBase
import typing

try:
    import aiohttp
except ImportError:
    aiohttp = None


class Oauth2TokenExchangeCredentials(AbstractExpiringTokenCredentials, Oauth2TokenExchangeCredentialsBase):
    def __init__(
        self,
        token_endpoint: str,
        subject_token_source: typing.Optional[TokenSource] = None,
        actor_token_source: typing.Optional[TokenSource] = None,
        audience: typing.Union[typing.List[str], str, None] = None,
        scope: typing.Union[typing.List[str], str, None] = None,
        resource: typing.Optional[str] = None,
        grant_type: str = "urn:ietf:params:oauth:grant-type:token-exchange",
        requested_token_type: str = "urn:ietf:params:oauth:token-type:access_token",
    ):
        assert aiohttp is not None, "Install aiohttp library to use OAuth 2.0 token exchange credentials provider"
        super(Oauth2TokenExchangeCredentials, self).__init__()
        Oauth2TokenExchangeCredentialsBase.__init__(
            self,
            token_endpoint,
            subject_token_source,
            actor_token_source,
            audience,
            scope,
            resource,
            grant_type,
            requested_token_type,
        )

    async def _make_token_request(self):
        params = self._make_token_request_params()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        timeout = aiohttp.ClientTimeout(total=2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(self._token_endpoint, data=params, headers=headers) as response:
                self._process_response_status_code(await response.text(), response.status)
                return self._process_response_json(await response.json())
