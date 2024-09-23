# -*- coding: utf-8 -*-
import typing
import json
import abc

try:
    import requests
except ImportError:
    requests = None

from ydb import credentials, tracing, issues
from .token_source import TokenSource


class Oauth2TokenExchangeCredentialsBase(abc.ABC):
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
        self._token_endpoint = token_endpoint
        self._subject_token_source = subject_token_source
        self._actor_token_source = actor_token_source
        self._audience = audience
        self._scope = scope
        self._resource = resource
        self._grant_type = grant_type
        self._requested_token_type = requested_token_type

        if not self._token_endpoint:
            raise Exception("Oauth2 token exchange: no token endpoint specified")

    def _process_response_status_code(self, content: str, status_code: int):
        if status_code == 403:
            raise issues.Unauthenticated(content)
        if status_code >= 500:
            raise issues.Unavailable(content)
        if status_code >= 400:
            raise issues.BadRequest(content)
        if status_code != 200:
            raise issues.Error(content)

    def _process_response_json(self, response_json):
        access_token = response_json["access_token"]
        expires_in = response_json["expires_in"]
        token_type = response_json["token_type"]
        scope = response_json.get("scope")
        if token_type.lower() != "bearer":
            raise Exception("Oauth2 token exchange: unsupported token type: {}".format(token_type))
        if expires_in <= 0:
            raise Exception("Oauth2 token exchange: incorrect expiration time: {}".format(expires_in))
        if scope and scope != self._get_scope_param():
            raise Exception(
                'Oauth2 token exchange: different scope. Expected: "{}", but got: "{}"'.format(
                    self._get_scope_param(), scope
                )
            )
        return {"access_token": "Bearer " + access_token, "expires_in": expires_in}

    def _get_scope_param(self) -> typing.Optional[str]:
        if self._scope is None:
            return None
        if isinstance(self._scope, str):
            return self._scope
        # list
        return " ".join(self._scope)

    def _make_token_request_params(self):
        params = {
            "grant_type": self._grant_type,
            "requested_token_type": self._requested_token_type,
        }
        if self._resource:
            params["resource"] = self._resource
        if self._audience:
            params["audience"] = self._audience
        scope = self._get_scope_param()
        if scope:
            params["scope"] = scope
        if self._subject_token_source is not None:
            t = self._subject_token_source.token()
            params["subject_token"] = t.token
            params["subject_token_type"] = t.token_type
        if self._actor_token_source is not None:
            t = self._actor_token_source.token()
            params["actor_token"] = t.token
            params["actor_token_type"] = t.token_type

        return params


class Oauth2TokenExchangeCredentials(credentials.AbstractExpiringTokenCredentials, Oauth2TokenExchangeCredentialsBase):
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
        tracer=None,
    ):
        super(Oauth2TokenExchangeCredentials, self).__init__(tracer)
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

    @tracing.with_trace()
    def _make_token_request(self):
        assert (
            requests is not None
        ), "Install requests library to use Oauth2TokenExchangeCredentials credentials provider"

        params = self._make_token_request_params()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self._token_endpoint, data=params, headers=headers)
        self._process_response_status_code(response.content, response.status_code)
        response_json = json.loads(response.content)
        return self._process_response_json(response_json)
