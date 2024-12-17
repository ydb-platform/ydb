# -*- coding: utf-8 -*-
import typing
import json
import abc
import os
import base64

try:
    import requests
except ImportError:
    requests = None

from ydb import credentials, tracing, issues
from .token_source import TokenSource, FixedTokenSource, JwtTokenSource


# method -> is HMAC
_supported_uppercase_jwt_algs = {
    "HS256": True,
    "HS384": True,
    "HS512": True,
    "RS256": False,
    "RS384": False,
    "RS512": False,
    "PS256": False,
    "PS384": False,
    "PS512": False,
    "ES256": False,
    "ES384": False,
    "ES512": False,
}


class Oauth2TokenExchangeCredentialsBase(abc.ABC):
    def __init__(
        self,
        token_endpoint: str,
        subject_token_source: typing.Optional[TokenSource] = None,
        actor_token_source: typing.Optional[TokenSource] = None,
        audience: typing.Union[typing.List[str], str, None] = None,
        scope: typing.Union[typing.List[str], str, None] = None,
        resource: typing.Union[typing.List[str], str, None] = None,
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

    @classmethod
    def _jwt_token_source_from_config(cls, cfg_json):
        signing_method = cls._required_string_from_config(cfg_json, "alg")
        is_hmac = _supported_uppercase_jwt_algs.get(signing_method.upper(), None)
        if is_hmac is not None:  # we know this method => do uppercase
            signing_method = signing_method.upper()
        private_key = cls._required_string_from_config(cfg_json, "private-key")
        if is_hmac:  # decode from base64
            private_key = base64.b64decode(private_key + "===")  # to allow unpadded strings
        return JwtTokenSource(
            signing_method=signing_method,
            private_key=private_key,
            key_id=cls._string_with_default_from_config(cfg_json, "kid", None),
            issuer=cls._string_with_default_from_config(cfg_json, "iss", None),
            subject=cls._string_with_default_from_config(cfg_json, "sub", None),
            audience=cls._list_of_strings_or_single_from_config(cfg_json, "aud"),
            id=cls._string_with_default_from_config(cfg_json, "jti", None),
            token_ttl_seconds=cls._duration_seconds_from_config(cfg_json, "ttl", 3600),
        )

    @classmethod
    def _fixed_token_source_from_config(cls, cfg_json):
        return FixedTokenSource(
            cls._required_string_from_config(cfg_json, "token"),
            cls._required_string_from_config(cfg_json, "token-type"),
        )

    @classmethod
    def _token_source_from_config(cls, cfg_json, key_name):
        value = cfg_json.get(key_name, None)
        if value is None:
            return None
        if not isinstance(value, dict):
            raise Exception('Key "{}" is expected to be a json map'.format(key_name))

        source_type = cls._required_string_from_config(value, "type")
        if source_type.upper() == "FIXED":
            return cls._fixed_token_source_from_config(value)
        if source_type.upper() == "JWT":
            return cls._jwt_token_source_from_config(value)
        raise Exception('"{}": unknown token source type: "{}"'.format(key_name, source_type))

    @classmethod
    def _list_of_strings_or_single_from_config(cls, cfg_json, key_name):
        value = cfg_json.get(key_name, None)
        if value is None:
            return None
        if isinstance(value, list):
            for val in value:
                if not isinstance(val, str) or not val:
                    raise Exception(
                        'Key "{}" is expected to be a single string or list of nonempty strings'.format(key_name)
                    )
            return value
        else:
            if isinstance(value, str):
                return value
            raise Exception('Key "{}" is expected to be a single string or list of nonempty strings'.format(key_name))

    @classmethod
    def _required_string_from_config(cls, cfg_json, key_name):
        value = cfg_json.get(key_name, None)
        if value is None or not isinstance(value, str) or not value:
            raise Exception('Key "{}" is expected to be a nonempty string'.format(key_name))
        return value

    @classmethod
    def _string_with_default_from_config(cls, cfg_json, key_name, default_value):
        value = cfg_json.get(key_name, None)
        if value is None:
            return default_value
        if not isinstance(value, str):
            raise Exception('Key "{}" is expected to be a string'.format(key_name))
        return value

    @classmethod
    def _duration_seconds_from_config(cls, cfg_json, key_name, default_value):
        value = cfg_json.get(key_name, None)
        if value is None:
            return default_value
        if not isinstance(value, str):
            raise Exception('Key "{}" is expected to be a string'.format(key_name))
        multiplier = 1
        if value.endswith("s"):
            multiplier = 1
            value = value[:-1]
        elif value.endswith("m"):
            multiplier = 60
            value = value[:-1]
        elif value.endswith("h"):
            multiplier = 3600
            value = value[:-1]
        elif value.endswith("d"):
            multiplier = 3600 * 24
            value = value[:-1]
        elif value.endswith("ms"):
            multiplier = 1.0 / 1000
            value = value[:-2]
        elif value.endswith("us"):
            multiplier = 1.0 / 1000000
            value = value[:-2]
        elif value.endswith("ns"):
            multiplier = 1.0 / 1000000000
            value = value[:-2]
        f = float(value)
        if f < 0.0:
            raise Exception("{}: negative duration is not allowed".format(value))
        return int(f * multiplier)

    @classmethod
    def from_file(cls, cfg_file, iam_endpoint=None):
        """
        Create OAuth 2.0 token exchange protocol credentials from config file.

        https://www.rfc-editor.org/rfc/rfc8693
        Config file must be a valid json file

        Fields of json file
            grant-type:           [string] Grant type option (default: "urn:ietf:params:oauth:grant-type:token-exchange")
            res:                  [string | list of strings] Resource option (optional)
            aud:                  [string | list of strings] Audience option for token exchange request (optional)
            scope:                [string | list of strings] Scope option (optional)
            requested-token-type: [string] Requested token type option (default: "urn:ietf:params:oauth:token-type:access_token")
            subject-credentials:  [creds_json] Subject credentials options (optional)
            actor-credentials:    [creds_json] Actor credentials options (optional)
            token-endpoint:       [string] Token endpoint

        Fields of creds_json (JWT):
            type:                 [string] Token source type. Set JWT
            alg:                  [string] Algorithm for JWT signature.
                                        Supported algorithms can be listed
                                        with GetSupportedOauth2TokenExchangeJwtAlgorithms()
            private-key:          [string] (Private) key in PEM format (RSA, EC) or Base64 format (HMAC) for JWT signature
            kid:                  [string] Key id JWT standard claim (optional)
            iss:                  [string] Issuer JWT standard claim (optional)
            sub:                  [string] Subject JWT standard claim (optional)
            aud:                  [string | list of strings] Audience JWT standard claim (optional)
            jti:                  [string] JWT ID JWT standard claim (optional)
            ttl:                  [string] Token TTL (default: 1h)

        Fields of creds_json (FIXED):
            type:                 [string] Token source type. Set FIXED
            token:                [string] Token value
            token-type:           [string] Token type value. It will become
                                        subject_token_type/actor_token_type parameter
                                        in token exchange request (https://www.rfc-editor.org/rfc/rfc8693)
        """
        with open(os.path.expanduser(cfg_file), "r") as r:
            cfg = r.read()

        return cls.from_content(cfg, iam_endpoint=iam_endpoint)

    @classmethod
    def from_content(cls, cfg, iam_endpoint=None):
        try:
            cfg_json = json.loads(cfg)
        except Exception as ex:
            raise Exception("Failed to parse json config: {}".format(ex))

        if iam_endpoint is not None:
            token_endpoint = iam_endpoint
        else:
            token_endpoint = cfg_json.get("token-endpoint", "")

        subject_token_source = cls._token_source_from_config(cfg_json, "subject-credentials")
        actor_token_source = cls._token_source_from_config(cfg_json, "actor-credentials")
        audience = cls._list_of_strings_or_single_from_config(cfg_json, "aud")
        scope = cls._list_of_strings_or_single_from_config(cfg_json, "scope")
        resource = cls._list_of_strings_or_single_from_config(cfg_json, "res")
        grant_type = cls._string_with_default_from_config(
            cfg_json, "grant-type", "urn:ietf:params:oauth:grant-type:token-exchange"
        )
        requested_token_type = cls._string_with_default_from_config(
            cfg_json, "requested-token-type", "urn:ietf:params:oauth:token-type:access_token"
        )

        return cls(
            token_endpoint=token_endpoint,
            subject_token_source=subject_token_source,
            actor_token_source=actor_token_source,
            audience=audience,
            scope=scope,
            resource=resource,
            grant_type=grant_type,
            requested_token_type=requested_token_type,
        )


class Oauth2TokenExchangeCredentials(credentials.AbstractExpiringTokenCredentials, Oauth2TokenExchangeCredentialsBase):
    def __init__(
        self,
        token_endpoint: str,
        subject_token_source: typing.Optional[TokenSource] = None,
        actor_token_source: typing.Optional[TokenSource] = None,
        audience: typing.Union[typing.List[str], str, None] = None,
        scope: typing.Union[typing.List[str], str, None] = None,
        resource: typing.Union[typing.List[str], str, None] = None,
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
