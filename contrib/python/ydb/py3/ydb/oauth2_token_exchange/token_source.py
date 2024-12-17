# -*- coding: utf-8 -*-
import abc
import typing
import time
import os
from datetime import datetime

try:
    import jwt
except ImportError:
    jwt = None


class Token(abc.ABC):
    def __init__(self, token: str, token_type: str):
        self.token = token
        self.token_type = token_type


class TokenSource(abc.ABC):
    @abc.abstractmethod
    def token(self) -> Token:
        """
        :return: A Token object ready for exchange
        """
        pass


class FixedTokenSource(TokenSource):
    def __init__(self, token: str, token_type: str):
        self._token = Token(token, token_type)

    def token(self) -> Token:
        return self._token


class JwtTokenSource(TokenSource):
    def __init__(
        self,
        signing_method: str,
        private_key: typing.Optional[str] = None,
        private_key_file: typing.Optional[str] = None,
        key_id: typing.Optional[str] = None,
        issuer: typing.Optional[str] = None,
        subject: typing.Optional[str] = None,
        audience: typing.Union[typing.List[str], str, None] = None,
        id: typing.Optional[str] = None,
        token_ttl_seconds: int = 3600,
    ):
        assert jwt is not None, "Install pyjwt library to use jwt tokens"
        self._signing_method = signing_method
        self._key_id = key_id
        if private_key and private_key_file:
            raise Exception("JWT: both private_key and private_key_file are set")
        self._private_key = ""
        if private_key:
            self._private_key = private_key
        if private_key_file:
            private_key_file = os.path.expanduser(private_key_file)
            with open(private_key_file, "r") as key_file:
                self._private_key = key_file.read()
        self._issuer = issuer
        self._subject = subject
        self._audience = audience
        self._id = id
        self._token_ttl_seconds = token_ttl_seconds
        if not self._signing_method:
            raise Exception("JWT: no signing method specified")
        if not self._private_key:
            raise Exception("JWT: no private key specified")
        if self._token_ttl_seconds <= 0:
            raise Exception("JWT: invalid jwt token TTL")

    def token(self) -> Token:
        now = time.time()
        now_utc = datetime.utcfromtimestamp(now)
        exp_utc = datetime.utcfromtimestamp(now + self._token_ttl_seconds)
        payload = {
            "iat": now_utc,
            "exp": exp_utc,
        }
        if self._audience:
            payload["aud"] = self._audience
        if self._issuer:
            payload["iss"] = self._issuer
        if self._subject:
            payload["sub"] = self._subject
        if self._id:
            payload["jti"] = self._id

        headers = {
            "alg": self._signing_method,
            "typ": "JWT",
        }
        if self._key_id:
            headers["kid"] = self._key_id

        token = jwt.encode(
            key=self._private_key,
            algorithm=self._signing_method,
            headers=headers,
            payload=payload,
        )
        return Token(token, "urn:ietf:params:oauth:token-type:jwt")
