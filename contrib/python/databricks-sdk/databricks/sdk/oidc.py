"""
Package oidc provides utilities for working with OIDC ID tokens.

This package is experimental and subject to change.
"""

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from . import oauth

logger = logging.getLogger(__name__)


@dataclass
class IdToken:
    """Represents an OIDC ID token that can be exchanged for a Databricks access token.

    Parameters
    ----------
    jwt : str
        The signed JWT token string.
    """

    jwt: str


class IdTokenSource(ABC):
    """Abstract base class representing anything that returns an IDToken.

    This class defines the interface for token sources that can provide OIDC ID tokens.
    """

    @abstractmethod
    def id_token(self) -> IdToken:
        """Get an ID token.

        Returns
        -------
        IdToken
            An ID token.

        Raises
        ------
        Exception
            Implementation specific exceptions.
        """


class EnvIdTokenSource(IdTokenSource):
    """IDTokenSource that reads the ID token from an environment variable.

    Parameters
    ----------
    env_var : str
        The name of the environment variable containing the ID token.
    """

    def __init__(self, env_var: str):
        self.env_var = env_var

    def id_token(self) -> IdToken:
        """Get an ID token from an environment variable.

        Returns
        -------
        IdToken
            An ID token.

        Raises
        ------
        ValueError
            If the environment variable is not set.
        """
        token = os.getenv(self.env_var)
        if not token:
            raise ValueError(f"Missing env var {self.env_var!r}")
        return IdToken(jwt=token)


class FileIdTokenSource(IdTokenSource):
    """IDTokenSource that reads the ID token from a file.

    Parameters
    ----------
    path : str
        The path to the file containing the ID token.
    """

    def __init__(self, path: str):
        self.path = path

    def id_token(self) -> IdToken:
        """Get an ID token from a file.

        Returns
        -------
        IdToken
            An ID token.

        Raises
        ------
        ValueError
            If the file is empty, does not exist, or cannot be read.
        """
        if not self.path:
            raise ValueError("Missing path")

        token = None
        try:
            with open(self.path, "r") as f:
                token = f.read().strip()
        except FileNotFoundError:
            raise ValueError(f"File {self.path!r} does not exist")
        except Exception as e:
            raise ValueError(f"Error reading token file: {str(e)}")

        if not token:
            raise ValueError(f"File {self.path!r} is empty")
        return IdToken(jwt=token)


class DatabricksOidcTokenSource(oauth.TokenSource):
    """A TokenSource which exchanges a token using Workload Identity Federation.

    Parameters
    ----------
    host : str
        The host of the Databricks account or workspace.
    id_token_source : IdTokenSource
        IDTokenSource that returns the IDToken to be used for the token exchange.
    token_endpoint_provider : Callable[[], dict]
        Returns the token endpoint for the Databricks OIDC application.
    client_id : Optional[str], optional
        ClientID of the Databricks OIDC application. It corresponds to the
        Application ID of the Databricks Service Principal. Only required for
        Workload Identity Federation and should be empty for Account-wide token
        federation.
    account_id : Optional[str], optional
        The account ID of the Databricks Account. Only required for
        Account-wide token federation.
    audience : Optional[str], optional
        The audience of the Databricks OIDC application. Only used for
        Workspace level tokens.
    """

    def __init__(
        self,
        host: str,
        token_endpoint: str,
        id_token_source: IdTokenSource,
        client_id: Optional[str] = None,
        account_id: Optional[str] = None,
        audience: Optional[str] = None,
        disable_async: bool = False,
        scopes: Optional[str] = None,
    ):
        self._host = host
        self._id_token_source = id_token_source
        self._token_endpoint = token_endpoint
        self._client_id = client_id
        self._account_id = account_id
        self._audience = audience
        self._disable_async = disable_async
        self._scopes = scopes

    def token(self) -> oauth.Token:
        """Get a token by exchanging the ID token.

        Returns
        -------
        dict
            The exchanged token.

        Raises
        ------
        ValueError
            If the host is missing or other configuration errors occur.
        """
        if not self._host:
            logger.debug("Missing Host")
            raise ValueError("missing Host")

        if not self._client_id:
            logger.debug("No ClientID provided, authenticating with Account-wide token federation")
        else:
            logger.debug("Client ID provided, authenticating with Workload Identity Federation")

        id_token = self._id_token_source.id_token()
        return self._exchange_id_token(id_token)

    # This function is used to create the OAuth client.
    # It exists to make it easier to test.
    def _exchange_id_token(self, id_token: IdToken) -> oauth.Token:
        client = oauth.ClientCredentials(
            client_id=self._client_id,
            client_secret="",  # there is no (rotatable) secrets in the OIDC flow
            token_url=self._token_endpoint,
            endpoint_params={
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "subject_token": id_token.jwt,
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            },
            scopes=self._scopes,
            use_params=True,
            disable_async=self._disable_async,
        )

        return client.token()
