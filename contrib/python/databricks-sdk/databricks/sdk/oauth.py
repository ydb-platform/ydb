import base64
import functools
import hashlib
import json
import logging
import os
import secrets
import threading
import urllib.parse
import webbrowser
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, List, Optional

import requests
import requests.auth

from ._base_client import _BaseClient, _fix_host_if_needed

# Error code for PKCE flow in Azure Active Directory, that gets additional retry.
# See https://stackoverflow.com/a/75466778/277035 for more info
NO_ORIGIN_FOR_SPA_CLIENT_ERROR = "AADSTS9002327"

URL_ENCODED_CONTENT_TYPE = "application/x-www-form-urlencoded"
JWT_BEARER_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
OIDC_TOKEN_PATH = "/oidc/v1/token"

logger = logging.getLogger(__name__)


@dataclass
class AuthorizationDetail:
    type: str
    object_type: str
    object_path: str
    actions: List[str]

    def as_dict(self) -> dict:
        return {
            "type": self.type,
            "object_type": self.object_type,
            "object_path": self.object_path,
            "actions": self.actions,
        }

    def from_dict(self, d: dict) -> "AuthorizationDetail":
        return AuthorizationDetail(
            type=d.get("type"),
            object_type=d.get("object_type"),
            object_path=d.get("object_path"),
            actions=d.get("actions"),
        )


class IgnoreNetrcAuth(requests.auth.AuthBase):
    """This auth method is a no-op.

    We use it to force requestslib to not use .netrc to write auth headers
    when making .post() requests to the oauth token endpoints, since these
    don't require authentication.

    In cases where .netrc is outdated or corrupt, these requests will fail.

    See issue #121
    """

    def __call__(self, r):
        return r


@dataclass
class OidcEndpoints:
    """
    The endpoints used for OAuth-based authentication in Databricks.
    """

    authorization_endpoint: str  # ../v1/authorize
    """The authorization endpoint for the OAuth flow. The user-agent should be directed to this endpoint in order for
    the user to login and authorize the client for user-to-machine (U2M) flows."""

    token_endpoint: str  # ../v1/token
    """The token endpoint for the OAuth flow."""

    @staticmethod
    def from_dict(d: dict) -> "OidcEndpoints":
        return OidcEndpoints(
            authorization_endpoint=d.get("authorization_endpoint"),
            token_endpoint=d.get("token_endpoint"),
        )

    def as_dict(self) -> dict:
        return {
            "authorization_endpoint": self.authorization_endpoint,
            "token_endpoint": self.token_endpoint,
        }


@dataclass
class Token:
    access_token: str
    token_type: Optional[str] = None
    refresh_token: Optional[str] = None
    expiry: Optional[datetime] = None

    @property
    def expired(self):
        if not self.expiry:
            return False
        # Azure Databricks rejects tokens that expire in 30 seconds or less,
        # so we refresh the token 40 seconds before it expires.
        potentially_expired = self.expiry - timedelta(seconds=40)
        now = datetime.now(tz=potentially_expired.tzinfo)
        is_expired = potentially_expired < now
        return is_expired

    @property
    def valid(self):
        return self.access_token and not self.expired

    def as_dict(self) -> dict:
        raw = {
            "access_token": self.access_token,
            "token_type": self.token_type,
        }
        if self.expiry:
            raw["expiry"] = self.expiry.isoformat()
        if self.refresh_token:
            raw["refresh_token"] = self.refresh_token
        return raw

    @staticmethod
    def from_dict(raw: dict) -> "Token":
        return Token(
            access_token=raw["access_token"],
            token_type=raw["token_type"],
            expiry=datetime.fromisoformat(raw["expiry"]),
            refresh_token=raw.get("refresh_token"),
        )

    def jwt_claims(self) -> Dict[str, str]:
        """Get claims from the access token or return an empty dictionary if it is not a JWT token.

        All refreshable tokens we're dealing with are JSON Web Tokens (JWT).

        The common claims are:
        - 'aud' represents the intended recipient of the token. In case of Azure, this is an app's Application ID
                assigned within the Azure portal.
        - 'iss' serves to identify the security token service (STS) responsible for creating and delivering the token.
                In case of Azure, it includes the Azure AD tenant where user authentication occurred.
        - 'appid' stands for the application ID of the client utilizing this token. This application can operate either
                autonomously or on behalf of a user. The application ID commonly represents an application object but
                may also denote a service principal object in case of Azure.
        - 'idp' is used to document the identity provider that authenticated the subject of the token.
        - 'oid' is the unchanging identifier for an entity within the identity system.
        - 'sub' identifies the primary entity for the token, such as the user of an app. This value is specific to
                a particular application ID. If a single user logs into two different apps using distinct client IDs,
                these apps will receive different values for the subject claim.
        - 'tid' In case of Azure, this value represents Azure Tenant ID.

        See https://datatracker.ietf.org/doc/html/rfc7519 for specification.
        See https://jwt.ms for debugger.
        """
        try:
            jwt_split = self.access_token.split(".")
            if len(jwt_split) != 3:
                logger.debug(f"Tried to decode access token as JWT, but failed: {len(jwt_split)} components")
                return {}
            payload_with_padding = jwt_split[1] + "=="
            payload_bytes = base64.standard_b64decode(payload_with_padding)
            payload_json = payload_bytes.decode("utf8")
            claims = json.loads(payload_json)
            return claims
        except ValueError as err:
            logger.debug(f"Tried to decode access token as JWT, but failed: {err}")
            return {}


class TokenSource:
    @abstractmethod
    def token(self) -> Token:
        pass


def retrieve_token(
    client_id,
    client_secret,
    token_url,
    params,
    use_params=False,
    use_header=False,
    headers=None,
) -> Token:
    logger.debug(f"Retrieving token for {client_id}")
    if use_params:
        if client_id:
            params["client_id"] = client_id
        if client_secret:
            params["client_secret"] = client_secret
    auth = None
    if use_header:
        auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    else:
        auth = IgnoreNetrcAuth()
    resp = requests.post(token_url, params, auth=auth, headers=headers)
    if not resp.ok:
        if resp.headers["Content-Type"].startswith("application/json"):
            err = resp.json()
            code = err.get("errorCode", err.get("error", "unknown"))
            summary = err.get("errorSummary", err.get("error_description", "unknown"))
            summary = summary.replace("\r\n", " ")
            raise ValueError(f"{code}: {summary}")
        raise ValueError(resp.content)
    try:
        j = resp.json()
        expires_in = int(j["expires_in"])
        expiry = datetime.now() + timedelta(seconds=expires_in)
        return Token(
            access_token=j["access_token"],
            refresh_token=j.get("refresh_token"),
            token_type=j["token_type"],
            expiry=expiry,
        )
    except Exception as e:
        raise NotImplementedError(f"Not supported yet: {e}")


class _TokenState(Enum):
    """
    Represents the state of a token. Each token can be in one of
    the following three states:
      - FRESH: The token is valid.
      - STALE: The token is valid but will expire soon.
      - EXPIRED: The token has expired and cannot be used.
    """

    FRESH = 1  # The token is valid.
    STALE = 2  # The token is valid but will expire soon.
    EXPIRED = 3  # The token has expired and cannot be used.


class Refreshable(TokenSource):
    """A token source that supports refreshing expired tokens."""

    _EXECUTOR = None
    _EXECUTOR_LOCK = threading.Lock()
    # Default duration for the stale period. This value is chosen to cover the
    # maximum monthly downtime allowed by a 99.99% uptime SLA (~4.38 minutes).
    _DEFAULT_STALE_DURATION = timedelta(minutes=5)

    @classmethod
    def _get_executor(cls):
        """Lazy initialization of the ThreadPoolExecutor."""
        if cls._EXECUTOR is None:
            with cls._EXECUTOR_LOCK:
                if cls._EXECUTOR is None:
                    # This thread pool has multiple workers because it is shared by all instances of Refreshable.
                    cls._EXECUTOR = ThreadPoolExecutor(max_workers=10)
        return cls._EXECUTOR

    def __init__(
        self,
        token: Optional[Token] = None,
        disable_async: bool = True,
        stale_duration: timedelta = _DEFAULT_STALE_DURATION,
    ):
        # Config properties
        self._stale_duration = stale_duration
        self._disable_async = disable_async
        # Lock
        self._lock = threading.Lock()
        # Non Thread safe properties. They should be accessed only when protected by the lock above.
        self._token = token or Token("")
        self._is_refreshing = False
        self._refresh_err = False

    # This is the main entry point for the Token. Do not access the token
    # using any of the internal functions.
    def token(self) -> Token:
        """Returns a valid token, blocking if async refresh is disabled."""
        with self._lock:
            if self._disable_async:
                return self._blocking_token()
            return self._async_token()

    def _async_token(self) -> Token:
        """
        Returns a token.
        If the token is stale, triggers an asynchronous refresh.
        If the token is expired, refreshes it synchronously, blocking until the refresh is complete.
        """
        state = self._token_state()
        token = self._token

        if state == _TokenState.FRESH:
            return token
        if state == _TokenState.STALE:
            self._trigger_async_refresh()
            return token
        return self._blocking_token()

    def _token_state(self) -> _TokenState:
        """Returns the current state of the token."""
        if not self._token or not self._token.valid:
            return _TokenState.EXPIRED
        if not self._token.expiry:
            return _TokenState.FRESH

        lifespan = self._token.expiry - datetime.now()
        if lifespan < timedelta(seconds=0):
            return _TokenState.EXPIRED
        if lifespan < self._stale_duration:
            return _TokenState.STALE
        return _TokenState.FRESH

    def _blocking_token(self) -> Token:
        """Returns a token, blocking if necessary to refresh it."""
        state = self._token_state()
        # This is important to recover from potential previous failed attempts
        # to refresh the token asynchronously.
        self._refresh_err = False
        self._is_refreshing = False

        # It's possible that the token got refreshed (either by a _blocking_refresh or
        # an _async_refresh call) while this particular call was waiting to acquire
        # the lock. This check avoids refreshing the token again in such cases.
        if state != _TokenState.EXPIRED:
            return self._token

        self._token = self.refresh()
        return self._token

    def _trigger_async_refresh(self):
        """Starts an asynchronous refresh if none is in progress."""

        def _refresh_internal():
            new_token = None
            try:
                new_token = self.refresh()
            except Exception as e:
                # This happens on a thread, so we don't want to propagate the error.
                # Instead, if there is no new_token for any reason, we will disable async refresh below
                # But we will do it inside the lock.
                logger.warning(f"Tried to refresh token asynchronously, but failed: {e}")

            with self._lock:
                if new_token is not None:
                    self._token = new_token
                else:
                    self._refresh_err = True
                self._is_refreshing = False

        # The token may have been refreshed by another thread.
        if self._token_state() == _TokenState.FRESH:
            return
        if not self._is_refreshing and not self._refresh_err:
            self._is_refreshing = True
            Refreshable._get_executor().submit(_refresh_internal)

    @abstractmethod
    def refresh(self) -> Token:
        pass


class _OAuthCallback(BaseHTTPRequestHandler):
    def __init__(self, feedback: list, *args):
        self._feedback = feedback
        super().__init__(*args)

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.debug(fmt, *args)

    def do_GET(self):
        from urllib.parse import parse_qsl

        parts = self.path.split("?")
        if len(parts) != 2:
            self.send_error(400, "Missing Query")
            return

        query = dict(parse_qsl(parts[1]))
        self._feedback.append(query)

        if "error" in query:
            self.send_error(400, query["error"], query.get("error_description"))
            return

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        # TODO: show better message
        self.wfile.write(b"You can close this tab.")


@dataclass
class HostMetadata:
    """Parsed response from the /.well-known/databricks-config discovery endpoint."""

    oidc_endpoint: str
    account_id: Optional[str] = None
    workspace_id: Optional[str] = None

    @staticmethod
    def from_dict(d: dict) -> "HostMetadata":
        return HostMetadata(
            oidc_endpoint=d.get("oidc_endpoint", ""),
            account_id=d.get("account_id"),
            workspace_id=d.get("workspace_id"),
        )

    def as_dict(self) -> dict:
        return {
            "oidc_endpoint": self.oidc_endpoint,
            "account_id": self.account_id,
            "workspace_id": self.workspace_id,
        }


def get_host_metadata(host: str, client: _BaseClient = _BaseClient()) -> HostMetadata:
    """
    [Experimental] Fetch the raw Databricks well-known configuration from {host}/.well-known/databricks-config.

    :param host: The Databricks host (workspace or account console).
    :return: Parsed :class:`HostMetadata` as returned by the server.
    """
    host = _fix_host_if_needed(host)
    try:
        resp = client.do("GET", f"{host}/.well-known/databricks-config")
    except Exception as e:
        raise ValueError(f"Failed to fetch host metadata from {host}/.well-known/databricks-config: {e}") from e
    return HostMetadata.from_dict(resp)


def get_endpoints_from_url(url: str, client: _BaseClient = _BaseClient()) -> OidcEndpoints:
    """
    Fetch OIDC endpoints directly from a discovery URL.

    :param url: Full URL to the OIDC discovery document (e.g. the value of discovery_url config).
    :return: Parsed :class:`OidcEndpoints`.
    """
    resp = client.do("GET", url)
    return OidcEndpoints.from_dict(resp)


def get_account_endpoints(host: str, account_id: str, client: _BaseClient = _BaseClient()) -> OidcEndpoints:
    """
    Get the OIDC endpoints for a given account.
    :param host: The Databricks account host.
    :param account_id: The account ID.
    :return: The account's OIDC endpoints.
    """
    host = _fix_host_if_needed(host)
    oidc = f"{host}/oidc/accounts/{account_id}/.well-known/oauth-authorization-server"
    resp = client.do("GET", oidc)
    return OidcEndpoints.from_dict(resp)


def get_workspace_endpoints(host: str, client: _BaseClient = _BaseClient()) -> OidcEndpoints:
    """
    Get the OIDC endpoints for a given workspace.
    :param host: The Databricks workspace host.
    :return: The workspace's OIDC endpoints.
    """
    host = _fix_host_if_needed(host)
    oidc = f"{host}/oidc/.well-known/oauth-authorization-server"
    resp = client.do("GET", oidc)
    return OidcEndpoints.from_dict(resp)


def get_unified_endpoints(host: str, account_id: str, client: _BaseClient = _BaseClient()) -> OidcEndpoints:
    """
    Get the OIDC endpoints for a unified host.
    :param host: The Databricks unified host.
    :param account_id: The account ID.
    :return: The OIDC endpoints for the unified host.
    """
    host = _fix_host_if_needed(host)
    oidc = f"{host}/oidc/accounts/{account_id}/.well-known/oauth-authorization-server"
    resp = client.do("GET", oidc)
    return OidcEndpoints.from_dict(resp)


def get_azure_entra_id_workspace_endpoints(
    host: str,
) -> Optional[OidcEndpoints]:
    """
    Get the Azure Entra ID endpoints for a given workspace. Can only be used when authenticating to Azure Databricks
    using an application registered in Azure Entra ID.
    :param host: The Databricks workspace host.
    :return: The OIDC endpoints for the workspace's Azure Entra ID tenant.
    """
    # In Azure, this workspace endpoint redirects to the Entra ID authorization endpoint
    host = _fix_host_if_needed(host)
    res = requests.get(f"{host}/oidc/oauth2/v2.0/authorize", allow_redirects=False)
    real_auth_url = res.headers.get("location")
    if not real_auth_url:
        return None
    return OidcEndpoints(
        authorization_endpoint=real_auth_url,
        token_endpoint=real_auth_url.replace("/authorize", "/token"),
    )


class SessionCredentials(Refreshable):
    def __init__(
        self,
        token: Token,
        token_endpoint: str,
        client_id: str,
        client_secret: str = None,
        redirect_url: str = None,
        disable_async: bool = True,
    ):
        self._token_endpoint = token_endpoint
        self._client_id = client_id
        self._client_secret = client_secret
        self._redirect_url = redirect_url
        super().__init__(
            token=token,
            disable_async=disable_async,
        )

    def as_dict(self) -> dict:
        return {"token": self.token().as_dict()}

    @staticmethod
    def from_dict(
        raw: dict,
        token_endpoint: str,
        client_id: str,
        client_secret: str = None,
        redirect_url: str = None,
    ) -> "SessionCredentials":
        return SessionCredentials(
            token=Token.from_dict(raw["token"]),
            token_endpoint=token_endpoint,
            client_id=client_id,
            client_secret=client_secret,
            redirect_url=redirect_url,
        )

    def auth_type(self):
        """Implementing CredentialsProvider protocol"""
        # TODO: distinguish between Databricks IDP and Azure AD
        return "oauth"

    def __call__(self, *args, **kwargs):
        """Implementing CredentialsProvider protocol"""

        def inner() -> Dict[str, str]:
            return {"Authorization": f"Bearer {self.token().access_token}"}

        return inner

    def refresh(self) -> Token:
        refresh_token = self._token.refresh_token
        if not refresh_token:
            raise ValueError("oauth2: token expired and refresh token is not set")
        params = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        headers = {}
        if "microsoft" in self._token_endpoint:
            # Tokens issued for the 'Single-Page Application' client-type may
            # only be redeemed via cross-origin requests
            headers = {"Origin": self._redirect_url}
        return retrieve_token(
            client_id=self._client_id,
            client_secret=self._client_secret,
            token_url=self._token_endpoint,
            params=params,
            use_params=True,
            headers=headers,
        )


class Consent:
    def __init__(
        self,
        state: str,
        verifier: str,
        authorization_url: str,
        redirect_url: str,
        token_endpoint: str,
        client_id: str,
        client_secret: str = None,
    ) -> None:
        self._verifier = verifier
        self._state = state
        self._authorization_url = authorization_url
        self._redirect_url = redirect_url
        self._token_endpoint = token_endpoint
        self._client_id = client_id
        self._client_secret = client_secret

    def as_dict(self) -> dict:
        return {
            "state": self._state,
            "verifier": self._verifier,
            "authorization_url": self._authorization_url,
            "redirect_url": self._redirect_url,
            "token_endpoint": self._token_endpoint,
            "client_id": self._client_id,
        }

    @property
    def authorization_url(self) -> str:
        return self._authorization_url

    @staticmethod
    def from_dict(raw: dict, client_secret: str = None) -> "Consent":
        return Consent(
            raw["state"],
            raw["verifier"],
            authorization_url=raw["authorization_url"],
            redirect_url=raw["redirect_url"],
            token_endpoint=raw["token_endpoint"],
            client_id=raw["client_id"],
            client_secret=client_secret,
        )

    def launch_external_browser(self) -> SessionCredentials:
        redirect_url = urllib.parse.urlparse(self._redirect_url)
        if redirect_url.hostname not in ("localhost", "127.0.0.1"):
            raise ValueError(f"cannot listen on {redirect_url.hostname}")
        feedback = []
        logger.info(f"Opening {self._authorization_url} in a browser")
        webbrowser.open_new(self._authorization_url)
        port = redirect_url.port
        handler_factory = functools.partial(_OAuthCallback, feedback)
        with HTTPServer(("localhost", port), handler_factory) as httpd:
            logger.info(f"Waiting for redirect to http://localhost:{port}")
            httpd.handle_request()
        if not feedback:
            raise ValueError("No data received in callback")
        query = feedback.pop()
        return self.exchange_callback_parameters(query)

    def exchange_callback_parameters(self, query: Dict[str, str]) -> SessionCredentials:
        if "error" in query:
            raise ValueError("{error}: {error_description}".format(**query))
        if "code" not in query or "state" not in query:
            raise ValueError("No code returned in callback")
        return self.exchange(query["code"], query["state"])

    def exchange(self, code: str, state: str) -> SessionCredentials:
        if self._state != state:
            raise ValueError("state mismatch")
        params = {
            "redirect_uri": self._redirect_url,
            "grant_type": "authorization_code",
            "code_verifier": self._verifier,
            "code": code,
        }
        headers = {}
        while True:
            try:
                token = retrieve_token(
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    token_url=self._token_endpoint,
                    params=params,
                    headers=headers,
                    use_params=True,
                )
                return SessionCredentials(
                    token,
                    self._token_endpoint,
                    self._client_id,
                    self._client_secret,
                    self._redirect_url,
                )
            except ValueError as e:
                if NO_ORIGIN_FOR_SPA_CLIENT_ERROR in str(e):
                    # Retry in cases of 'Single-Page Application' client-type with
                    # 'Origin' header equal to client's redirect URL.
                    headers["Origin"] = self._redirect_url
                    msg = f"Retrying OAuth token exchange with {self._redirect_url} origin"
                    logger.debug(msg)
                    continue
                raise e


class OAuthClient:
    """Enables 3-legged OAuth2 flow with PKCE

    For a regular web app running on a server, it's recommended to use
    the Authorization Code Flow to obtain an Access Token and a Refresh
    Token. This method is considered safe because the Access Token is
    transmitted directly to the server hosting the app, without passing
    through the user's web browser and risking exposure.

    To enhance the security of the Authorization Code Flow, the PKCE
    (Proof Key for Code Exchange) mechanism can be employed. With PKCE,
    the calling application generates a secret called the Code Verifier,
    which is verified by the authorization server. The app also creates
    a transform value of the Code Verifier, called the Code Challenge,
    and sends it over HTTPS to obtain an Authorization Code.
    By intercepting the Authorization Code, a malicious attacker cannot
    exchange it for a token without possessing the Code Verifier.
    """

    def __init__(
        self,
        oidc_endpoints: OidcEndpoints,
        redirect_url: str,
        client_id: str,
        scopes: List[str] = None,
        client_secret: str = None,
    ):
        if not scopes:
            # Default for direct OAuthClient users (e.g., via from_host()).
            # When used via credentials_provider.external_browser(), scopes are always
            # passed explicitly from Config.get_scopes(), with offline_access handling
            # controlled by the disable_oauth_refresh_token flag.
            scopes = ["all-apis", "offline_access"]

        self.redirect_url = redirect_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._oidc_endpoints = oidc_endpoints
        self._scopes = scopes

    @staticmethod
    def from_host(
        host: str,
        client_id: str,
        redirect_url: str,
        *,
        scopes: List[str] = None,
        client_secret: str = None,
    ) -> "OAuthClient":
        from .core import Config
        from .credentials_provider import credentials_strategy

        @credentials_strategy("noop", [])
        def noop_credentials(_: any):
            return lambda: {}

        config = Config(host=host, credentials_strategy=noop_credentials)
        oidc = config.databricks_oidc_endpoints
        if not oidc:
            raise ValueError(f"{host} does not support OAuth")
        return OAuthClient(oidc, redirect_url, client_id, scopes, client_secret)

    def initiate_consent(self) -> Consent:
        state = secrets.token_urlsafe(16)

        # token_urlsafe() already returns base64-encoded string
        verifier = secrets.token_urlsafe(32)
        digest = hashlib.sha256(verifier.encode("UTF-8")).digest()
        challenge = base64.urlsafe_b64encode(digest).decode("UTF-8").replace("=", "")

        params = {
            "response_type": "code",
            "client_id": self._client_id,
            "redirect_uri": self.redirect_url,
            "scope": " ".join(self._scopes),
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
        auth_url = f"{self._oidc_endpoints.authorization_endpoint}?{urllib.parse.urlencode(params)}"
        return Consent(
            state,
            verifier,
            authorization_url=auth_url,
            redirect_url=self.redirect_url,
            token_endpoint=self._oidc_endpoints.token_endpoint,
            client_id=self._client_id,
            client_secret=self._client_secret,
        )

    def __repr__(self) -> str:
        return f"<OAuthClient client_id={self._client_id} token_url={self._oidc_endpoints.token_endpoint} auth_url={self._oidc_endpoints.authorization_endpoint}>"


@dataclass
class ClientCredentials(Refreshable):
    """Enables client credentials 2-legged OAuth2 flow

    When it comes to authorizing machine-to-machine interactions,
    the need for end-user authorization is eliminated because the SDK
    functions as both the Resource Owner and Client. Typical example is
    the CI/CD process or any other automated job. In this scenario,
    the background job uses the Client ID and Client Secret to obtain
    an Access Token from the Authorization Server.
    """

    client_id: str
    client_secret: str
    token_url: str
    endpoint_params: dict = None
    scopes: str = None
    use_params: bool = False
    use_header: bool = False
    disable_async: bool = True
    authorization_details: str = None

    def __post_init__(self):
        super().__init__(disable_async=self.disable_async)

    def refresh(self) -> Token:
        params = {"grant_type": "client_credentials"}
        if self.scopes:
            params["scope"] = self.scopes
        if self.authorization_details:
            params["authorization_details"] = self.authorization_details
        if self.endpoint_params:
            for k, v in self.endpoint_params.items():
                params[k] = v
        return retrieve_token(
            self.client_id,
            self.client_secret,
            self.token_url,
            params,
            use_params=self.use_params,
            use_header=self.use_header,
        )


@dataclass
class PATOAuthTokenExchange(Refreshable):
    """Performs OAuth token exchange using a Personal Access Token (PAT) as the subject token.

    This class implements the OAuth 2.0 Token Exchange flow (RFC 8693) to exchange a Databricks
    Internal PAT Token for an access token with specific scopes and authorization details.

    Args:
        get_original_token: A callable that returns the PAT to be exchanged. This is a callable
            rather than a string value to ensure that a fresh Internal PAT Token is retrieved
            at the time of refresh.
        host: The Databricks workspace URL (e.g., "https://my-workspace.cloud.databricks.com").
        scopes: Space-delimited string of OAuth scopes to request (e.g., "all-apis offline_access").
        authorization_details: Optional JSON string containing authorization details as defined in
            AuthorizationDetail class above.
        disable_async: Whether to disable asynchronous token refresh. Defaults to True.
    """

    get_original_token: Callable[[], Optional[str]]
    host: str
    scopes: str
    authorization_details: str = None
    disable_async: bool = True

    def __post_init__(self):
        super().__init__(disable_async=self.disable_async)

    def refresh(self) -> Token:
        token_exchange_url = f"{self.host}/oidc/v1/token"
        params = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": self.get_original_token(),
            "subject_token_type": "urn:databricks:params:oauth:token-type:personal-access-token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": self.scopes,
        }
        if self.authorization_details:
            params["authorization_details"] = self.authorization_details

        resp = requests.post(token_exchange_url, params)
        if not resp.ok:
            if resp.headers["Content-Type"].startswith("application/json"):
                err = resp.json()
                code = err.get("errorCode", err.get("error", "unknown"))
                summary = err.get("errorSummary", err.get("error_description", "unknown"))
                summary = summary.replace("\r\n", " ")
                raise ValueError(f"{code}: {summary}")
            raise ValueError(resp.content)
        try:
            j = resp.json()
            expires_in = int(j["expires_in"])
            expiry = datetime.now() + timedelta(seconds=expires_in)
            return Token(
                access_token=j["access_token"],
                expiry=expiry,
                token_type=j["token_type"],
            )
        except Exception as e:
            raise ValueError(f"Failed to exchange PAT for OAuth token: {e}")


class TokenCache:
    BASE_PATH = "~/.config/databricks-sdk-py/oauth"

    def __init__(
        self,
        host: str,
        oidc_endpoints: OidcEndpoints,
        client_id: str,
        redirect_url: Optional[str] = None,
        client_secret: Optional[str] = None,
        scopes: Optional[List[str]] = None,
    ) -> None:
        self._host = host
        self._client_id = client_id
        self._oidc_endpoints = oidc_endpoints
        self._redirect_url = redirect_url
        self._client_secret = client_secret
        self._scopes = scopes or []

    @property
    def filename(self) -> str:
        # Include host, client_id, and scopes in the cache filename to make it unique.
        hash = hashlib.sha256()
        for chunk in [
            self._host,
            self._client_id,
            ",".join(self._scopes),
        ]:
            hash.update(chunk.encode("utf-8"))
        return os.path.expanduser(os.path.join(self.__class__.BASE_PATH, hash.hexdigest() + ".json"))

    def load(self) -> Optional[SessionCredentials]:
        """
        Load credentials from cache file. Return None if the cache file does not exist or is invalid.
        """
        if not os.path.exists(self.filename):
            return None

        try:
            with open(self.filename, "r") as f:
                raw = json.load(f)
                return SessionCredentials.from_dict(
                    raw,
                    token_endpoint=self._oidc_endpoints.token_endpoint,
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    redirect_url=self._redirect_url,
                )
        except Exception:
            return None

    def save(self, credentials: SessionCredentials) -> None:
        """
        Save credentials to cache file.
        """
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(self.filename, "w") as f:
            json.dump(credentials.as_dict(), f)
        os.chmod(self.filename, 0o600)
