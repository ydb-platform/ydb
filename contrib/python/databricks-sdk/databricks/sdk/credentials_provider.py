import abc
import base64
import functools
import io
import json
import logging
import os
import pathlib
import platform
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import google.auth  # type: ignore
import requests
from google.auth import impersonated_credentials  # type: ignore
from google.auth.transport.requests import Request  # type: ignore
from google.oauth2 import service_account  # type: ignore

from databricks.sdk.oauth import get_azure_entra_id_workspace_endpoints

from . import azure, oauth, oidc, oidc_token_supplier
from .client_types import ClientType

CredentialsProvider = Callable[[], Dict[str, str]]

logger = logging.getLogger("databricks.sdk")


class OAuthCredentialsProvider:
    """OAuthCredentialsProvider is a type of CredentialsProvider which exposes OAuth tokens."""

    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        token_provider: Callable[[], oauth.Token],
    ):
        self._credentials_provider = credentials_provider
        self._token_provider = token_provider

    def __call__(self) -> Dict[str, str]:
        return self._credentials_provider()

    def oauth_token(self) -> oauth.Token:
        return self._token_provider()


class CredentialsStrategy(abc.ABC):
    """CredentialsProvider is the protocol (call-side interface)
    for authenticating requests to Databricks REST APIs"""

    @abc.abstractmethod
    def auth_type(self) -> str: ...

    @abc.abstractmethod
    def __call__(self, cfg: "Config") -> CredentialsProvider: ...


class OauthCredentialsStrategy(CredentialsStrategy):
    """OauthCredentialsProvider is a CredentialsProvider which
    supports Oauth tokens"""

    def __init__(
        self,
        auth_type: str,
        headers_provider: Callable[["Config"], OAuthCredentialsProvider],
    ):
        self._headers_provider = headers_provider
        self._auth_type = auth_type

    def auth_type(self) -> str:
        return self._auth_type

    def __call__(self, cfg: "Config") -> OAuthCredentialsProvider:
        return self._headers_provider(cfg)

    def oauth_token(self, cfg: "Config") -> oauth.Token:
        return self._headers_provider(cfg).oauth_token()


def credentials_strategy(name: str, require: List[str]):
    """Given the function that receives a Config and returns RequestVisitor,
    create CredentialsProvider with a given name and required configuration
    attribute names to be present for this function to be called."""

    def inner(
        func: Callable[["Config"], CredentialsProvider],
    ) -> CredentialsStrategy:
        @functools.wraps(func)
        def wrapper(cfg: "Config") -> Optional[CredentialsProvider]:
            for attr in require:
                if not getattr(cfg, attr):
                    return None
            return func(cfg)

        wrapper.auth_type = lambda: name
        return wrapper

    return inner


def oauth_credentials_strategy(name: str, require: List[str]):
    """Given the function that receives a Config and returns an OauthHeaderFactory,
    create an OauthCredentialsProvider with a given name and required configuration
    attribute names to be present for this function to be called.

    Args:
        name: The name of the authentication strategy
        require: List of config attributes that must be present
    """

    def inner(
        func: Callable[["Config"], OAuthCredentialsProvider],
    ) -> OauthCredentialsStrategy:
        @functools.wraps(func)
        def wrapper(cfg: "Config") -> Optional[OAuthCredentialsProvider]:
            for attr in require:
                if not getattr(cfg, attr):
                    return None
            return func(cfg)

        return OauthCredentialsStrategy(name, wrapper)

    return inner


@credentials_strategy("basic", ["host", "username", "password"])
def basic_auth(cfg: "Config") -> CredentialsProvider:
    """Given username and password, add base64-encoded Basic credentials"""
    encoded = base64.b64encode(f"{cfg.username}:{cfg.password}".encode()).decode()
    static_credentials = {"Authorization": f"Basic {encoded}"}

    def inner() -> Dict[str, str]:
        return static_credentials

    return inner


@credentials_strategy("pat", ["host", "token"])
def pat_auth(cfg: "Config") -> CredentialsProvider:
    """Adds Databricks Personal Access Token to every request"""
    static_credentials = {"Authorization": f"Bearer {cfg.token}"}

    def inner() -> Dict[str, str]:
        return static_credentials

    return inner


@credentials_strategy("runtime", [])
def runtime_native_auth(cfg: "Config") -> Optional[CredentialsProvider]:
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        return None

    # This import MUST be after the "DATABRICKS_RUNTIME_VERSION" check
    # above, so that we are not throwing import errors when not in
    # runtime and no config variables are set.
    from databricks.sdk.runtime import (init_runtime_legacy_auth,
                                        init_runtime_native_auth,
                                        init_runtime_repl_auth)

    for init in [
        init_runtime_native_auth,
        init_runtime_repl_auth,
        init_runtime_legacy_auth,
    ]:
        if init is None:
            continue
        host, inner = init()
        if host is None:
            logger.debug(f"[{init.__name__}] no host detected")
            continue
        cfg.host = host
        logger.debug(f"[{init.__name__}] runtime native auth configured")
        return inner
    return None


@oauth_credentials_strategy("runtime-oauth", ["scopes"])
def runtime_oauth(cfg: "Config") -> Optional[CredentialsProvider]:
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        return None

    def get_notebook_pat_token() -> Optional[str]:
        native_auth = runtime_native_auth(cfg)
        if native_auth is None:
            return None
        notebook_pat_token = None
        notebook_pat_authorization = native_auth().get("Authorization", "").strip()
        if notebook_pat_authorization.lower().startswith("bearer "):
            notebook_pat_token = notebook_pat_authorization[len("bearer ") :].strip()
        return notebook_pat_token

    notebook_pat_token = get_notebook_pat_token()
    if notebook_pat_token is None:
        return None

    token_source = oauth.PATOAuthTokenExchange(
        get_original_token=get_notebook_pat_token,
        host=cfg.host,
        scopes=cfg.get_scopes_as_string(),
        authorization_details=cfg.authorization_details,
    )

    def inner() -> Dict[str, str]:
        token = token_source.token()
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    def token() -> oauth.Token:
        return token_source.token()

    return OAuthCredentialsProvider(inner, token)


@oauth_credentials_strategy("oauth-m2m", ["host", "client_id", "client_secret"])
def oauth_service_principal(cfg: "Config") -> Optional[CredentialsProvider]:
    """Adds refreshed Databricks machine-to-machine OAuth Bearer token to every request,
    if /oidc/.well-known/oauth-authorization-server is available on the given host.
    """
    oidc = cfg.databricks_oidc_endpoints
    if oidc is None:
        return None

    token_source = oauth.ClientCredentials(
        client_id=cfg.client_id,
        client_secret=cfg.client_secret,
        token_url=oidc.token_endpoint,
        scopes=cfg.get_scopes_as_string(),
        use_header=True,
        disable_async=cfg.disable_async_token_refresh,
        authorization_details=cfg.authorization_details,
    )

    def inner() -> Dict[str, str]:
        token = token_source.token()
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    def token() -> oauth.Token:
        return token_source.token()

    return OAuthCredentialsProvider(inner, token)


@credentials_strategy("external-browser", ["host", "auth_type"])
def external_browser(cfg: "Config") -> Optional[CredentialsProvider]:
    if cfg.auth_type != "external-browser":
        return None

    client_id, client_secret = None, None
    oidc_endpoints = None
    if cfg.client_id:
        client_id = cfg.client_id
        client_secret = cfg.client_secret
        oidc_endpoints = cfg.databricks_oidc_endpoints
    elif cfg.azure_client_id:
        client_id = cfg.azure_client_id
        client_secret = cfg.azure_client_secret
        oidc_endpoints = get_azure_entra_id_workspace_endpoints(cfg.host)
    if not client_id:
        client_id = "databricks-cli"
        oidc_endpoints = cfg.databricks_oidc_endpoints

    if not oidc_endpoints:
        return None

    scopes = cfg.get_scopes()
    if not cfg.disable_oauth_refresh_token:
        if "offline_access" not in scopes:
            scopes = scopes + ["offline_access"]

    # Load cached credentials from disk if they exist. Note that these are
    # local to the Python SDK and not reused by other SDKs.
    redirect_url = "http://localhost:8020"
    token_cache = oauth.TokenCache(
        host=cfg.host,
        oidc_endpoints=oidc_endpoints,
        client_id=client_id,
        client_secret=client_secret,
        redirect_url=redirect_url,
        scopes=scopes,
    )
    credentials = token_cache.load()
    if credentials:
        try:
            # Pro-actively refresh the loaded credentials. This is done
            # to detect if the token is expired and needs to be refreshed
            # by going through the OAuth login flow.
            credentials.token()
            return credentials(cfg)
        # TODO: We should ideally use more specific exceptions.
        except Exception as e:
            logger.warning(f"Failed to refresh cached token: {e}. Initiating new OAuth login flow")

    oauth_client = oauth.OAuthClient(
        oidc_endpoints=oidc_endpoints,
        client_id=client_id,
        redirect_url=redirect_url,
        client_secret=client_secret,
        scopes=scopes,
    )
    consent = oauth_client.initiate_consent()
    if not consent:
        return None

    credentials = consent.launch_external_browser()
    token_cache.save(credentials)
    return credentials(cfg)


def _ensure_host_present(cfg: "Config", token_source_for: Callable[[str], oauth.TokenSource]):
    """Resolves Azure Databricks workspace URL from ARM Resource ID"""
    if cfg.host:
        return
    if not cfg.azure_workspace_resource_id:
        return
    arm = cfg.arm_environment.resource_manager_endpoint
    token = token_source_for(arm).token()
    resp = requests.get(
        f"{arm}{cfg.azure_workspace_resource_id}?api-version=2018-04-01",
        headers={"Authorization": f"Bearer {token.access_token}"},
    )
    if not resp.ok:
        raise ValueError(f"Cannot resolve Azure Databricks workspace: {resp.content}")
    cfg.host = f"https://{resp.json()['properties']['workspaceUrl']}"


@oauth_credentials_strategy(
    "azure-client-secret",
    ["azure_client_id", "azure_client_secret"],
)
def azure_service_principal(cfg: "Config") -> CredentialsProvider:
    """Adds refreshed Azure Active Directory (AAD) Service Principal OAuth tokens
    to every request, while automatically resolving different Azure environment endpoints.
    """

    def token_source_for(resource: str) -> oauth.TokenSource:
        aad_endpoint = cfg.arm_environment.active_directory_endpoint
        return oauth.ClientCredentials(
            client_id=cfg.azure_client_id,
            client_secret=cfg.azure_client_secret,
            token_url=f"{aad_endpoint}{cfg.azure_tenant_id}/oauth2/token",
            endpoint_params={"resource": resource},
            use_params=True,
            disable_async=cfg.disable_async_token_refresh,
            scopes=cfg.get_scopes_as_string(),
            authorization_details=cfg.authorization_details,
        )

    _ensure_host_present(cfg, token_source_for)
    cfg.load_azure_tenant_id()
    logger.info("Configured AAD token for Service Principal (%s)", cfg.azure_client_id)
    inner = token_source_for(cfg.effective_azure_login_app_id)
    cloud = token_source_for(cfg.arm_environment.service_management_endpoint)

    def refreshed_headers() -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {inner.token().access_token}",
        }
        azure.add_workspace_id_header(cfg, headers)
        azure.add_sp_management_token(cloud, headers)
        return headers

    def token() -> oauth.Token:
        return inner.token()

    return OAuthCredentialsProvider(refreshed_headers, token)


@credentials_strategy("env-oidc", ["host"])
def env_oidc(cfg) -> Optional[CredentialsProvider]:
    # Search for an OIDC ID token in DATABRICKS_OIDC_TOKEN environment variable
    # by default. This can be overridden by setting DATABRICKS_OIDC_TOKEN_ENV
    # to the name of an environment variable that contains the OIDC ID token.
    env_var = "DATABRICKS_OIDC_TOKEN"
    if cfg.oidc_token_env:
        env_var = cfg.oidc_token_env

    return oidc_credentials_provider(cfg, oidc.EnvIdTokenSource(env_var))


@credentials_strategy("file-oidc", ["host", "oidc_token_filepath"])
def file_oidc(cfg) -> Optional[CredentialsProvider]:
    return oidc_credentials_provider(cfg, oidc.FileIdTokenSource(cfg.oidc_token_filepath))


def oidc_credentials_provider(cfg, id_token_source: oidc.IdTokenSource) -> Optional[CredentialsProvider]:
    """Creates a CredentialsProvider to sign requests with an OAuth token obtained
    by automatically performing the token exchange using the given IdTokenSource."""

    try:
        id_token_source.id_token()  # validate the id_token_source
    except Exception as e:
        logger.debug(f"Failed to get OIDC token: {e}")
        return None

    token_source = oidc.DatabricksOidcTokenSource(
        host=cfg.host,
        token_endpoint=cfg.databricks_oidc_endpoints.token_endpoint,
        client_id=cfg.client_id,
        account_id=cfg.account_id,
        id_token_source=id_token_source,
        disable_async=cfg.disable_async_token_refresh,
        scopes=cfg.get_scopes_as_string(),
    )

    def refreshed_headers() -> Dict[str, str]:
        token = token_source.token()
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    def token() -> oauth.Token:
        return token_source.token()

    return OAuthCredentialsProvider(refreshed_headers, token)


def _oidc_credentials_provider(
    cfg: "Config", supplier_factory: Callable[[], Any], provider_name: str
) -> Optional[CredentialsProvider]:
    """
    Generic OIDC credentials provider that works with any OIDC token supplier.

    Args:
        cfg: Databricks configuration
        supplier_factory: Callable that returns an OIDC token supplier instance
        provider_name: Human-readable name (e.g., "GitHub OIDC", "Azure DevOps OIDC")

    Returns:
        OAuthCredentialsProvider if successful, None if supplier unavailable or token retrieval fails
    """
    # Try to create the supplier
    try:
        supplier = supplier_factory()
    except Exception as e:
        logger.debug(f"{provider_name}: {str(e)}")
        return None

    # Determine the audience for token exchange
    audience = cfg.token_audience
    if audience is None and cfg.client_type == ClientType.ACCOUNT:
        audience = cfg.account_id
    if audience is None and cfg.client_type != ClientType.ACCOUNT:
        audience = cfg.databricks_oidc_endpoints.token_endpoint

    # Try to get an OIDC token. If no supplier returns a token, we cannot use this authentication mode.
    id_token = supplier.get_oidc_token(audience)
    if not id_token:
        logger.debug(f"{provider_name}: no token available, skipping authentication method")
        return None

    logger.info(f"Configured {provider_name} authentication")

    def token_source_for(audience: str) -> oauth.TokenSource:
        id_token = supplier.get_oidc_token(audience)
        if not id_token:
            # Should not happen, since we checked it above.
            raise Exception(f"Cannot get {provider_name} token")

        return oauth.ClientCredentials(
            client_id=cfg.client_id,
            client_secret="",  # we have no (rotatable) secrets in OIDC flow
            token_url=cfg.databricks_oidc_endpoints.token_endpoint,
            endpoint_params={
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "subject_token": id_token,
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            },
            scopes=cfg.get_scopes_as_string(),
            use_params=True,
            disable_async=cfg.disable_async_token_refresh,
            authorization_details=cfg.authorization_details,
        )

    def refreshed_headers() -> Dict[str, str]:
        token = token_source_for(audience).token()
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    def token() -> oauth.Token:
        return token_source_for(audience).token()

    return OAuthCredentialsProvider(refreshed_headers, token)


@oauth_credentials_strategy("github-oidc", ["host", "client_id"])
def github_oidc(cfg: "Config") -> Optional[CredentialsProvider]:
    """
    GitHub OIDC authentication uses a Token Supplier to get a JWT Token and exchanges
    it for a Databricks Token.

    Supported in GitHub Actions with OIDC service connections.
    """
    return _oidc_credentials_provider(
        cfg=cfg,
        supplier_factory=lambda: oidc_token_supplier.GitHubOIDCTokenSupplier(),
        provider_name="GitHub OIDC",
    )


@oauth_credentials_strategy("azure-devops-oidc", ["host", "client_id"])
def azure_devops_oidc(cfg: "Config") -> Optional[CredentialsProvider]:
    """
    Azure DevOps OIDC authentication uses a Token Supplier to get a JWT Token
    and exchanges it for a Databricks Token.

    Supported in Azure DevOps pipelines with OIDC service connections.
    """
    return _oidc_credentials_provider(
        cfg=cfg,
        supplier_factory=lambda: oidc_token_supplier.AzureDevOpsOIDCTokenSupplier(),
        provider_name="Azure DevOps OIDC",
    )


# Azure Client ID is the minimal thing we need, as otherwise we get AADSTS700016: Application with
# identifier 'https://token.actions.githubusercontent.com' was not found in the directory '...'.
@oauth_credentials_strategy("github-oidc-azure", ["host", "azure_client_id"])
def github_oidc_azure(cfg: "Config") -> Optional[CredentialsProvider]:
    if "ACTIONS_ID_TOKEN_REQUEST_TOKEN" not in os.environ:
        # not in GitHub actions
        return None

    token = oidc_token_supplier.GitHubOIDCTokenSupplier().get_oidc_token("api://AzureADTokenExchange")
    if not token:
        return None

    logger.info(
        "Configured AAD token for GitHub Actions OIDC (%s)",
        cfg.azure_client_id,
    )

    aad_endpoint = cfg.arm_environment.active_directory_endpoint
    if not cfg.azure_tenant_id:
        # detect Azure AD Tenant ID if it's not specified directly
        token_endpoint = get_azure_entra_id_workspace_endpoints(cfg.host).token_endpoint
        cfg.azure_tenant_id = token_endpoint.replace(aad_endpoint, "").split("/")[0]

    inner = oauth.ClientCredentials(
        client_id=cfg.azure_client_id,
        client_secret="",  # we have no (rotatable) secrets in OIDC flow
        token_url=f"{aad_endpoint}{cfg.azure_tenant_id}/oauth2/token",
        endpoint_params={
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "resource": cfg.effective_azure_login_app_id,
            "client_assertion": token,
        },
        use_params=True,
        disable_async=cfg.disable_async_token_refresh,
        scopes=cfg.get_scopes_as_string(),
        authorization_details=cfg.authorization_details,
    )

    def refreshed_headers() -> Dict[str, str]:
        token = inner.token()
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    def token() -> oauth.Token:
        return inner.token()

    return OAuthCredentialsProvider(refreshed_headers, token)


GcpScopes = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/compute",
]


@oauth_credentials_strategy("google-credentials", ["host", "google_credentials"])
def google_credentials(cfg: "Config") -> Optional[CredentialsProvider]:
    # Reads credentials as JSON. Credentials can be either a path to JSON file, or actual JSON string.
    # Obtain the id token by providing the json file path and target audience.
    if os.path.isfile(cfg.google_credentials):
        with io.open(cfg.google_credentials, "r", encoding="utf-8") as json_file:
            account_info = json.load(json_file)
    else:
        # If the file doesn't exist, assume that the config is the actual JSON content.
        account_info = json.loads(cfg.google_credentials)

    credentials = service_account.IDTokenCredentials.from_service_account_info(
        info=account_info, target_audience=cfg.host
    )

    request = Request()

    gcp_credentials = service_account.Credentials.from_service_account_info(info=account_info, scopes=GcpScopes)

    def token() -> oauth.Token:
        credentials.refresh(request)
        return credentials.token

    def refreshed_headers() -> Dict[str, str]:
        credentials.refresh(request)
        headers = {"Authorization": f"Bearer {credentials.token}"}
        if cfg.client_type == ClientType.ACCOUNT:
            gcp_credentials.refresh(request)
            headers["X-Databricks-GCP-SA-Access-Token"] = gcp_credentials.token
        return headers

    return OAuthCredentialsProvider(refreshed_headers, token)


@oauth_credentials_strategy("google-id", ["host", "google_service_account"])
def google_id(cfg: "Config") -> Optional[CredentialsProvider]:
    credentials, _project_id = google.auth.default()

    # Create the impersonated credential.
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=cfg.google_service_account,
        target_scopes=[],
    )

    # Set the impersonated credential, target audience and token options.
    id_creds = impersonated_credentials.IDTokenCredentials(
        target_credentials, target_audience=cfg.host, include_email=True
    )

    gcp_impersonated_credentials = impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=cfg.google_service_account,
        target_scopes=GcpScopes,
    )

    request = Request()

    def token() -> oauth.Token:
        id_creds.refresh(request)
        return id_creds.token

    def refreshed_headers() -> Dict[str, str]:
        id_creds.refresh(request)
        headers = {"Authorization": f"Bearer {id_creds.token}"}
        if cfg.client_type == ClientType.ACCOUNT:
            gcp_impersonated_credentials.refresh(request)
            headers["X-Databricks-GCP-SA-Access-Token"] = gcp_impersonated_credentials.token
        return headers

    return OAuthCredentialsProvider(refreshed_headers, token)


class CliTokenSource(oauth.Refreshable):
    def __init__(
        self,
        cmd: List[str],
        token_type_field: str,
        access_token_field: str,
        expiry_field: str,
        disable_async: bool = True,
        fallback_cmd: Optional[List[str]] = None,
    ):
        super().__init__(disable_async=disable_async)
        self._cmd = cmd
        # fallback_cmd is tried when the primary command fails with "unknown flag: --profile",
        # indicating the CLI is too old to support --profile. Can be removed once support
        # for CLI versions predating --profile is dropped.
        # See: https://github.com/databricks/databricks-sdk-go/pull/1497
        self._fallback_cmd = fallback_cmd
        self._token_type_field = token_type_field
        self._access_token_field = access_token_field
        self._expiry_field = expiry_field

    @staticmethod
    def _parse_expiry(expiry: str) -> datetime:
        expiry = expiry.rstrip("Z").split(".")[0]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(expiry, fmt)
            except ValueError as e:
                last_e = e
        if last_e:
            raise last_e

    def _exec_cli_command(self, cmd: List[str]) -> oauth.Token:
        try:
            out = _run_subprocess(cmd, capture_output=True, check=True)
            it = json.loads(out.stdout.decode())
            expires_on = self._parse_expiry(it[self._expiry_field])
            return oauth.Token(
                access_token=it[self._access_token_field],
                token_type=it[self._token_type_field],
                expiry=expires_on,
            )
        except ValueError as e:
            raise ValueError(f"cannot unmarshal CLI result: {e}")
        except subprocess.CalledProcessError as e:
            stdout = e.stdout.decode().strip()
            stderr = e.stderr.decode().strip()
            message = "\n".join(filter(None, [stdout, stderr]))
            raise IOError(f"cannot get access token: {message}") from e

    def refresh(self) -> oauth.Token:
        try:
            return self._exec_cli_command(self._cmd)
        except IOError as e:
            if self._fallback_cmd is not None and "unknown flag: --profile" in str(e):
                logger.warning(
                    "Databricks CLI does not support --profile flag. Falling back to --host. "
                    "Please upgrade your CLI to the latest version."
                )
                return self._exec_cli_command(self._fallback_cmd)
            raise


def _run_subprocess(
    popenargs,
    input=None,
    capture_output=True,
    timeout=None,
    check=False,
    **kwargs,
) -> subprocess.CompletedProcess:
    """Runs subprocess with given arguments.
    This handles OS-specific modifications that need to be made to the invocation of subprocess.run.
    """
    kwargs["shell"] = sys.platform.startswith("win")
    # windows requires shell=True to be able to execute 'az login' or other commands
    # cannot use shell=True all the time, as it breaks macOS
    logging.debug(f"Running command: {' '.join(popenargs)}")
    return subprocess.run(
        popenargs,
        input=input,
        capture_output=capture_output,
        timeout=timeout,
        check=check,
        **kwargs,
    )


class AzureCliTokenSource(CliTokenSource):
    """Obtain the token granted by `az login` CLI command"""

    def __init__(
        self,
        resource: str,
        subscription: Optional[str] = None,
        tenant: Optional[str] = None,
    ):
        cmd = [
            "az",
            "account",
            "get-access-token",
            "--resource",
            resource,
            "--output",
            "json",
        ]
        if subscription is not None:
            cmd.append("--subscription")
            cmd.append(subscription)
        if tenant and not self.__is_cli_using_managed_identity():
            cmd.extend(["--tenant", tenant])
        super().__init__(
            cmd=cmd,
            token_type_field="tokenType",
            access_token_field="accessToken",
            expiry_field="expiresOn",
        )

    @staticmethod
    def __is_cli_using_managed_identity() -> bool:
        """Checks whether the current CLI session is authenticated using managed identity."""
        try:
            cmd = ["az", "account", "show", "--output", "json"]
            out = _run_subprocess(cmd, capture_output=True, check=True)
            account = json.loads(out.stdout.decode())
            user = account.get("user")
            if user is None:
                return False
            return user.get("type") == "servicePrincipal" and user.get("name") in [
                "systemAssignedIdentity",
                "userAssignedIdentity",
            ]
        except subprocess.CalledProcessError as e:
            logger.debug("Failed to get account information from Azure CLI", exc_info=e)
            return False

    def is_human_user(self) -> bool:
        """The UPN claim is the username of the user, but not the Service Principal.

        Azure CLI can be authenticated by both human users (`az login`) and service principals. In case of service
        principals, it can be either OIDC from GitHub or login with a password:

            ~ $ az login --service-principal --user $clientID --password $clientSecret --tenant $tenantID

        Human users get more claims:
        - 'amr' - how the subject of the token was authenticated
        - 'name', 'family_name', 'given_name' - human-readable values that identifies the subject of the token
        - 'scp' with `user_impersonation` value, that shows the set of scopes exposed by your application for which
              the client application has requested (and received) consent
        - 'unique_name' - a human-readable value that identifies the subject of the token. This value is not
              guaranteed to be unique within a tenant and should be used only for display purposes.
        - 'upn' - The username of the user.
        """
        return "upn" in self.token().jwt_claims()

    @staticmethod
    def for_resource(cfg: "Config", resource: str) -> "AzureCliTokenSource":
        subscription = AzureCliTokenSource.get_subscription(cfg)
        if subscription is not None:
            token_source = AzureCliTokenSource(resource, subscription=subscription, tenant=cfg.azure_tenant_id)
            try:
                # This will fail if the user has access to the workspace, but not to the subscription
                # itself.
                # In such case, we fall back to not using the subscription.
                token_source.token()
                return token_source
            except OSError:
                logger.warning("Failed to get token for subscription. Using resource only token.")

        token_source = AzureCliTokenSource(resource, subscription=None, tenant=cfg.azure_tenant_id)
        token_source.token()
        return token_source

    @staticmethod
    def get_subscription(cfg: "Config") -> Optional[str]:
        resource = cfg.azure_workspace_resource_id
        if resource is None or resource == "":
            return None
        components = resource.split("/")
        if len(components) < 3:
            logger.warning("Invalid azure workspace resource ID")
            return None
        return components[2]


@credentials_strategy("azure-cli", ["effective_azure_login_app_id"])
def azure_cli(cfg: "Config") -> Optional[CredentialsProvider]:
    """Adds refreshed OAuth token granted by `az login` command to every request."""
    cfg.load_azure_tenant_id()
    token_source = None
    mgmt_token_source = None
    try:
        token_source = AzureCliTokenSource.for_resource(cfg, cfg.effective_azure_login_app_id)
    except FileNotFoundError:
        doc = "https://docs.microsoft.com/en-us/cli/azure/?view=azure-cli-latest"
        logger.debug(f"Most likely Azure CLI is not installed. See {doc} for details")
        return None
    except OSError as e:
        logger.debug("skipping Azure CLI auth", exc_info=e)
        logger.debug("This may happen if you are attempting to login to a dev or staging workspace")
        return None

    if not token_source.is_human_user():
        try:
            management_endpoint = cfg.arm_environment.service_management_endpoint
            mgmt_token_source = AzureCliTokenSource.for_resource(cfg, management_endpoint)
        except Exception as e:
            logger.debug(
                "Not including service management token in headers",
                exc_info=e,
            )
            mgmt_token_source = None

    _ensure_host_present(cfg, lambda resource: AzureCliTokenSource.for_resource(cfg, resource))
    logger.info("Using Azure CLI authentication with AAD tokens")

    def inner() -> Dict[str, str]:
        token = token_source.token()
        headers = {"Authorization": f"{token.token_type} {token.access_token}"}
        azure.add_workspace_id_header(cfg, headers)
        if mgmt_token_source:
            azure.add_sp_management_token(mgmt_token_source, headers)
        return headers

    return inner


class DatabricksCliTokenSource(CliTokenSource):
    """Obtain the token granted by `databricks auth login` CLI command"""

    def __init__(self, cfg: "Config"):
        cli_path = cfg.databricks_cli_path

        # If the path is not specified look for "databricks" / "databricks.exe" in PATH.
        if not cli_path:
            try:
                # Try to find "databricks" in PATH
                cli_path = self.__class__._find_executable("databricks")
            except FileNotFoundError as e:
                # If "databricks" is not found, try to find "databricks.exe" in PATH (Windows)
                if platform.system() == "Windows":
                    cli_path = self.__class__._find_executable("databricks.exe")
                else:
                    raise e

        # If the path is unqualified, look it up in PATH.
        elif cli_path.count("/") == 0:
            cli_path = self.__class__._find_executable(cli_path)

        fallback_cmd = None
        if cfg.profile:
            # When profile is set, use --profile as the primary command.
            # The profile contains the full config (host, account_id, etc.).
            args = ["auth", "token", "--profile", cfg.profile]
            # Build a --host fallback for older CLIs that don't support --profile.
            if cfg.host:
                fallback_cmd = [cli_path, *self.__class__._build_host_args(cfg)]
        else:
            args = self.__class__._build_host_args(cfg)

        super().__init__(
            cmd=[cli_path, *args],
            token_type_field="token_type",
            access_token_field="access_token",
            expiry_field="expiry",
            disable_async=cfg.disable_async_token_refresh,
            fallback_cmd=fallback_cmd,
        )

    @staticmethod
    def _build_host_args(cfg: "Config") -> List[str]:
        """Build CLI arguments using --host (legacy path)."""
        args = ["auth", "token", "--host", cfg.host]
        if cfg.experimental_is_unified_host:
            # For unified hosts, pass account_id, workspace_id, and experimental flag
            args += ["--experimental-is-unified-host"]
            if cfg.account_id:
                args += ["--account-id", cfg.account_id]
            if cfg.workspace_id:
                args += ["--workspace-id", str(cfg.workspace_id)]
        elif cfg.client_type == ClientType.ACCOUNT:
            args += ["--account-id", cfg.account_id]
        return args

    @staticmethod
    def _find_executable(name) -> str:
        err = FileNotFoundError("Most likely the Databricks CLI is not installed")
        for dir in os.getenv("PATH", default="").split(os.path.pathsep):
            path = pathlib.Path(dir).joinpath(name).resolve()
            if not path.is_file():
                continue

            # The new Databricks CLI is a single binary with size > 1MB.
            # We use the size as a signal to determine which Databricks CLI is installed.
            stat = path.stat()
            if stat.st_size < (1024 * 1024):
                err = FileNotFoundError("Databricks CLI version <0.100.0 detected")
                continue

            return str(path)

        raise err


@oauth_credentials_strategy("databricks-cli", ["host"])
def databricks_cli(cfg: "Config") -> Optional[CredentialsProvider]:
    try:
        token_source = DatabricksCliTokenSource(cfg)
    except FileNotFoundError as e:
        logger.debug(e)
        return None

    try:
        token_source.token()
    except IOError as e:
        if "databricks OAuth is not" in str(e):
            logger.debug(f"OAuth not configured or not available: {e}")
            return None
        raise e

    logger.info("Using Databricks CLI authentication")

    def inner() -> Dict[str, str]:
        token = token_source.token()
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    def token() -> oauth.Token:
        return token_source.token()

    return OAuthCredentialsProvider(inner, token)


class MetadataServiceTokenSource(oauth.Refreshable):
    """Obtain the token granted by Databricks Metadata Service"""

    METADATA_SERVICE_VERSION = "1"
    METADATA_SERVICE_VERSION_HEADER = "X-Databricks-Metadata-Version"
    METADATA_SERVICE_HOST_HEADER = "X-Databricks-Host"
    _metadata_service_timeout = 10  # seconds

    def __init__(self, cfg: "Config"):
        super().__init__()
        self.url = cfg.metadata_service_url
        self.host = cfg.host

    def refresh(self) -> oauth.Token:
        resp = requests.get(
            self.url,
            timeout=self._metadata_service_timeout,
            headers={
                self.METADATA_SERVICE_VERSION_HEADER: self.METADATA_SERVICE_VERSION,
                self.METADATA_SERVICE_HOST_HEADER: self.host,
            },
            proxies={
                # Explicitly exclude localhost from being proxied. This is necessary
                # for Metadata URLs which typically point to localhost.
                "no_proxy": "localhost,127.0.0.1"
            },
        )
        json_resp: dict[str, Union[str, float]] = resp.json()
        access_token = json_resp.get("access_token", None)
        if access_token is None:
            raise ValueError("Metadata Service returned empty token")
        token_type = json_resp.get("token_type", None)
        if token_type is None:
            raise ValueError("Metadata Service returned empty token type")
        if json_resp["expires_on"] in ["", None]:
            raise ValueError("Metadata Service returned invalid expiry")
        try:
            expiry = datetime.fromtimestamp(json_resp["expires_on"])
        except:
            raise ValueError("Metadata Service returned invalid expiry")

        return oauth.Token(access_token=access_token, token_type=token_type, expiry=expiry)


@credentials_strategy("metadata-service", ["host", "metadata_service_url"])
def metadata_service(cfg: "Config") -> Optional[CredentialsProvider]:
    """Adds refreshed token granted by Databricks Metadata Service to every request."""

    token_source = MetadataServiceTokenSource(cfg)
    token_source.token()
    logger.info("Using Databricks Metadata Service authentication")

    def inner() -> Dict[str, str]:
        token = token_source.token()
        return {"Authorization": f"{token.token_type} {token.access_token}"}

    return inner


# This Code is derived from Mlflow DatabricksModelServingConfigProvider
# https://github.com/mlflow/mlflow/blob/1219e3ef1aac7d337a618a352cd859b336cf5c81/mlflow/legacy_databricks_cli/configure/provider.py#L332
class ModelServingAuthProvider:
    USER_CREDENTIALS = "user_credentials"

    _MODEL_DEPENDENCY_OAUTH_TOKEN_FILE_PATH = "/var/credentials-secret/model-dependencies-oauth-token"

    def __init__(self, credential_type: Optional[str]):
        self.expiry_time = -1
        self.current_token = None
        self.refresh_duration = 300  # 300 Seconds
        self.credential_type = credential_type

    def should_fetch_model_serving_environment_oauth() -> bool:
        """
        Check whether this is the model serving environment
        Additionally check if the oauth token file path exists
        """

        is_in_model_serving_env = (
            os.environ.get("IS_IN_DB_MODEL_SERVING_ENV")
            or os.environ.get("IS_IN_DATABRICKS_MODEL_SERVING_ENV")
            or "false"
        )
        return is_in_model_serving_env == "true" and os.path.isfile(
            ModelServingAuthProvider._MODEL_DEPENDENCY_OAUTH_TOKEN_FILE_PATH
        )

    def _get_model_dependency_oauth_token(self, should_retry=True) -> str:
        # Use Cached value if it is valid
        if self.current_token is not None and self.expiry_time > time.time():
            return self.current_token

        try:
            with open(ModelServingAuthProvider._MODEL_DEPENDENCY_OAUTH_TOKEN_FILE_PATH) as f:
                oauth_dict = json.load(f)
                self.current_token = oauth_dict["OAUTH_TOKEN"][0]["oauthTokenValue"]
                self.expiry_time = time.time() + self.refresh_duration
        except Exception as e:
            # sleep and retry in case of any race conditions with OAuth refreshing
            if should_retry:
                logger.warning(
                    "Unable to read oauth token on first attmept in Model Serving Environment",
                    exc_info=e,
                )
                time.sleep(0.5)
                return self._get_model_dependency_oauth_token(should_retry=False)
            else:
                raise RuntimeError(
                    "Unable to read OAuth credentials from the file mounted in Databricks Model Serving"
                ) from e
        return self.current_token

    def _get_invokers_token(self):
        main_thread = threading.main_thread()
        thread_data = main_thread.__dict__
        invokers_token = None
        if "invokers_token" in thread_data:
            invokers_token = thread_data["invokers_token"]

        if invokers_token is None:
            raise RuntimeError("Unable to read Invokers Token in Databricks Model Serving")

        return invokers_token

    def get_databricks_host_token(self) -> Optional[Tuple[str, str]]:
        if not ModelServingAuthProvider.should_fetch_model_serving_environment_oauth():
            return None

        # read from DB_MODEL_SERVING_HOST_ENV_VAR if available otherwise MODEL_SERVING_HOST_ENV_VAR
        host = os.environ.get("DATABRICKS_MODEL_SERVING_HOST_URL") or os.environ.get("DB_MODEL_SERVING_HOST_URL")

        if self.credential_type == ModelServingAuthProvider.USER_CREDENTIALS:
            return (host, self._get_invokers_token())
        else:
            return (host, self._get_model_dependency_oauth_token())


def model_serving_auth_visitor(cfg: "Config", credential_type: Optional[str] = None) -> Optional[CredentialsProvider]:
    try:
        model_serving_auth_provider = ModelServingAuthProvider(credential_type)
        host, token = model_serving_auth_provider.get_databricks_host_token()
        if token is None:
            raise ValueError(
                "Got malformed auth (empty token) when fetching auth implicitly available in Model Serving Environment. Please contact Databricks support"
            )
        if cfg.host is None:
            cfg.host = host
    except Exception as e:
        logger.warning(
            "Unable to get auth from Databricks Model Serving Environment",
            exc_info=e,
        )
        return None
    logger.info("Using Databricks Model Serving Authentication")

    def inner() -> Dict[str, str]:
        # Call here again to get the refreshed token
        _, token = model_serving_auth_provider.get_databricks_host_token()
        return {"Authorization": f"Bearer {token}"}

    return inner


@credentials_strategy("model-serving", [])
def model_serving_auth(cfg: "Config") -> Optional[CredentialsProvider]:
    if not ModelServingAuthProvider.should_fetch_model_serving_environment_oauth():
        logger.debug("model-serving: Not in Databricks Model Serving, skipping")
        return None

    return model_serving_auth_visitor(cfg)


class DefaultCredentials:
    """Select the first applicable credential provider from the chain"""

    def __init__(self) -> None:
        self._auth_type = "default"
        self._auth_providers = [
            pat_auth,
            basic_auth,
            metadata_service,
            oauth_service_principal,
            env_oidc,
            file_oidc,
            github_oidc,
            azure_service_principal,
            github_oidc_azure,
            azure_cli,
            azure_devops_oidc,
            external_browser,
            databricks_cli,
            runtime_oauth,
            runtime_native_auth,
            google_credentials,
            google_id,
            model_serving_auth,
        ]

    def auth_type(self) -> str:
        return self._auth_type

    def oauth_token(self, cfg: "Config") -> oauth.Token:
        for provider in self._auth_providers:
            auth_type = provider.auth_type()
            if auth_type != self._auth_type:
                # ignore other auth types if they don't match the selected one
                continue
            return provider.oauth_token(cfg)

    def __call__(self, cfg: "Config") -> CredentialsProvider:
        for provider in self._auth_providers:
            auth_type = provider.auth_type()
            if cfg.auth_type and auth_type != cfg.auth_type:
                # ignore other auth types if one is explicitly enforced
                logger.debug(f"Ignoring {auth_type} auth, because {cfg.auth_type} is preferred")
                continue
            logger.debug(f"Attempting to configure auth: {auth_type}")
            try:
                # The header factory might be None if the provider cannot be
                # configured for the current environment. For example, if the
                # provider requires some missing environment variables.
                header_factory = provider(cfg)
                if not header_factory:
                    continue

                self._auth_type = auth_type
                return header_factory
            except Exception as e:
                raise ValueError(f"{auth_type}: {e}") from e
        auth_flow_url = "https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication"
        raise ValueError(
            f"cannot configure default credentials, please check {auth_flow_url} to configure credentials for your preferred authentication method."
        )


class ModelServingUserCredentials(CredentialsStrategy):
    """
    This credential strategy is designed for authenticating the Databricks SDK in the model serving environment using user-specific rights.
    In the model serving environment, the strategy retrieves a downscoped user token from the thread-local variable.
    In any other environments, the class defaults to the DefaultCredentialStrategy.
    To use this credential strategy, instantiate the WorkspaceClient with the ModelServingUserCredentials strategy as follows:

    invokers_client = WorkspaceClient(credential_strategy = ModelServingUserCredentials())
    """

    def __init__(self):
        self.credential_type = ModelServingAuthProvider.USER_CREDENTIALS
        self.default_credentials = DefaultCredentials()

    def auth_type(self):
        if ModelServingAuthProvider.should_fetch_model_serving_environment_oauth():
            return "model_serving_" + self.credential_type
        else:
            return self.default_credentials.auth_type()

    def __call__(self, cfg: "Config") -> CredentialsProvider:
        if ModelServingAuthProvider.should_fetch_model_serving_environment_oauth():
            header_factory = model_serving_auth_visitor(cfg, self.credential_type)
            if not header_factory:
                raise ValueError(
                    f"Unable to authenticate using {self.credential_type} in Databricks Model Serving Environment"
                )
            return header_factory
        else:
            return self.default_credentials(cfg)
