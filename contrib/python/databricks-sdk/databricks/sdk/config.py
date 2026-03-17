import configparser
import copy
import datetime
import logging
import os
import pathlib
import re
import sys
import urllib.parse
from typing import Dict, Iterable, List, Optional

import requests

from . import useragent
from ._base_client import _fix_host_if_needed
from .client_types import ClientType, HostType
from .clock import Clock, RealClock
from .credentials_provider import (CredentialsStrategy, DefaultCredentials,
                                   OAuthCredentialsProvider)
from .environments import (ALL_ENVS, AzureEnvironment, Cloud,
                           DatabricksEnvironment, get_environment_for_hostname)
from .oauth import (OidcEndpoints, Token, get_account_endpoints,
                    get_azure_entra_id_workspace_endpoints,
                    get_endpoints_from_url, get_host_metadata,
                    get_unified_endpoints, get_workspace_endpoints)

logger = logging.getLogger("databricks.sdk")


class ConfigAttribute:
    """Configuration attribute metadata and descriptor protocols."""

    # name and transform are discovered from Config.__new__
    name: str = None
    transform: type = str
    _custom_transform = None

    def __init__(self, env: str = None, auth: str = None, sensitive: bool = False, transform=None):
        self.env = env
        self.auth = auth
        self.sensitive = sensitive
        self._custom_transform = transform

    def __get__(self, cfg: "Config", owner):
        if not cfg:
            return None
        return cfg._inner.get(self.name, None)

    def __set__(self, cfg: "Config", value: any):
        cfg._inner[self.name] = self.transform(value)

    def __repr__(self) -> str:
        return f"<ConfigAttribute '{self.name}' {self.transform.__name__}>"


def _parse_scopes(value):
    """Parse scopes into a deduplicated, sorted list."""
    if value is None:
        return None
    if isinstance(value, list):
        result = sorted(set(s for s in value if s))
        return result if result else None
    if isinstance(value, str):
        parsed: list = sorted(set(s for s in re.split(r"[, ]+", value) if s))
        return parsed if parsed else None
    return None


def with_product(product: str, product_version: str):
    """[INTERNAL API] Change the product name and version used in the User-Agent header."""
    useragent.with_product(product, product_version)


def with_user_agent_extra(key: str, value: str):
    """[INTERNAL API] Add extra metadata to the User-Agent header when developing a library."""
    useragent.with_extra(key, value)


class Config:
    host: str = ConfigAttribute(env="DATABRICKS_HOST")
    account_id: str = ConfigAttribute(env="DATABRICKS_ACCOUNT_ID")
    workspace_id: str = ConfigAttribute(env="DATABRICKS_WORKSPACE_ID")

    # Experimental flag to indicate if the host is a unified host (supports both workspace and account APIs)
    experimental_is_unified_host: bool = ConfigAttribute(env="DATABRICKS_EXPERIMENTAL_IS_UNIFIED_HOST")

    # [Experimental] OpenID Connect discovery URL. When set, OIDC endpoints are fetched directly
    # from this URL instead of the default host-type-based well-known endpoint logic.
    discovery_url: str = ConfigAttribute(env="DATABRICKS_DISCOVERY_URL")

    # PAT token.
    token: str = ConfigAttribute(env="DATABRICKS_TOKEN", auth="pat", sensitive=True)

    # Audience for OIDC ID token source accepting an audience as a parameter.
    # For example, the GitHub action ID token source.
    token_audience: str = ConfigAttribute(env="DATABRICKS_TOKEN_AUDIENCE", auth="github-oidc")

    # Environment variable for OIDC token.
    oidc_token_env: str = ConfigAttribute(env="DATABRICKS_OIDC_TOKEN_ENV", auth="env-oidc")
    oidc_token_filepath: str = ConfigAttribute(env="DATABRICKS_OIDC_TOKEN_FILE", auth="file-oidc")

    username: str = ConfigAttribute(env="DATABRICKS_USERNAME", auth="basic")
    password: str = ConfigAttribute(env="DATABRICKS_PASSWORD", auth="basic", sensitive=True)

    client_id: str = ConfigAttribute(env="DATABRICKS_CLIENT_ID", auth="oauth")
    client_secret: str = ConfigAttribute(env="DATABRICKS_CLIENT_SECRET", auth="oauth", sensitive=True)
    profile: str = ConfigAttribute(env="DATABRICKS_CONFIG_PROFILE")
    config_file: str = ConfigAttribute(env="DATABRICKS_CONFIG_FILE")
    google_service_account: str = ConfigAttribute(env="DATABRICKS_GOOGLE_SERVICE_ACCOUNT", auth="google")
    google_credentials: str = ConfigAttribute(env="GOOGLE_CREDENTIALS", auth="google", sensitive=True)
    azure_workspace_resource_id: str = ConfigAttribute(env="DATABRICKS_AZURE_RESOURCE_ID", auth="azure")
    azure_use_msi: bool = ConfigAttribute(env="ARM_USE_MSI", auth="azure")
    azure_client_secret: str = ConfigAttribute(env="ARM_CLIENT_SECRET", auth="azure", sensitive=True)
    azure_client_id: str = ConfigAttribute(env="ARM_CLIENT_ID", auth="azure")
    azure_tenant_id: str = ConfigAttribute(env="ARM_TENANT_ID", auth="azure")
    azure_environment: str = ConfigAttribute(env="ARM_ENVIRONMENT")
    databricks_cli_path: str = ConfigAttribute(env="DATABRICKS_CLI_PATH")
    auth_type: str = ConfigAttribute(env="DATABRICKS_AUTH_TYPE")
    cluster_id: str = ConfigAttribute(env="DATABRICKS_CLUSTER_ID")
    warehouse_id: str = ConfigAttribute(env="DATABRICKS_WAREHOUSE_ID")
    serverless_compute_id: str = ConfigAttribute(env="DATABRICKS_SERVERLESS_COMPUTE_ID")
    skip_verify: bool = ConfigAttribute()
    http_timeout_seconds: float = ConfigAttribute()
    debug_truncate_bytes: int = ConfigAttribute(env="DATABRICKS_DEBUG_TRUNCATE_BYTES")
    debug_headers: bool = ConfigAttribute(env="DATABRICKS_DEBUG_HEADERS")
    rate_limit: int = ConfigAttribute(env="DATABRICKS_RATE_LIMIT")
    retry_timeout_seconds: int = ConfigAttribute()
    metadata_service_url = ConfigAttribute(
        env="DATABRICKS_METADATA_SERVICE_URL",
        auth="metadata-service",
        sensitive=True,
    )
    max_connection_pools: int = ConfigAttribute()
    max_connections_per_pool: int = ConfigAttribute()
    databricks_environment: Optional[DatabricksEnvironment] = None

    disable_async_token_refresh: bool = ConfigAttribute(env="DATABRICKS_DISABLE_ASYNC_TOKEN_REFRESH")

    disable_experimental_files_api_client: bool = ConfigAttribute(
        env="DATABRICKS_DISABLE_EXPERIMENTAL_FILES_API_CLIENT"
    )

    scopes: list = ConfigAttribute(transform=_parse_scopes)
    authorization_details: str = ConfigAttribute()

    # disable_oauth_refresh_token controls whether a refresh token should be requested
    # during the U2M authentication flow (default to false).
    disable_oauth_refresh_token: bool = ConfigAttribute(env="DATABRICKS_DISABLE_OAUTH_REFRESH_TOKEN")

    files_ext_client_download_streaming_chunk_size: int = 2 * 1024 * 1024  # 2 MiB

    # When downloading a file, the maximum number of attempts to retry downloading the whole file. Default is no limit.
    files_ext_client_download_max_total_recovers: Optional[int] = None

    # When downloading a file, the maximum number of attempts to retry downloading from the same offset without progressing.
    # This is to avoid infinite retrying when the download is not making any progress. Default is 1.
    files_ext_client_download_max_total_recovers_without_progressing = 1

    # File multipart upload/download parameters
    # ----------------------

    # Minimal input stream size (bytes) to use multipart / resumable uploads.
    # For small files it's more efficient to make one single-shot upload request.
    # When uploading a file, SDK will initially buffer this many bytes from input stream.
    # This parameter can be less or bigger than multipart_upload_chunk_size.
    files_ext_multipart_upload_min_stream_size: int = 50 * 1024 * 1024

    # Maximum number of presigned URLs that can be requested at a time.
    #
    # The more URLs we request at once, the higher chance is that some of the URLs will expire
    # before we get to use it. We discover the presigned URL is expired *after* sending the
    # input stream partition to the server. So to retry the upload of this partition we must rewind
    # the stream back. In case of a non-seekable stream we cannot rewind, so we'll abort
    # the upload. To reduce the chance of this, we're requesting presigned URLs one by one
    # and using them immediately.
    files_ext_multipart_upload_batch_url_count: int = 1

    # Size of the chunk to use for multipart uploads & downloads.
    #
    # The smaller chunk is, the less chance for network errors (or URL get expired),
    # but the more requests we'll make.
    # For AWS, minimum is 5Mb: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    # For GCP, minimum is 256 KiB (and also recommended multiple is 256 KiB)
    # boto uses 8Mb: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
    files_ext_multipart_upload_default_part_size: int = 10 * 1024 * 1024  # 10 MiB

    # List of multipart upload part sizes that can be automatically selected
    files_ext_multipart_upload_part_size_options: List[int] = [
        10 * 1024 * 1024,  # 10 MiB
        20 * 1024 * 1024,  # 20 MiB
        50 * 1024 * 1024,  # 50 MiB
        100 * 1024 * 1024,  # 100 MiB
        200 * 1024 * 1024,  # 200 MiB
        500 * 1024 * 1024,  # 500 MiB
        1 * 1024 * 1024 * 1024,  # 1 GiB
        2 * 1024 * 1024 * 1024,  # 2 GiB
        4 * 1024 * 1024 * 1024,  # 4 GiB
    ]

    # Maximum size of a single part in multipart upload.
    # For AWS, maximum is 5 GiB: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    # For Azure, maximum is 4 GiB: https://learn.microsoft.com/en-us/rest/api/storageservices/put-block
    # For CloudFlare R2, maximum is 5 GiB: https://developers.cloudflare.com/r2/objects/multipart-objects/
    files_ext_multipart_upload_max_part_size: int = 4 * 1024 * 1024 * 1024  # 4 GiB

    # Default parallel multipart upload concurrency. Set to 10 because of the experiment results show that it
    # gives good performance result.
    files_ext_multipart_upload_default_parallelism: int = 10

    # The expiration duration for presigned URLs used in multipart uploads and downloads.
    # The client will request new presigned URLs if the previous one is expired. The duration should be long enough
    # to complete the upload or download of a single part.
    files_ext_multipart_upload_url_expiration_duration: datetime.timedelta = datetime.timedelta(hours=1)
    files_ext_presigned_download_url_expiration_duration: datetime.timedelta = datetime.timedelta(hours=1)

    # When downloading a file in parallel, how many worker threads to use.
    files_ext_parallel_download_default_parallelism: int = 10

    # When downloading a file, if the file size is smaller than this threshold,
    # We'll use a single-threaded download even if the parallel download is enabled.
    files_ext_parallel_download_min_file_size: int = 50 * 1024 * 1024  # 50 MiB

    # Default chunk size to use when downloading a file in parallel. Not effective for single threaded download.
    files_ext_parallel_download_default_part_size: int = 10 * 1024 * 1024  # 10 MiB

    # This is not a "wall time" cutoff for the whole upload request,
    # but a maximum time between consecutive data reception events (even 1 byte) from the server
    files_ext_network_transfer_inactivity_timeout_seconds: float = 60

    # Cap on the number of custom retries during incremental uploads:
    # 1) multipart: upload part URL is expired, so new upload URLs must be requested to continue upload
    # 2) resumable: chunk upload produced a retryable response (or exception), so upload status must be
    # retrieved to continue the upload.
    # In these two cases standard SDK retries (which are capped by the `retry_timeout_seconds` option) are not used.
    # Note that retry counter is reset when upload is successfully resumed.
    files_ext_multipart_upload_max_retries = 3

    # Cap on the number of custom retries during parallel downloads.
    files_ext_parallel_download_max_retries = 3

    # Maximum number of retry attempts for FilesExt cloud API operations.
    # This works in conjunction with retry_timeout_seconds - whichever limit
    # is hit first will stop the retry loop.
    experimental_files_ext_cloud_api_max_retries: int = 3

    def __init__(
        self,
        *,
        # Deprecated. Use credentials_strategy instead.
        credentials_provider: Optional[CredentialsStrategy] = None,
        credentials_strategy: Optional[CredentialsStrategy] = None,
        product=None,
        product_version=None,
        clock: Optional[Clock] = None,
        custom_headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """Initialize a Config object.

        Args:
            credentials_provider: (Deprecated) Use credentials_strategy instead.
            credentials_strategy: Custom credentials strategy for authentication.
            product: Product name for User-Agent header.
            product_version: Product version for User-Agent header.
            clock: Clock instance for time-related operations.
            custom_headers: Optional dictionary of custom HTTP headers to include in all API requests.
                These headers will be automatically added to every request made by the client.
                Request-specific headers passed to individual API calls will override these custom headers
                if there is a conflict. Example: {"X-Request-ID": "123", "X-Custom-Header": "value"}
            **kwargs: Additional configuration parameters.
        """
        self._header_factory = None
        self._inner = {}
        self._user_agent_other_info = []
        self._custom_headers = custom_headers or {}
        if credentials_strategy and credentials_provider:
            raise ValueError("When providing `credentials_strategy` field, `credential_provider` cannot be specified.")
        if credentials_provider:
            logger.warning("parameter 'credentials_provider' is deprecated. Use 'credentials_strategy' instead.")
        self._credentials_strategy = next(
            s
            for s in [
                credentials_strategy,
                credentials_provider,
                DefaultCredentials(),
            ]
            if s is not None
        )
        if "databricks_environment" in kwargs:
            self.databricks_environment = kwargs["databricks_environment"]
            del kwargs["databricks_environment"]
        self._clock = clock if clock is not None else RealClock()
        try:
            self._set_inner_config(kwargs)
            self._load_from_env()
            self._known_file_config_loader()
            self._fix_host_if_needed()
            self._validate()
            self.init_auth()
            self._init_product(product, product_version)
        except ValueError as e:
            message = self.wrap_debug_info(str(e))
            raise ValueError(message) from e

    def oauth_token(self) -> Token:
        """Returns the OAuth token from the current credential provider.

        This method only works when using OAuth-based authentication methods.
        If the current credential provider is an OAuthCredentialsProvider, it reuses
        the existing provider. Otherwise, it raises a ValueError indicating that
        OAuth tokens are not available for the current authentication method.
        """
        if isinstance(self._header_factory, OAuthCredentialsProvider):
            return self._header_factory.oauth_token()
        raise ValueError(
            f"OAuth tokens are not available for {self.auth_type} authentication. "
            f"Use an OAuth-based authentication method to access OAuth tokens."
        )

    def wrap_debug_info(self, message: str) -> str:
        debug_string = self.debug_string()
        if debug_string:
            message = f"{message.rstrip('.')}. {debug_string}"
        return message

    @staticmethod
    def parse_dsn(dsn: str) -> "Config":
        uri = urllib.parse.urlparse(dsn)
        if uri.scheme != "databricks":
            raise ValueError(f"Expected databricks:// scheme, got {uri.scheme}://")
        kwargs = {"host": f"https://{uri.hostname}"}
        if uri.username:
            kwargs["username"] = uri.username
        if uri.password:
            kwargs["password"] = uri.password
        query = dict(urllib.parse.parse_qsl(uri.query))
        for attr in Config.attributes():
            if attr.name not in query:
                continue
            kwargs[attr.name] = query[attr.name]
        return Config(**kwargs)

    def authenticate(self) -> Dict[str, str]:
        """Returns a list of fresh authentication headers"""
        return self._header_factory()

    def as_dict(self) -> dict:
        return self._inner

    def _get_azure_environment_name(self) -> str:
        if not self.azure_environment:
            return "PUBLIC"
        env = self.azure_environment.upper()
        # Compatibility with older versions of the SDK that allowed users to specify AzurePublicCloud or AzureChinaCloud
        if env.startswith("AZURE"):
            env = env[len("AZURE") :]
        if env.endswith("CLOUD"):
            env = env[: -len("CLOUD")]
        return env

    @property
    def environment(self) -> DatabricksEnvironment:
        """Returns the environment based on configuration."""
        if self.databricks_environment:
            return self.databricks_environment
        if not self.host and self.azure_workspace_resource_id:
            azure_env = self._get_azure_environment_name()
            for environment in ALL_ENVS:
                if environment.cloud != Cloud.AZURE:
                    continue
                if environment.azure_environment.name != azure_env:
                    continue
                if environment.dns_zone.startswith(".dev") or environment.dns_zone.startswith(".staging"):
                    continue
                return environment
        return get_environment_for_hostname(self.host)

    @property
    def is_azure(self) -> bool:
        if self.azure_workspace_resource_id:
            return True
        return self.environment.cloud == Cloud.AZURE

    @property
    def is_gcp(self) -> bool:
        return self.environment.cloud == Cloud.GCP

    @property
    def is_aws(self) -> bool:
        return self.environment.cloud == Cloud.AWS

    @property
    def host_type(self) -> HostType:
        """Determine the type of host based on the configuration.

        Returns the HostType which can be ACCOUNTS, WORKSPACE, or UNIFIED.
        """
        # Check if explicitly marked as unified host
        if self.experimental_is_unified_host:
            return HostType.UNIFIED

        if not self.host:
            return HostType.WORKSPACE

        # Check for accounts host pattern
        if self.host.startswith("https://accounts.") or self.host.startswith("https://accounts-dod."):
            return HostType.ACCOUNTS

        return HostType.WORKSPACE

    @property
    def client_type(self) -> ClientType:
        """Determine the type of client configuration.

        This is separate from host_type. For example, a unified host can support both
        workspace and account client types.

        Returns ClientType.ACCOUNT or ClientType.WORKSPACE based on the configuration.

        For unified hosts, account_id must be set. If workspace_id is also set,
        returns WORKSPACE, otherwise returns ACCOUNT.
        """
        host_type = self.host_type

        if host_type == HostType.ACCOUNTS:
            return ClientType.ACCOUNT

        if host_type == HostType.WORKSPACE:
            return ClientType.WORKSPACE

        if host_type == HostType.UNIFIED:
            if not self.account_id:
                raise ValueError("Unified host requires account_id to be set")
            if self.workspace_id:
                return ClientType.WORKSPACE
            return ClientType.ACCOUNT

        # Default to workspace for backward compatibility
        return ClientType.WORKSPACE

    @property
    def is_account_client(self) -> bool:
        """[Deprecated] Use host_type or client_type instead.

        Determines if this is an account client based on the host URL.
        """
        if self.experimental_is_unified_host:
            raise ValueError(
                "is_account_client cannot be used with unified hosts; use host_type or client_type instead"
            )
        if not self.host:
            return False
        return self.host.startswith("https://accounts.") or self.host.startswith("https://accounts-dod.")

    @property
    def arm_environment(self) -> AzureEnvironment:
        return self.environment.azure_environment

    @property
    def effective_azure_login_app_id(self):
        return self.environment.azure_application_id

    @property
    def hostname(self) -> str:
        url = urllib.parse.urlparse(self.host)
        return url.netloc

    @property
    def is_any_auth_configured(self) -> bool:
        for attr in Config.attributes():
            if not attr.auth:
                continue
            value = self._inner.get(attr.name, None)
            if value:
                return True
        return False

    @property
    def user_agent(self):
        """Returns User-Agent header used by this SDK"""

        # global user agent includes SDK version, product name & version, platform info,
        # and global extra info. Config can have specific extra info associated with it,
        # such as an override product, auth type, and other user-defined information.
        return useragent.to_string(
            self._product_info,
            [("auth", self.auth_type)] + self._user_agent_other_info,
        )

    @property
    def _upstream_user_agent(self) -> str:
        return " ".join(f"{k}/{v}" for k, v in useragent._get_upstream_user_agent_info())

    def with_user_agent_extra(self, key: str, value: str) -> "Config":
        self._user_agent_other_info.append((key, value))
        return self

    @property
    def databricks_oidc_endpoints(self) -> Optional[OidcEndpoints]:
        """Get OIDC endpoints for Databricks OAuth.

        If discovery_url is set, OIDC endpoints are fetched directly from it. Otherwise
        falls back to the host-type-based well-known endpoint logic.

        Note: This method does NOT return Azure Entra ID endpoints. For Azure authentication,
        use get_azure_entra_id_workspace_endpoints() directly.

        Returns:
            OidcEndpoints for Databricks OAuth, or None if host is not configured.
        """
        self._fix_host_if_needed()
        if not self.host:
            return None

        if self.discovery_url:
            return get_endpoints_from_url(self.discovery_url)

        # Handle unified hosts
        if self.host_type == HostType.UNIFIED:
            if not self.account_id:
                raise ValueError("Unified host requires account_id to be set for OAuth endpoints")
            return get_unified_endpoints(self.host, self.account_id)

        # Handle traditional account hosts
        if self.host_type == HostType.ACCOUNTS and self.account_id:
            return get_account_endpoints(self.host, self.account_id)

        # Default to workspace endpoints
        return get_workspace_endpoints(self.host)

    @property
    def oidc_endpoints(self) -> Optional[OidcEndpoints]:
        """[DEPRECATED] Get OIDC endpoints with automatic Azure detection (deprecated).

        This method incorrectly returns Azure OIDC endpoints when azure_client_id
        is set, even for Databricks OAuth flows that don't use Azure authentication. This caused
        bugs where Databricks M2M OAuth would fail when ARM_CLIENT_ID was set for other purposes.

        Use instead:
        - databricks_oidc_endpoints: For Databricks OAuth (oauth-m2m, external-browser, etc.)
        - get_azure_entra_id_workspace_endpoints(): For Azure Entra ID authentication

        Returns:
            OidcEndpoints (Azure or Databricks depending on config), or None if host is not configured.
        """
        self._fix_host_if_needed()
        if not self.host:
            return None
        if self.is_azure and self.azure_client_id:
            return get_azure_entra_id_workspace_endpoints(self.host)
        return self.databricks_oidc_endpoints

    def debug_string(self) -> str:
        """Returns log-friendly representation of configured attributes"""
        buf = []
        attrs_used = []
        envs_used = []
        for attr in Config.attributes():
            if attr.env and os.environ.get(attr.env):
                envs_used.append(attr.env)
            value = getattr(self, attr.name)
            if not value:
                continue
            safe = "***" if attr.sensitive else f"{value}"
            attrs_used.append(f"{attr.name}={safe}")
        if attrs_used:
            buf.append(f"Config: {', '.join(attrs_used)}")
        if envs_used:
            buf.append(f"Env: {', '.join(envs_used)}")
        return ". ".join(buf)

    def to_dict(self) -> Dict[str, any]:
        return self._inner

    @property
    def sql_http_path(self) -> Optional[str]:
        """(Experimental) Return HTTP path for SQL Drivers.

        If `cluster_id` or `warehouse_id` are configured, return a valid HTTP Path argument
        used in construction of JDBC/ODBC DSN string.

        See https://docs.databricks.com/integrations/jdbc-odbc-bi.html
        """
        if (not self.cluster_id) and (not self.warehouse_id):
            return None
        if self.cluster_id and self.warehouse_id:
            raise ValueError("cannot have both cluster_id and warehouse_id")
        headers = self.authenticate()
        headers["User-Agent"] = f"{self.user_agent} sdk-feature/sql-http-path"
        if self.cluster_id:
            response = requests.get(f"{self.host}/api/2.0/preview/scim/v2/Me", headers=headers)
            # get workspace ID from the response header
            workspace_id = response.headers.get("x-databricks-org-id")
            return f"sql/protocolv1/o/{workspace_id}/{self.cluster_id}"
        if self.warehouse_id:
            return f"/sql/1.0/warehouses/{self.warehouse_id}"

    @property
    def clock(self) -> Clock:
        return self._clock

    @classmethod
    def attributes(cls) -> Iterable[ConfigAttribute]:
        """Returns a list of Databricks SDK configuration metadata"""
        if hasattr(cls, "_attributes"):
            return cls._attributes
        if sys.version_info[1] >= 10:
            import inspect

            anno = inspect.get_annotations(cls)
        else:
            # Python 3.7 compatibility: getting type hints require extra hop, as described in
            # "Accessing The Annotations Dict Of An Object In Python 3.9 And Older" section of
            # https://docs.python.org/3/howto/annotations.html
            anno = cls.__dict__["__annotations__"]
        attrs = []
        for name, v in cls.__dict__.items():
            if type(v) != ConfigAttribute:
                continue
            v.name = name
            v.transform = v._custom_transform if v._custom_transform else anno.get(name, str)
            attrs.append(v)
        cls._attributes = attrs
        return cls._attributes

    def _resolve_host_metadata(self) -> None:
        """[Experimental] Populate missing config fields from the host's
        /.well-known/databricks-config discovery endpoint.

        Fills in account_id, workspace_id, and discovery_url (derived from oidc_endpoint,
        with any {account_id} placeholder substituted) if not already set.
        """
        if not self.host:
            return
        meta = get_host_metadata(self.host)
        if not self.account_id and meta.account_id:
            logger.debug(f"Resolved account_id from host metadata: {meta.account_id}")
            self.account_id = meta.account_id
        if not self.account_id:
            raise ValueError("account_id is not configured and could not be resolved from host metadata")
        if not self.workspace_id and meta.workspace_id:
            logger.debug(f"Resolved workspace_id from host metadata: {meta.workspace_id}")
            self.workspace_id = meta.workspace_id
        if not self.discovery_url:
            if meta.oidc_endpoint:
                logger.debug(f"Resolved discovery_url from host metadata: {meta.oidc_endpoint}")
                self.discovery_url = meta.oidc_endpoint.replace("{account_id}", self.account_id)
            else:
                raise ValueError("discovery_url is not configured and could not be resolved from host metadata")

    def _fix_host_if_needed(self):
        updated_host = _fix_host_if_needed(self.host)
        if updated_host:
            self.host = updated_host

    def load_azure_tenant_id(self):
        """[Internal] Load the Azure tenant ID from the Azure Databricks login page.

        If the tenant ID is already set, this method does nothing."""
        if self.azure_tenant_id is not None or self.host is None:
            return
        login_url = f"{self.host}/aad/auth"
        logger.debug(f"Loading tenant ID from {login_url}")
        resp = requests.get(login_url, allow_redirects=False)
        if resp.status_code // 100 != 3:
            logger.debug(f"Failed to get tenant ID from {login_url}: expected status code 3xx, got {resp.status_code}")
            return
        entra_id_endpoint = resp.headers.get("Location")
        if entra_id_endpoint is None:
            logger.debug(f"No Location header in response from {login_url}")
            return
        # The Location header has the following form: https://login.microsoftonline.com/<tenant-id>/oauth2/authorize?...
        # The domain may change depending on the Azure cloud (e.g. login.microsoftonline.us for US Government cloud).
        url = urllib.parse.urlparse(entra_id_endpoint)
        path_segments = url.path.split("/")
        if len(path_segments) < 2:
            logger.debug(f"Invalid path in Location header: {url.path}")
            return
        self.azure_tenant_id = path_segments[1]
        logger.debug(f"Loaded tenant ID: {self.azure_tenant_id}")

    def _set_inner_config(self, keyword_args: Dict[str, any]):
        for attr in self.attributes():
            if attr.name not in keyword_args:
                continue
            if keyword_args.get(attr.name, None) is None:
                continue
            self.__setattr__(attr.name, keyword_args[attr.name])

    def _load_from_env(self):
        found = False
        for attr in self.attributes():
            if not attr.env:
                continue
            if attr.name in self._inner:
                continue
            value = os.environ.get(attr.env)
            if not value:
                continue
            self.__setattr__(attr.name, value)
            found = True
        if found:
            logger.debug("Loaded from environment")

    def _known_file_config_loader(self):
        if not self.profile and (self.is_any_auth_configured or self.host or self.azure_workspace_resource_id):
            # skip loading configuration file if there's any auth configured
            # directly as part of the Config() constructor.
            return
        config_file = self.config_file
        if not config_file:
            config_file = "~/.databrickscfg"
        config_path = pathlib.Path(config_file).expanduser()
        if not config_path.exists():
            logger.debug("%s does not exist", config_path)
            return
        ini_file = configparser.ConfigParser()
        ini_file.read(config_path)
        profile = self.profile
        has_explicit_profile = self.profile is not None
        # In Go SDK, we skip merging the profile with DEFAULT section, though Python's ConfigParser.items()
        # is returning profile key-value pairs _including those from DEFAULT_. This is not what we expect
        # from Unified Auth test suite at the moment. Hence, the private variable access.
        # See: https://docs.python.org/3/library/configparser.html#mapping-protocol-access
        if not has_explicit_profile and not ini_file.defaults():
            logger.debug(f"{config_path} has no DEFAULT profile configured")
            return
        if not has_explicit_profile:
            profile = "DEFAULT"
        profiles = ini_file._sections
        if ini_file.defaults():
            profiles["DEFAULT"] = ini_file.defaults()
        if profile not in profiles:
            raise ValueError(f"resolve: {config_path} has no {profile} profile configured")
        raw_config = profiles[profile]
        logger.info(f"loading {profile} profile from {config_file}: {', '.join(raw_config.keys())}")
        for k, v in raw_config.items():
            if k in self._inner:
                # don't overwrite a value previously set
                continue
            self.__setattr__(k, v)

    def _validate(self):
        auths_used = set()
        for attr in Config.attributes():
            if attr.name not in self._inner:
                continue
            if not attr.auth:
                continue
            auths_used.add(attr.auth)
        if len(auths_used) <= 1:
            return
        if self.auth_type:
            # client has auth preference set
            return
        names = " and ".join(sorted(auths_used))
        raise ValueError(f"validate: more than one authorization method configured: {names}")

    def init_auth(self):
        try:
            self._header_factory = self._credentials_strategy(self)
            self.auth_type = self._credentials_strategy.auth_type()
            if not self._header_factory:
                raise ValueError("not configured")
        except ValueError as e:
            raise ValueError(f"{self._credentials_strategy.auth_type()} auth: {e}") from e

    def _init_product(self, product, product_version):
        if product is not None or product_version is not None:
            default_product, default_version = useragent.product()
            self._product_info = (
                product or default_product,
                product_version or default_version,
            )
        else:
            self._product_info = None

    def get_scopes(self) -> list:
        """Get OAuth scopes with proper defaulting.

        Returns ["all-apis"] if no scopes configured.
        This is the single source of truth for scope defaulting across all OAuth methods.
        """
        return self.scopes if self.scopes else ["all-apis"]

    def get_scopes_as_string(self) -> str:
        """Get OAuth scopes as a space-separated string.

        Returns "all-apis" if no scopes configured.
        """
        return " ".join(self.get_scopes())

    def __repr__(self):
        return f"<{self.debug_string()}>"

    def copy(self):
        """Creates a copy of the config object.
        All the copies share most of their internal state (ie, shared reference to fields such as credential_provider).
        Copies have their own instances of the following fields
            - `_user_agent_other_info`
        """
        cpy: Config = copy.copy(self)
        cpy._user_agent_other_info = copy.deepcopy(self._user_agent_other_info)
        return cpy

    def deep_copy(self):
        """Creates a deep copy of the config object."""
        return copy.deepcopy(self)
