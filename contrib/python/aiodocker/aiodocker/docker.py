from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import ssl
import sys
import warnings
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    Optional,
    Type,
    Union,
)

import aiohttp
import attrs
from multidict import CIMultiDict
from yarl import URL

# Sub-API classes
from .configs import DockerConfigs
from .containers import DockerContainer, DockerContainers
from .events import DockerEvents
from .exceptions import (
    DockerContextInvalidError,
    DockerContextTLSError,
    DockerError,
)
from .images import DockerImages
from .logs import DockerLog
from .networks import DockerNetwork, DockerNetworks
from .nodes import DockerSwarmNodes
from .secrets import DockerSecrets
from .services import DockerServices
from .swarm import DockerSwarm
from .system import DockerSystem
from .tasks import DockerTasks
from .types import SENTINEL, JSONObject, Sentinel
from .utils import _suppress_timeout_deprecation, httpize, parse_result
from .volumes import DockerVolume, DockerVolumes


__all__ = (
    "Docker",
    "DockerConfigs",
    "DockerContainer",
    "DockerContainers",
    "DockerContextEndpoint",
    "DockerEvents",
    "DockerImages",
    "DockerLog",
    "DockerNetwork",
    "DockerNetworks",
    "DockerSecrets",
    "DockerServices",
    "DockerSwarm",
    "DockerSwarmNodes",
    "DockerSystem",
    "DockerTasks",
    "DockerVolume",
    "DockerVolumes",
)

log = logging.getLogger(__name__)

_sock_search_paths = [
    Path("/run/docker.sock"),
    Path("/var/run/docker.sock"),
    Path.home() / ".docker/run/docker.sock",
]

_rx_version = re.compile(r"^v\d+\.\d+$")
_rx_tcp_schemes = re.compile(r"^(tcp|http|https)://")


@attrs.define
class DockerContextEndpoint:
    """Docker context endpoint configuration.

    Holds the endpoint settings from a Docker context, including
    connection host and TLS configuration.
    """

    host: str
    context_name: Optional[str] = None
    skip_tls_verify: bool = False
    tls_ca: Optional[bytes] = None
    tls_cert: Optional[bytes] = None
    tls_key: Optional[bytes] = None

    @property
    def has_tls(self) -> bool:
        """Check if TLS credentials are available."""
        return self.tls_ca is not None or self.tls_cert is not None


class Docker:
    """
    The Docker client as the main entrypoint to the sub-APIs such as
    container, images, networks, swarm and services, etc.
    You may access such sub-API collections via the attributes of the client instance,
    like:

    .. code-block:: python

        docker = aiodocker.Docker()
        await docker.containers.list()
        await docker.images.pull(...)

    Docker Host Resolution Precedence
    ----------------------------------
    The Docker host is determined using the following precedence order (highest to lowest):

    1. **url parameter** - Explicitly provided Docker daemon address
    2. **context parameter** - Explicitly specified Docker context name
    3. **DOCKER_HOST environment variable** - Falls back when no explicit context specified
    4. **DOCKER_CONTEXT environment variable** - Specifies which Docker context to use
    5. **currentContext from ~/.docker/config.json** - Default context set by docker CLI
    6. **Socket auto-detection** - Searches common socket paths (e.g., /var/run/docker.sock)

    Note: Explicit constructor parameters (``url``, ``context``) take precedence over
    environment variables (``DOCKER_HOST``, ``DOCKER_CONTEXT``). This follows the principle
    that explicit code configuration overrides implicit environment configuration, matching
    the pattern used in the official Docker Go client.

    Args:
        url: The Docker daemon address as the full URL string (e.g.,
            ``"unix:///var/run/docker.sock"``, ``"tcp://127.0.0.1:2375"``,
            ``"npipe:////./pipe/docker_engine"``, ``"ssh://user@host:port"``).
            Takes highest precedence when specified.
            Refer to :doc:`ssh` for more details about SSH transports in the URL.
        connector: Custom :class:`aiohttp.BaseConnector` implementation to establish new connections to the docker host.
            If provided, it will be used instead of creating a connector based on the **url** value.
        session: Custom :class:`aiohttp.ClientSession`. If None, a new session will be
            created with the connector and timeout settings.
            The timeout configuration in this object is *ignored* by the **timeout** argument.
        timeout: :class:`aiohttp.ClientTimeout` configuration for API requests.
            If None, there is no timeout at all.
        ssl_context: SSL context for HTTPS connections. If None and ``DOCKER_TLS_VERIFY``
            is set, will create a context using ``DOCKER_CERT_PATH`` certificates.
        api_version: Pin the Docker API version (e.g., "v1.43"). Use "auto" to
            automatically detect the API version from the daemon.
        context: Docker context name to use (e.g., "production", "staging").
            When specified, loads the endpoint configuration from the named context
            in ``~/.docker/contexts/``. Overrides all environment variables
            (``DOCKER_HOST``, ``DOCKER_CONTEXT``) and config file settings.

    Raises:
        ValueError: Raised if the docker host cannot be determined,
            if both url and connector are incompatible,
            or if api_version format is invalid.
        OSError: On Windows, if named pipe access fails unexpectedly.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        connector: Optional[aiohttp.BaseConnector] = None,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: Optional[aiohttp.ClientTimeout] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        api_version: str = "auto",
        context: Optional[str] = None,
    ) -> None:
        docker_host = url  # rename
        context_endpoint: Optional[DockerContextEndpoint] = None

        if docker_host is None:
            context_endpoint = self._get_docker_context_endpoint(context)
            if context_endpoint is not None:
                docker_host = context_endpoint.host
        if docker_host is None:
            docker_host = os.environ.get("DOCKER_HOST", None)
        if docker_host is None:
            for sockpath in _sock_search_paths:
                if sockpath.is_socket():
                    docker_host = "unix://" + str(sockpath)
                    break
        if docker_host is None and sys.platform == "win32":
            try:
                if Path(r"\\.\pipe\docker_engine").exists():
                    docker_host = "npipe:////./pipe/docker_engine"
                else:
                    # The default address used by Docker Client on Windows
                    docker_host = "https://127.0.0.1:2376"
            except OSError as ex:
                if ex.winerror == 231:  # type: ignore
                    # All pipe instances are busy
                    # but the pipe definitely exists
                    docker_host = "npipe:////./pipe/docker_engine"
                else:
                    raise

        assert docker_host is not None
        self.docker_host = docker_host

        if api_version != "auto" and _rx_version.search(api_version) is None:
            raise ValueError("Invalid API version format")
        self.api_version = api_version

        self._timeout = timeout or aiohttp.ClientTimeout()

        if docker_host is None:
            raise ValueError(
                "Missing valid docker_host."
                "Either DOCKER_HOST or local sockets are not available."
            )

        self._connection_info = docker_host
        if connector is None:
            UNIX_PRE = "unix://"
            UNIX_PRE_LEN = len(UNIX_PRE)
            WIN_PRE = "npipe://"
            WIN_PRE_LEN = len(WIN_PRE)
            SSH_PRE = "ssh://"

            if _rx_tcp_schemes.search(docker_host):
                # Determine SSL context: user-provided > context TLS > DOCKER_TLS_VERIFY
                if ssl_context is None and context_endpoint is not None:
                    ssl_context = self._create_context_ssl_context(
                        context_endpoint, context_name=context_endpoint.context_name
                    )
                    if ssl_context is not None:
                        docker_host = _rx_tcp_schemes.sub("https://", docker_host)
                if (
                    ssl_context is None
                    and os.environ.get("DOCKER_TLS_VERIFY", "0") == "1"
                ):
                    ssl_context = self._docker_machine_ssl_context()
                    docker_host = _rx_tcp_schemes.sub("https://", docker_host)
                connector = aiohttp.TCPConnector(ssl=ssl_context)  # type: ignore[arg-type]
                self.docker_host = docker_host
            elif docker_host.startswith(UNIX_PRE):
                connector = aiohttp.UnixConnector(docker_host[UNIX_PRE_LEN:])
                # dummy hostname for URL composition
                self.docker_host = UNIX_PRE + "localhost"
            elif docker_host.startswith(WIN_PRE):
                connector = aiohttp.NamedPipeConnector(
                    docker_host[WIN_PRE_LEN:].replace("/", "\\")
                )
                # dummy hostname for URL composition
                self.docker_host = WIN_PRE + "localhost"
            elif docker_host.startswith(SSH_PRE):
                from .ssh import SSHConnector

                connector = SSHConnector(docker_host)
                # dummy hostname for URL composition
                self.docker_host = "unix://localhost"
            else:
                raise ValueError("Missing protocol scheme in docker_host.")
        self.connector = connector
        if session is None:
            session = aiohttp.ClientSession(
                connector=self.connector,
                timeout=self._timeout,
            )
        self.session = session

        self.events = DockerEvents(self)
        self.containers = DockerContainers(self)
        self.swarm = DockerSwarm(self)
        self.services = DockerServices(self)
        self.configs = DockerConfigs(self)
        self.secrets = DockerSecrets(self)
        self.tasks = DockerTasks(self)
        self.images = DockerImages(self)
        self.volumes = DockerVolumes(self)
        self.networks = DockerNetworks(self)
        self.nodes = DockerSwarmNodes(self)
        self.system = DockerSystem(self)

        # legacy aliases
        self.pull = self.images.pull
        self.push = self.images.push

    async def __aenter__(self) -> Docker:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the Docker client and release resources.

        Stops the event monitoring and closes the underlying aiohttp session,
        releasing all associated resources including connections.
        """
        await self.events.stop()
        await self.session.close()

    async def auth(self, **credentials: Any) -> dict[str, Any]:
        """Authenticate with Docker registry.

        Validates registry credentials and returns authentication information.

        Args:
            credentials: Registry authentication credentials.
                Typically includes:

                - ``username`` (str): Registry username
                - ``password`` (str): Registry password
                - ``serveraddress`` (str, optional): Registry server address without a protocol

                If you have an identity token issued in prior, you may pass ``identitytoken`` only.

        Returns:
            Authentication response from the Docker daemon,
            including:

            - ``Status`` (str): A string message like "Login Succeeded"
            - ``IdentityToken`` (str): The identity token

        Raises:
            DockerError: If authentication fails or credentials are invalid.
        """
        response = await self._query_json("auth", "POST", data=credentials)
        return response

    async def version(self) -> dict[str, Any]:
        """Get Docker daemon version information.

        Retrieves version details about the Docker daemon including API version,
        OS, architecture, and component versions.

        Returns:
            A dict containing version information with keys
            like:

            - ``Version`` (str): Docker version
            - ``ApiVersion`` (str): API version
            - ``Os`` (str): Operating system
            - ``Arch`` (str): Architecture
            - ``KernelVersion`` (str): Kernel version
            - ``GitCommit`` (str): Git commit hash

            and additional component-specific information.

        Raises:
            DockerError: If the request fails or daemon is unreachable.
        """
        data = await self._query_json("version")
        return data

    def _canonicalize_url(
        self, path: Union[str, URL], *, versioned_api: bool = True
    ) -> URL:
        if isinstance(path, URL):
            assert not path.is_absolute()
        if versioned_api:
            return URL(
                "{self.docker_host}/{self.api_version}/{path}".format(
                    self=self, path=path
                )
            )
        else:
            return URL(f"{self.docker_host}/{path}")

    def _resolve_long_running_timeout(
        self,
        timeout: float | aiohttp.ClientTimeout | Sentinel | None,
    ) -> aiohttp.ClientTimeout:
        """Resolve timeout for long-running operations (logs, stats, build, etc.).

        For long-running operations, defaults to infinite timeout by setting
        both total and sock_read to None while preserving other timeout settings
        from the client configuration.

        Args:
            timeout: The timeout value to resolve. If SENTINEL, returns an infinite
                    timeout based on the client's base timeout configuration, overriding
                    both total and sock_read but preserving other timeouts such as
                    connection establishment timeouts.
                    If None, returns infinite timeout. If float, returns a ClientTimeout
                    based on the client's base timeout configuration with its total and
                    sock_read timeout overridden.

        Returns:
            An explicit ClientTimeout configuration.
        """
        if timeout is SENTINEL:
            # Inherit the parent client's timeout but override total and sock_read
            # because they don't make sense for long-running operations.
            return attrs.evolve(self._timeout, total=None, sock_read=None)
        elif timeout is None:
            # Infinite timeout
            return aiohttp.ClientTimeout()
        elif isinstance(timeout, float):
            # Inherit the parent client's timeout but override total and sock_read
            # as the given value.
            return attrs.evolve(self._timeout, total=timeout, sock_read=timeout)
        else:
            # Already a manually configured ClientTimeout, return as-is
            return timeout

    async def _check_version(self) -> None:
        if self.api_version == "auto":
            ver = await self._query_json("version", versioned_api=False)
            self.api_version = "v" + str(ver["ApiVersion"])

    @asynccontextmanager
    async def _query(
        self,
        path: str | URL,
        method: str = "GET",
        *,
        params: Optional[JSONObject] = None,
        data: Optional[Any] = None,
        headers: Optional[Mapping[str, str | int | bool]] = None,
        timeout: float | aiohttp.ClientTimeout | None | Sentinel = SENTINEL,
        chunked: Optional[bool] = None,
        read_until_eof: bool = True,
        versioned_api: bool = True,
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """
        Get the response object by performing the HTTP request.
        The caller is responsible to finalize the response object
        via the async context manager protocol.
        """
        yield await self._do_query(
            path=path,
            method=method,
            params=params,
            data=data,
            headers=headers,
            timeout=timeout,
            chunked=chunked,
            read_until_eof=read_until_eof,
            versioned_api=versioned_api,
        )

    async def _do_query(
        self,
        path: str | URL,
        method: str,
        *,
        params: Optional[JSONObject] = None,
        data: Any = None,
        headers: Optional[Mapping[str, str | int | bool]] = None,
        timeout: float | aiohttp.ClientTimeout | None | Sentinel = SENTINEL,
        chunked: Optional[bool] = None,
        read_until_eof: bool = True,
        versioned_api: bool = True,
    ) -> aiohttp.ClientResponse:
        if versioned_api:
            await self._check_version()
        url = self._canonicalize_url(path, versioned_api=versioned_api)
        _headers: CIMultiDict[str | int | bool] = CIMultiDict()
        if headers:
            _headers.update(headers)
        if "Content-Type" not in _headers:
            _headers["Content-Type"] = "application/json"
        # Derive from the timeout configured upon the client instance creation.
        _timeout = self._timeout
        match timeout:
            case float():
                if not _suppress_timeout_deprecation.get():
                    warnings.warn(
                        "Manually setting timeouts via float is highly discouraged. "
                        "Use asyncio.timeout() block or pass aiohttp.ClientTimeout instead.",
                        DeprecationWarning,
                        stacklevel=2,
                    )
                # Set both total and sock_read consistently for float timeouts
                _timeout = attrs.evolve(_timeout, total=timeout, sock_read=timeout)
            case aiohttp.ClientTimeout():
                # Override with the caller's decision.
                _timeout = timeout
            case None:
                # Infinite timeout.
                _timeout = aiohttp.ClientTimeout()
            case Sentinel.TOKEN:
                # Use the client-level config.
                pass
        try:
            real_params = httpize(params)
            real_headers = httpize(_headers)
            response = await self.session.request(
                method,
                url,
                params=real_params,
                headers=real_headers,
                data=data,
                timeout=_timeout,
                chunked=chunked,
                read_until_eof=read_until_eof,
            )
        except asyncio.TimeoutError:
            raise
        except aiohttp.ClientConnectionError as exc:
            raise DockerError(
                900,
                f"Cannot connect to Docker Engine via {self._connection_info} [{exc}]",
            )
        if (response.status // 100) in [4, 5]:
            what = await response.read()
            content_type = response.headers.get("content-type", "")
            response.close()
            if content_type == "application/json":
                data = json.loads(what.decode("utf8"))
                raise DockerError(response.status, data["message"])
            else:
                raise DockerError(response.status, what.decode("utf8"))
        return response

    async def _query_json(
        self,
        path: str | URL,
        method: str = "GET",
        *,
        params: Optional[JSONObject] = None,
        data: Optional[Any] = None,
        headers: Optional[Mapping[str, str | int | bool]] = None,
        timeout: float | aiohttp.ClientTimeout | None | Sentinel = SENTINEL,
        read_until_eof: bool = True,
        versioned_api: bool = True,
    ) -> Any:
        """
        A shorthand of _query() that treats the input as JSON.
        """
        _headers: CIMultiDict[str | int | bool] = CIMultiDict()
        if headers:
            _headers.update(headers)
        if "Content-Type" not in _headers:
            _headers["Content-Type"] = "application/json"
        if data is not None and not isinstance(data, (str, bytes)):
            data = json.dumps(data)
        async with self._query(
            path,
            method,
            params=params,
            data=data,
            headers=_headers,
            timeout=timeout,
            read_until_eof=read_until_eof,
            versioned_api=versioned_api,
        ) as response:
            data = await parse_result(response)
            return data

    def _query_chunked_post(
        self,
        path: str | URL,
        method: str = "POST",
        *,
        params: Optional[JSONObject] = None,
        data: Optional[Any] = None,
        headers: Optional[Mapping[str, str | int | bool]] = None,
        timeout: float | aiohttp.ClientTimeout | None | Sentinel = SENTINEL,
        read_until_eof: bool = True,
        versioned_api: bool = True,
    ) -> AbstractAsyncContextManager[aiohttp.ClientResponse]:
        """
        A shorthand for uploading data by chunks
        """
        _headers: CIMultiDict[str | int | bool] = CIMultiDict()
        if headers:
            _headers.update(headers)
        if "Content-Type" not in _headers:
            _headers["Content-Type"] = "application/octet-stream"
        return self._query(
            path,
            method,
            params=params,
            data=data,
            headers=headers,
            timeout=timeout,
            chunked=True,
            read_until_eof=read_until_eof,
        )

    async def _websocket(
        self, path: Union[str, URL], **params: Any
    ) -> aiohttp.ClientWebSocketResponse:
        if not params:
            params = {"stdin": True, "stdout": True, "stderr": True, "stream": True}
        url = self._canonicalize_url(path)
        # ws_connect() does not have params arg.
        url = url.with_query(httpize(params))
        ws = await self.session.ws_connect(
            url,
            protocols=["chat"],
            origin="http://localhost",
            autoping=True,
            autoclose=True,
        )
        return ws

    @staticmethod
    def _docker_machine_ssl_context() -> ssl.SSLContext:
        """
        Create a SSLContext object using DOCKER_* env vars.
        """
        context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        context.set_ciphers(ssl._RESTRICTED_SERVER_CIPHERS)  # type: ignore
        certs_path = os.environ.get("DOCKER_CERT_PATH", None)
        if certs_path is None:
            raise ValueError("Cannot create ssl context, DOCKER_CERT_PATH is not set!")
        certs_path2 = Path(certs_path)
        context.load_verify_locations(cafile=str(certs_path2 / "ca.pem"))
        context.load_cert_chain(
            certfile=str(certs_path2 / "cert.pem"), keyfile=str(certs_path2 / "key.pem")
        )
        return context

    @staticmethod
    def _get_context_dir_name(context_name: str) -> str:
        """Compute the SHA256 hash used for context directory names.

        Docker CLI uses SHA256 of the context name as the directory name
        under ~/.docker/contexts/meta/ and ~/.docker/contexts/tls/.
        """
        return hashlib.sha256(context_name.encode("utf-8")).hexdigest()

    @staticmethod
    def _get_docker_context_endpoint(
        context_name: Optional[str] = None,
    ) -> Optional[DockerContextEndpoint]:
        """Get the Docker endpoint configuration from the current Docker context.

        Resolution order:
        1. context_name parameter (if provided)
        2. DOCKER_CONTEXT environment variable
        3. currentContext from ~/.docker/config.json
        4. If context name is "default" or not found, return None
           (caller should fall back to DOCKER_HOST or socket search)

        Args:
            context_name: Explicit context name to use. If provided, takes precedence
                over environment variables and config file.

        Returns:
            DockerContextEndpoint with host, TLS settings, and certificates,
            or None if no context is configured or the context is "default".

        Raises:
            DockerContextInvalidError: If the context is configured but has invalid
                data (e.g., invalid JSON, missing required fields, or context
                directory not found).
        """
        current_context_name = context_name
        if current_context_name is None:
            current_context_name = os.environ.get("DOCKER_CONTEXT", None)
        if current_context_name is None:
            try:
                docker_config_path = Path.home() / ".docker" / "config.json"
                docker_config = json.loads(docker_config_path.read_bytes())
                current_context_name = docker_config.get("currentContext")
            except OSError:
                # Config file doesn't exist - fallback to DOCKER_HOST
                return None
            except json.JSONDecodeError as e:
                raise DockerContextInvalidError(
                    f"Invalid JSON in Docker config file: {e}"
                )

        # "default" is a virtual context that doesn't exist as files.
        # It means "use DOCKER_HOST or search for local sockets".
        if current_context_name is None or current_context_name == "default":
            return None

        # Compute the SHA256 hash of the context name to find its directory
        context_dir_name = Docker._get_context_dir_name(current_context_name)
        contexts_dir = Path.home() / ".docker" / "contexts"
        meta_path = contexts_dir / "meta" / context_dir_name / "meta.json"

        try:
            context_data = json.loads(meta_path.read_bytes())
        except OSError as e:
            # Context directory/file doesn't exist
            # If context was set via DOCKER_CONTEXT env var, this is an error
            # If context was set via config.json, this is also an error since
            # the config explicitly references a non-existent context
            raise DockerContextInvalidError(
                f"Context metadata file not found: {meta_path}",
                context_name=current_context_name,
            ) from e
        except json.JSONDecodeError as e:
            raise DockerContextInvalidError(
                f"Invalid JSON in context metadata file: {e}",
                context_name=current_context_name,
            ) from e

        try:
            docker_endpoint = context_data["Endpoints"]["docker"]
        except KeyError as e:
            raise DockerContextInvalidError(
                f"Missing required field in context metadata: {e}",
                context_name=current_context_name,
            ) from e

        try:
            host = docker_endpoint["Host"]
        except KeyError as e:
            raise DockerContextInvalidError(
                "Missing 'Host' field in docker endpoint configuration",
                context_name=current_context_name,
            ) from e

        skip_tls_verify = docker_endpoint.get("SkipTLSVerify", False)

        # Load TLS certificates if available
        tls_dir = contexts_dir / "tls" / context_dir_name / "docker"
        tls_ca, tls_cert, tls_key = Docker._load_context_tls(
            tls_dir, context_name=current_context_name
        )

        return DockerContextEndpoint(
            host=host,
            context_name=current_context_name,
            skip_tls_verify=skip_tls_verify,
            tls_ca=tls_ca,
            tls_cert=tls_cert,
            tls_key=tls_key,
        )

    @staticmethod
    def _load_context_tls(
        tls_dir: Path,
        *,
        context_name: Optional[str] = None,
    ) -> tuple[Optional[bytes], Optional[bytes], Optional[bytes]]:
        """Load TLS certificate files from a context's TLS directory.

        Args:
            tls_dir: Path to the TLS directory (e.g., ~/.docker/contexts/tls/{hash}/docker)
            context_name: Name of the context (for error messages).

        Returns:
            Tuple of (ca_data, cert_data, key_data), each being bytes or None
            if the file doesn't exist.

        Raises:
            DockerContextTLSError: If a TLS file exists but cannot be read.
        """
        ca_data: Optional[bytes] = None
        cert_data: Optional[bytes] = None
        key_data: Optional[bytes] = None

        ca_path = tls_dir / "ca.pem"
        if ca_path.exists():
            try:
                ca_data = ca_path.read_bytes()
            except OSError as e:
                raise DockerContextTLSError(
                    f"Failed to read CA certificate: {ca_path}: {e}",
                    context_name=context_name,
                ) from e

        cert_path = tls_dir / "cert.pem"
        if cert_path.exists():
            try:
                cert_data = cert_path.read_bytes()
            except OSError as e:
                raise DockerContextTLSError(
                    f"Failed to read client certificate: {cert_path}: {e}",
                    context_name=context_name,
                ) from e

        key_path = tls_dir / "key.pem"
        if key_path.exists():
            try:
                key_data = key_path.read_bytes()
            except OSError as e:
                raise DockerContextTLSError(
                    f"Failed to read private key: {key_path}: {e}",
                    context_name=context_name,
                ) from e

        return ca_data, cert_data, key_data

    @staticmethod
    def _create_context_ssl_context(
        endpoint: DockerContextEndpoint,
        *,
        context_name: Optional[str] = None,
    ) -> Optional[ssl.SSLContext]:
        """Create an SSL context from Docker context endpoint TLS data.

        Args:
            endpoint: The Docker context endpoint with TLS data.
            context_name: Name of the context (for error messages).

        Returns:
            An ssl.SSLContext configured with the context's certificates,
            or None if no TLS data is available.

        Raises:
            DockerContextTLSError: If the TLS certificates are invalid or
                cannot be loaded into an SSL context.
        """
        if not endpoint.has_tls:
            return None

        import tempfile

        context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        context.set_ciphers(ssl._RESTRICTED_SERVER_CIPHERS)  # type: ignore

        if endpoint.skip_tls_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        else:
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED

        # SSL context methods require file paths, so we need to write temp files
        # for the in-memory certificate data
        ca_path: Optional[str] = None
        cert_path: Optional[str] = None
        key_path: Optional[str] = None
        try:
            if endpoint.tls_ca is not None:
                with tempfile.NamedTemporaryFile(
                    mode="wb", suffix=".pem", delete=False
                ) as ca_file:
                    ca_file.write(endpoint.tls_ca)
                    ca_path = ca_file.name
                try:
                    context.load_verify_locations(cafile=ca_path)
                except ssl.SSLError as e:
                    raise DockerContextTLSError(
                        f"Invalid CA certificate: {e}",
                        context_name=context_name,
                    ) from e

            if endpoint.tls_cert is not None and endpoint.tls_key is not None:
                with tempfile.NamedTemporaryFile(
                    mode="wb", suffix=".pem", delete=False
                ) as cert_file:
                    cert_file.write(endpoint.tls_cert)
                    cert_path = cert_file.name
                with tempfile.NamedTemporaryFile(
                    mode="wb", suffix=".pem", delete=False
                ) as key_file:
                    key_file.write(endpoint.tls_key)
                    key_path = key_file.name
                try:
                    context.load_cert_chain(certfile=cert_path, keyfile=key_path)
                except ssl.SSLError as e:
                    raise DockerContextTLSError(
                        f"Invalid client certificate or key: {e}",
                        context_name=context_name,
                    ) from e

            return context
        finally:
            # Clean up temp files
            if ca_path is not None:
                Path(ca_path).unlink(missing_ok=True)
            if cert_path is not None:
                Path(cert_path).unlink(missing_ok=True)
            if key_path is not None:
                Path(key_path).unlink(missing_ok=True)
