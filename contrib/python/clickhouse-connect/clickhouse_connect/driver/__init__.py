from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from inspect import signature
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, unquote, urlparse

import clickhouse_connect.driver.ctypes  # noqa: F401 -- side-effect import
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.httpclient import HttpClient

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient

__all__ = ["Client", "AsyncClient", "create_client", "create_async_client"]

logger = logging.getLogger(__name__)


def __getattr__(name):
    if name == "AsyncClient":
        try:
            from clickhouse_connect.driver.asyncclient import AsyncClient
        except ModuleNotFoundError as ex:
            if ex.name == "aiohttp" or (ex.name and ex.name.startswith("aiohttp.")):
                raise ImportError("Async support requires aiohttp. Install with: pip install clickhouse-connect[async]") from ex
            raise
        return AsyncClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def default_port(interface: str, secure: bool) -> int:
    """Get default port for the given interface."""
    if interface.startswith("http"):
        return 8443 if secure else 8123
    raise ValueError("Unrecognized ClickHouse interface")


def _unquote(value: str | None) -> str | None:
    """Percent-decode a DSN component, passing through None/empty."""
    return unquote(value) if value else value


def _parse_connection_params(
    host: str | None,
    username: str | None,
    password: str,
    port: int | None,
    database: str | None,
    interface: str | None,
    secure: bool | str,
    dsn: str | None,
    kwargs: dict[str, Any],
) -> tuple[str, str | None, str, int, str | None, str]:
    """Parse and normalize connection parameters including DSN parsing."""
    if database == "__default__":  # legacy sentinel for "not specified"
        database = None
    if dsn:
        parsed = urlparse(dsn)
        username = username or _unquote(parsed.username)
        password = password or _unquote(parsed.password) or ""
        host = host or parsed.hostname
        port = port or parsed.port
        if not database and parsed.path:
            database = unquote(parsed.path[1:].split("/")[0]) or None
        for k, v in parse_qs(parsed.query).items():
            kwargs[k] = v[0]
    use_tls = str(secure).lower() == "true" or interface == "https" or (not interface and str(port) in ("443", "8443"))
    if not host:
        host = "localhost"
    if not interface:
        interface = "https" if use_tls else "http"
    port = port or default_port(interface, use_tls)
    if username is None and "user" in kwargs:
        username = kwargs.pop("user")
    if username is None and "user_name" in kwargs:
        username = kwargs.pop("user_name")
    if password and username is None:
        username = "default"
    if "compression" in kwargs and "compress" not in kwargs:
        kwargs["compress"] = kwargs.pop("compression")

    return host, username, password, port, database, interface


def _validate_access_token(
    access_token: str | None,
    token_provider: Callable[[], str | Awaitable[str]] | None,
    username: str | None,
    password: str,
) -> None:
    """Validate that token-based and username/password auth are not mixed."""
    if (access_token or token_provider) and (username or password):
        raise ProgrammingError("Cannot use both token authentication and username/password")
    if access_token and token_provider:
        raise ProgrammingError("Cannot use both access_token and token_provider")


def _pop_headers_arg(headers: Any | None, kwargs: dict[str, Any]) -> Any | None:
    """Hoist headers parsed through generic kwargs while preserving explicit headers."""
    if "headers" in kwargs:
        kwargs_headers = kwargs.pop("headers")
        if headers is None:
            headers = kwargs_headers
    return headers


def _validate_headers(headers: Any | None) -> None:
    if headers is not None and not isinstance(headers, dict):
        raise ProgrammingError("headers must be a dictionary of HTTP header names and values")


def _is_chdb_target(interface: str | None, dsn: str | None) -> bool:
    return interface == "chdb" or bool(dsn and dsn.startswith("chdb:"))


def _create_chdb_client(
    database: str | None,
    settings: dict[str, Any],
    dsn: str | None,
    kwargs: dict[str, Any],
    generic_args: dict[str, Any] | None,
    ignored_args: dict[str, Any],
) -> Client:
    """Build a chDB-backed client from a chdb:// DSN and/or keyword arguments."""
    try:
        from clickhouse_connect.driver._chdbclient import ChdbClient
    except ModuleNotFoundError as ex:
        if ex.name == "chdb" or (ex.name and ex.name.startswith("chdb.")):
            raise ImportError("The chdb backend requires the chdb package. Install with: pip install clickhouse-connect[chdb]") from ex
        raise
    path = kwargs.pop("path", None)
    if dsn:
        parsed = urlparse(dsn)
        if parsed.netloc:
            # chdb://memory/db form: the netloc is the location, the path the database
            location = parsed.netloc.rpartition("@")[2]
            if not database and parsed.path:
                database = unquote(parsed.path[1:].split("/")[0]) or None
        else:
            # chdb:///abs/path form: the whole path is the location
            location = parsed.path
        if not path and location not in ("", "memory", ":memory:"):
            path = location
        for key, value in parse_qs(parsed.query).items():
            kwargs.setdefault(key, value[0])
    client_params = signature(ChdbClient).parameters
    if generic_args:
        for name, value in generic_args.items():
            if name in client_params:
                kwargs[name] = value
            else:
                if name.startswith("ch_"):
                    name = name[3:]
                settings[name] = value
    # path/database may also arrive through DSN query params or generic_args
    path = path or kwargs.pop("path", None)
    database = database or kwargs.pop("database", None)
    client_kwargs = {
        name: kwargs.pop(name) for name in list(kwargs) if name in client_params and name not in ("database", "settings", "path")
    }
    ignored = list(ignored_args)
    for name, value in kwargs.items():
        # Remaining arguments are HTTP transport parameters with no chdb
        # meaning (host, port, auth, pooling, ...) unless prefixed as settings
        if name.startswith("ch_"):
            settings[name[3:]] = value
        else:
            ignored.append(name)
    if ignored:
        logger.warning("Ignoring arguments with no chdb meaning: %s", ", ".join(sorted(ignored)))
    return ChdbClient(path=path, database=database, settings=settings, **client_kwargs)


def create_client(
    *,
    host: str | None = None,
    username: str | None = None,
    password: str = "",
    access_token: str | None = None,
    token_provider: Callable[[], str] | None = None,
    database: str | None = None,
    interface: str | None = None,
    port: int | None = None,
    secure: bool | str = False,
    dsn: str | None = None,
    settings: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    generic_args: dict[str, Any] | None = None,
    **kwargs,
) -> Client:
    """
    The preferred method to get a ClickHouse Connect Client instance

    :param host: The hostname or IP address of the ClickHouse server. If not set, localhost will be used.
    :param username: The ClickHouse username. If not set, the default ClickHouse user will be used.
      Should not be set if `access_token` is used.
    :param password: The password for username.
      Should not be set if `access_token` is used.
    :param access_token: JWT access token (ClickHouse Cloud feature).
      Should not be set if `username`/`password` are used.
    :param token_provider: A callable returning a JWT access token (ClickHouse Cloud feature). Called for the initial token and
      again to refresh it whenever the server rejects the current one.
      Should not be set if `access_token` or `username`/`password` are used.
    :param database:  The default database for the connection. If not set, ClickHouse Connect will use the
     default database for username.
    :param interface: Must be http, https, or chdb.  Defaults to http, or to https if port is set to 8443 or 443.
      The experimental chdb value returns a client backed by an embedded in-process chDB engine instead of a
      ClickHouse server. It requires the chdb package and accepts the path and chdb_options keyword arguments,
      while HTTP connection arguments are ignored.
    :param port: The ClickHouse HTTP or HTTPS port. If not set will default to 8123, or to 8443 if secure=True
      or interface=https.
    :param secure: Use https/TLS. This overrides inferred values from the interface or port arguments.
    :param dsn: A string in standard DSN (Data Source Name) format. Other connection values (such as host or user)
      will be extracted from this string if not set otherwise. A chdb scheme selects the chdb backend, e.g.
      chdb://memory, chdb://memory/my_database, or chdb:///on/disk/path. As with HTTP connections, a database
      named in the DSN must already exist, so the my_database form only works when joining an engine where an
      earlier client created it.
    :param settings: ClickHouse server settings to be used with the session/every request
    :param headers: Additional HTTP headers to send with every request. This can be used for proxy or gateway
      authentication, such as Cloudflare Access service token headers. These headers are applied after driver defaults,
      so they can intentionally override headers such as Authorization or User-Agent.
    :param generic_args: Used internally to parse DBAPI connection strings into keyword arguments and ClickHouse settings.
      It is not recommended to use this parameter externally.

    :param kwargs -- Recognized keyword arguments (used by the HTTP client), see below

    :param compress: Enable compression for ClickHouse HTTP inserts and query results.  True will select the preferred
      compression method (lz4).  A str of 'lz4', 'zstd', 'br', or 'gzip' can be used to use a specific compression type
    :param query_limit: Default LIMIT on returned rows.  0 means no limit
    :param connect_timeout:  Timeout in seconds for the http connection
    :param send_receive_timeout: Read timeout in seconds for http connection
    :param client_name: client_name prepended to the HTTP User Agent header. Set this to track client queries
      in the ClickHouse system.query_log.
    :param send_progress: Deprecated, has no effect.  Previous functionality is now automatically determined
    :param verify: Verify the server certificate in secure/https mode
    :param ca_cert: If verify is True, the file path to Certificate Authority root to validate ClickHouse server
     certificate, in .pem format.  Ignored if verify is False.  This is not necessary if the ClickHouse server
     certificate is trusted by the operating system.  To trust the maintained list of "global" public root
     certificates maintained by the Python 'certifi' package, set ca_cert to 'certifi'
    :param client_cert: File path to a TLS Client certificate in .pem format.  This file should contain any
      applicable intermediate certificates
    :param client_cert_key: File path to the private key for the Client Certificate.  Required if the private key
      is not included the Client Certificate key file
    :param session_id ClickHouse session id.  If not specified and the common setting 'autogenerate_session_id'
      is True, the client will generate a UUID1 session id
    :param pool_mgr Optional urllib3 PoolManager for this client.  Useful for creating separate connection
      pools for multiple client endpoints for applications with many clients
    :param http_proxy  http proxy address.  Equivalent to setting the HTTP_PROXY environment variable
    :param https_proxy https proxy address.  Equivalent to setting the HTTPS_PROXY environment variable
    :param server_host_name  This is the server host name that will be checked against a TLS certificate for
      validity.  This option can be used if using an ssh_tunnel or other indirect means to an ClickHouse server
      where the `host` argument refers to the tunnel or proxy and not the actual ClickHouse server
    :param tz_source Controls how the client determines the fallback timezone for DateTime columns without an
      explicit timezone. "auto" (default) auto-detects based on DST safety of server timezone. "server" always
      uses the server timezone. "local" always uses the local timezone.
    :param tz_mode Controls timezone-aware behavior for UTC DateTime columns. "naive_utc" (default) returns
      naive UTC timestamps. "aware" forces timezone-aware UTC datetimes. "schema" returns datetimes that
      match the server's column definition which means timezone-aware when the column defines a timezone and naive
      for bare DateTime columns.
    :param autogenerate_session_id  If set, this will override the 'autogenerate_session_id' common setting.
    :param form_encode_query_params  If True, always send query parameters as form-encoded data in the request body
      instead of as URL parameters. When False, large parameter payloads are still automatically sent as form data to
      avoid exceeding URL length limits, except for queries using binary parameter binds, which are only form-encoded
      when this is True. Only available for query operations (not inserts). Default: False
    :return: ClickHouse Connect Client instance
    """
    if _is_chdb_target(interface, dsn):
        ignored_args = {
            name: value
            for name, value in (
                ("host", host),
                ("port", port),
                ("username", username),
                ("password", password),
                ("access_token", access_token),
                ("token_provider", token_provider),
                ("secure", secure),
                ("headers", headers),
            )
            if value not in (None, "", False)
        }
        return _create_chdb_client(database, dict(settings or {}), dsn, kwargs, generic_args, ignored_args)
    host, username, password, port, database, interface = _parse_connection_params(
        host, username, password, port, database, interface, secure, dsn, kwargs
    )
    headers = _pop_headers_arg(headers, kwargs)
    _validate_access_token(access_token, token_provider, username, password)

    settings = settings or {}
    if interface.startswith("http"):
        if generic_args:
            client_params = signature(HttpClient).parameters
            for name, value in generic_args.items():
                if name == "headers":
                    if headers is None:
                        headers = value
                elif name in client_params:
                    kwargs[name] = value
                elif name == "compression":
                    if "compress" not in kwargs:
                        kwargs["compress"] = value
                else:
                    if name.startswith("ch_"):
                        name = name[3:]
                    settings[name] = value
        # token auth may also arrive via generic_args (DB-API connect_args); pop both so neither is passed twice
        generic_access = kwargs.pop("access_token", None)
        generic_token = kwargs.pop("token_provider", None)
        access_token = access_token or generic_access
        token_provider = token_provider or generic_token
        _validate_access_token(access_token, token_provider, username, password)
        _validate_headers(headers)
        return HttpClient(
            interface,
            host,
            port,
            username or "",
            password,
            database,
            access_token,
            token_provider=token_provider,
            settings=settings,
            headers=headers,
            **kwargs,
        )
    raise ProgrammingError(f"Unrecognized client type {interface}")


async def create_async_client(
    *,
    host: str | None = None,
    username: str | None = None,
    password: str = "",
    access_token: str | None = None,
    token_provider: Callable[[], str | Awaitable[str]] | None = None,
    database: str | None = None,
    interface: str | None = None,
    port: int | None = None,
    secure: bool | str = False,
    dsn: str | None = None,
    settings: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    generic_args: dict[str, Any] | None = None,
    connector_limit: int = 100,
    connector_limit_per_host: int = 20,
    keepalive_timeout: float = 30.0,
    **kwargs,
) -> AsyncClient:
    """
    The preferred method to get an async ClickHouse Connect Client instance.
    Requires the async extra: pip install clickhouse-connect[async]

    For sync version, see create_client.

    Unlike sync version, the 'autogenerate_session_id' setting by default is False.

    :param host: The hostname or IP address of the ClickHouse server. If not set, localhost will be used.
    :param username: The ClickHouse username. If not set, the default ClickHouse user will be used.
    :param password: The password for username.
    :param access_token: JWT access token.
    :param token_provider: A callable returning a JWT access token. Called for the initial token and
      again to refresh it whenever the server rejects the current one. Because multiple in-flight requests
      may each trigger a refresh concurrently, the callable must be safe to invoke in parallel.
    :param database:  The default database for the connection. If not set, ClickHouse Connect will use the
     default database for username.
    :param interface: Must be http or https.  Defaults to http, or to https if port is set to 8443 or 443
    :param port: The ClickHouse HTTP or HTTPS port. If not set will default to 8123, or to 8443 if secure=True
      or interface=https.
    :param secure: Use https/TLS. This overrides inferred values from the interface or port arguments.
    :param dsn: A string in standard DSN (Data Source Name) format. Other connection values (such as host or user)
      will be extracted from this string if not set otherwise.
    :param settings: ClickHouse server settings to be used with the session/every request
    :param headers: Additional HTTP headers to send with every request. This can be used for proxy or gateway
      authentication, such as Cloudflare Access service token headers. These headers are applied after driver defaults,
      so they can intentionally override headers such as Authorization or User-Agent.
    :param generic_args: Used internally to parse DBAPI connection strings into keyword arguments and ClickHouse settings.
      It is not recommended to use this parameter externally
    :param connector_limit: Maximum number of allowable connections to the server
    :param connector_limit_per_host: Maximum number of connections per host
    :param keepalive_timeout: Time limit on idle keepalive connections
    :param kwargs -- Recognized keyword arguments (used by the async HTTP client), see below

    :param compress: Enable compression for ClickHouse HTTP inserts and query results.  True will select the preferred
      compression method (lz4).  A str of 'lz4', 'zstd', 'br', or 'gzip' can be used to use a specific compression type
    :param query_limit: Default LIMIT on returned rows.  0 means no limit
    :param connect_timeout:  Timeout in seconds for the http connection
    :param send_receive_timeout: Read timeout in seconds for http connection
    :param client_name: client_name prepended to the HTTP User Agent header. Set this to track client queries
      in the ClickHouse system.query_log.
    :param verify: Verify the server certificate in secure/https mode
    :param ca_cert: If verify is True, the file path to Certificate Authority root to validate ClickHouse server
     certificate, in .pem format.  Ignored if verify is False.  This is not necessary if the ClickHouse server
     certificate is trusted by the operating system.  To trust the maintained list of "global" public root
     certificates maintained by the Python 'certifi' package, set ca_cert to 'certifi'
    :param client_cert: File path to a TLS Client certificate in .pem format.  This file should contain any
      applicable intermediate certificates
    :param client_cert_key: File path to the private key for the Client Certificate.  Required if the private key
      is not included the Client Certificate key file
    :param session_id ClickHouse session id.  If not specified and the common setting 'autogenerate_session_id'
      is True, the client will generate a UUID1 session id
    :param http_proxy  http proxy address.  Equivalent to setting the HTTP_PROXY environment variable
    :param https_proxy https proxy address.  Equivalent to setting the HTTPS_PROXY environment variable
    :param server_host_name  This is the server host name that will be checked against a TLS certificate for
      validity.  This option can be used if using an ssh_tunnel or other indirect means to an ClickHouse server
      where the `host` argument refers to the tunnel or proxy and not the actual ClickHouse server
    :param tz_source Controls how the client determines the fallback timezone for DateTime columns without an
      explicit timezone. "auto" (default) auto-detects based on DST safety of server timezone. "server" always
      uses the server timezone. "local" always uses the local timezone.
    :param tz_mode Controls timezone-aware behavior for UTC DateTime columns. "naive_utc" (default) returns
      naive UTC timestamps. "aware" forces timezone-aware UTC datetimes. "schema" returns datetimes that
      match the server's column definition which means timezone-aware when the column defines a timezone and naive
      for bare DateTime columns.
    :param autogenerate_session_id  If set, this will override the 'autogenerate_session_id' common setting.
    :param form_encode_query_params  If True, always send query parameters as form-encoded data in the request body
      instead of as URL parameters. When False, large parameter payloads are still automatically sent as form data to
      avoid exceeding URL length limits, except for queries using binary parameter binds, which are only form-encoded
      when this is True. Only available for query operations (not inserts). Default: False
    :return: ClickHouse Connect AsyncClient instance
    """
    if _is_chdb_target(interface, dsn):
        raise ProgrammingError("The chdb backend does not support the async client. Use get_client instead.")

    try:
        from clickhouse_connect.driver.asyncclient import AsyncClient as _AsyncClient
    except ModuleNotFoundError as ex:
        if ex.name == "aiohttp" or (ex.name and ex.name.startswith("aiohttp.")):
            raise ImportError("Async support requires aiohttp. Install with: pip install clickhouse-connect[async]") from ex
        raise

    if "pool_mgr" in kwargs:
        raise ProgrammingError(
            "pool_mgr is not supported by the async client. "
            "Use connector_limit and connector_limit_per_host to configure connection pooling."
        )

    host, username, password, port, database, interface = _parse_connection_params(
        host, username, password, port, database, interface, secure, dsn, kwargs
    )
    headers = _pop_headers_arg(headers, kwargs)
    _validate_access_token(access_token, token_provider, username, password)

    settings = settings or {}
    if generic_args:
        client_params = signature(_AsyncClient).parameters
        for name, value in generic_args.items():
            if name == "headers":
                if headers is None:
                    headers = value
            elif name in client_params:
                kwargs[name] = value
            elif name == "compression":
                if "compress" not in kwargs:
                    kwargs["compress"] = value
            else:
                if name.startswith("ch_"):
                    name = name[3:]
                settings[name] = value

    if "autogenerate_session_id" not in kwargs:
        kwargs["autogenerate_session_id"] = False

    # token auth may also arrive via generic_args (DB-API connect_args); pop both so neither is passed twice
    generic_access = kwargs.pop("access_token", None)
    generic_token = kwargs.pop("token_provider", None)
    access_token = access_token or generic_access
    token_provider = token_provider or generic_token
    _validate_access_token(access_token, token_provider, username, password)
    _validate_headers(headers)
    client = _AsyncClient(
        interface=interface,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        access_token=access_token,
        token_provider=token_provider,
        settings=settings,
        headers=headers,
        connector_limit=connector_limit,
        connector_limit_per_host=connector_limit_per_host,
        keepalive_timeout=keepalive_timeout,
        **kwargs,
    )
    await client._initialize()
    return client
