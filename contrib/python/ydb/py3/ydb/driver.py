# -*- coding: utf-8 -*-
import grpc
import logging
import os
from typing import Any, List, Optional, Tuple, Type, TYPE_CHECKING

from . import credentials as credentials_impl, table, scheme, pool
from . import tracing
from . import iam
from . import _utilities

if TYPE_CHECKING:
    from .credentials import Credentials
    from .table import TableClientSettings
    from .query.base import QueryClientSettings


logger = logging.getLogger(__name__)


class RPCCompression:
    """Indicates the compression method to be used for an RPC."""

    NoCompression = grpc.Compression.NoCompression
    Deflate = grpc.Compression.Deflate
    Gzip = grpc.Compression.Gzip


def default_credentials(
    credentials: Optional["Credentials"] = None,
    tracer: Optional[tracing.Tracer] = None,
) -> "Credentials":
    tracer = tracer if tracer is not None else tracing.Tracer(None)
    with tracer.trace("Driver.default_credentials") as ctx:
        if credentials is None:
            ctx.trace({"credentials.anonymous": True})
            return credentials_impl.AnonymousCredentials()
        else:
            ctx.trace({"credentials.prepared": True})
            return credentials


def credentials_from_env_variables(tracer: Optional[tracing.Tracer] = None) -> "Credentials":
    tracer = tracer if tracer is not None else tracing.Tracer(None)
    with tracer.trace("Driver.credentials_from_env_variables") as ctx:
        service_account_key_file = os.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
        if service_account_key_file is not None:
            ctx.trace({"credentials.service_account_key_file": True})
            import ydb.iam

            return ydb.iam.ServiceAccountCredentials.from_file(service_account_key_file)

        anonymous_credetials = os.getenv("YDB_ANONYMOUS_CREDENTIALS", "0") == "1"
        if anonymous_credetials:
            ctx.trace({"credentials.anonymous": True})
            return credentials_impl.AnonymousCredentials()

        metadata_credentials = os.getenv("YDB_METADATA_CREDENTIALS", "0") == "1"
        if metadata_credentials:
            ctx.trace({"credentials.metadata": True})

            return iam.MetadataUrlCredentials(tracer=tracer)

        access_token = os.getenv("YDB_ACCESS_TOKEN_CREDENTIALS")
        if access_token is not None:
            ctx.trace({"credentials.access_token": True})
            return credentials_impl.AuthTokenCredentials(access_token)

        oauth2_key_file = os.getenv("YDB_OAUTH2_KEY_FILE")
        if oauth2_key_file:
            ctx.trace({"credentials.oauth2_key_file": True})
            import ydb.oauth2_token_exchange

            return ydb.oauth2_token_exchange.Oauth2TokenExchangeCredentials.from_file(oauth2_key_file)

        ctx.trace(
            {
                "credentials.env_default": True,
                "credentials.metadata": True,
            }
        )
        return iam.MetadataUrlCredentials(tracer=tracer)


class DriverConfig(object):
    __slots__ = (
        "endpoint",
        "database",
        "ca_cert",
        "channel_options",
        "credentials",
        "use_all_nodes",
        "root_certificates",
        "certificate_chain",
        "private_key",
        "grpc_keep_alive_timeout",
        "secure_channel",
        "table_client_settings",
        "topic_client_settings",
        "query_client_settings",
        "endpoints",
        "primary_user_agent",
        "tracer",
        "grpc_lb_policy_name",
        "discovery_request_timeout",
        "compression",
        "disable_discovery",
    )

    def __init__(
        self,
        endpoint: str,
        database: Optional[str] = None,
        ca_cert: Optional[str] = None,
        auth_token: Optional[str] = None,
        channel_options: Optional[List[Tuple[str, Any]]] = None,
        credentials: Optional["Credentials"] = None,
        use_all_nodes: bool = True,
        root_certificates: Optional[bytes] = None,
        certificate_chain: Optional[bytes] = None,
        private_key: Optional[bytes] = None,
        grpc_keep_alive_timeout: Optional[int] = None,
        table_client_settings: Optional["TableClientSettings"] = None,
        topic_client_settings: Optional[Any] = None,
        query_client_settings: Optional["QueryClientSettings"] = None,
        endpoints: Optional[List[str]] = None,
        primary_user_agent: str = "python-library",
        tracer: Optional[tracing.Tracer] = None,
        grpc_lb_policy_name: str = "round_robin",
        discovery_request_timeout: int = 10,
        compression: Optional[grpc.Compression] = None,
        disable_discovery: bool = False,
    ) -> None:
        """
        A driver config to initialize a driver instance

        :param endpoint: A endpoint specified in pattern host:port to be used for initial channel initialization and for YDB endpoint discovery mechanism
        :param database: A name of the database
        :param ca_cert: A CA certificate when SSL should be used
        :param auth_token: A authentication token
        :param credentials: An instance of AbstractCredentials
        :param use_all_nodes: A balancing policy that forces to use all available nodes.
        :param root_certificates: The PEM-encoded root certificates as a byte string.
        :param private_key: The PEM-encoded private key as a byte string, or None if no\
        private key should be used.
        :param certificate_chain: The PEM-encoded certificate chain as a byte string\
        to use or or None if no certificate chain should be used.
        :param grpc_keep_alive_timeout: GRpc KeepAlive timeout, ms
        :param ydb.Tracer tracer: ydb.Tracer instance to trace requests in driver.\
        If tracing aio ScopeManager must be ContextVarsScopeManager
        :param grpc_lb_policy_name: A load balancing policy to be used for discovery channel construction. Default value is `round_round`
        :param discovery_request_timeout: A default timeout to complete the discovery. The default value is 10 seconds.
        :param disable_discovery: If True, endpoint discovery is disabled and only the start endpoint is used for all requests.

        """
        self.endpoint = endpoint
        self.database = database
        self.ca_cert = ca_cert
        self.channel_options = channel_options
        self.secure_channel = _utilities.is_secure_protocol(endpoint)
        self.endpoint = _utilities.wrap_endpoint(self.endpoint)
        self.endpoints = []
        if endpoints is not None:
            self.endpoints = [_utilities.wrap_endpoint(endp) for endp in endpoints]
        if auth_token is not None:
            credentials = credentials_impl.AuthTokenCredentials(auth_token)
        self.credentials = credentials
        self.use_all_nodes = use_all_nodes
        self.root_certificates = root_certificates
        self.certificate_chain = certificate_chain
        self.private_key = private_key
        self.grpc_keep_alive_timeout = grpc_keep_alive_timeout
        self.table_client_settings = table_client_settings
        self.topic_client_settings = topic_client_settings
        self.query_client_settings = query_client_settings
        self.primary_user_agent = primary_user_agent
        self.tracer = tracer if tracer is not None else tracing.Tracer(None)
        self.grpc_lb_policy_name = grpc_lb_policy_name
        self.discovery_request_timeout = discovery_request_timeout
        self.compression = compression
        self.disable_discovery = disable_discovery

    def set_database(self, database: str) -> "DriverConfig":
        self.database = database
        return self

    @classmethod
    def default_from_endpoint_and_database(
        cls,
        endpoint: str,
        database: Optional[str] = None,
        root_certificates: Optional[bytes] = None,
        credentials: Optional["Credentials"] = None,
        **kwargs: Any,
    ) -> "DriverConfig":
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs,
        )

    @classmethod
    def default_from_connection_string(
        cls,
        connection_string: str,
        root_certificates: Optional[bytes] = None,
        credentials: Optional["Credentials"] = None,
        **kwargs: Any,
    ) -> "DriverConfig":
        endpoint, database = _utilities.parse_connection_string(connection_string)
        return cls(
            endpoint,
            database,
            credentials=default_credentials(credentials),
            root_certificates=root_certificates,
            **kwargs,
        )

    def set_grpc_keep_alive_timeout(self, timeout: int) -> "DriverConfig":
        self.grpc_keep_alive_timeout = timeout
        return self

    def _update_attrs_by_kwargs(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if value is not None:
                if getattr(self, key) is not None:
                    logger.warning(
                        f"Arg {key} was used in both DriverConfig and Driver. Value from Driver will be used."
                    )
                setattr(self, key, value)


ConnectionParams = DriverConfig


def get_config(
    driver_config: Optional[DriverConfig] = None,
    connection_string: Optional[str] = None,
    endpoint: Optional[str] = None,
    database: Optional[str] = None,
    root_certificates: Optional[bytes] = None,
    credentials: Optional["Credentials"] = None,
    config_class: Type[DriverConfig] = DriverConfig,
    **kwargs: Any,
) -> DriverConfig:
    if driver_config is None:
        if connection_string is not None:
            driver_config = config_class.default_from_connection_string(
                connection_string, root_certificates, credentials, **kwargs
            )
        else:
            driver_config = config_class.default_from_endpoint_and_database(
                endpoint,  # type: ignore[arg-type]
                database,
                root_certificates,
                credentials,
                **kwargs,
            )
    else:
        kwargs["endpoint"] = endpoint
        kwargs["database"] = database
        kwargs["root_certificates"] = root_certificates
        kwargs["credentials"] = credentials

        driver_config._update_attrs_by_kwargs(**kwargs)

    if driver_config.credentials is not None:
        driver_config.credentials._update_driver_config(driver_config)

    return driver_config


class Driver(pool.ConnectionPool):
    __slots__ = ("scheme_client", "table_client")

    def __init__(
        self,
        driver_config: Optional[DriverConfig] = None,
        connection_string: Optional[str] = None,
        endpoint: Optional[str] = None,
        database: Optional[str] = None,
        root_certificates: Optional[bytes] = None,
        credentials: Optional["Credentials"] = None,
        **kwargs: Any,
    ) -> None:
        """
        Constructs a driver instance to be used in table and scheme clients.
        It encapsulates endpoints discovery mechanism and provides ability to execute RPCs
        on discovered endpoints

        :param driver_config: A driver config
        :param connection_string: A string in the following format: <protocol>://<hostame>:<port>/?database=/path/to/the/database
        :param endpoint: An endpoint specified in the following format: <protocol>://<hostame>:<port>
        :param database: A database path
        :param credentials: A credentials. If not specifed credentials constructed by default.
        """
        from . import topic  # local import for prevent cycle import error
        from . import coordination  # local import for prevent cycle import error

        driver_config = get_config(
            driver_config,
            connection_string,
            endpoint,
            database,
            root_certificates,
            credentials,
        )

        super(Driver, self).__init__(driver_config)

        self._credentials = driver_config.credentials

        self.scheme_client = scheme.SchemeClient(self)
        self.table_client = table.TableClient(self, driver_config.table_client_settings)
        self.topic_client = topic.TopicClient(self, driver_config.topic_client_settings)
        self.coordination_client = coordination.CoordinationClient(self)

    def stop(self, timeout: int = 10) -> None:
        self.table_client._stop_pool_if_needed(timeout=timeout)
        self.topic_client.close()
        super().stop(timeout=timeout)
