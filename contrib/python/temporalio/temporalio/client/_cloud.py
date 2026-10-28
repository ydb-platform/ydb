"""Client support for accessing Temporal."""

from __future__ import annotations

from collections.abc import (
    Mapping,
)

import temporalio.runtime
import temporalio.service
from temporalio.service import (
    DnsLoadBalancingConfig,
    GrpcCompression,
    HttpConnectProxyConfig,
    KeepAliveConfig,
    RetryConfig,
    TLSConfig,
)


class CloudOperationsClient:
    """Client for accessing Temporal Cloud Operations API.

    .. warning::
        This client and the API are experimental

    Most users will use :py:meth:`connect` to create a client. The
    :py:attr:`cloud_service` property provides access to a raw gRPC cloud
    service client.

    Clients are not thread-safe and should only be used in the event loop they
    are first connected in. If a client needs to be used from another thread
    than where it was created, make sure the event loop where it was created is
    captured, and then call :py:func:`asyncio.run_coroutine_threadsafe` with the
    client call and that event loop.

    Clients do not work across forks since runtimes do not work across forks.
    """

    @staticmethod
    async def connect(
        *,
        api_key: str | None = None,
        version: str | None = None,
        target_host: str = "saas-api.tmprl.cloud:443",
        tls: bool | TLSConfig = True,
        retry_config: RetryConfig | None = None,
        keep_alive_config: KeepAliveConfig | None = KeepAliveConfig.default,
        rpc_metadata: Mapping[str, str | bytes] = {},
        identity: str | None = None,
        lazy: bool = False,
        runtime: temporalio.runtime.Runtime | None = None,
        http_connect_proxy_config: HttpConnectProxyConfig | None = None,
        dns_load_balancing_config: DnsLoadBalancingConfig | None = None,
        grpc_compression: GrpcCompression = GrpcCompression.GZIP,
    ) -> CloudOperationsClient:
        """Connect to a Temporal Cloud Operations API.

        .. warning::
            This client and the API are experimental

        Args:
            api_key: API key for Temporal. This becomes the "Authorization"
                HTTP header with "Bearer " prepended. This is only set if RPC
                metadata doesn't already have an "authorization" key. This is
                essentially required for access to the cloud API.
            version: Version header for safer mutations. May or may not be
                required depending on cloud settings.
            target_host: ``host:port`` for the Temporal server. The default is
                to the common cloud endpoint.
            tls: If true, the default, use system default TLS configuration. If
                false, the default, do not use TLS. If TLS configuration
                present, that TLS configuration will be used. The default is
                usually required to access the API.
            retry_config: Retry configuration for direct service calls (when
                opted in) or all high-level calls made by this client (which all
                opt-in to retries by default). If unset, a default retry
                configuration is used.
            keep_alive_config: Keep-alive configuration for the client
                connection. Default is to check every 30s and kill the
                connection if a response doesn't come back in 15s. Can be set to
                ``None`` to disable.
            rpc_metadata: Headers to use for all calls to the server. Keys here
                can be overriden by per-call RPC metadata keys.
            identity: Identity for this client. If unset, a default is created
                based on the version of the SDK.
            lazy: If true, the client will not connect until the first call is
                attempted or a worker is created with it. Lazy clients cannot be
                used for workers.
            runtime: The runtime for this client, or the default if unset.
            http_connect_proxy_config: Configuration for HTTP CONNECT proxy.
            dns_load_balancing_config: DNS load balancing configuration for the
                client connection. Default is disabled. Silently disabled when
                ``http_connect_proxy_config`` is set, since the two are mutually
                exclusive.
            grpc_compression: Transport-level gRPC compression for the client
                connection. Default is gzip. Set to
                :py:attr:`GrpcCompression.NONE` to disable compression.
        """
        # Add version if given
        if version:
            rpc_metadata = dict(rpc_metadata)
            rpc_metadata["temporal-cloud-api-version"] = version
        connect_config = temporalio.service.ConnectConfig(
            target_host=target_host,
            api_key=api_key,
            tls=tls,
            retry_config=retry_config,
            keep_alive_config=keep_alive_config,
            rpc_metadata=rpc_metadata,
            identity=identity or "",
            lazy=lazy,
            runtime=runtime,
            http_connect_proxy_config=http_connect_proxy_config,
            dns_load_balancing_config=dns_load_balancing_config,
            grpc_compression=grpc_compression,
        )
        return CloudOperationsClient(
            await temporalio.service.ServiceClient.connect(connect_config)
        )

    def __init__(
        self,
        service_client: temporalio.service.ServiceClient,
    ):
        """Create a Temporal Cloud Operations client from a service client.

        .. warning::
            This client and the API are experimental

        Args:
            service_client: Existing service client to use.
        """
        self._service_client = service_client

    @property
    def service_client(self) -> temporalio.service.ServiceClient:
        """Raw gRPC service client."""
        return self._service_client

    @property
    def cloud_service(self) -> temporalio.service.CloudService:
        """Raw gRPC cloud service client."""
        return self._service_client.cloud_service

    @property
    def identity(self) -> str:
        """Identity used in calls by this client."""
        return self._service_client.config.identity

    @property
    def rpc_metadata(self) -> Mapping[str, str | bytes]:
        """Headers for every call made by this client.

        Do not use mutate this mapping. Rather, set this property with an
        entirely new mapping to change the headers. This may include the
        ``temporal-cloud-api-version`` header if set.
        """
        return self.service_client.config.rpc_metadata

    @rpc_metadata.setter
    def rpc_metadata(self, value: Mapping[str, str | bytes]) -> None:
        """Update the headers for this client.

        Do not mutate this mapping after set. Rather, set an entirely new
        mapping if changes are needed. Currently this must be set with the
        ``temporal-cloud-api-version`` header if it is needed.
        """
        # Update config and perform update
        self.service_client.config.rpc_metadata = value
        self.service_client.update_rpc_metadata(value)

    @property
    def api_key(self) -> str | None:
        """API key for every call made by this client."""
        return self.service_client.config.api_key

    @api_key.setter
    def api_key(self, value: str | None) -> None:
        """Update the API key for this client.

        This is only set if RPCmetadata doesn't already have an "authorization"
        key.
        """
        # Update config and perform update
        self.service_client.config.api_key = value
        self.service_client.update_api_key(value)
