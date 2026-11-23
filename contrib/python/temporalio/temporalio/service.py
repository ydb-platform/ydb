"""Underlying gRPC services."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import warnings
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from enum import IntEnum
from typing import ClassVar, TypeVar

import google.protobuf.message

import temporalio.api.common.v1
import temporalio.bridge.client
import temporalio.bridge.proto.health.v1
import temporalio.bridge.services_generated
import temporalio.exceptions
import temporalio.runtime
from temporalio.bridge.client import RPCError as BridgeRPCError

__version__ = "1.32.0"

ServiceRequest = TypeVar("ServiceRequest", bound=google.protobuf.message.Message)
ServiceResponse = TypeVar("ServiceResponse", bound=google.protobuf.message.Message)

logger = logging.getLogger(__name__)

# Set to true to log all requests and responses
LOG_PROTOS = False


@dataclass
class TLSConfig:
    """TLS configuration for connecting to Temporal server."""

    server_root_ca_cert: bytes | None = None
    """Root CA to validate the server certificate against."""

    domain: str | None = None
    """SNI host and HTTP/2 authority override, and the default name the server
    certificate is verified against (see
    :py:attr:`verification_server_name`)."""

    client_cert: bytes | None = None
    """Client certificate for mTLS.

    This must be combined with :py:attr:`client_private_key`."""

    client_private_key: bytes | None = None
    """Client private key for mTLS.

    This must be combined with :py:attr:`client_cert`."""

    verification_server_name: str | None = None
    """Name to verify the server certificate against, instead of the
    :py:attr:`domain` / connected host.

    Unlike :py:attr:`domain`, this does not change the TLS SNI or HTTP/2
    authority values, which continue to follow the connected host (or
    :py:attr:`domain` if set). Use this when the server's certificate does not
    carry the name being dialed, e.g. when the connection traverses an
    SNI-inspecting proxy that must be able to resolve the SNI value.

    Requires :py:attr:`server_root_ca_cert`; the system root store is not
    consulted when this is set."""

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientTlsConfig:
        return temporalio.bridge.client.ClientTlsConfig(
            server_root_ca_cert=self.server_root_ca_cert,
            domain=self.domain,
            client_cert=self.client_cert,
            client_private_key=self.client_private_key,
            verification_server_name=self.verification_server_name,
        )


@dataclass
class RetryConfig:
    """Retry configuration for server calls."""

    initial_interval_millis: int = 100
    """Initial backoff interval."""
    randomization_factor: float = 0.2
    """Randomization jitter to add."""
    multiplier: float = 1.5
    """Backoff multiplier."""
    max_interval_millis: int = 5000
    """Maximum backoff interval."""
    max_elapsed_time_millis: int | None = 10000
    """Maximum total time."""
    max_retries: int = 10
    """Maximum number of retries."""

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientRetryConfig:
        return temporalio.bridge.client.ClientRetryConfig(
            initial_interval_millis=self.initial_interval_millis,
            randomization_factor=self.randomization_factor,
            multiplier=self.multiplier,
            max_interval_millis=self.max_interval_millis,
            max_elapsed_time_millis=self.max_elapsed_time_millis,
            max_retries=self.max_retries,
        )


@dataclass(frozen=True)
class KeepAliveConfig:
    """Keep-alive configuration for client connections."""

    interval_millis: int = 30000
    """Interval to send HTTP2 keep alive pings."""
    timeout_millis: int = 15000
    """Timeout that the keep alive must be responded to within or the connection
    will be closed."""
    default: ClassVar[KeepAliveConfig]
    """Default keep alive config."""

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientKeepAliveConfig:
        return temporalio.bridge.client.ClientKeepAliveConfig(
            interval_millis=self.interval_millis,
            timeout_millis=self.timeout_millis,
        )


KeepAliveConfig.default = KeepAliveConfig()


@dataclass(frozen=True)
class HttpConnectProxyConfig:
    """Configuration for HTTP CONNECT proxy for client connections."""

    target_host: str
    """Target host:port for the HTTP CONNECT proxy."""
    basic_auth: tuple[str, str] | None = None
    """Basic auth for the HTTP CONNECT proxy if any as a user/pass tuple."""

    def _to_bridge_config(
        self,
    ) -> temporalio.bridge.client.ClientHttpConnectProxyConfig:
        return temporalio.bridge.client.ClientHttpConnectProxyConfig(
            target_host=self.target_host,
            basic_auth=self.basic_auth,
        )


@dataclass(frozen=True)
class DnsLoadBalancingConfig:
    """DNS load balancing configuration for client connections.

    When enabled, Core periodically re-resolves the target host's DNS records
    and round-robins requests across the resolved addresses. Cannot be used
    together with :py:class:`HttpConnectProxyConfig` -- DNS load balancing is
    silently disabled when an HTTP CONNECT proxy is configured.
    """

    resolution_interval_millis: int = 30000
    """How often to re-resolve DNS, in milliseconds."""
    default: ClassVar[DnsLoadBalancingConfig]
    """Default DNS load balancing config."""

    def _to_bridge_config(
        self,
    ) -> temporalio.bridge.client.ClientDnsLoadBalancingConfig:
        return temporalio.bridge.client.ClientDnsLoadBalancingConfig(
            resolution_interval_millis=self.resolution_interval_millis,
        )


DnsLoadBalancingConfig.default = DnsLoadBalancingConfig()


class GrpcCompression(ABC):
    """Transport-level gRPC compression mode.

    This is a base type for concrete compression modes. Current modes are
    available as singleton constants on this class.
    """

    NONE: ClassVar[GrpcCompression]
    """Do not compress gRPC requests or advertise support for compressed responses."""

    GZIP: ClassVar[GrpcCompression]
    """Gzip-compress gRPC requests and accept gzip-compressed responses."""

    @abstractmethod
    def _to_bridge_config(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class _NoGrpcCompression(GrpcCompression):
    def _to_bridge_config(self) -> str:
        return "none"


@dataclass(frozen=True)
class _GzipGrpcCompression(GrpcCompression):
    def _to_bridge_config(self) -> str:
        return "gzip"


GrpcCompression.NONE = _NoGrpcCompression()
GrpcCompression.GZIP = _GzipGrpcCompression()


@dataclass(frozen=True)
class PayloadLimitsConfig:
    """Warning thresholds for outbound payload/memo sizes."""

    # Defaults mirror the Temporal server's `limit.blobSize.warn` (512 KiB) and `limit.memoSize.warn`
    # (2 KiB) dynamic-config defaults, so the SDK warns at the same sizes the server would.
    payloads_warn_size: int = 512 * 1024
    """Warning threshold, in bytes, for the size of an outbound payload-bearing field. Set to 0 to
    disable."""

    memo_warn_size: int = 2 * 1024
    """Warning threshold, in bytes, for outbound memo size. Set to 0 to disable."""


@dataclass
class ConnectConfig:
    """Config for connecting to the server."""

    target_host: str
    api_key: str | None = None
    tls: bool | TLSConfig | None = None
    retry_config: RetryConfig | None = None
    keep_alive_config: KeepAliveConfig | None = KeepAliveConfig.default
    rpc_metadata: Mapping[str, str | bytes] = field(default_factory=dict)
    identity: str = ""
    lazy: bool = False
    runtime: temporalio.runtime.Runtime | None = None
    http_connect_proxy_config: HttpConnectProxyConfig | None = None
    dns_load_balancing_config: DnsLoadBalancingConfig | None = None
    grpc_compression: GrpcCompression = GrpcCompression.GZIP
    payload_limits: PayloadLimitsConfig = field(default_factory=PayloadLimitsConfig)

    def __post_init__(self) -> None:
        """Set extra defaults on unset properties."""
        if not self.identity:
            self.identity = f"{os.getpid()}@{socket.gethostname()}"

    def _to_bridge_config(self) -> temporalio.bridge.client.ClientConfig:
        # Need to create the URL from the host:port. We allowed scheme in the
        # past so we'll leave it for only one more version with a warning.
        # Otherwise we'll prepend the scheme.
        target_url: str
        tls_config: temporalio.bridge.client.ClientTlsConfig | None
        if "://" in self.target_host:
            warnings.warn(
                "Target host as URL with scheme no longer supported. This will be an error in future versions."
            )
            target_url = self.target_host
            tls_config = (
                self.tls._to_bridge_config()
                if isinstance(self.tls, TLSConfig)
                else None
            )
        elif isinstance(self.tls, TLSConfig):
            target_url = f"https://{self.target_host}"
            tls_config = self.tls._to_bridge_config()
        elif self.tls:
            target_url = f"https://{self.target_host}"
            tls_config = TLSConfig()._to_bridge_config()
        # Enable TLS by default when API key is provided and tls not explicitly set
        elif self.tls is None and self.api_key is not None:
            target_url = f"https://{self.target_host}"
            tls_config = TLSConfig()._to_bridge_config()
        else:
            target_url = f"http://{self.target_host}"
            tls_config = None

        return temporalio.bridge.client.ClientConfig(
            target_url=target_url,
            api_key=self.api_key,
            tls_config=tls_config,
            retry_config=(
                self.retry_config._to_bridge_config() if self.retry_config else None
            ),
            keep_alive_config=(
                self.keep_alive_config._to_bridge_config()
                if self.keep_alive_config
                else None
            ),
            metadata=self.rpc_metadata,
            identity=self.identity,
            client_name="temporal-python",
            client_version=__version__,
            http_connect_proxy_config=(
                self.http_connect_proxy_config._to_bridge_config()
                if self.http_connect_proxy_config
                else None
            ),
            dns_load_balancing_config=(
                self.dns_load_balancing_config._to_bridge_config()
                if self.dns_load_balancing_config
                else None
            ),
            grpc_compression=self.grpc_compression._to_bridge_config(),
            payloads_warn_size=self.payload_limits.payloads_warn_size,
            memo_warn_size=self.payload_limits.memo_warn_size,
        )


class ServiceClient(ABC):
    """Direct client to Temporal services."""

    @staticmethod
    async def connect(config: ConnectConfig) -> ServiceClient:
        """Connect directly to Temporal services."""
        return await _BridgeServiceClient.connect(config)

    def __init__(self, config: ConnectConfig) -> None:
        """Initialize the base service client."""
        super().__init__()
        self.config = config
        self.workflow_service = WorkflowService(self)
        self.operator_service = OperatorService(self)
        self.cloud_service = CloudService(self)
        self.test_service = TestService(self)
        self.health_service = HealthService(self)

    async def check_health(
        self,
        *,
        service: str = "temporal.api.workflowservice.v1.WorkflowService",
        retry: bool = False,
        metadata: Mapping[str, str | bytes] = {},
        timeout: timedelta | None = None,
    ) -> bool:
        """Check whether the provided service is up. If no service is specified,
         the WorkflowService is used.

        Returns:
            True when available, false if the server is running but the service
            is unavailable (rare), or raises an error if server/service cannot
            be reached.
        """
        resp = await self.health_service.check(
            temporalio.bridge.proto.health.v1.HealthCheckRequest(service=service),
            retry=retry,
            metadata=metadata,
            timeout=timeout,
        )

        return (
            resp.status
            == temporalio.bridge.proto.health.v1.HealthCheckResponse.ServingStatus.SERVING
        )

    @property
    @abstractmethod
    def worker_service_client(self) -> _BridgeServiceClient:
        """Underlying service client."""
        raise NotImplementedError

    @abstractmethod
    def update_rpc_metadata(self, metadata: Mapping[str, str | bytes]) -> None:
        """Update service client's RPC metadata."""
        raise NotImplementedError

    @abstractmethod
    def update_api_key(self, api_key: str | None) -> None:
        """Update service client's API key."""
        raise NotImplementedError

    @abstractmethod
    async def _rpc_call(
        self,
        rpc: str,
        req: google.protobuf.message.Message,
        resp_type: type[ServiceResponse],
        *,
        service: str,
        retry: bool,
        metadata: Mapping[str, str | bytes],
        timeout: timedelta | None,
    ) -> ServiceResponse:
        raise NotImplementedError


class WorkflowService(temporalio.bridge.services_generated.WorkflowService):
    """Client to the Temporal server's workflow service."""


class OperatorService(temporalio.bridge.services_generated.OperatorService):
    """Client to the Temporal server's operator service."""


class CloudService(temporalio.bridge.services_generated.CloudService):
    """Client to the Temporal server's cloud service."""


class TestService(temporalio.bridge.services_generated.TestService):
    """Client to the Temporal test server's test service."""


class HealthService(temporalio.bridge.services_generated.HealthService):
    """Client to the Temporal server's health service."""


class _BridgeServiceClient(ServiceClient):
    @staticmethod
    async def connect(config: ConnectConfig) -> _BridgeServiceClient:
        client = _BridgeServiceClient(config)
        # If not lazy, try to connect
        if not config.lazy:
            await client._connected_client()
        return client

    def __init__(self, config: ConnectConfig) -> None:
        super().__init__(config)
        self._bridge_config = config._to_bridge_config()
        self._bridge_client: temporalio.bridge.client.Client | None = None
        self._bridge_client_connect_lock = asyncio.Lock()

    async def _connected_client(self) -> temporalio.bridge.client.Client:
        # Fast path avoids touching the lock once connected. This keeps the
        # lock off the per-RPC hot path so it never binds to (or is contended
        # across) an event loop, letting a connected client be reused from any
        # loop.
        if self._bridge_client is not None:
            return self._bridge_client
        async with self._bridge_client_connect_lock:
            if not self._bridge_client:
                runtime = self.config.runtime or temporalio.runtime.Runtime.default()
                self._bridge_client = await temporalio.bridge.client.Client.connect(
                    runtime._core_runtime,
                    self._bridge_config,
                )
            return self._bridge_client

    @property
    def worker_service_client(self) -> _BridgeServiceClient:
        """Underlying service client."""
        return self

    def update_rpc_metadata(self, metadata: Mapping[str, str | bytes]) -> None:
        """Update Core client metadata."""
        # Mutate the bridge config and then only mutate the running client
        # metadata if already connected
        self._bridge_config.metadata = metadata
        if self._bridge_client:
            self._bridge_client.update_metadata(metadata)

    def update_api_key(self, api_key: str | None) -> None:
        """Update Core client API key."""
        # Mutate the bridge config and then only mutate the running client
        # metadata if already connected
        self._bridge_config.api_key = api_key
        if self._bridge_client:
            self._bridge_client.update_api_key(api_key)

    async def _rpc_call(
        self,
        rpc: str,
        req: google.protobuf.message.Message,
        resp_type: type[ServiceResponse],
        *,
        service: str,
        retry: bool,
        metadata: Mapping[str, str | bytes],
        timeout: timedelta | None,
    ) -> ServiceResponse:
        global LOG_PROTOS
        if LOG_PROTOS:
            logger.debug("Service %s request to %s: %s", service, rpc, req)
        try:
            client = await self._connected_client()
            resp = await client.call(
                service=service,
                rpc=rpc,
                req=req,
                resp_type=resp_type,
                retry=retry,
                metadata=metadata,
                timeout=timeout,
            )
            if LOG_PROTOS:
                logger.debug("Service %s response from %s: %s", service, rpc, resp)
            return resp
        except BridgeRPCError as err:
            # Intentionally swallowing the cause instead of using "from"
            status, message, details = err.args
            raise RPCError(message, RPCStatusCode(status), details)


class RPCStatusCode(IntEnum):
    """Status code for :py:class:`RPCError`."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16


class RPCError(temporalio.exceptions.TemporalError):
    """Error during RPC call."""

    def __init__(
        self, message: str, status: RPCStatusCode, raw_grpc_status: bytes
    ) -> None:
        """Initialize RPC error."""
        super().__init__(message)
        self._message = message
        self._status = status
        self._raw_grpc_status = raw_grpc_status
        self._grpc_status: temporalio.api.common.v1.GrpcStatus | None = None

    @property
    def message(self) -> str:
        """Message for the error."""
        return self._message

    @property
    def status(self) -> RPCStatusCode:
        """Status code for the error."""
        return self._status

    @property
    def raw_grpc_status(self) -> bytes:
        """Raw gRPC status bytes."""
        return self._raw_grpc_status

    @property
    def grpc_status(self) -> temporalio.api.common.v1.GrpcStatus:
        """Status of the gRPC call with details."""
        if self._grpc_status is None:
            status = temporalio.api.common.v1.GrpcStatus()
            status.ParseFromString(self._raw_grpc_status)
            self._grpc_status = status
        return self._grpc_status
