"""RPC client using SDK Core. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Mapping, Optional, Tuple, Type, TypeVar

import google.protobuf.message

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge
from temporalio.bridge.temporal_sdk_bridge import RPCError


@dataclass
class ClientTlsConfig:
    """Python representation of the Rust struct for configuring TLS."""

    server_root_ca_cert: Optional[bytes]
    domain: Optional[str]
    client_cert: Optional[bytes]
    client_private_key: Optional[bytes]


@dataclass
class ClientRetryConfig:
    """Python representation of the Rust struct for configuring retry."""

    initial_interval_millis: int
    randomization_factor: float
    multiplier: float
    max_interval_millis: int
    max_elapsed_time_millis: Optional[int]
    max_retries: int


@dataclass
class ClientKeepAliveConfig:
    """Python representation of the Rust struct for configuring keep alive."""

    interval_millis: int
    timeout_millis: int


@dataclass
class ClientHttpConnectProxyConfig:
    """Python representation of the Rust struct for configuring HTTP proxy."""

    target_host: str
    basic_auth: Optional[Tuple[str, str]]


@dataclass
class ClientConfig:
    """Python representation of the Rust struct for configuring the client."""

    target_url: str
    metadata: Mapping[str, str]
    api_key: Optional[str]
    identity: str
    tls_config: Optional[ClientTlsConfig]
    retry_config: Optional[ClientRetryConfig]
    keep_alive_config: Optional[ClientKeepAliveConfig]
    client_name: str
    client_version: str
    http_connect_proxy_config: Optional[ClientHttpConnectProxyConfig]


@dataclass
class RpcCall:
    """Python representation of the Rust struct for an RPC call."""

    rpc: str
    req: bytes
    retry: bool
    metadata: Mapping[str, str]
    timeout_millis: Optional[int]


ProtoMessage = TypeVar("ProtoMessage", bound=google.protobuf.message.Message)


class Client:
    """RPC client using SDK Core."""

    @staticmethod
    async def connect(
        runtime: temporalio.bridge.runtime.Runtime, config: ClientConfig
    ) -> Client:
        """Establish connection with server."""
        return Client(
            runtime,
            await temporalio.bridge.temporal_sdk_bridge.connect_client(
                runtime._ref, config
            ),
        )

    def __init__(
        self,
        runtime: temporalio.bridge.runtime.Runtime,
        ref: temporalio.bridge.temporal_sdk_bridge.ClientRef,
    ):
        """Initialize client with underlying SDK Core reference."""
        self._runtime = runtime
        self._ref = ref

    def update_metadata(self, metadata: Mapping[str, str]) -> None:
        """Update underlying metadata on Core client."""
        self._ref.update_metadata(metadata)

    def update_api_key(self, api_key: Optional[str]) -> None:
        """Update underlying API key on Core client."""
        self._ref.update_api_key(api_key)

    async def call(
        self,
        *,
        service: str,
        rpc: str,
        req: google.protobuf.message.Message,
        resp_type: Type[ProtoMessage],
        retry: bool,
        metadata: Mapping[str, str],
        timeout: Optional[timedelta],
    ) -> ProtoMessage:
        """Make RPC call using SDK Core."""
        # Prepare call
        timeout_millis = round(timeout.total_seconds() * 1000) if timeout else None
        call = RpcCall(rpc, req.SerializeToString(), retry, metadata, timeout_millis)

        # Do call (this throws an RPCError on failure)
        if service == "workflow":
            resp_fut = self._ref.call_workflow_service(call)
        elif service == "operator":
            resp_fut = self._ref.call_operator_service(call)
        elif service == "cloud":
            resp_fut = self._ref.call_cloud_service(call)
        elif service == "test":
            resp_fut = self._ref.call_test_service(call)
        elif service == "health":
            resp_fut = self._ref.call_health_service(call)
        else:
            raise ValueError(f"Unrecognized service {service}")

        # Convert response
        resp = resp_type()
        resp.ParseFromString(await resp_fut)
        return resp
