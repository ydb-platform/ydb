"""Test server management. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge


@dataclass
class DevServerConfig:
    """Python representation of the Rust struct for configuring dev server."""

    existing_path: Optional[str]
    sdk_name: str
    sdk_version: str
    download_version: str
    download_dest_dir: Optional[str]
    download_ttl_ms: Optional[int]
    namespace: str
    ip: str
    port: Optional[int]
    database_filename: Optional[str]
    ui: bool
    log_format: str
    log_level: str
    extra_args: Sequence[str]


@dataclass
class TestServerConfig:
    """Python representation of the Rust struct for configuring test server."""

    existing_path: Optional[str]
    sdk_name: str
    sdk_version: str
    download_version: str
    download_dest_dir: Optional[str]
    download_ttl_ms: Optional[int]
    port: Optional[int]
    extra_args: Sequence[str]


class EphemeralServer:
    """Python representation of a Rust ephemeral server."""

    @staticmethod
    async def start_dev_server(
        runtime: temporalio.bridge.runtime.Runtime, config: DevServerConfig
    ) -> EphemeralServer:
        """Start a dev server instance."""
        return EphemeralServer(
            await temporalio.bridge.temporal_sdk_bridge.start_dev_server(
                runtime._ref, config
            )
        )

    @staticmethod
    async def start_test_server(
        runtime: temporalio.bridge.runtime.Runtime, config: TestServerConfig
    ) -> EphemeralServer:
        """Start a test server instance."""
        return EphemeralServer(
            await temporalio.bridge.temporal_sdk_bridge.start_test_server(
                runtime._ref, config
            )
        )

    def __init__(self, ref: temporalio.bridge.temporal_sdk_bridge.EphemeralServerRef):
        """Initialize an ephemeral server."""
        self._ref = ref

    @property
    def target(self) -> str:
        """Frontend address."""
        return self._ref.target

    @property
    def has_test_service(self) -> bool:
        """Whether this server supports the test service."""
        return self._ref.has_test_service

    async def shutdown(self) -> None:
        """Shutdown this server."""
        await self._ref.shutdown()
