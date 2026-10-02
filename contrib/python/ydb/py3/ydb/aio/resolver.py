from typing import Any, Optional

from . import connection as conn_impl

from ydb import _apis, settings as settings_impl
from ydb.driver import DriverConfig
from ydb.resolver import (
    DiscoveryResult,
    DiscoveryEndpointsResolver as _DiscoveryEndpointsResolver,
    _list_endpoints_request_factory,
)


class _FakeLock:
    """No-op lock for async context where threading locks aren't needed."""

    def __init__(self) -> None:
        pass

    def __enter__(self) -> "_FakeLock":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


class DiscoveryEndpointsResolver(_DiscoveryEndpointsResolver):
    def __init__(self, driver_config: DriverConfig) -> None:
        super().__init__(driver_config)
        self._lock = _FakeLock()

    async def resolve(self) -> Optional[DiscoveryResult]:  # type: ignore[override]  # async override of sync method
        self.logger.debug("Preparing initial endpoint to resolve endpoints")
        endpoint = next(self._endpoints_iter)
        connection = conn_impl.Connection(endpoint, self._driver_config)
        try:
            await connection.connection_ready()
        except BaseException:
            self._add_debug_details(
                'Failed to establish connection to YDB discovery endpoint: "%s". Check endpoint correctness.' % endpoint
            )
            return None
        self.logger.debug("Resolving endpoints for database %s", self._driver_config.database)

        try:
            resolved = await connection(
                _list_endpoints_request_factory(self._driver_config),
                _apis.DiscoveryService.Stub,
                _apis.DiscoveryService.ListEndpoints,
                DiscoveryResult.from_response,
                settings=settings_impl.BaseRequestSettings().with_timeout(self._ready_timeout),
            )

            self._add_debug_details(
                "Resolved endpoints for database %s: %s",
                self._driver_config.database,
                resolved,
            )

            return resolved
        except BaseException as e:

            self._add_debug_details(
                'Failed to resolve endpoints for database %s. Endpoint: "%s". Error details:\n %s',
                self._driver_config.database,
                endpoint,
                e,
            )

        finally:
            await connection.close()

        return None
