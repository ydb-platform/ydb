from . import connection as conn_impl

from ydb import _apis, settings as settings_impl
from ydb.resolver import (
    DiscoveryResult,
    DiscoveryEndpointsResolver as _DiscoveryEndpointsResolver,
    _list_endpoints_request_factory,
)


class _FakeLock:
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DiscoveryEndpointsResolver(_DiscoveryEndpointsResolver):
    def __init__(self, driver_config):
        super().__init__(driver_config)
        self._lock = _FakeLock()

    async def resolve(self):
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
