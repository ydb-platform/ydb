import asyncio
import logging
import random
import typing

from ydb import issues
from ydb.pool import ConnectionsCache as _ConnectionsCache, IConnectionPool

from .connection import Connection, EndpointKey

from . import resolver

logger = logging.getLogger(__name__)


class ConnectionsCache(_ConnectionsCache):
    def __init__(self, use_all_nodes: bool = False):
        super().__init__(use_all_nodes)
        self.lock = resolver._FakeLock()  # Mock lock to emulate thread safety
        self._event = asyncio.Event()
        self._fast_fail_event = asyncio.Event()

        self._fast_fail_error = None

    async def get(self, preferred_endpoint: typing.Optional[EndpointKey] = None, fast_fail=False, wait_timeout=10):

        if fast_fail:
            await asyncio.wait_for(self._fast_fail_event.wait(), timeout=wait_timeout)
            if self._fast_fail_error:
                raise self._fast_fail_error
        else:
            await asyncio.wait_for(self._event.wait(), timeout=wait_timeout)

        if preferred_endpoint is not None and preferred_endpoint.node_id in self.connections_by_node_id:
            return self.connections_by_node_id[preferred_endpoint.node_id]

        if preferred_endpoint is not None and preferred_endpoint.endpoint in self.connections:
            return self.connections[preferred_endpoint.endpoint]

        for conn_lst in self.conn_lst_order:
            try:
                endpoint, connection = conn_lst.popitem(last=False)
                conn_lst[endpoint] = connection
                return connection
            except KeyError:
                continue

        raise issues.ConnectionLost("Couldn't find valid connection")

    def add(self, connection, preferred=False):

        if connection is None:
            return False

        connection.add_cleanup_callback(self.remove)

        if preferred:
            self.preferred[connection.endpoint] = connection

        self.connections_by_node_id[connection.node_id] = connection
        self.connections[connection.endpoint] = connection

        self._event.set()

        if len(self.connections) > 0:
            self.complete_discovery(None)

        return True

    def complete_discovery(self, error):
        self._fast_fail_error = error
        self._fast_fail_event.set()

    def remove(self, connection):
        self.connections_by_node_id.pop(connection.node_id, None)
        self.preferred.pop(connection.endpoint, None)
        self.connections.pop(connection.endpoint, None)
        self.outdated.pop(connection.endpoint, None)
        if len(self.connections) == 0:
            self._event.clear()
            if not self._fast_fail_error:
                self._fast_fail_event.clear()

    async def cleanup(self):
        actual_connections = list(self.connections.values())
        for connection in actual_connections:
            await connection.close()

    async def cleanup_outdated(self):
        outdated_connections = list(self.outdated.values())
        for outdated_connection in outdated_connections:
            await outdated_connection.close()
        return self


class Discovery:
    def __init__(self, store: ConnectionsCache, driver_config):
        self.logger = logger.getChild(self.__class__.__name__)
        self._cache = store
        self._driver_config = driver_config
        self._resolver = resolver.DiscoveryEndpointsResolver(self._driver_config)
        self._base_discovery_interval = 60
        self._ready_timeout = 4
        self._discovery_request_timeout = 2
        self._should_stop = False
        self._wake_up_event = asyncio.Event()
        self._max_size = 9
        self._base_emergency_retry_interval = 1
        self._ssl_required = False
        if driver_config.root_certificates is not None or driver_config.secure_channel:
            self._ssl_required = True

    def discovery_debug_details(self):
        return self._resolver.debug_details()

    def notify_disconnected(self):
        self._wake_up_event.set()

    def _emergency_retry_interval(self):
        return (1 + random.random()) * self._base_emergency_retry_interval

    def _discovery_interval(self):
        return (1 + random.random()) * self._base_discovery_interval

    async def execute_discovery(self):

        resolve_details = await self._resolver.resolve()

        if resolve_details is None:
            return False

        resolved_endpoints = set(
            endpoint
            for resolved_endpoint in resolve_details.endpoints
            for endpoint, endpoint_options in resolved_endpoint.endpoints_with_options()
        )
        for cached_endpoint in self._cache.values():
            if cached_endpoint.endpoint not in resolved_endpoints:
                self._cache.make_outdated(cached_endpoint)

        for resolved_endpoint in resolve_details.endpoints:
            if self._ssl_required and not resolved_endpoint.ssl:
                continue

            if not self._ssl_required and resolved_endpoint.ssl:
                continue

            preferred = resolve_details.self_location == resolved_endpoint.location

            for (
                endpoint,
                endpoint_options,
            ) in resolved_endpoint.endpoints_with_options():
                if self._cache.size >= self._max_size or self._cache.already_exists(endpoint):
                    continue

                ready_connection = Connection(endpoint, self._driver_config, endpoint_options=endpoint_options)
                await ready_connection.connection_ready(ready_timeout=self._ready_timeout)

                self._cache.add(ready_connection, preferred)

        await self._cache.cleanup_outdated()
        return self._cache.size > 0

    def stop(self):
        self._should_stop = True
        self._wake_up_event.set()

    async def run(self):
        while True:
            try:
                successful = await self.execute_discovery()
            except Exception:
                successful = False
            if successful:
                self._cache.complete_discovery(None)
            else:
                self._cache.complete_discovery(issues.ConnectionFailure(str(self.discovery_debug_details())))

            interval = self._discovery_interval() if successful else self._emergency_retry_interval()

            try:
                await asyncio.wait_for(self._wake_up_event.wait(), timeout=interval)
                if self._should_stop:
                    break
                else:
                    self._wake_up_event.clear()
                    continue
            except asyncio.TimeoutError:
                continue

        await self._cache.cleanup()
        self.logger.info("Successfully terminated discovery process")


class ConnectionPool(IConnectionPool):
    def __init__(self, driver_config):
        self._driver_config = driver_config
        self._store = ConnectionsCache(driver_config.use_all_nodes)
        self._grpc_init = Connection(self._driver_config.endpoint, self._driver_config)
        self._stopped = False
        self._discovery = Discovery(self._store, self._driver_config)

        self._discovery_task = asyncio.get_event_loop().create_task(self._discovery.run())

    async def stop(self, timeout=10):
        self._discovery.stop()
        await self._grpc_init.close()
        try:
            await asyncio.wait_for(self._discovery_task, timeout=timeout)
        except asyncio.TimeoutError:
            self._discovery_task.cancel()
        self._stopped = True

    def _on_disconnected(self, connection):
        async def __wrapper__():
            await connection.close()
            self._discovery.notify_disconnected()

        return __wrapper__

    async def wait(self, timeout=7, fail_fast=False):
        await self._store.get(fast_fail=fail_fast, wait_timeout=timeout)

    def discovery_debug_details(self):
        return self._discovery.discovery_debug_details()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    async def __call__(
        self,
        request,
        stub,
        rpc_name,
        wrap_result=None,
        settings=None,
        wrap_args=(),
        preferred_endpoint=None,
        fast_fail=False,
    ):
        if self._stopped:
            raise issues.Error("Driver was stopped")
        wait_timeout = settings.timeout if settings else 10
        try:
            connection = await self._store.get(preferred_endpoint, fast_fail=fast_fail, wait_timeout=wait_timeout)
        except Exception:
            self._discovery.notify_disconnected()
            raise

        return await connection(
            request,
            stub,
            rpc_name,
            wrap_result,
            settings,
            wrap_args,
            self._on_disconnected(connection),
        )
