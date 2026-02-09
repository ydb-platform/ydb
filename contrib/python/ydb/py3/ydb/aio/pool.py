from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Callable, Optional, Tuple, TYPE_CHECKING

from ydb import issues
from ydb.pool import ConnectionsCache as _ConnectionsCache, IConnectionPool

from .connection import Connection, EndpointKey

from . import resolver

if TYPE_CHECKING:
    from ydb.driver import DriverConfig
    from ydb.settings import BaseRequestSettings

logger = logging.getLogger(__name__)


class ConnectionsCache(_ConnectionsCache):
    def __init__(self, use_all_nodes: bool = False) -> None:
        super().__init__(use_all_nodes)
        self.lock = resolver._FakeLock()  # Mock lock to emulate thread safety
        self._event: asyncio.Event = asyncio.Event()
        self._fast_fail_event: asyncio.Event = asyncio.Event()
        self._fast_fail_error: Optional[Exception] = None

    async def get(  # async version with different Connection type
        self,
        preferred_endpoint: Optional[EndpointKey] = None,
        fast_fail: bool = False,
        wait_timeout: float = 10.0,
    ) -> Connection:
        if fast_fail:
            await asyncio.wait_for(self._fast_fail_event.wait(), timeout=wait_timeout)
            if self._fast_fail_error:
                raise self._fast_fail_error
        else:
            await asyncio.wait_for(self._event.wait(), timeout=wait_timeout)

        if preferred_endpoint is not None and preferred_endpoint.node_id in self.connections_by_node_id:
            return self.connections_by_node_id[preferred_endpoint.node_id]  # type: ignore[return-value]

        if preferred_endpoint is not None and preferred_endpoint.endpoint in self.connections:
            return self.connections[preferred_endpoint.endpoint]  # type: ignore[return-value]

        for conn_lst in self.conn_lst_order:
            try:
                endpoint, connection = conn_lst.popitem(last=False)
                conn_lst[endpoint] = connection
                return connection  # type: ignore[return-value]
            except KeyError:
                continue

        raise issues.ConnectionLost("Couldn't find valid connection")

    def add(self, connection: Optional[Connection], preferred: bool = False) -> bool:  # type: ignore[override]  # async Connection type
        if connection is None:
            return False

        connection.add_cleanup_callback(self.remove)

        if preferred:
            self.preferred[connection.endpoint] = connection  # type: ignore[assignment]

        self.connections_by_node_id[connection.node_id] = connection  # type: ignore[assignment]
        self.connections[connection.endpoint] = connection  # type: ignore[assignment]

        self._event.set()

        if len(self.connections) > 0:
            self.complete_discovery(None)

        return True

    def complete_discovery(self, error: Optional[Exception]) -> None:
        self._fast_fail_error = error
        self._fast_fail_event.set()

    def remove(self, connection: Connection) -> None:  # type: ignore[override]  # async Connection type
        self.connections_by_node_id.pop(connection.node_id, None)
        self.preferred.pop(connection.endpoint, None)
        self.connections.pop(connection.endpoint, None)
        self.outdated.pop(connection.endpoint, None)
        if len(self.connections) == 0:
            self._event.clear()
            if not self._fast_fail_error:
                self._fast_fail_event.clear()

    async def cleanup(self) -> None:  # type: ignore[override]  # async override of sync method
        actual_connections = list(self.connections.values())
        for connection in actual_connections:
            await connection.close()

    async def cleanup_outdated(self) -> "ConnectionsCache":  # type: ignore[override]  # async override of sync method
        outdated_connections = list(self.outdated.values())
        for outdated_connection in outdated_connections:
            await outdated_connection.close()
        return self


class Discovery:
    def __init__(self, store: ConnectionsCache, driver_config: "DriverConfig") -> None:
        self.logger = logger.getChild(self.__class__.__name__)
        self._cache = store
        self._driver_config = driver_config
        self._resolver = resolver.DiscoveryEndpointsResolver(self._driver_config)
        self._base_discovery_interval = 60
        self._ready_timeout = 4
        self._discovery_request_timeout = 2
        self._should_stop = False
        self._wake_up_event: asyncio.Event = asyncio.Event()
        self._max_size = 9
        self._base_emergency_retry_interval = 1
        self._ssl_required = False
        if driver_config.root_certificates is not None or driver_config.secure_channel:
            self._ssl_required = True

    def discovery_debug_details(self) -> str:
        return self._resolver.debug_details()

    def notify_disconnected(self) -> None:
        self._wake_up_event.set()

    def _emergency_retry_interval(self) -> float:
        return (1 + random.random()) * self._base_emergency_retry_interval

    def _discovery_interval(self) -> float:
        return (1 + random.random()) * self._base_discovery_interval

    async def execute_discovery(self) -> bool:
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

    def stop(self) -> None:
        self._should_stop = True
        self._wake_up_event.set()

    async def run(self) -> None:
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
    def __init__(self, driver_config: "DriverConfig") -> None:
        self._driver_config = driver_config
        self._store = ConnectionsCache(driver_config.use_all_nodes)
        self._grpc_init = Connection(self._driver_config.endpoint, self._driver_config)
        self._stopped = False
        self._discovery: Optional[Discovery] = None
        self._discovery_task: "asyncio.Task[None]"

        if driver_config.disable_discovery:
            # If discovery is disabled, just add the initial endpoint to the store
            async def init_connection() -> None:
                ready_connection = Connection(self._driver_config.endpoint, self._driver_config)
                await ready_connection.connection_ready(
                    ready_timeout=getattr(self._driver_config, "discovery_request_timeout", 10)
                )
                self._store.add(ready_connection)

            # Create and schedule the task to initialize the connection
            self._discovery_task = asyncio.get_event_loop().create_task(init_connection())
        else:
            # Start discovery as usual
            self._discovery = Discovery(self._store, self._driver_config)
            self._discovery_task = asyncio.get_event_loop().create_task(self._discovery.run())

    async def stop(self, timeout: int = 10) -> None:  # type: ignore[override]  # async override of sync method
        if self._discovery:
            self._discovery.stop()
        await self._grpc_init.close()
        try:
            await asyncio.wait_for(self._discovery_task, timeout=timeout)
        except asyncio.TimeoutError:
            self._discovery_task.cancel()
        self._stopped = True

    def _on_disconnected(self, connection: Connection) -> Callable[[], Any]:
        async def __wrapper__() -> None:
            await connection.close()
            if self._discovery:
                self._discovery.notify_disconnected()

        return __wrapper__

    async def wait(self, timeout: Optional[float] = 7.0, fail_fast: bool = False) -> None:  # type: ignore[override]  # async override of sync method
        await self._store.get(fast_fail=fail_fast, wait_timeout=timeout if timeout is not None else 7.0)

    def discovery_debug_details(self) -> str:
        if self._discovery:
            return self._discovery.discovery_debug_details()
        return "Discovery is disabled, using only the initial endpoint"

    async def __aenter__(self) -> "ConnectionPool":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.stop()

    async def __call__(
        self,
        request: Any,
        stub: Any,
        rpc_name: str,
        wrap_result: Optional[Callable[..., Any]] = None,
        settings: Optional["BaseRequestSettings"] = None,
        wrap_args: Tuple[Any, ...] = (),
        preferred_endpoint: Optional[EndpointKey] = None,
        fast_fail: bool = False,
    ) -> Any:
        if self._stopped:
            raise issues.Error("Driver was stopped")
        wait_timeout: float = settings.timeout if settings else 10  # type: ignore[assignment]
        try:
            connection = await self._store.get(preferred_endpoint, fast_fail=fast_fail, wait_timeout=wait_timeout)
        except BaseException:
            if self._discovery:
                self._discovery.notify_disconnected()
            raise

        res = await connection(
            request,
            stub,
            rpc_name,
            wrap_result,
            settings,
            wrap_args,
            self._on_disconnected(connection),
        )

        return res
