from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Protocol

from typing_extensions import LiteralString

from procrastinate import exceptions, utils

Pool = Any
Engine = Any

LISTEN_TIMEOUT = 30.0


class Notify(Protocol):
    def __call__(self, *, channel: str, payload: str) -> Awaitable[None]: ...


class BaseConnector:
    json_dumps: Callable | None = None
    json_loads: Callable | None = None

    def get_sync_connector(self) -> BaseConnector:
        raise NotImplementedError

    def open(self, pool: Pool | None = None) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def execute_query(self, query: LiteralString, **arguments: Any) -> None:
        raise NotImplementedError

    def execute_query_one(
        self, query: LiteralString, **arguments: Any
    ) -> dict[str, Any]:
        raise NotImplementedError

    def execute_query_all(
        self, query: LiteralString, **arguments: Any
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def open_async(self, pool: Pool | None = None) -> None:
        raise exceptions.SyncConnectorConfigurationError

    async def close_async(self) -> None:
        raise exceptions.SyncConnectorConfigurationError

    async def execute_query_async(self, query: LiteralString, **arguments: Any) -> None:
        raise exceptions.SyncConnectorConfigurationError

    async def execute_query_one_async(
        self, query: LiteralString, **arguments: Any
    ) -> dict[str, Any]:
        raise exceptions.SyncConnectorConfigurationError

    async def execute_query_all_async(
        self, query: LiteralString, **arguments: Any
    ) -> list[dict[str, Any]]:
        raise exceptions.SyncConnectorConfigurationError

    async def listen_notify(
        self,
        on_notification: Notify,
        channels: Iterable[str],
    ) -> None:
        raise exceptions.SyncConnectorConfigurationError


class BaseAsyncConnector(BaseConnector):
    async def open_async(self, pool: Pool | None = None) -> None:
        raise NotImplementedError

    async def close_async(self) -> None:
        raise NotImplementedError

    async def execute_query_async(self, query: LiteralString, **arguments: Any) -> None:
        raise NotImplementedError

    async def execute_query_one_async(
        self, query: LiteralString, **arguments: Any
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def execute_query_all_async(
        self, query: LiteralString, **arguments: Any
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def execute_query(self, query: LiteralString, **arguments: Any) -> None:
        return utils.async_to_sync(self.execute_query_async, query, **arguments)

    def execute_query_one(
        self, query: LiteralString, **arguments: Any
    ) -> dict[str, Any]:
        return utils.async_to_sync(self.execute_query_one_async, query, **arguments)

    def execute_query_all(
        self, query: LiteralString, **arguments: Any
    ) -> list[dict[str, Any]]:
        return utils.async_to_sync(self.execute_query_all_async, query, **arguments)

    async def listen_notify(
        self, on_notification: Notify, channels: Iterable[str]
    ) -> None:
        raise NotImplementedError
