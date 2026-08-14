"""Contracts a pluggable execution backend implements.

Facades own query binding, `QueryRuntime` construction, and result
post-processing; a backend owns transport mechanics behind the typed
execute_* methods plus connection lifecycle and health checks.
`QueryContext`/`InsertContext` are the operation objects.

There is no separate open() step: a backend acquires transport resources at
construction or lazily on first use, and the server handshake is driven by
`orchestration.init_sequence` through the client's semantic methods.

The execute_* parameters are positional-only: mypy does not enforce
parameter-name agreement between a protocol and its implementations, so
keyword calls across backends would not be checkable.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from clickhouse_connect.driver._backend.models import Capabilities, CommandExecution, QueryExecution, QueryRuntime

if TYPE_CHECKING:
    import io

    from clickhouse_connect.driver.external import ExternalData
    from clickhouse_connect.driver.insert import InsertContext
    from clickhouse_connect.driver.query import QueryContext


@runtime_checkable
class SyncBackend(Protocol):
    capabilities: Capabilities

    def execute_query(self, context: QueryContext, runtime: QueryRuntime, prepped_query: str | bytes, /) -> QueryExecution: ...

    def execute_command(
        self,
        bound_cmd: str | bytes,
        bind_params: dict[str, str],
        data: str | bytes | None,
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> CommandExecution: ...

    def execute_data_insert(
        self,
        context: InsertContext,
        runtime: QueryRuntime,
        body: Any,
        retry_body: Callable[[], Any],
        /,
    ) -> dict[str, Any]: ...

    def execute_raw_insert(
        self,
        table: str | None,
        column_names: Sequence[str] | None,
        insert_block: Any,
        fmt: str,
        compression: str | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> dict[str, Any]: ...

    def execute_raw_query(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> bytes: ...

    def execute_raw_stream(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> io.IOBase: ...

    def ping(self) -> bool: ...

    def close(self) -> None: ...

    def close_connections(self) -> None: ...


@runtime_checkable
class AsyncBackend(Protocol):
    capabilities: Capabilities

    async def execute_query(self, context: QueryContext, runtime: QueryRuntime, prepped_query: str | bytes, /) -> QueryExecution: ...

    async def execute_command(
        self,
        bound_cmd: str | bytes,
        bind_params: dict[str, str],
        data: str | bytes | None,
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> CommandExecution: ...

    async def execute_data_insert(
        self,
        context: InsertContext,
        runtime: QueryRuntime,
        body: Any,
        retry_body: Callable[[], Awaitable[Any]],
        /,
    ) -> dict[str, Any]: ...

    async def execute_raw_insert(
        self,
        table: str | None,
        column_names: Sequence[str] | None,
        insert_block: Any,
        fmt: str,
        compression: str | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> dict[str, Any]: ...

    async def execute_raw_query(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> bytes: ...

    async def execute_raw_stream(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
        /,
    ) -> Any: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...

    async def close_connections(self) -> None: ...
