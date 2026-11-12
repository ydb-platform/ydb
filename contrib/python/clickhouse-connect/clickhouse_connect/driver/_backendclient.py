"""Shared synchronous facade driving a pluggable execution backend.

`SyncBackendClient` implements the semantic client methods (queries, commands,
inserts, raw access, lifecycle) purely against the typed `SyncBackend`
execute_* seam, so a concrete facade only supplies construction, settings
storage, and any transport-specific compatibility surface.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any, BinaryIO, cast

from clickhouse_connect.driver._backend.httpcommon import parse_command_body
from clickhouse_connect.driver._backend.models import QueryRuntime
from clickhouse_connect.driver.binding import bind_query
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.exceptions import Error
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext, QueryResult
from clickhouse_connect.driver.streaming import _SyncStreamingInsertSource
from clickhouse_connect.driver.summary import QuerySummary

if TYPE_CHECKING:
    from clickhouse_connect.driver._backend.contracts import SyncBackend
    from clickhouse_connect.driver.transform import Transform

logger = logging.getLogger(__name__)


class SyncBackendClient(Client):
    _backend: SyncBackend
    _transform: Transform
    _write_format = "Native"
    _rename_response_column: str | None = None

    def _query_with_context(self, context: QueryContext) -> QueryResult:
        context.rename_response_column = self._rename_response_column
        if self.protocol_version:
            context.block_info = True
        runtime = QueryRuntime(
            database=self.database,
            protocol_version=self.protocol_version,
            settings=self._validate_settings(context.settings),
            retries=self.query_retries,
        )
        execution = self._backend.execute_query(context, runtime, self._prep_query(context))
        if execution.columns is not None:
            return self._columns_only_result(context, execution.columns)
        byte_source = RespBuffCls(execution.source)
        response_tz = self._check_tz_change(execution.response_tz_name)
        if response_tz is not None:
            context.set_response_tz(response_tz)
        query_result = self._transform.parse_response(byte_source, context)
        query_result.summary = execution.summary
        return cast(QueryResult, query_result)

    def data_insert(self, context: InsertContext) -> QuerySummary:
        """
        See BaseClient doc_string for this method
        """
        if context.empty:
            logger.debug("No data included in insert, skipping")
            return QuerySummary()

        if context.compression is None:
            context.compression = self.write_compression
        threaded = self._transform.threaded_insert
        active_source = None
        if threaded:
            active_source = _SyncStreamingInsertSource(transform=self._transform, context=context, maxsize=10)
            active_source.start_producer()
            block_gen = active_source.gen
        else:
            block_gen = self._transform.build_insert(context)

        def rebuild_block_gen():
            nonlocal active_source
            recorded = context.insert_exception
            if isinstance(recorded, Error):
                # Deterministic client-side refusal; a rebuilt insert would fail identically.
                context.insert_exception = None
                raise recorded
            # Reset so a failure on the rebuilt attempt is not masked by the first attempt's error.
            context.insert_exception = None
            if active_source is not None:
                active_source.close(timeout=None)
            context.current_row = 0
            context.current_block = 0
            if threaded:
                active_source = _SyncStreamingInsertSource(transform=self._transform, context=context, maxsize=10)
                active_source.start_producer()
                return active_source.gen
            return self._transform.build_insert(context)

        runtime = QueryRuntime(database=self.database, settings=self._validate_settings(context.settings))
        try:
            return QuerySummary(self._backend.execute_data_insert(context, runtime, block_gen, rebuild_block_gen))
        finally:
            if active_source is not None:
                active_source.close()
            context.data = None

    def raw_insert(
        self,
        table: str | None = None,
        column_names: Sequence[str] | None = None,
        insert_block: str | bytes | Generator[bytes, None, None] | BinaryIO | None = None,
        settings: dict | None = None,
        fmt: str | None = None,
        compression: str | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        """
        See BaseClient doc_string for this method
        """
        runtime = QueryRuntime(database=self.database, settings=self._validate_settings(settings or {}))
        summary = self._backend.execute_raw_insert(
            table, column_names, insert_block, fmt if fmt else self._write_format, compression, runtime, transport_settings
        )
        return QuerySummary(summary)

    def command(
        self,
        cmd: str,
        parameters: Sequence | dict[str, Any] | None = None,
        data: str | bytes | None = None,
        settings: dict | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> str | int | Sequence[str] | QuerySummary:
        """
        See BaseClient doc_string for this method
        """
        bound_cmd, bind_params = bind_query(cmd, parameters, self.server_tz)
        runtime = QueryRuntime(
            database=self.database if use_database else None,
            settings=self._validate_settings(settings or {}),
        )
        execution = self._backend.execute_command(bound_cmd, bind_params, data, external_data, runtime, transport_settings)
        if execution.body:
            return parse_command_body(execution.body)
        # A result-producing statement reports its output format even when the result is empty
        if execution.result_format is not None:
            return ""
        return QuerySummary(execution.summary)

    def raw_query(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> bytes:
        """
        See BaseClient doc_string for this method
        """
        final_query, bind_params, runtime = self._prep_raw_query_runtime(query, parameters, settings, fmt, use_database)
        return self._backend.execute_raw_query(final_query, bind_params, external_data, runtime, transport_settings)

    def raw_stream(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> io.IOBase:
        """
        See BaseClient doc_string for this method
        """
        final_query, bind_params, runtime = self._prep_raw_query_runtime(query, parameters, settings, fmt, use_database)
        return self._backend.execute_raw_stream(final_query, bind_params, external_data, runtime, transport_settings)

    def ping(self) -> bool:
        """
        See BaseClient doc_string for this method
        """
        return self._backend.ping()

    def close_connections(self) -> None:
        self._backend.close_connections()

    def close(self) -> None:
        self._backend.close()
