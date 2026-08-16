from __future__ import annotations

import asyncio
import logging
import ssl
import sys
import uuid
from base64 import b64encode
from collections.abc import Awaitable, Callable, Generator, Sequence
from datetime import tzinfo
from typing import TYPE_CHECKING, Any, BinaryIO, cast

import aiohttp

if TYPE_CHECKING:
    import numpy
    import pandas
    import polars
    import pyarrow

from clickhouse_connect import common
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import httputil, options
from clickhouse_connect.driver._backend.http_async import HttpAsyncBackend, release_lease
from clickhouse_connect.driver._backend.httpcommon import (
    add_integration_tag,
    apply_http_server_settings,
    auth_failed_ex_code,  # noqa: F401  (compatibility re-export)
    columns_only_re,  # noqa: F401  (compatibility re-export)
    decompress_response,  # noqa: F401  (compatibility re-export)
    ex_header,  # noqa: F401  (compatibility re-export)
    ex_tag_header,
    negotiate_compression,
    parse_command_body,
)
from clickhouse_connect.driver._backend.models import ClientConfig, QueryRuntime
from clickhouse_connect.driver._backend.operations import CommandOp, Operation, QueryOp, RawQueryOp
from clickhouse_connect.driver._backend.orchestration import init_sequence, insert_context_sequence, run_async
from clickhouse_connect.driver.binding import (
    bind_query,
    use_form_encoding,  # noqa: F401  (compatibility re-export)
)
from clickhouse_connect.driver.client import _INTERNAL_QUERY_FORMATS, Client, _apply_arrow_tz_policy
from clickhouse_connect.driver.common import (
    StreamContext,
    coerce_bool,
    dict_copy,  # noqa: F401  (compatibility re-export)
)
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.exceptions import DataError, ProgrammingError
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.options import check_arrow, check_numpy, check_pandas, check_polars
from clickhouse_connect.driver.query import (
    QueryContext,
    QueryResult,
    TzMode,
    TzSource,
    arrow_buffer,
)
from clickhouse_connect.driver.streaming import (
    QueuedStreamSource,
    StreamingFileAdapter,
    StreamingInsertSource,
    StreamingResponseSource,
    start_streaming_response,
)
from clickhouse_connect.driver.summary import QuerySummary
from clickhouse_connect.driver.transform import NativeTransform
from clickhouse_connect.driver.types import Closable

logger = logging.getLogger(__name__)


class BytesSource:
    """Wrapper to make bytes compatible with ResponseBuffer expectations."""

    def __init__(self, data: bytes):
        self.data = data
        self.gen = self._make_generator()

    def _make_generator(self):
        yield self.data

    def close(self):
        """No-op close method for compatibility."""


class AsyncClient(Client):
    valid_transport_settings = {
        "database",
        "buffer_size",
        "session_id",
        "compress",
        "decompress",
        "session_timeout",
        "session_check",
        "query_id",
        "quota_key",
        "wait_end_of_query",
        "client_protocol_version",
        "role",
    }
    optional_transport_settings = {
        "send_progress_in_http_headers",
        "http_headers_progress_interval_ms",
        "enable_http_compression",
    }

    def __init__(
        self,
        interface: str,
        host: str,
        port: int,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
        access_token: str | None = None,
        token_provider: Callable[[], str | Awaitable[str]] | None = None,
        compress: bool | str = True,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
        client_name: str | None = None,
        verify: bool | str = True,
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_cert_key: str | None = None,
        http_proxy: str | None = None,
        https_proxy: str | None = None,
        server_host_name: str | None = None,
        tls_mode: str | None = None,
        proxy_path: str = "",
        connector_limit: int = 100,
        connector_limit_per_host: int = 20,
        keepalive_timeout: float = 30.0,
        session_id: str | None = None,
        settings: dict[str, Any] | None = None,
        query_limit: int = 0,
        query_retries: int = 2,
        tz_source: TzSource | None = None,
        tz_mode: TzMode | None = None,
        show_clickhouse_errors: bool | None = None,
        autogenerate_session_id: bool | None = None,
        autogenerate_query_id: bool | None = None,
        form_encode_query_params: bool = False,
        rename_response_column: str | None = None,
        headers: dict[str, str] | None = None,
    ):
        """
        Async HTTP Client using aiohttp. Initialization is handled via _initialize().
        """
        proxy_path = proxy_path.lstrip("/")
        if proxy_path:
            proxy_path = "/" + proxy_path
        self.uri = f"{interface}://{host}:{port}{proxy_path}"
        self.url = self.uri
        self._rename_response_column = rename_response_column
        self._initial_settings = settings
        self.headers = {}

        if interface == "https":
            if isinstance(verify, str) and verify.lower() == "proxy":
                verify = True
                tls_mode = tls_mode or "proxy"

        # The initial token from token_provider is resolved in _initialize()

        # Auth headers follow the sync client: mutual TLS headers are set
        # independently, and a bearer token wins over basic auth.
        if client_cert and (tls_mode is None or tls_mode == "mutual"):
            if not username:
                raise ProgrammingError("username parameter is required for Mutual TLS authentication")
            self.headers["X-ClickHouse-User"] = username
            self.headers["X-ClickHouse-SSL-Certificate-Auth"] = "on"
        if access_token:
            self.headers["Authorization"] = f"Bearer {access_token}"
        elif (not client_cert or tls_mode in ("strict", "proxy")) and username:
            credentials = b64encode(f"{username}:{password}".encode()).decode()
            self.headers["Authorization"] = f"Basic {credentials}"

        self.headers["User-Agent"] = common.build_client_name(client_name)
        # Prevent aiohttp from automatically requesting compressed responses
        # We'll manually set Accept-Encoding when compression is desired
        self.headers["Accept-Encoding"] = "identity"
        self._send_receive_timeout = send_receive_timeout

        connect_timeout_val = float(connect_timeout) if connect_timeout is not None else None
        send_receive_timeout_val = float(send_receive_timeout) if send_receive_timeout is not None else None

        self._timeout = aiohttp.ClientTimeout(
            total=None,
            connect=connect_timeout_val,
            sock_connect=connect_timeout_val,
            sock_read=send_receive_timeout_val,
        )
        connector_limit_per_host = min(connector_limit_per_host, connector_limit)

        proxy_url = None
        if http_proxy:
            if not http_proxy.startswith("http://") and not http_proxy.startswith("https://"):
                proxy_url = f"http://{http_proxy}"
            else:
                proxy_url = http_proxy
        elif https_proxy:
            if not https_proxy.startswith("http://") and not https_proxy.startswith("https://"):
                proxy_url = f"http://{https_proxy}"
            else:
                proxy_url = https_proxy
        else:
            scheme = "https" if self.url.startswith("https://") else "http"
            env_proxy = httputil.check_env_proxy(scheme, host, port)
            if env_proxy:
                if not env_proxy.startswith("http://") and not env_proxy.startswith("https://"):
                    proxy_url = f"http://{env_proxy}"
                else:
                    proxy_url = env_proxy

        ssl_context = None
        if interface == "https":
            ssl_context = ssl.create_default_context()
            ssl_verify = verify if isinstance(verify, bool) else coerce_bool(verify)
            if not ssl_verify:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            elif ca_cert:
                ssl_context.load_verify_locations(httputil.resolve_ca_cert(ca_cert))
            if client_cert:
                ssl_context.load_cert_chain(client_cert, client_cert_key)

        connector_kwargs: dict[str, Any] = {
            "limit": connector_limit,
            "limit_per_host": connector_limit_per_host,
            "keepalive_timeout": keepalive_timeout,
            "force_close": False,
            "ssl": ssl_context,
        }
        # enable_cleanup_closed is only needed for Python < 3.12.7 or == 3.13.0
        # The underlying SSL connection leak was fixed in 3.12.7 and 3.13.1+
        # https://github.com/python/cpython/pull/118960
        if sys.version_info < (3, 12, 7) or sys.version_info[:3] == (3, 13, 0):
            connector_kwargs["enable_cleanup_closed"] = True

        self._write_format = "Native"
        self._transform = NativeTransform()
        self._client_settings: dict[str, str] = {}
        self._initialized = False
        self._reported_libs: set[str] = set()
        self.headers["User-Agent"] = self.headers["User-Agent"].replace("mode:sync;", "mode:async;")
        if headers:
            self.headers.update(headers)

        # Store aiohttp-specific params for deferred initialization
        self._compress_param = compress
        self._session_id_param = session_id
        self._autogenerate_session_id_param = autogenerate_session_id

        # The backend owns transport state. The headers and client_settings
        # dicts are shared by reference with this facade, so they are mutated
        # in place, never rebound.
        self._backend = HttpAsyncBackend(
            url=self.url,
            headers=self.headers,
            client_settings=self._client_settings,
            timeout=self._timeout,
            connector_kwargs=connector_kwargs,
            ssl_context=ssl_context,
            proxy_url=proxy_url,
            server_host_name=server_host_name,
            token_provider=token_provider,
            autogenerate_query_id=(common.get_setting("autogenerate_query_id") if autogenerate_query_id is None else autogenerate_query_id),
            read_format="Native",
            form_encode_query_params=form_encode_query_params,
        )

        # Call parent init with autoconnect=False to set up config without blocking I/O
        super().__init__(
            database=database,
            query_limit=query_limit,
            uri=self.uri,
            query_retries=query_retries,
            server_host_name=server_host_name,
            tz_source=tz_source,
            tz_mode=tz_mode,
            show_clickhouse_errors=show_clickhouse_errors,
            autoconnect=False,
        )

    @property
    def _session(self) -> aiohttp.ClientSession | None:
        return self._backend.session

    @_session.setter
    def _session(self, value: aiohttp.ClientSession | None) -> None:
        self._backend.session = value

    @property
    def show_clickhouse_errors(self) -> bool:  # type: ignore[override]
        return self._backend.show_clickhouse_errors

    @show_clickhouse_errors.setter
    def show_clickhouse_errors(self, value: bool) -> None:
        self._backend.show_clickhouse_errors = value

    @property
    def _autogenerate_query_id(self) -> bool:
        return self._backend.autogenerate_query_id

    @_autogenerate_query_id.setter
    def _autogenerate_query_id(self, value: bool) -> None:
        self._backend.autogenerate_query_id = value

    @property
    def _token_provider(self) -> Callable[[], str | Awaitable[str]] | None:
        return self._backend.token_provider

    @property
    def _proxy_url(self) -> str | None:
        return self._backend.proxy_url

    @property
    def form_encode_query_params(self) -> bool:
        return self._backend.form_encode_query_params

    @form_encode_query_params.setter
    def form_encode_query_params(self, value: bool) -> None:
        self._backend.form_encode_query_params = value

    @property
    def _read_format(self) -> str:
        return self._backend.read_format

    @_read_format.setter
    def _read_format(self, value: str) -> None:
        self._backend.read_format = value

    @property
    def compression(self) -> str | None:  # type: ignore[override]
        return self._backend.compression

    @compression.setter
    def compression(self, value: str | None) -> None:
        self._backend.compression = value

    async def _initialize(self):
        """
        Async equivalent of Client._init_common_settings.
        Fetches server version, timezone, and settings.
        """
        self._backend.ensure_session()

        if self._initialized:
            return

        if self._token_provider:
            self.set_access_token(await self._resolve_token())

        try:
            config = ClientConfig(settings=self._initial_settings or {}, timezone_policy=self._deferred_tz_source)
            init_result = await run_async(init_sequence(config), self._execute_operation)
            self._apply_init_result(init_result)

            if self._initial_settings:
                for key, value in self._initial_settings.items():
                    self.set_client_setting(key, value)

            compression, write_compression = negotiate_compression(self._compress_param)
            if write_compression:
                self.write_compression = write_compression

            session_id = self._session_id_param
            autogenerate_session_id = self._autogenerate_session_id_param

            if autogenerate_session_id is None:
                autogenerate_session_id = common.get_setting("autogenerate_session_id")

            if session_id:
                self.set_client_setting("session_id", session_id)
            elif self.get_client_setting("session_id"):
                pass
            elif autogenerate_session_id:
                self.set_client_setting("session_id", str(uuid.uuid4()))

            apply_http_server_settings(self, self._backend, compression, self._send_receive_timeout)

            self._initialized = True
        except Exception:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
            raise

    async def _execute_operation(self, operation: Operation) -> object:
        """Execute an orchestration operation through this client's semantic methods."""
        settings = dict(operation.settings) or None
        if isinstance(operation, CommandOp):
            return await self.command(operation.text, settings=settings, use_database=operation.use_database)
        if isinstance(operation, QueryOp):
            return await self.query(operation.text, settings=settings, query_formats=dict(_INTERNAL_QUERY_FORMATS))
        if isinstance(operation, RawQueryOp):
            return await self.raw_query(operation.text, settings=settings, fmt=operation.fmt)
        raise TypeError(f"Unsupported operation type: {type(operation).__name__}")

    async def __aenter__(self) -> AsyncClient:
        """Async context manager entry."""
        if not self._initialized:
            await self._initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Async context manager exit."""
        await self.close()
        return False

    async def close(self) -> None:  # type: ignore[override]
        await self._backend.close()

    async def close_connections(self) -> None:  # type: ignore[override]
        """Rotate the connection pool: new requests use a fresh session; in-flight
        requests keep using the old session until they complete, then it's closed."""
        await self._backend.close_connections()

    def set_client_setting(self, key: str, value: Any) -> None:
        str_value = self._validate_setting(key, value, common.get_setting("invalid_setting_action"))
        if str_value is not None:
            self._client_settings[key] = str_value

    def get_client_setting(self, key) -> str | None:
        return self._client_settings.get(key)

    async def _resolve_token(self) -> str:
        return await self._backend.resolve_token()

    def set_access_token(self, access_token: str) -> None:
        self._backend.set_access_token(access_token)

    async def _query_with_context(self, context: QueryContext) -> QueryResult:  # type: ignore[override]
        context.rename_response_column = self._rename_response_column
        if self.protocol_version:
            context.block_info = True
        runtime = QueryRuntime(
            database=self.database,
            protocol_version=self.protocol_version,
            settings=self._validate_settings(context.settings),
            retries=self.query_retries,
        )
        execution = await self._backend.execute_query(context, runtime, self._prep_query(context))
        if execution.columns is not None:
            return self._columns_only_result(context, execution.columns)

        streaming_source = cast(StreamingResponseSource, execution.source)
        loop = asyncio.get_running_loop()

        def parse_streaming():
            """Parse response from streaming queue (runs in executor)."""
            # Wrap streaming source with ResponseBuffer. The streaming source provides a
            #  .gen property that yields decompressed chunks.
            byte_source = RespBuffCls(streaming_source)
            context.set_response_tz(self._check_tz_change(execution.response_tz_name))
            result = self._transform.parse_response(byte_source, context)

            # For Pandas/Numpy, we must materialize in the executor because the resulting objects
            # (DataFrame, Array) are fully in-memory structures.
            # For standard queries, we return a lazy QueryResult. Accessing .result_set on the event loop
            # will raise a ProgrammingError (deadlock check), encouraging usage of .rows_stream.
            if not context.streaming:
                if context.as_pandas and hasattr(result, "df_result"):
                    _ = result.df_result
                elif context.use_numpy and hasattr(result, "np_result"):
                    _ = result.np_result
                elif isinstance(result, QueryResult):
                    _ = result.result_set

            return result

        # Run parser in executor (pulls from queue, decompresses & parses)
        try:
            query_result = await loop.run_in_executor(None, parse_streaming)
        except Exception:
            await streaming_source.aclose()
            raise
        query_result.summary = execution.summary

        # Attach streaming_source to query_result.source to ensure it gets closed
        #  when the query result is closed (e.g. by StreamContext.__exit__)
        query_result.source = streaming_source

        return query_result

    async def query(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        column_oriented: bool | None = None,
        use_numpy: bool | None = None,
        max_str_len: int | None = None,
        context: QueryContext | None = None,
        query_tz: str | tzinfo | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        tz_mode: TzMode | None = None,
    ) -> QueryResult:
        """
        Main query method for SELECT, DESCRIBE and other SQL statements that return a result matrix.  For
        parameters, see the create_query_context method
        :return: QueryResult -- data and metadata from response
        """
        if query and query.lower().strip().startswith("select __connect_version__"):
            return QueryResult(
                [[f"ClickHouse Connect v.{common.version()}  ⓒ ClickHouse Inc."]],
                None,  # type: ignore[arg-type]  # QueryContext.generator not yet Optional; widen after #805 merges
                ("connect_version",),
                (get_from_name("String"),),  # type: ignore[arg-type]
            )
        if not context:
            context = self.create_query_context(
                query=query,
                parameters=parameters,
                settings=settings,
                query_formats=query_formats,
                column_formats=column_formats,
                encoding=encoding,
                use_none=use_none,
                column_oriented=column_oriented,
                use_numpy=use_numpy,
                max_str_len=max_str_len,
                query_tz=query_tz,
                column_tzs=column_tzs,
                external_data=external_data,
                transport_settings=transport_settings,
                tz_mode=tz_mode,
            )

        if context.is_command:
            response = await self.command(
                query,
                parameters=context.parameters,
                settings=context.settings,
                external_data=context.external_data,
                transport_settings=context.transport_settings,
            )
            if isinstance(response, QuerySummary):
                return response.as_query_result()
            return QueryResult([response] if isinstance(response, list) else [[response]])

        return await self._query_with_context(context)

    async def query_column_block_stream(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        context: QueryContext | None = None,
        query_tz: str | tzinfo | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        tz_mode: TzMode | None = None,
    ) -> StreamContext:
        """
        Async version of query_column_block_stream.
        Returns a StreamContext that yields column-oriented blocks.
        """
        return (await self._context_query(locals(), use_numpy=False, streaming=True)).column_block_stream

    async def query_row_block_stream(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        context: QueryContext | None = None,
        query_tz: str | tzinfo | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        tz_mode: TzMode | None = None,
    ) -> StreamContext:
        """
        Async version of query_row_block_stream.
        Returns a StreamContext that yields row-oriented blocks.
        """
        return (await self._context_query(locals(), use_numpy=False, streaming=True)).row_block_stream

    async def query_rows_stream(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        context: QueryContext | None = None,
        query_tz: str | tzinfo | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        tz_mode: TzMode | None = None,
    ) -> StreamContext:
        """
        Async version of query_rows_stream.
        Returns a StreamContext that yields individual rows.
        """
        return (await self._context_query(locals(), use_numpy=False, streaming=True)).rows_stream

    async def query_np(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        max_str_len: int | None = None,
        context: QueryContext | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> numpy.ndarray:
        check_numpy()
        self._add_integration_tag("numpy")
        return (await self._context_query(locals(), use_numpy=True)).np_result

    async def query_np_stream(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        max_str_len: int | None = None,
        context: QueryContext | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> StreamContext:
        check_numpy()
        self._add_integration_tag("numpy")
        return (await self._context_query(locals(), use_numpy=True, streaming=True)).np_stream

    async def query_df(
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        max_str_len: int | None = None,
        use_na_values: bool | None = None,
        query_tz: str | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        context: QueryContext | None = None,
        external_data: ExternalData | None = None,
        use_extended_dtypes: bool | None = None,
        transport_settings: dict[str, str] | None = None,
        tz_mode: TzMode | None = None,
    ) -> pandas.DataFrame:
        check_pandas()
        self._add_integration_tag("pandas")
        return (await self._context_query(locals(), use_numpy=True, as_pandas=True)).df_result

    async def query_df_stream(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        max_str_len: int | None = None,
        use_na_values: bool | None = None,
        query_tz: str | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        context: QueryContext | None = None,
        external_data: ExternalData | None = None,
        use_extended_dtypes: bool | None = None,
        transport_settings: dict[str, str] | None = None,
        tz_mode: TzMode | None = None,
    ) -> StreamContext:
        check_pandas()
        self._add_integration_tag("pandas")
        return (await self._context_query(locals(), use_numpy=True, as_pandas=True, streaming=True)).df_stream

    async def _context_query(self, lcls: dict, **overrides):
        """
        Helper method to create query context and execute query.
        Matches sync client pattern for consistency.
        """
        kwargs = lcls.copy()
        kwargs.pop("self")
        kwargs.update(overrides)
        return await self._query_with_context(self.create_query_context(**kwargs))

    async def command(  # type: ignore[override]
        self,
        cmd,
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
        execution = await self._backend.execute_command(bound_cmd, bind_params, data, external_data, runtime, transport_settings)
        if execution.body:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, parse_command_body, execution.body)
        # A result-producing statement reports its output format even when the result is empty
        if execution.result_format is not None:
            return ""
        return QuerySummary(execution.summary)

    async def ping(self) -> bool:  # type: ignore[override]
        return await self._backend.ping()

    async def raw_query(  # type: ignore[override]
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
        return await self._backend.execute_raw_query(final_query, bind_params, external_data, runtime, transport_settings)

    async def raw_stream(  # type: ignore[override]
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> StreamContext:

        final_query, bind_params, runtime = self._prep_raw_query_runtime(query, parameters, settings, fmt, use_database)
        response = await self._backend.execute_raw_stream(final_query, bind_params, external_data, runtime, transport_settings)

        async def byte_iterator():
            async for chunk in response.content.iter_any():
                yield chunk

        class _RawStreamSource(Closable):
            def close(self):
                try:
                    response.close()
                finally:
                    release_lease(response)

        return StreamContext(_RawStreamSource(), byte_iterator())

    async def insert(  # type: ignore[override]
        self,
        table: str | None = None,
        data: Sequence[Sequence[Any]] | None = None,
        column_names: str | Sequence[str] | None = "*",
        database: str | None = None,
        column_types: Sequence[ClickHouseType] | None = None,
        column_type_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        settings: dict[str, Any] | None = None,
        context: InsertContext | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        """
        Method to insert multiple rows/data matrix of native Python objects.  If context is specified arguments
        other than data are ignored
        :param table: Target table
        :param data: Sequence of sequences of Python data
        :param column_names: Ordered list of column names or '*' if column types should be retrieved from the
            ClickHouse table definition
        :param database: Target database -- will use client default database if not specified.
        :param column_types: ClickHouse column types.  If set then column data does not need to be retrieved from
            the server
        :param column_type_names: ClickHouse column type names.  If set then column data does not need to be
            retrieved from the server
        :param column_oriented: If true the data is already "pivoted" in column form
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param context: Optional reusable insert context to allow repeated inserts into the same table with
            different data batches
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :return: QuerySummary with summary information, throws exception if insert fails
        """
        if (context is None or context.empty) and data is None:
            raise ProgrammingError("No data specified for insert") from None
        if context is None:
            if table is None:
                raise ProgrammingError("No table specified for insert") from None
            context = await self.create_insert_context(
                table,
                column_names,
                database,
                column_types,
                column_type_names,
                column_oriented,
                settings,
                transport_settings=transport_settings,
            )
        if data is not None:
            if not context.empty:
                raise ProgrammingError("Attempting to insert new data with non-empty insert context") from None
            context.data = data
        return await self.data_insert(context)

    async def query_arrow(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        use_strings: bool | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> pyarrow.Table:
        """
        Query method using the ClickHouse Arrow format to return a PyArrow table
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param use_strings: Convert ClickHouse String type to Arrow string type (instead of binary)
        :param external_data: ClickHouse "external data" to send with query
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :return: PyArrow.Table
        """
        check_arrow()
        self._add_integration_tag("arrow")
        settings = self._update_arrow_settings(settings, use_strings)

        final_query, bind_params, runtime = self._prep_raw_query_runtime(query, parameters, settings, "ArrowStream", True)
        response = await self._backend.execute_raw_stream(final_query, bind_params, external_data, runtime, transport_settings)
        encoding = response.headers.get("Content-Encoding")
        exception_tag = response.headers.get(ex_tag_header)

        loop = asyncio.get_running_loop()
        streaming_source = await start_streaming_response(response, encoding=encoding, exception_tag=exception_tag)

        def parse_arrow_stream():
            file_adapter = StreamingFileAdapter(streaming_source)
            reader = options.arrow.ipc.open_stream(file_adapter)
            table = reader.read_all()
            return _apply_arrow_tz_policy(table, self.tz_mode)

        try:
            return await loop.run_in_executor(None, parse_arrow_stream)
        finally:
            await streaming_source.aclose()

    async def query_arrow_stream(  # type: ignore[override]
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        use_strings: bool | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> StreamContext:
        """
        Query method that returns the results as a stream of Arrow record batches.

        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param use_strings: Convert ClickHouse String type to Arrow string type (instead of binary)
        :param external_data: ClickHouse "external data" to send with query
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :return: StreamContext that yields PyArrow RecordBatch objects asynchronously
        """
        check_arrow()
        self._add_integration_tag("arrow")
        settings = self._update_arrow_settings(settings, use_strings)

        final_query, bind_params, runtime = self._prep_raw_query_runtime(query, parameters, settings, "ArrowStream", True)
        response = await self._backend.execute_raw_stream(final_query, bind_params, external_data, runtime, transport_settings)
        encoding = response.headers.get("Content-Encoding")
        exception_tag = response.headers.get(ex_tag_header)

        streaming_source = await start_streaming_response(response, encoding=encoding, exception_tag=exception_tag)
        return self._arrow_batch_stream(streaming_source, lambda batch: _apply_arrow_tz_policy(batch, self.tz_mode))

    def _arrow_batch_stream(self, streaming_source: StreamingResponseSource, converter) -> StreamContext:
        """Parse an Arrow byte stream in an executor, yielding converted record
        batches to the event loop through the queued stream source."""
        queued = QueuedStreamSource(streaming_source)

        def batches():
            file_adapter = StreamingFileAdapter(streaming_source)
            reader = options.arrow.ipc.open_stream(file_adapter)
            for batch in reader:
                yield converter(batch)

        queued.pump(batches)
        return StreamContext(queued, queued.items())

    async def query_df_arrow(
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        use_strings: bool | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        dataframe_library: str = "pandas",
    ) -> pandas.DataFrame | polars.DataFrame:
        """
        Query method using the ClickHouse Arrow format to return a DataFrame
        with PyArrow dtype backend. This provides better performance and memory efficiency
        compared to the standard query_df method, though fewer output formatting options.

        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param use_strings: Convert ClickHouse String type to Arrow string type (instead of binary)
        :param external_data: ClickHouse "external data" to send with query
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :param dataframe_library: Library to use for DataFrame creation ("pandas" or "polars")
        :return: DataFrame (pandas or polars based on dataframe_library parameter)
        """
        check_arrow()

        if dataframe_library == "pandas":
            check_pandas()
            self._add_integration_tag("pandas")

            def converter(table: pyarrow.Table) -> pandas.DataFrame:
                table = _apply_arrow_tz_policy(table, self.tz_mode)
                return table.to_pandas(types_mapper=options.pd.ArrowDtype, safe=False)

        elif dataframe_library == "polars":
            check_polars()
            self._add_integration_tag("polars")

            def converter(table: pyarrow.Table) -> polars.DataFrame:  # type: ignore[misc]
                table = _apply_arrow_tz_policy(table, self.tz_mode)
                return options.pl.from_arrow(table)

        else:
            raise ValueError(f"dataframe_library must be 'pandas' or 'polars', got '{dataframe_library}'")

        arrow_table = await self.query_arrow(
            query=query,
            parameters=parameters,
            settings=settings,
            use_strings=use_strings,
            external_data=external_data,
            transport_settings=transport_settings,
        )

        return converter(arrow_table)

    async def query_df_arrow_stream(  # type: ignore[override]
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        use_strings: bool | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        dataframe_library: str = "pandas",
    ) -> StreamContext:
        """
        Query method that returns the results as a stream of DataFrames with PyArrow dtype backend.
        Each DataFrame represents a record batch from the ClickHouse response.

        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param use_strings: Convert ClickHouse String type to Arrow string type (instead of binary)
        :param external_data: ClickHouse "external data" to send with query
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :param dataframe_library: Library to use for DataFrame creation ("pandas" or "polars")
        :return: StreamContext that yields DataFrames asynchronously (pandas or polars based on dataframe_library parameter)
        """
        check_arrow()
        if dataframe_library == "pandas":
            check_pandas()
            self._add_integration_tag("pandas")

            def converter(table: pyarrow.Table) -> pandas.DataFrame:
                table = _apply_arrow_tz_policy(table, self.tz_mode)
                return table.to_pandas(types_mapper=options.pd.ArrowDtype, safe=False)

        elif dataframe_library == "polars":
            check_polars()
            self._add_integration_tag("polars")

            def converter(table: pyarrow.Table) -> polars.DataFrame:  # type: ignore[misc]
                table = _apply_arrow_tz_policy(table, self.tz_mode)
                return options.pl.from_arrow(table)

        else:
            raise ValueError(f"dataframe_library must be 'pandas' or 'polars', got '{dataframe_library}'")
        settings = self._update_arrow_settings(settings, use_strings)

        final_query, bind_params, runtime = self._prep_raw_query_runtime(query, parameters, settings, "ArrowStream", True)
        response = await self._backend.execute_raw_stream(final_query, bind_params, external_data, runtime, transport_settings)
        encoding = response.headers.get("Content-Encoding")
        exception_tag = response.headers.get(ex_tag_header)

        streaming_source = await start_streaming_response(response, encoding=encoding, exception_tag=exception_tag)
        return self._arrow_batch_stream(streaming_source, converter)

    async def insert_arrow(  # type: ignore[override]
        self,
        table: str,
        arrow_table,
        database: str | None = None,
        settings: dict | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        """
        Insert a PyArrow table DataFrame into ClickHouse using raw Arrow format
        :param table: ClickHouse table
        :param arrow_table: PyArrow Table object
        :param database: Optional ClickHouse database
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        """
        check_arrow()
        self._add_integration_tag("arrow")
        full_table = table if "." in table or not database else f"{database}.{table}"
        compression = self.write_compression if self.write_compression in ("zstd", "lz4") else None
        column_names, insert_block = arrow_buffer(arrow_table, compression)
        if hasattr(insert_block, "to_pybytes"):
            insert_block = insert_block.to_pybytes()
        return await self.raw_insert(full_table, column_names, insert_block, settings, "Arrow", transport_settings=transport_settings)

    async def insert_df_arrow(  # type: ignore[override]
        self,
        table: str,
        df: pandas.DataFrame | polars.DataFrame,
        database: str | None = None,
        settings: dict | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        """
        Insert a pandas DataFrame with PyArrow backend or a polars DataFrame into ClickHouse using Arrow format.
        This method is optimized for DataFrames that already use Arrow format, providing
        better performance than the standard insert_df method.

        Validation is performed and an exception will be raised if this requirement is not met.
        Polars DataFrames are natively Arrow-based and don't require additional validation.

        :param table: ClickHouse table name
        :param df: Pandas DataFrame with PyArrow dtype backend or Polars DataFrame
        :param database: Optional ClickHouse database name
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :return: QuerySummary with summary information, throws exception if insert fails
        """
        check_arrow()

        if options.pd is not None and isinstance(df, options.pd.DataFrame):
            df_lib = "pandas"
        elif options.pl is not None and isinstance(df, options.pl.DataFrame):
            df_lib = "polars"
        else:
            if options.pd is None and options.pl is None:
                raise ImportError("A DataFrame library (pandas or polars) must be installed to use insert_df_arrow.")
            raise TypeError(f"df must be either a pandas DataFrame or polars DataFrame, got {type(df).__name__}")

        if df_lib == "pandas":
            non_arrow_cols = [col for col, dtype in df.dtypes.items() if not isinstance(dtype, options.pd.ArrowDtype)]
            if non_arrow_cols:
                raise ProgrammingError(
                    f"insert_df_arrow requires all columns to use PyArrow dtypes. Non-Arrow columns found: [{', '.join(non_arrow_cols)}]. "
                )
            try:
                arrow_table = options.arrow.Table.from_pandas(df, preserve_index=False)
            except Exception as e:
                raise DataError(f"Failed to convert pandas DataFrame to Arrow table: {e}") from e
        else:
            try:
                arrow_table = df.to_arrow()
            except Exception as e:
                raise DataError(f"Failed to convert polars DataFrame to Arrow table: {e}") from e

        self._add_integration_tag(df_lib)
        return await self.insert_arrow(
            table=table,
            arrow_table=arrow_table,
            database=database,
            settings=settings,
            transport_settings=transport_settings,
        )

    async def create_insert_context(  # type: ignore[override]
        self,
        table: str,
        column_names: str | Sequence[str] | None = None,
        database: str | None = None,
        column_types: Sequence[ClickHouseType] | None = None,
        column_type_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        settings: dict[str, Any] | None = None,
        data: Sequence[Sequence[Any]] | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> InsertContext:
        """
        Builds a reusable insert context to hold state for a duration of an insert
        :param table: Target table
        :param database: Target database.  If not set, uses the client default database
        :param column_names: Optional ordered list of column names.  If not set, all columns ('*') will be assumed
          in the order specified by the table definition
        :param database: Target database -- will use client default database if not specified
        :param column_types: ClickHouse column types.  Optional  Sequence of ClickHouseType objects.  If neither column
           types nor column type names are set, actual column types will be retrieved from the server.
        :param column_type_names: ClickHouse column type names.  Specified column types by name string
        :param column_oriented: If true the data is already "pivoted" in column form
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param data: Initial dataset for insert
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :return: Reusable insert context
        """
        return await run_async(
            insert_context_sequence(
                table,
                column_names,
                database,
                column_types,
                column_type_names,
                column_oriented,
                settings,
                data,
                transport_settings,
            ),
            self._execute_operation,
        )

    async def data_insert(self, context: InsertContext) -> QuerySummary:  # type: ignore[override]
        """
        See BaseClient doc_string for this method.

        Uses true streaming via reverse bridge pattern:
        - Sync producer (serializer) runs in executor, puts blocks in queue
        - Async consumer (network) pulls from queue and yields to aiohttp
        - Bounded queue provides backpressure to prevent memory bloat
        """
        if context.empty:
            logger.debug("No data included in insert, skipping")
            return QuerySummary()

        if context.compression is None:
            context.compression = self.write_compression

        loop = asyncio.get_running_loop()

        active_source = StreamingInsertSource(transform=self._transform, context=context, loop=loop, maxsize=10)
        active_source.start_producer()

        async def rebuild_body():
            nonlocal active_source
            await active_source.close(timeout=None)
            context.current_row = 0
            context.current_block = 0
            active_source = StreamingInsertSource(transform=self._transform, context=context, loop=loop, maxsize=10)
            active_source.start_producer()
            return active_source.async_generator()

        runtime = QueryRuntime(database=self.database, settings=self._validate_settings(context.settings))
        try:
            summary = await self._backend.execute_data_insert(context, runtime, active_source.async_generator(), rebuild_body)
        except Exception:
            await active_source.close()

            if context.insert_exception:
                ex = context.insert_exception
                context.insert_exception = None
                raise ex from None
            raise
        finally:
            await active_source.close()
            context.data = None

        return QuerySummary(summary)

    async def insert_df(  # type: ignore[override]
        self,
        table: str | None = None,
        df=None,
        database: str | None = None,
        settings: dict | None = None,
        column_names: Sequence[str] | None = None,
        column_types: Sequence[ClickHouseType] | None = None,
        column_type_names: Sequence[str] | None = None,
        context: InsertContext | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        """
        Insert a pandas DataFrame into ClickHouse.  If context is specified arguments other than df are ignored
        :param table: ClickHouse table
        :param df: two-dimensional pandas dataframe
        :param database: Optional ClickHouse database
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param column_names: An optional list of ClickHouse column names.  If not set, the DataFrame column names
           will be used
        :param column_types: ClickHouse column types.  If set then column data does not need to be retrieved from
            the server
        :param column_type_names: ClickHouse column type names.  If set then column data does not need to be
            retrieved from the server
        :param context: Optional reusable insert context to allow repeated inserts into the same table with
            different data batches
        :param transport_settings: Optional dictionary of transport level settings (HTTP headers, etc.)
        :return: QuerySummary with summary information, throws exception if insert fails
        """
        check_pandas()
        self._add_integration_tag("pandas")
        if context is None:
            if column_names is None:
                column_names = df.columns
            elif len(column_names) != len(df.columns):
                raise ProgrammingError("DataFrame column count does not match insert_columns") from None
        return await self.insert(
            table,
            df,
            column_names,
            database,
            column_types=column_types,
            column_type_names=column_type_names,
            settings=settings,
            transport_settings=transport_settings,
            context=context,
        )

    async def raw_insert(  # type: ignore[override]
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
        summary = await self._backend.execute_raw_insert(
            table, column_names, insert_block, fmt if fmt else self._write_format, compression, runtime, transport_settings
        )
        return QuerySummary(summary)

    def _add_integration_tag(self, name: str):
        """
        Dynamically adds a product (like pandas or sqlalchemy) to the User-Agent string details section.
        """
        new_ua = add_integration_tag(self.headers, self._reported_libs, name)
        if new_ua and self._session:
            self._session.headers["User-Agent"] = new_ua

    async def _error_handler(self, response: aiohttp.ClientResponse, retried: bool = False):
        await self._backend.error_handler(response, retried)

    async def _raw_request(
        self,
        data,
        params,
        headers=None,
        files=None,
        method="POST",
        stream=False,
        server_wait=True,
        retries: int = 0,
        retry_body: Callable[[], Awaitable[Any]] | None = None,
    ) -> aiohttp.ClientResponse:
        return await self._backend.request(
            data,
            params,
            headers=headers,
            files=files,
            method=method,
            stream=stream,
            server_wait=server_wait,
            retries=retries,
            retry_body=retry_body,
        )
