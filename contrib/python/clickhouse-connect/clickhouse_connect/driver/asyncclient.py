from __future__ import annotations

import asyncio
import gzip
import io
import json
import logging
import re
import ssl
import sys
import time
import uuid
import zlib
import zoneinfo
from base64 import b64encode
from collections.abc import Awaitable, Callable, Generator, Iterable, Sequence
from datetime import timezone, tzinfo
from importlib import import_module
from importlib.metadata import version as dist_version
from typing import TYPE_CHECKING, Any, BinaryIO

import aiohttp
import lz4.frame
import zstandard

if TYPE_CHECKING:
    import pandas
    import polars
    import pyarrow

from clickhouse_connect import common
from clickhouse_connect.datatypes import dynamic as dynamic_module
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import httputil, options, tzutil
from clickhouse_connect.driver.asyncqueue import EOF_SENTINEL, AsyncSyncQueue
from clickhouse_connect.driver.binding import bind_query, quote_identifier
from clickhouse_connect.driver.client import Client, _apply_arrow_tz_policy
from clickhouse_connect.driver.common import StreamContext, coerce_bool, dict_copy
from clickhouse_connect.driver.compression import available_compression
from clickhouse_connect.driver.constants import CH_VERSION_WITH_PROTOCOL, PROTOCOL_VERSION_WITH_LOW_CARD
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.exceptions import DatabaseError, DataError, OperationalError, ProgrammingError
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.models import ColumnDef, SettingDef
from clickhouse_connect.driver.options import check_arrow, check_numpy, check_pandas, check_polars
from clickhouse_connect.driver.query import QueryContext, QueryResult, TzMode, TzSource, arrow_buffer
from clickhouse_connect.driver.streaming import StreamingFileAdapter, StreamingInsertSource, StreamingResponseSource
from clickhouse_connect.driver.summary import QuerySummary
from clickhouse_connect.driver.transform import NativeTransform

logger = logging.getLogger(__name__)
columns_only_re = re.compile(r"LIMIT 0\s*$", re.IGNORECASE)
ex_header = "X-ClickHouse-Exception-Code"
ex_tag_header = "X-ClickHouse-Exception-Tag"

if "br" in available_compression:
    import brotli
else:
    brotli = None


def decompress_response(data: bytes, encoding: str | None) -> bytes:
    """Decompress response data based on Content-Encoding header."""

    if not encoding or encoding == "identity":
        return data

    if encoding == "lz4":
        lz4_decom = lz4.frame.LZ4FrameDecompressor()
        return lz4_decom.decompress(data, len(data))
    if encoding == "zstd":
        zstd_decom = zstandard.ZstdDecompressor()
        return zstd_decom.stream_reader(io.BytesIO(data)).read()
    if encoding == "br":
        if brotli is not None:
            return brotli.decompress(data)
        raise OperationalError("Brotli compression requested but not installed.")
    if encoding == "gzip":
        return gzip.decompress(data)
    if encoding == "deflate":
        return zlib.decompress(data)
    raise OperationalError(f"Unsupported compression type: '{encoding}'. Supported compression: {', '.join(available_compression)}")


class BytesSource:
    """Wrapper to make bytes compatible with ResponseBuffer expectations."""

    def __init__(self, data: bytes):
        self.data = data
        self.gen = self._make_generator()

    def _make_generator(self):
        yield self.data

    def close(self):
        """No-op close method for compatibility."""


class _SessionLease:
    """An aiohttp.ClientSession with an in-flight request count, so close()
    can wait for outstanding requests to drain before tearing down the session."""

    __slots__ = ("session", "_inflight", "_drained")

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._inflight = 0
        self._drained = asyncio.Event()
        self._drained.set()

    def acquire(self) -> None:
        self._inflight += 1
        if self._inflight == 1:
            self._drained.clear()

    def release(self) -> None:
        self._inflight -= 1
        if self._inflight == 0:
            self._drained.set()

    async def wait_drained(self) -> None:
        await self._drained.wait()


def _one_shot(fn: Callable[[], None]) -> Callable[[], None]:
    """Returns a wrapper that invokes fn at most once."""
    fired = False

    def call():
        nonlocal fired
        if not fired:
            fired = True
            fn()

    return call


def _release_lease(response: aiohttp.ClientResponse | None) -> None:
    if response is None:
        return
    release = getattr(response, "_lease_release", None)
    if release is not None:
        release()


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
    ):
        """
        Async HTTP Client using aiohttp. Initialization is handled via _initialize().
        """
        proxy_path = proxy_path.lstrip("/")
        if proxy_path:
            proxy_path = "/" + proxy_path
        self.uri = f"{interface}://{host}:{port}{proxy_path}"
        self.url = self.uri
        self.form_encode_query_params = form_encode_query_params
        self._rename_response_column = rename_response_column
        self._initial_settings = settings
        self.headers = {}

        if interface == "https":
            if isinstance(verify, str) and verify.lower() == "proxy":
                verify = True
                tls_mode = tls_mode or "proxy"

        # Priority: access_token > mutual TLS > basic auth
        if client_cert and (tls_mode is None or tls_mode == "mutual"):
            if not username:
                raise ProgrammingError("username parameter is required for Mutual TLS authentication")
            self.headers["X-ClickHouse-User"] = username
            self.headers["X-ClickHouse-SSL-Certificate-Auth"] = "on"
        elif access_token:
            self.headers["Authorization"] = f"Bearer {access_token}"
        elif username and (not client_cert or tls_mode in ("strict", "proxy")):
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

        self._ssl_context = ssl_context
        self._proxy_url = proxy_url
        self._connector_kwargs = {
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
            self._connector_kwargs["enable_cleanup_closed"] = True

        self._session_lease: _SessionLease | None = None
        self._session_lock = asyncio.Lock()
        self._read_format = "Native"
        self._write_format = "Native"
        self._transform = NativeTransform()
        self._client_settings = {}
        self._initialized = False
        self._reported_libs = set()
        self._last_pool_reset = None
        self.headers["User-Agent"] = self.headers["User-Agent"].replace("mode:sync;", "mode:async;")

        # Store aiohttp-specific params for deferred initialization
        self._compress_param = compress
        self._session_id_param = session_id
        self._autogenerate_session_id_param = autogenerate_session_id
        self._autogenerate_query_id = (
            common.get_setting("autogenerate_query_id") if autogenerate_query_id is None else autogenerate_query_id
        )
        self._active_session = None
        self._send_progress = None
        self._progress_interval = None

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
        lease = self._session_lease
        return lease.session if lease is not None else None

    @_session.setter
    def _session(self, value: aiohttp.ClientSession | None) -> None:
        self._session_lease = _SessionLease(value) if value is not None else None

    async def _initialize(self):
        """
        Async equivalent of Client._init_common_settings.
        Fetches server version, timezone, and settings.
        """
        if not self._session:
            connector = aiohttp.TCPConnector(**self._connector_kwargs)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
                headers=self.headers,
                trust_env=False,
                auto_decompress=False,
                skip_auto_headers={"Accept-Encoding"},
            )

        if self._initialized:
            return

        try:
            tz_source = self._deferred_tz_source

            self.server_tz, self._dst_safe = timezone.utc, True
            row = await self.command("SELECT version(), timezone()", use_database=False)
            self.server_version, server_tz_str = tuple(row)
            try:
                server_tz = tzutil.resolve_zone(server_tz_str)
                server_tz, self._dst_safe = tzutil.normalize_timezone(server_tz, trust_fixed_offset=True)
                self.server_tz = server_tz
            except zoneinfo.ZoneInfoNotFoundError:
                logger.warning(
                    "Server timezone %s could not be resolved, falling back to UTC; %s",
                    server_tz_str,
                    tzutil.TZDATA_HINT,
                )
            if tz_source == "auto":
                self._apply_server_tz = self._dst_safe
            else:
                self._apply_server_tz = tz_source == "server"

            if not self._apply_server_tz and not tzutil.local_tz_dst_safe:
                logger.warning("local timezone %s may return unexpected times due to Daylight Savings Time", tzutil.local_tz.tzname(None))

            readonly = "readonly"
            if not self.min_version("19.17"):
                readonly = common.get_setting("readonly")

            server_settings = await self.query(f"SELECT name, value, {readonly} as readonly FROM system.settings LIMIT 10000")
            self.server_settings = {row["name"]: SettingDef(**row) for row in server_settings.named_results()}

            if self.min_version(CH_VERSION_WITH_PROTOCOL) and common.get_setting("use_protocol_version"):
                try:
                    test_data = await self.raw_query(
                        "SELECT 1 AS check", fmt="Native", settings={"client_protocol_version": PROTOCOL_VERSION_WITH_LOW_CARD}
                    )
                    if test_data[8:16] == b"\x01\x01\x05check":
                        self.protocol_version = PROTOCOL_VERSION_WITH_LOW_CARD
                except Exception:
                    pass

            cancel_setting = self._setting_status("cancel_http_readonly_queries_on_client_close")
            if (
                cancel_setting.is_writable
                and not cancel_setting.is_set
                and "cancel_http_readonly_queries_on_client_close" not in (self._initial_settings or {})
            ):
                self._client_settings["cancel_http_readonly_queries_on_client_close"] = "1"

            if self._initial_settings:
                for key, value in self._initial_settings.items():
                    self.set_client_setting(key, value)

            compress = self._compress_param
            if coerce_bool(compress):
                compression = ",".join(available_compression)
                self.write_compression = available_compression[0]
            elif compress and compress not in ("False", "false", "0"):
                if compress not in available_compression:
                    raise ProgrammingError(f"Unsupported compression method {compress}")
                compression = compress
                self.write_compression = compress
            else:
                compression = None

            comp_setting = self._setting_status("enable_http_compression")
            self._send_comp_setting = not comp_setting.is_set and comp_setting.is_writable
            if comp_setting.is_set or comp_setting.is_writable:
                self.compression = compression

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

            send_setting = self._setting_status("send_progress_in_http_headers")
            self._send_progress = not send_setting.is_set and send_setting.is_writable
            if (send_setting.is_set or send_setting.is_writable) and self._setting_status("http_headers_progress_interval_ms").is_writable:
                self._progress_interval = str(min(120000, max(10000, (self._send_receive_timeout - 5) * 1000)))

            if self._setting_status("date_time_input_format").is_writable:
                self.set_client_setting("date_time_input_format", "best_effort")
            if (
                self._setting_status("allow_experimental_json_type").is_set
                and self._setting_status("cast_string_to_dynamic_use_inference").is_writable
            ):
                self.set_client_setting("cast_string_to_dynamic_use_inference", "1")
            if self.min_version("24.8") and not self.min_version("24.10"):
                dynamic_module.json_serialization_format = 0

            self._initialized = True
        except Exception:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
            raise

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._initialized:
            await self._initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return False

    async def close(self):  # type: ignore[override]
        async with self._session_lock:
            old_lease = self._session_lease
            self._session_lease = None
        if old_lease is not None:
            await old_lease.wait_drained()
            await old_lease.session.close()

    async def close_connections(self):  # type: ignore[override]
        """Rotate the connection pool: new requests use a fresh session; in-flight
        requests keep using the old session until they complete, then it's closed."""
        async with self._session_lock:
            old_lease = self._session_lease
            connector = aiohttp.TCPConnector(**self._connector_kwargs)
            new_session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
                headers=self.headers,
                trust_env=False,
                auto_decompress=False,
                skip_auto_headers={"Accept-Encoding"},
            )
            self._session_lease = _SessionLease(new_session)
        if old_lease is not None:
            await old_lease.wait_drained()
            await old_lease.session.close()

    def set_client_setting(self, key, value):
        str_value = self._validate_setting(key, value, common.get_setting("invalid_setting_action"))
        if str_value is not None:
            self._client_settings[key] = str_value

    def get_client_setting(self, key) -> str | None:
        return self._client_settings.get(key)

    def set_access_token(self, access_token: str):
        auth_header = self.headers.get("Authorization")
        if auth_header and not auth_header.startswith("Bearer"):
            raise ProgrammingError("Cannot set access token when a different auth type is used")
        self.headers["Authorization"] = f"Bearer {access_token}"
        if self._session:
            self._session.headers["Authorization"] = f"Bearer {access_token}"

    def _prep_query(self, context: QueryContext):
        final_query = super()._prep_query(context)
        if context.is_insert:
            return final_query
        fmt = f"\n FORMAT {self._read_format}"
        if isinstance(final_query, bytes):
            return final_query + fmt.encode()
        return final_query + fmt

    async def _query_with_context(self, context: QueryContext) -> QueryResult:  # type: ignore[override]
        headers = {}
        params = {}
        if self.database:
            params["database"] = self.database
        if self.protocol_version:
            params["client_protocol_version"] = self.protocol_version
            context.block_info = True
        params.update(self._validate_settings(context.settings))
        context.rename_response_column = self._rename_response_column

        if not context.is_insert and columns_only_re.search(context.uncommented_query):
            fmt_json_query = f"{context.final_query}\n FORMAT JSON"
            fields = {"query": fmt_json_query}
            fields.update(context.bind_params)

            if self.form_encode_query_params:
                files = {}
                if context.external_data:
                    params.update(context.external_data.query_params)
                    files.update(context.external_data.form_data)

                for k, v in fields.items():
                    files[k] = (None, str(v))
                response = await self._raw_request(None, params, headers, files=files, retries=self.query_retries)
            elif context.external_data:
                params.update(context.bind_params)
                params.update(context.external_data.query_params)
                params["query"] = fmt_json_query
                response = await self._raw_request(None, params, headers, files=context.external_data.form_data, retries=self.query_retries)
            else:
                params.update(context.bind_params)
                response = await self._raw_request(fmt_json_query, params, headers, retries=self.query_retries)

            try:
                body = await response.read()
                encoding = response.headers.get("Content-Encoding")
            finally:
                _release_lease(response)
            loop = asyncio.get_running_loop()

            def decompress_and_parse_json():
                if encoding:
                    decompressed_body = decompress_response(body, encoding)
                else:
                    decompressed_body = body
                return json.loads(decompressed_body)

            # Offload to executor
            json_result = await loop.run_in_executor(None, decompress_and_parse_json)

            names: list[str] = []
            types: list[ClickHouseType] = []
            renamer = context.column_renamer
            for col in json_result["meta"]:
                name = col["name"]
                if renamer is not None:
                    try:
                        name = renamer(name)
                    except Exception as e:
                        logger.debug("Failed to rename col '%s'. Skipping rename. Error: %s", name, e)
                names.append(name)
                types.append(get_from_name(col["type"]))
            return QueryResult([], None, tuple(names), tuple(types))

        if self.compression:
            headers["Accept-Encoding"] = self.compression
            if self._send_comp_setting:
                params["enable_http_compression"] = "1"

        final_query = self._prep_query(context)

        files = None
        data = None

        if self.form_encode_query_params:
            fields = {"query": final_query}
            fields.update(context.bind_params)

            files = {}
            if context.external_data:
                params.update(context.external_data.query_params)
                files.update(context.external_data.form_data)

            for k, v in fields.items():
                files[k] = (None, str(v))
        elif context.external_data:
            params.update(context.bind_params)
            params.update(context.external_data.query_params)
            params["query"] = final_query
            files = context.external_data.form_data
        else:
            params.update(context.bind_params)
            data = final_query
            headers["Content-Type"] = "text/plain; charset=utf-8"

        headers = dict_copy(headers, context.transport_settings)

        response = await self._raw_request(
            data,
            params,
            headers,
            files=files,
            server_wait=not context.streaming,
            stream=True,
            retries=self.query_retries,
        )
        encoding = response.headers.get("Content-Encoding")
        tz_header = response.headers.get("X-ClickHouse-Timezone")
        exception_tag = response.headers.get(ex_tag_header)

        loop = asyncio.get_running_loop()
        streaming_source = StreamingResponseSource(response, encoding=encoding, exception_tag=exception_tag)
        await streaming_source.start_producer(loop)

        def parse_streaming():
            """Parse response from streaming queue (runs in executor)."""
            # Wrap streaming source with ResponseBuffer. The streaming source provides a
            #  .gen property that yields decompressed chunks.
            byte_source = RespBuffCls(streaming_source)
            context.set_response_tz(self._check_tz_change(tz_header))
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
        query_result.summary = self._summary(response)

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
                [[f"ClickHouse Connect v.{common.version()}  ⓒ ClickHouse Inc."]], None, ("connect_version",), (get_from_name("String"),)
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

    async def query_np(
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
    ):
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
    ):
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

    async def _context_query(self, lcls: dict, **overrides):  # type: ignore[override]
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
        cmd, bind_params = bind_query(cmd, parameters, self.server_tz)
        params = bind_params.copy()
        headers = {}
        payload = None
        files = None

        if external_data:
            if data:
                raise ProgrammingError("Cannot combine command data with external data") from None
            files = external_data.form_data
            params.update(external_data.query_params)
        elif isinstance(data, str):
            headers["Content-Type"] = "text/plain; charset=utf-8"
            payload = data.encode()
        elif isinstance(data, bytes):
            headers["Content-Type"] = "application/octet-stream"
            payload = data

        if payload is None and not cmd:
            raise ProgrammingError("Command sent without query or recognized data") from None

        if payload or files:
            params["query"] = cmd
        else:
            payload = cmd

        if use_database and self.database:
            params["database"] = self.database
        params.update(self._validate_settings(settings or {}))
        headers = dict_copy(headers, transport_settings)
        method = "POST" if payload or files else "GET"
        response = await self._raw_request(payload, params, headers, files=files, method=method, server_wait=False)
        try:
            body = await response.read()
            encoding = response.headers.get("Content-Encoding")
            summary = self._summary(response)
        finally:
            _release_lease(response)

        if not body:
            return QuerySummary(summary)

        loop = asyncio.get_running_loop()

        def decompress_and_decode():
            if encoding:
                decompressed_body = decompress_response(body, encoding)
            else:
                decompressed_body = body
            try:
                result = decompressed_body.decode()[:-1].split("\t")
                if len(result) == 1:
                    try:
                        return int(result[0])
                    except ValueError:
                        return result[0]
                return result
            except UnicodeDecodeError:
                return str(decompressed_body)

        return await loop.run_in_executor(None, decompress_and_decode)

    async def ping(self) -> bool:  # type: ignore[override]
        async with self._session_lock:
            lease = self._session_lease
            if lease is None or lease.session.closed:
                return False
            session = lease.session
            lease.acquire()
        try:
            url = f"{self.url}/ping"
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with session.get(url, timeout=timeout) as response:
                return 200 <= response.status < 300
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.debug("ping failed", exc_info=True)
            return False
        finally:
            lease.release()

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
        body, params, headers, files = self._prep_raw_query(query, parameters, settings, fmt, use_database, external_data)
        if transport_settings:
            headers = dict_copy(headers, transport_settings)

        response = await self._raw_request(body, params, headers=headers, files=files, retries=self.query_retries)
        try:
            response_data = await response.read()
            encoding = response.headers.get("Content-Encoding")
        finally:
            _release_lease(response)

        if encoding:
            loop = asyncio.get_running_loop()
            response_data = await loop.run_in_executor(None, decompress_response, response_data, encoding)

        return response_data

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

        body, params, headers, files = self._prep_raw_query(query, parameters, settings, fmt, use_database, external_data)
        if transport_settings:
            headers = dict_copy(headers, transport_settings)

        response = await self._raw_request(
            body, params, headers=headers, files=files, stream=True, server_wait=False, retries=self.query_retries
        )

        async def byte_iterator():
            async for chunk in response.content.iter_any():
                yield chunk

        class _RawStreamSource:
            def close(self):
                try:
                    response.close()
                finally:
                    _release_lease(response)

        return StreamContext(_RawStreamSource(), byte_iterator())

    def _prep_raw_query(self, query, parameters, settings, fmt, use_database, external_data):
        """
        Prepare raw query for execution.

        Note: Unlike sync client which returns (body, params, fields), this async version
        returns (body, params, headers, files) because aiohttp requires headers to be
        configured before the request() call, while urllib3 can add them during request.
        """
        if fmt:
            query += f"\n FORMAT {fmt}"

        final_query, bind_params = bind_query(query, parameters, self.server_tz)
        params = self._validate_settings(settings or {})
        if use_database and self.database:
            params["database"] = self.database

        headers = {}
        files = None
        body = None

        if external_data and not self.form_encode_query_params and isinstance(final_query, bytes):
            raise ProgrammingError("Binary query cannot be placed in URL when using External Data; enable form encoding.")

        if self.form_encode_query_params:
            files = {}
            files["query"] = (None, final_query if isinstance(final_query, str) else final_query.decode())
            for k, v in bind_params.items():
                files[k] = (None, str(v))

            if external_data:
                params.update(external_data.query_params)
                files.update(external_data.form_data)

            body = None
        elif external_data:
            params.update(bind_params)
            params["query"] = final_query
            params.update(external_data.query_params)
            files = external_data.form_data
            body = None
        else:
            params.update(bind_params)
            body = final_query.encode() if isinstance(final_query, str) else final_query

        return body, params, headers, files

    async def insert(  # type: ignore[override]
        self,
        table: str | None = None,
        data: Sequence[Sequence[Any]] | None = None,
        column_names: str | Iterable[str] = "*",
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
    ):
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

        body, params, headers, files = self._prep_raw_query(
            query,
            parameters,
            settings,
            fmt="ArrowStream",
            use_database=True,
            external_data=external_data,
        )
        if transport_settings:
            headers = dict_copy(headers, transport_settings)

        response = await self._raw_request(
            body,
            params,
            headers=headers,
            files=files,
            stream=True,
            server_wait=False,
            retries=self.query_retries,
        )
        encoding = response.headers.get("Content-Encoding")
        exception_tag = response.headers.get(ex_tag_header)

        loop = asyncio.get_running_loop()
        streaming_source = StreamingResponseSource(response, encoding=encoding, exception_tag=exception_tag)
        await streaming_source.start_producer(loop)

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

        body, params, headers, files = self._prep_raw_query(
            query, parameters, settings, fmt="ArrowStream", use_database=True, external_data=external_data
        )
        if transport_settings:
            headers = dict_copy(headers, transport_settings)

        response = await self._raw_request(
            body, params, headers=headers, files=files, stream=True, server_wait=False, retries=self.query_retries
        )
        encoding = response.headers.get("Content-Encoding")
        exception_tag = response.headers.get(ex_tag_header)

        loop = asyncio.get_running_loop()
        streaming_source = StreamingResponseSource(response, encoding=encoding, exception_tag=exception_tag)
        await streaming_source.start_producer(loop)

        queue = AsyncSyncQueue(maxsize=10)

        class _ArrowStreamSource:
            def __init__(self, source, q):
                self._source = source
                self._queue = q

            async def aclose(self):
                self._queue.shutdown()
                await self._source.aclose()

            def close(self):
                self._queue.shutdown()
                self._source.close()

        def parse_arrow_streaming():
            """Parse Arrow stream incrementally in executor (off event loop)."""
            try:
                file_adapter = StreamingFileAdapter(streaming_source)
                reader = options.arrow.ipc.open_stream(file_adapter)

                for batch in reader:
                    try:
                        batch = _apply_arrow_tz_policy(batch, self.tz_mode)
                        queue.sync_q.put(batch)
                    except RuntimeError:
                        return

                try:
                    queue.sync_q.put(EOF_SENTINEL)
                except RuntimeError:
                    return
            except Exception as e:
                try:
                    queue.sync_q.put(e)
                except Exception:
                    pass
            finally:
                queue.shutdown()

        loop.run_in_executor(None, parse_arrow_streaming)

        async def arrow_batch_generator():
            """Async generator that yields record batches without blocking event loop."""
            while True:
                item = await queue.async_q.get()
                if item is EOF_SENTINEL:
                    break
                if isinstance(item, Exception):
                    raise item
                yield item

        return StreamContext(_ArrowStreamSource(streaming_source, queue), arrow_batch_generator())

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

            def converter(table: pyarrow.Table) -> polars.DataFrame:
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

            def converter(table: pyarrow.Table) -> polars.DataFrame:
                table = _apply_arrow_tz_policy(table, self.tz_mode)
                return options.pl.from_arrow(table)

        else:
            raise ValueError(f"dataframe_library must be 'pandas' or 'polars', got '{dataframe_library}'")
        settings = self._update_arrow_settings(settings, use_strings)

        body, params, headers, files = self._prep_raw_query(
            query, parameters, settings, fmt="ArrowStream", use_database=True, external_data=external_data
        )
        if transport_settings:
            headers = dict_copy(headers, transport_settings)

        response = await self._raw_request(
            body, params, headers=headers, files=files, stream=True, server_wait=False, retries=self.query_retries
        )
        encoding = response.headers.get("Content-Encoding")
        exception_tag = response.headers.get(ex_tag_header)

        loop = asyncio.get_running_loop()
        streaming_source = StreamingResponseSource(response, encoding=encoding, exception_tag=exception_tag)
        await streaming_source.start_producer(loop)

        queue = AsyncSyncQueue(maxsize=10)

        class _ArrowDFStreamSource:
            def __init__(self, source, q):
                self._source = source
                self._queue = q

            async def aclose(self):
                self._queue.shutdown()
                await self._source.aclose()

            def close(self):
                self._queue.shutdown()
                self._source.close()

        def parse_and_convert_streaming():
            """Parse Arrow stream and convert to DataFrames in executor (off event loop)."""
            try:
                file_adapter = StreamingFileAdapter(streaming_source)

                # PyArrow reads incrementally from adapter (which pulls from queue)
                reader = options.arrow.ipc.open_stream(file_adapter)

                for batch in reader:
                    try:
                        queue.sync_q.put(converter(batch))
                    except RuntimeError:
                        return

                try:
                    queue.sync_q.put(EOF_SENTINEL)
                except RuntimeError:
                    return
            except Exception as e:
                try:
                    queue.sync_q.put(e)
                except Exception:
                    pass
            finally:
                queue.shutdown()

        loop.run_in_executor(None, parse_and_convert_streaming)

        async def df_generator():
            """Async generator that yields DataFrames without blocking event loop."""
            while True:
                item = await queue.async_q.get()
                if item is EOF_SENTINEL:
                    break
                if isinstance(item, Exception):
                    raise item
                yield item

        return StreamContext(_ArrowDFStreamSource(streaming_source, queue), df_generator())

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
        return await self.raw_insert(full_table, column_names, insert_block, settings, "Arrow", transport_settings)

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
        full_table = table
        if "." not in table:
            if database:
                full_table = f"{quote_identifier(database)}.{quote_identifier(table)}"
            else:
                full_table = quote_identifier(table)
        column_defs = []
        if column_types is None and column_type_names is None:
            describe_result = await self.query(f"DESCRIBE TABLE {full_table}", settings=settings)
            column_defs = [
                ColumnDef(**row) for row in describe_result.named_results() if row["default_type"] not in ("ALIAS", "MATERIALIZED")
            ]
        if column_names is None or isinstance(column_names, str) and column_names == "*":
            column_names = [cd.name for cd in column_defs]
            column_types = [cd.ch_type for cd in column_defs]
        elif isinstance(column_names, str):
            column_names = [column_names]
        if len(column_names) == 0:
            raise ValueError("Column names must be specified for insert")
        if not column_types:
            if column_type_names:
                column_types = [get_from_name(name) for name in column_type_names]
            else:
                column_map = {d.name: d for d in column_defs}
                try:
                    column_types = [column_map[name].ch_type for name in column_names]
                except KeyError as ex:
                    raise ProgrammingError(f"Unrecognized column {ex} in table {table}") from None
        if len(column_names) != len(column_types):
            raise ProgrammingError("Column names do not match column types") from None
        return InsertContext(
            full_table,
            column_names,
            column_types,
            column_oriented=column_oriented,
            settings=settings,
            transport_settings=transport_settings,
            data=data,
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

        headers = {"Content-Type": "application/octet-stream"}
        if context.compression:
            headers["Content-Encoding"] = context.compression

        params = {}
        if self.database:
            params["database"] = self.database
        params.update(self._validate_settings(context.settings))
        headers = dict_copy(headers, context.transport_settings)

        response = None
        try:
            response = await self._raw_request(
                active_source.async_generator(),
                params,
                headers=headers,
                server_wait=False,
                retry_body=rebuild_body,
            )
            logger.debug("Context insert response code: %d", response.status)
            summary = self._summary(response)
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
            if response is not None:
                response.close()
                _release_lease(response)

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
        params = {}
        headers = {"Content-Type": "application/octet-stream"}
        if compression:
            headers["Content-Encoding"] = compression

        if table:
            cols = f" ({', '.join([quote_identifier(x) for x in column_names])})" if column_names is not None else ""
            fmt_str = fmt if fmt else self._write_format
            query = f"INSERT INTO {table}{cols} FORMAT {fmt_str}"
            if not compression and isinstance(insert_block, str):
                insert_block = query + "\n" + insert_block
            elif not compression and isinstance(insert_block, (bytes, bytearray, BinaryIO)):
                insert_block = (query + "\n").encode() + insert_block
            else:
                params["query"] = query

        if self.database:
            params["database"] = self.database
        params.update(self._validate_settings(settings or {}))
        headers = dict_copy(headers, transport_settings)

        response = await self._raw_request(insert_block, params, headers, server_wait=False)
        try:
            logger.debug("Raw insert response code: %d", response.status)
            return QuerySummary(self._summary(response))
        finally:
            response.close()
            _release_lease(response)

    def _add_integration_tag(self, name: str):
        """
        Dynamically adds a product (like pandas or sqlalchemy) to the User-Agent string details section.
        """
        if not common.get_setting("send_integration_tags") or name in self._reported_libs:
            return

        try:
            ver = "unknown"
            try:
                ver = dist_version(name)
            except Exception:
                try:
                    mod = import_module(name)
                    ver = getattr(mod, "__version__", "unknown")
                except Exception:
                    pass

            product_info = f"{name}/{ver}"

            ua = self.headers.get("User-Agent", "")
            start = ua.find("(")
            if start == -1:
                return
            end = ua.find(")", start + 1)
            if end == -1:
                return

            details = ua[start + 1 : end].strip()

            if product_info in details:
                self._reported_libs.add(name)
                return

            new_details = f"{product_info}; {details}" if details else product_info
            new_ua = f"{ua[: start + 1]}{new_details}{ua[end:]}"
            self.headers["User-Agent"] = new_ua.strip()
            if self._session:
                self._session.headers["User-Agent"] = new_ua.strip()

            self._reported_libs.add(name)
            logger.debug("Added '%s' to User-Agent", product_info)

        except Exception as e:
            logger.debug("Problem adding '%s' to User-Agent: %s", name, e)

    async def _error_handler(self, response: aiohttp.ClientResponse, retried: bool = False):
        """
        Handles HTTP errors. Tries to be robust and provide maximum context.
        """
        try:
            body = ""
            try:
                raw_body = await response.read()
                encoding = response.headers.get("Content-Encoding")

                if encoding:
                    loop = asyncio.get_running_loop()

                    def decompress_and_decode():
                        decompressed = decompress_response(raw_body, encoding)
                        return common.format_error(decompressed.decode(errors="backslashreplace")).strip()

                    body = await loop.run_in_executor(None, decompress_and_decode)
                else:
                    loop = asyncio.get_running_loop()
                    body = await loop.run_in_executor(None, lambda: common.format_error(raw_body.decode(errors="backslashreplace")).strip())
            except Exception:
                logger.warning("Failed to read error response body", exc_info=True)

            if self.show_clickhouse_errors:
                err_code = response.headers.get(ex_header)
                if err_code:
                    err_str = f"Received ClickHouse exception, code: {err_code}"
                else:
                    err_str = f"HTTP driver received HTTP status {response.status}"

                if body:
                    err_str = f"{err_str}, server response: {body}"
            else:
                err_str = "The ClickHouse server returned an error"

            err_str = f"{err_str} (for url {self.url})"

        finally:
            response.close()

        raise OperationalError(err_str) if retried else DatabaseError(err_str) from None

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
        if self._session is None:
            raise ProgrammingError(
                "Session not initialized. Use 'async with get_async_client(...)' or call 'await client._initialize()' first."
            )

        reset_seconds = common.get_setting("max_connection_age")
        if reset_seconds:
            now = time.time()
            if self._last_pool_reset is None:
                self._last_pool_reset = now
            elif self._last_pool_reset < now - reset_seconds:
                # Stamp before await so concurrent callers don't all queue redundant resets.
                self._last_pool_reset = now
                logger.debug("connection expiration - resetting connection pool")
                await self.close_connections()

        final_params = dict_copy(self._client_settings, params)
        if server_wait:
            final_params.setdefault("wait_end_of_query", "1")
        if self._send_progress:
            final_params.setdefault("send_progress_in_http_headers", "1")
        if self._progress_interval:
            final_params.setdefault("http_headers_progress_interval_ms", self._progress_interval)
        if self._autogenerate_query_id and "query_id" not in final_params:
            final_params["query_id"] = str(uuid.uuid4())

        req_headers = dict_copy(self.headers, headers)
        if self.server_host_name:
            req_headers["Host"] = self.server_host_name
        query_session = final_params.get("session_id")
        attempts = 0

        while True:
            attempts += 1

            if query_session:
                if query_session == self._active_session:
                    raise ProgrammingError(
                        "Attempt to execute concurrent queries within the same session. "
                        "Please use a separate client instance per concurrent query."
                    )
                self._active_session = query_session

            # Snapshot+acquire under lock so close_connections() can't pass the
            # drain check between our session read and our refcount increment.
            async with self._session_lock:
                lease = self._session_lease
                if lease is None or lease.session.closed:
                    if query_session:
                        self._active_session = None
                    raise ProgrammingError("Client session is unavailable; the client may have been closed.")
                session = lease.session
                lease.acquire()
            lease_released = False
            try:
                # Construct full URL (aiohttp doesn't have base_url)
                url = f"{self.url}/"
                request_kwargs = {"method": method, "url": url, "params": final_params, "headers": req_headers}
                if hasattr(self, "_proxy_url") and self._proxy_url:
                    request_kwargs["proxy"] = self._proxy_url
                if files:
                    # IMPORTANT: Must set content_type on text fields to force multipart/form-data encoding
                    # Without content_type, aiohttp uses application/x-www-form-urlencoded
                    form = aiohttp.FormData()
                    for field_name, field_value in files.items():
                        if isinstance(field_value, tuple):
                            if field_value[0] is None:
                                form.add_field(field_name, str(field_value[1]), content_type="text/plain")
                            else:
                                filename = field_value[0]
                                file_data = field_value[1]
                                content_type = field_value[2] if len(field_value) > 2 else None
                                form.add_field(field_name, file_data, filename=filename, content_type=content_type)
                        else:
                            form.add_field(field_name, field_value, content_type="text/plain")
                    request_kwargs["data"] = form
                elif isinstance(data, dict):
                    request_kwargs["data"] = data
                else:
                    request_kwargs["data"] = data

                response = await session.request(**request_kwargs)
                if 200 <= response.status < 300 and not response.headers.get(ex_header):
                    # Caller releases lease after consuming the body.
                    response._lease_release = _one_shot(lease.release)
                    lease_released = True
                    return response

                if response.status in (429, 503, 504):
                    if attempts > retries:
                        await self._error_handler(response, retried=True)
                    else:
                        logger.debug("Retrying request with status code %s (attempt %s/%s)", response.status, attempts, retries + 1)
                        await asyncio.sleep(0.1 * attempts)
                        response.close()
                        continue
                await self._error_handler(response)

            except aiohttp.ServerConnectionError as e:
                msg = str(e)
                if "Connection reset" in msg or "Remote end closed" in msg or "Cannot connect" in msg or "Server disconnected" in msg:
                    if attempts == 1:
                        if retry_body is not None:
                            data = await retry_body()
                            logger.debug("Retrying after connection error with rebuilt body")
                            continue
                        if data is None or isinstance(data, (bytes, bytearray, str, dict)):
                            logger.debug("Retrying after connection error from remote host")
                            continue
                raise OperationalError(f"Network Error: {msg}") from e

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                raise OperationalError(f"Network Error: {str(e)}") from e

            finally:
                if not lease_released:
                    lease.release()
                if query_session:
                    self._active_session = None

    @staticmethod
    def _summary(response: aiohttp.ClientResponse):
        summary = {}
        if "X-ClickHouse-Summary" in response.headers:
            try:
                summary = json.loads(response.headers["X-ClickHouse-Summary"])
            except json.JSONDecodeError:
                pass
        summary["query_id"] = response.headers.get("X-ClickHouse-Query-Id", "")
        return summary
