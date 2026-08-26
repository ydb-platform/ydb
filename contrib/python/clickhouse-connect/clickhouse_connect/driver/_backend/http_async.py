"""Asynchronous HTTP transport backend built on aiohttp.

Owns session lifecycle (leases, rotation, drain-on-close), request execution,
retry and auth-refresh policy, ping, and HTTP error handling. During the 1.x
transition the headers and client_settings dicts are shared by reference with
the AsyncClient facade, which must mutate them in place rather than rebinding.
"""

from __future__ import annotations

import asyncio
import inspect
import io
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

import aiohttp

from clickhouse_connect import common
from clickhouse_connect.driver._backend.httpcommon import (
    auth_failed_ex_code,
    build_http_error,
    decompress_response,
    ex_header,
    ex_tag_header,
    plan_command_request,
    plan_data_insert_request,
    plan_query_request,
    plan_raw_insert_request,
    plan_raw_query_request,
    retryable_http_statuses,
    summary_from_headers,
)
from clickhouse_connect.driver._backend.models import Capabilities, CommandExecution, QueryExecution, QueryRuntime
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError
from clickhouse_connect.driver.streaming import start_streaming_response

if TYPE_CHECKING:
    from clickhouse_connect.driver._backend.contracts import AsyncBackend
    from clickhouse_connect.driver._backend.httpcommon import QueryRequestPlan
    from clickhouse_connect.driver.external import ExternalData
    from clickhouse_connect.driver.insert import InsertContext
    from clickhouse_connect.driver.query import QueryContext

logger = logging.getLogger(__name__)

_REMOTE_CLOSE_ERRORS = (ConnectionResetError, BrokenPipeError)


def _plan_files(plan: QueryRequestPlan) -> dict[str, Any] | None:
    """Merge a plan's form parts into aiohttp files: file parts first, then
    plain values wrapped as text fields."""
    if plan.form_values is None and plan.form_files is None:
        return None
    if plan.form_values is None:
        return plan.form_files
    files: dict[str, Any] = {}
    if plan.form_files:
        files.update(plan.form_files)
    for key, value in plan.form_values.items():
        files[key] = (None, str(value))
    return files


def _plan_raw_files(plan: QueryRequestPlan) -> dict[str, Any] | None:
    """Merge a raw-query plan's form parts into aiohttp files. Unlike
    _plan_files, the wrapped text parts come first (a bytes query is decoded,
    not str()-coerced), followed by external file parts."""
    if plan.form_values is None:
        return plan.form_files
    files: dict[str, Any] = {}
    for key, value in plan.form_values.items():
        if key == "query":
            files[key] = (None, value if isinstance(value, str) else value.decode())
        else:
            files[key] = (None, str(value))
    if plan.form_files:
        files.update(plan.form_files)
    return files


class SessionLease:
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


def release_lease(response: aiohttp.ClientResponse | None) -> None:
    if response is None:
        return
    release = getattr(response, "_lease_release", None)
    if release is not None:
        release()


def _is_retryable_async_connection_error(error: aiohttp.ClientConnectionError) -> bool:
    if isinstance(error, (aiohttp.ServerTimeoutError, aiohttp.ClientConnectorError, aiohttp.ServerFingerprintMismatch)):
        return False
    if isinstance(error, aiohttp.ServerDisconnectedError):
        return True
    if isinstance(error, _REMOTE_CLOSE_ERRORS):
        return True
    if isinstance(error.__cause__, _REMOTE_CLOSE_ERRORS):
        return True
    return isinstance(error.__context__, _REMOTE_CLOSE_ERRORS)


class HttpAsyncBackend:
    capabilities = Capabilities(native_async=True, sessions=True)

    def __init__(
        self,
        *,
        url: str,
        headers: dict[str, str],
        client_settings: dict[str, str],
        timeout: aiohttp.ClientTimeout,
        connector_kwargs: dict[str, Any],
        ssl_context: Any,
        proxy_url: str | None,
        server_host_name: str | None,
        token_provider: Callable[[], str | Awaitable[str]] | None,
        autogenerate_query_id: bool,
        read_format: str = "Native",
        form_encode_query_params: bool = False,
    ):
        self.url = url
        self.headers = headers
        self.client_settings = client_settings
        self.timeout = timeout
        self.connector_kwargs = connector_kwargs
        self.ssl_context = ssl_context
        self.proxy_url = proxy_url
        self.server_host_name = server_host_name
        self.token_provider = token_provider
        self.autogenerate_query_id = autogenerate_query_id
        self.read_format = read_format
        self.form_encode_query_params = form_encode_query_params
        self.show_clickhouse_errors = True
        self.compression: str | None = None
        self.send_comp_setting = False
        self.send_progress: bool | None = None
        self.progress_interval: str | None = None
        self.session_lease: SessionLease | None = None
        self.session_lock = asyncio.Lock()
        self._active_session: str | None = None
        self._last_pool_reset: float | None = None

    @property
    def session(self) -> aiohttp.ClientSession | None:
        lease = self.session_lease
        return lease.session if lease is not None else None

    @session.setter
    def session(self, value: aiohttp.ClientSession | None) -> None:
        self.session_lease = SessionLease(value) if value is not None else None

    def _new_session(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(**self.connector_kwargs)
        return aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers=self.headers,
            trust_env=False,
            auto_decompress=False,
            skip_auto_headers={"Accept-Encoding"},
        )

    def ensure_session(self) -> None:
        if not self.session:
            self.session = self._new_session()

    async def resolve_token(self) -> str:
        # Run sync providers off the event loop; await async providers.
        # The provider may be called concurrently if multiple requests get a 516 at the same time;
        # it must be safe to invoke in parallel (e.g. if it hits an IdP, consider rate limiting).
        result = await asyncio.get_running_loop().run_in_executor(None, cast(Callable[[], str | Awaitable[str]], self.token_provider))
        if inspect.isawaitable(result):
            result = await result
        return result

    def set_access_token(self, access_token: str) -> None:
        auth_header = self.headers.get("Authorization")
        if auth_header and not auth_header.startswith("Bearer"):
            raise ProgrammingError("Cannot set access token when a different auth type is used")
        self.headers["Authorization"] = f"Bearer {access_token}"
        if self.session:
            self.session.headers["Authorization"] = f"Bearer {access_token}"

    async def error_handler(self, response: aiohttp.ClientResponse, retried: bool = False):
        """
        Handles HTTP errors. Tries to be robust and provide maximum context.
        """
        try:
            full_body = ""
            try:
                raw_body = await response.read()
                encoding = response.headers.get("Content-Encoding")
                loop = asyncio.get_running_loop()

                def decompress_and_decode():
                    decompressed = decompress_response(raw_body, encoding) if encoding else raw_body
                    return decompressed.decode(errors="backslashreplace")

                full_body = await loop.run_in_executor(None, decompress_and_decode)
            except Exception:
                logger.warning("Failed to read error response body", exc_info=True)
        finally:
            response.close()
        raise build_http_error(
            response.status,
            response.headers.get(ex_header),
            full_body,
            self.show_clickhouse_errors,
            self.url,
            retried,
        ) from None

    async def execute_query(self, context: QueryContext, runtime: QueryRuntime, prepped_query: str | bytes) -> QueryExecution:
        """Execute a query context, returning either a started streaming byte
        source or the column metadata from a columns-only probe."""
        plan = plan_query_request(
            context,
            runtime,
            form_encode_query_params=self.form_encode_query_params,
            compression=self.compression,
            send_comp_setting=self.send_comp_setting,
            read_format=self.read_format,
            prepped_query=prepped_query,
        )
        files = _plan_files(plan)
        if plan.columns_only:
            response = await self.request(plan.body, plan.params, plan.headers, files=files, retries=runtime.retries)
            try:
                body = await response.read()
                encoding = response.headers.get("Content-Encoding")
            finally:
                release_lease(response)
            loop = asyncio.get_running_loop()

            def decompress_and_parse_json():
                decompressed_body = decompress_response(body, encoding) if encoding else body
                return json.loads(decompressed_body)

            json_result = await loop.run_in_executor(None, decompress_and_parse_json)
            return QueryExecution(columns=json_result["meta"])
        response = await self.request(
            plan.body,
            plan.params,
            dict_copy(plan.headers, context.transport_settings),
            files=files,
            server_wait=not context.streaming,
            stream=True,
            retries=runtime.retries,
        )
        source = await start_streaming_response(
            response,
            encoding=response.headers.get("Content-Encoding"),
            exception_tag=response.headers.get(ex_tag_header),
        )
        return QueryExecution(
            source=source,
            summary=summary_from_headers(response.headers),
            response_tz_name=response.headers.get("X-ClickHouse-Timezone"),
        )

    async def execute_data_insert(
        self,
        context: InsertContext,
        runtime: QueryRuntime,
        body: Any,
        retry_body: Callable[[], Awaitable[Any]],
    ) -> dict[str, Any]:
        """Send a built insert payload, returning the response summary."""
        plan = plan_data_insert_request(context, runtime)
        response = await self.request(body, plan.params, headers=plan.headers, server_wait=False, retry_body=retry_body)
        try:
            logger.debug("Context insert response code: %d", response.status)
            return summary_from_headers(response.headers)
        finally:
            response.close()
            release_lease(response)

    async def execute_raw_insert(
        self,
        table: str | None,
        column_names: Sequence[str] | None,
        insert_block: Any,
        fmt: str,
        compression: str | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> dict[str, Any]:
        """Send a raw insert payload, returning the response summary."""
        plan = plan_raw_insert_request(table, column_names, insert_block, fmt, compression, runtime, transport_settings)
        response = await self.request(plan.body, plan.params, plan.headers, server_wait=False)
        try:
            logger.debug("Raw insert response code: %d", response.status)
            return summary_from_headers(response.headers)
        finally:
            response.close()
            release_lease(response)

    async def execute_raw_query(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> bytes:
        """Execute an already-bound raw query, returning the decompressed response body."""
        plan = plan_raw_query_request(final_query, bind_params, external_data, runtime, self.form_encode_query_params, transport_settings)
        response = await self.request(plan.body, plan.params, headers=plan.headers, files=_plan_raw_files(plan), retries=runtime.retries)
        try:
            response_data = await response.read()
            encoding = response.headers.get("Content-Encoding")
        finally:
            release_lease(response)
        if encoding:
            loop = asyncio.get_running_loop()
            response_data = await loop.run_in_executor(None, decompress_response, response_data, encoding)
        return response_data

    async def execute_raw_stream(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> aiohttp.ClientResponse:
        """Execute an already-bound raw query, returning the streaming response."""
        plan = plan_raw_query_request(final_query, bind_params, external_data, runtime, self.form_encode_query_params, transport_settings)
        return await self.request(
            plan.body,
            plan.params,
            headers=plan.headers,
            files=_plan_raw_files(plan),
            stream=True,
            server_wait=False,
            retries=runtime.retries,
        )

    async def execute_command(
        self,
        bound_cmd: str | bytes,
        bind_params: dict[str, str],
        data: str | bytes | None,
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> CommandExecution:
        """Execute an already-bound command, returning its decompressed body and summary."""
        plan = plan_command_request(bound_cmd, bind_params, data, external_data, runtime, transport_settings)
        response = await self.request(plan.payload, plan.params, plan.headers, files=plan.form_files, method=plan.method, server_wait=False)
        try:
            body = await response.read()
            encoding = response.headers.get("Content-Encoding")
            summary = summary_from_headers(response.headers)
            result_format = response.headers.get("X-ClickHouse-Format")
        finally:
            release_lease(response)
        if body and encoding:
            loop = asyncio.get_running_loop()
            body = await loop.run_in_executor(None, decompress_response, body, encoding)
        return CommandExecution(body=body, summary=summary, result_format=result_format)

    async def request(
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
        if self.session is None:
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

        final_params = dict_copy(self.client_settings, params)
        if server_wait:
            final_params.setdefault("wait_end_of_query", "1")
        if self.send_progress:
            final_params.setdefault("send_progress_in_http_headers", "1")
        if self.progress_interval:
            final_params.setdefault("http_headers_progress_interval_ms", self.progress_interval)
        if self.autogenerate_query_id and "query_id" not in final_params:
            final_params["query_id"] = str(uuid.uuid4())

        req_headers = dict_copy(self.headers, headers)
        if self.server_host_name:
            req_headers["Host"] = self.server_host_name
        query_session = final_params.get("session_id")
        attempts = 0
        auth_retried = False

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
            async with self.session_lock:
                lease = self.session_lease
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
                if self.server_host_name and self.ssl_context is not None:
                    request_kwargs["ssl"] = self.ssl_context
                    request_kwargs["server_hostname"] = self.server_host_name
                if self.proxy_url:
                    request_kwargs["proxy"] = self.proxy_url
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
                elif isinstance(data, (bytes, bytearray, memoryview)):
                    request_kwargs["data"] = io.BytesIO(data)
                elif isinstance(data, str):
                    request_kwargs["data"] = io.BytesIO(data.encode())
                else:
                    request_kwargs["data"] = data

                response = await session.request(**request_kwargs)
                if 200 <= response.status < 300 and not response.headers.get(ex_header):
                    # Caller releases lease after consuming the body.
                    response._lease_release = _one_shot(lease.release)  # type: ignore[attr-defined]
                    lease_released = True
                    return response

                if response.status in retryable_http_statuses:
                    if attempts > retries:
                        await self.error_handler(response, retried=True)
                    else:
                        logger.debug("Retrying request with status code %s (attempt %s/%s)", response.status, attempts, retries + 1)
                        await asyncio.sleep(0.1 * attempts)
                        response.close()
                        continue
                if self.token_provider and not auth_retried and response.headers.get(ex_header) == auth_failed_ex_code:
                    if retry_body is None and not (data is None or isinstance(data, (bytes, bytearray, str, dict))):
                        await self.error_handler(response)  # non-replayable body, surface the auth error instead of retrying
                    auth_retried = True
                    self.set_access_token(await self.resolve_token())
                    req_headers["Authorization"] = self.headers["Authorization"]
                    if retry_body is not None:
                        data = await retry_body()
                    logger.debug("Refreshing access token after authentication failure")
                    response.close()
                    continue
                await self.error_handler(response)

            except aiohttp.ClientConnectionError as e:
                msg = str(e)
                if _is_retryable_async_connection_error(e):
                    # Always allow at least one retry on a clean connection error so a single stale
                    # keep-alive socket doesn't surface to the caller, and additionally honor the
                    # retries budget when it is larger (e.g. query_retries for reads), so that
                    # bursts of stale pooled connections can be drained before giving up.
                    max_attempts = max(2, retries + 1)
                    if attempts < max_attempts:
                        if retry_body is not None:
                            data = await retry_body()
                            logger.debug("Retrying after connection error with rebuilt body (attempt %s/%s)", attempts, max_attempts)
                            await asyncio.sleep(0.1 * attempts)
                            continue
                        if data is None or isinstance(data, (bytes, bytearray, str, dict)):
                            logger.debug("Retrying after connection error from remote host (attempt %s/%s)", attempts, max_attempts)
                            await asyncio.sleep(0.1 * attempts)
                            continue
                logger.debug("Non-retryable aiohttp connection error type=%s", type(e).__name__)
                raise OperationalError(f"Network Error: {msg}") from e

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                raise OperationalError(f"Network Error: {str(e)}") from e

            finally:
                if not lease_released:
                    lease.release()
                if query_session:
                    self._active_session = None

    async def ping(self) -> bool:
        async with self.session_lock:
            lease = self.session_lease
            if lease is None or lease.session.closed:
                return False
            session = lease.session
            lease.acquire()
        try:
            url = f"{self.url}/ping"
            timeout = aiohttp.ClientTimeout(total=3.0)
            get_kwargs: dict[str, Any] = {"timeout": timeout}
            if self.proxy_url:
                get_kwargs["proxy"] = self.proxy_url
            if self.server_host_name:
                get_kwargs["headers"] = {"Host": self.server_host_name}
                if self.ssl_context is not None:
                    get_kwargs["ssl"] = self.ssl_context
                    get_kwargs["server_hostname"] = self.server_host_name
            async with session.get(url, **get_kwargs) as response:
                return 200 <= response.status < 300
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.debug("ping failed", exc_info=True)
            return False
        finally:
            lease.release()

    async def close(self) -> None:
        async with self.session_lock:
            old_lease = self.session_lease
            self.session_lease = None
        if old_lease is not None:
            await old_lease.wait_drained()
            await old_lease.session.close()

    async def close_connections(self) -> None:
        """Rotate the connection pool: new requests use a fresh session; in-flight
        requests keep using the old session until they complete, then it's closed."""
        async with self.session_lock:
            old_lease = self.session_lease
            self.session_lease = SessionLease(self._new_session())
        if old_lease is not None:
            await old_lease.wait_drained()
            await old_lease.session.close()


if TYPE_CHECKING:

    def _contract_conformance(backend: HttpAsyncBackend) -> AsyncBackend:
        return backend
