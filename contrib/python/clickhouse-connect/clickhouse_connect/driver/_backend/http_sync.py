"""Synchronous HTTP transport backend built on urllib3.

Owns request execution, retry and auth-refresh policy, pool lifecycle, ping,
and HTTP error handling. During the 1.x transition the headers and params
dicts are shared by reference with the HttpClient facade, which must mutate
them in place rather than rebinding.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlencode

from urllib3 import Timeout
from urllib3.exceptions import HTTPError
from urllib3.poolmanager import PoolManager
from urllib3.response import HTTPResponse

from clickhouse_connect.driver._backend.httpcommon import (
    auth_failed_ex_code,
    build_http_error,
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
from clickhouse_connect.driver.httputil import ResponseSource, all_managers, check_conn_expiration, get_response_data

if TYPE_CHECKING:
    from clickhouse_connect.driver._backend.contracts import SyncBackend
    from clickhouse_connect.driver._backend.httpcommon import QueryRequestPlan
    from clickhouse_connect.driver.external import ExternalData
    from clickhouse_connect.driver.insert import InsertContext
    from clickhouse_connect.driver.query import QueryContext

logger = logging.getLogger(__name__)

_REMOTE_CLOSE_ERRORS = (ConnectionResetError, BrokenPipeError)


def _plan_fields(plan: QueryRequestPlan) -> dict[str, Any] | None:
    """Merge a plan's form parts into urllib3 fields: plain values first, then files."""
    if plan.form_values is None and plan.form_files is None:
        return None
    fields: dict[str, Any] = {}
    if plan.form_values:
        fields.update(plan.form_values)
    if plan.form_files:
        fields.update(plan.form_files)
    return fields


class HttpSyncBackend:
    capabilities = Capabilities(native_async=False, sessions=True)

    def __init__(
        self,
        *,
        url: str,
        pool_manager: PoolManager,
        owns_pool_manager: bool,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: Timeout,
        server_host_name: str | None,
        token_provider: Callable[[], str] | None,
        autogenerate_query_id: bool,
        http_retries: int = 1,
        read_format: str = "Native",
        form_encode_query_params: bool = False,
    ):
        self.url = url
        self.http = pool_manager
        self.owns_pool_manager = owns_pool_manager
        self.headers = headers
        self.params = params
        self.timeout = timeout
        self.server_host_name = server_host_name
        self.token_provider = token_provider
        self.autogenerate_query_id = autogenerate_query_id
        self.http_retries = http_retries
        self.read_format = read_format
        self.form_encode_query_params = form_encode_query_params
        self.show_clickhouse_errors = True
        self.compression: str | None = None
        self.send_comp_setting = False
        self.send_progress: bool | None = None
        self.progress_interval: str | None = None
        self._active_session: str | None = None

    def set_access_token(self, access_token: str) -> None:
        auth_header = self.headers.get("Authorization")
        if auth_header and not auth_header.startswith("Bearer"):
            raise ProgrammingError("Cannot set access token when a different auth type is used")
        self.headers["Authorization"] = f"Bearer {access_token}"

    def error_handler(self, response: HTTPResponse, retried: bool = False) -> None:
        """
        Handles HTTP errors. Tries to be robust and provide maximum context.
        """
        try:
            full_body = ""
            try:
                full_body = get_response_data(response).decode(errors="backslashreplace")
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

    def execute_query(self, context: QueryContext, runtime: QueryRuntime, prepped_query: str | bytes) -> QueryExecution:
        """Execute a query context, returning either a streaming byte source or
        the column metadata from a columns-only probe."""
        plan = plan_query_request(
            context,
            runtime,
            form_encode_query_params=self.form_encode_query_params,
            compression=self.compression,
            send_comp_setting=self.send_comp_setting,
            read_format=self.read_format,
            prepped_query=prepped_query,
        )
        if plan.columns_only:
            response = self.request(
                plan.body if plan.body is not None else b"",
                plan.params,
                plan.headers,
                retries=runtime.retries,
                fields=_plan_fields(plan),
            )
            return QueryExecution(columns=json.loads(response.data)["meta"])
        response = self.request(
            plan.body if plan.body is not None else b"",
            plan.params,
            dict_copy(plan.headers, context.transport_settings),
            stream=True,
            retries=runtime.retries,
            fields=_plan_fields(plan),
            server_wait=not context.streaming,
        )
        return QueryExecution(
            source=ResponseSource(response, exception_tag=response.headers.get(ex_tag_header)),
            summary=summary_from_headers(response.headers),
            response_tz_name=response.headers.get("X-ClickHouse-Timezone"),
        )

    def execute_data_insert(
        self,
        context: InsertContext,
        runtime: QueryRuntime,
        body: Any,
        retry_body: Callable[[], Any],
    ) -> dict[str, Any]:
        """Send a built insert payload, returning the response summary."""
        plan = plan_data_insert_request(context, runtime)

        def error_handler(response: HTTPResponse) -> None:
            # If we actually had a local exception when building the insert, throw that instead
            if context.insert_exception:
                ex = context.insert_exception
                context.insert_exception = None
                raise ex
            self.error_handler(response)

        response = self.request(
            body,
            plan.params,
            plan.headers,
            error_handler=error_handler,
            server_wait=False,
            retry_body=retry_body,
        )
        logger.debug("Context insert response code: %d, content: %s", response.status, response.data)
        return summary_from_headers(response.headers)

    def execute_raw_insert(
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
        response = self.request(plan.body, plan.params, plan.headers, server_wait=False)
        logger.debug("Raw insert response code: %d, content: %s", response.status, response.data)
        return summary_from_headers(response.headers)

    def execute_raw_query(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> bytes:
        """Execute an already-bound raw query, returning the response body."""
        plan = plan_raw_query_request(final_query, bind_params, external_data, runtime, self.form_encode_query_params, transport_settings)
        response = self.request(
            plan.body if plan.body is not None else b"",
            plan.params,
            plan.headers,
            fields=_plan_fields(plan),
            retries=runtime.retries,
        )
        return response.data

    def execute_raw_stream(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> HTTPResponse:
        """Execute an already-bound raw query, returning the streaming response."""
        plan = plan_raw_query_request(final_query, bind_params, external_data, runtime, self.form_encode_query_params, transport_settings)
        return self.request(
            plan.body if plan.body is not None else b"",
            plan.params,
            plan.headers,
            fields=_plan_fields(plan),
            stream=True,
            server_wait=False,
            retries=runtime.retries,
        )

    def execute_command(
        self,
        bound_cmd: str | bytes,
        bind_params: dict[str, str],
        data: str | bytes | None,
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> CommandExecution:
        """Execute an already-bound command, returning its body and summary."""
        plan = plan_command_request(bound_cmd, bind_params, data, external_data, runtime, transport_settings)
        response = self.request(plan.payload, plan.params, plan.headers, plan.method, fields=plan.form_files, server_wait=False)
        return CommandExecution(
            body=response.data or b"",
            summary=summary_from_headers(response.headers),
            result_format=response.headers.get("X-ClickHouse-Format"),
        )

    def request(
        self,
        data,
        params: dict[str, str],
        headers: dict[str, Any] | None = None,
        method: str = "POST",
        retries: int = 0,
        stream: bool = False,
        server_wait: bool = True,
        fields: dict[str, tuple] | None = None,
        error_handler: Callable | None = None,
        retry_body: Callable[[], Any] | None = None,
    ) -> HTTPResponse:
        if isinstance(data, str):
            data = data.encode()
        headers = dict_copy(self.headers, headers)
        attempts = 0
        auth_retried = False
        final_params = {}
        if server_wait:
            final_params["wait_end_of_query"] = "1"
        # We can't actually read the progress headers, but we enable them so ClickHouse sends something
        # to keep the connection alive when waiting for long-running queries and (2) to get summary information
        # if not streaming
        if self.send_progress:
            final_params["send_progress_in_http_headers"] = "1"
        if self.progress_interval:
            final_params["http_headers_progress_interval_ms"] = self.progress_interval
        final_params = dict_copy(self.params, final_params)
        final_params = dict_copy(final_params, params)

        if self.autogenerate_query_id and "query_id" not in final_params:
            final_params["query_id"] = str(uuid.uuid4())

        url = f"{self.url}?{urlencode(final_params)}"
        kwargs: dict[str, Any] = {"headers": headers, "timeout": self.timeout, "retries": self.http_retries, "preload_content": not stream}
        if self.server_host_name:
            kwargs["assert_same_host"] = False
            kwargs["headers"].update({"Host": self.server_host_name})
        if fields:
            kwargs["fields"] = fields
        else:
            kwargs["body"] = data
        check_conn_expiration(cast(PoolManager, self.http))
        query_session = final_params.get("session_id")
        while True:
            attempts += 1
            if query_session:
                if query_session == self._active_session:
                    raise ProgrammingError(
                        "Attempt to execute concurrent queries within the same session. "
                        + "Please use a separate client instance per thread/process."
                    )
                # There is a race condition here when using multiprocessing -- in that case the server will
                # throw an error instead, but in most cases this more helpful error will be thrown first
                self._active_session = query_session
            try:
                response: HTTPResponse = cast(HTTPResponse, cast(PoolManager, self.http).request(method, url, **kwargs))
            except HTTPError as ex:
                # Always allow at least one retry on a clean connection error so a single stale
                # keep-alive socket doesn't surface to the caller, and additionally honor the
                # retries budget when it is larger (e.g. query_retries for reads), so that
                # bursts of stale pooled connections can be drained before giving up.
                max_attempts = max(2, retries + 1)
                remote_close = isinstance(ex.__context__, _REMOTE_CLOSE_ERRORS) or isinstance(ex.__cause__, _REMOTE_CLOSE_ERRORS)
                if remote_close and attempts < max_attempts:
                    # The server closed the connection, probably because the Keep Alive has expired.
                    # We should be safe to retry, as ClickHouse should not have processed anything on
                    # a connection that it killed.
                    body = kwargs.get("body")
                    if retry_body is not None:
                        kwargs["body"] = retry_body()
                        logger.debug("Retrying remotely closed connection with rebuilt body (attempt %s/%s)", attempts, max_attempts)
                        time.sleep(0.1 * attempts)
                        continue
                    if body is None or isinstance(body, (bytes, bytearray, str)):
                        logger.debug("Retrying remotely closed connection (attempt %s/%s)", attempts, max_attempts)
                        time.sleep(0.1 * attempts)
                        continue
                logger.debug("Non-retryable HTTP transport error type=%s", type(ex).__name__)
                logger.warning("Unexpected Http Driver Exception")
                err_url = f" ({self.url})" if self.show_clickhouse_errors else ""
                raise OperationalError(f"Error {ex} executing HTTP request attempt {attempts}{err_url}") from ex
            finally:
                if query_session:
                    self._active_session = None  # Make sure we always clear this
            if 200 <= response.status < 300 and not response.headers.get(ex_header):
                return response
            if response.status in retryable_http_statuses:
                if attempts > retries:
                    self.error_handler(response, True)
                logger.debug("Retrying requests with status code %d", response.status)
            elif self.token_provider and not auth_retried and response.headers.get(ex_header) == auth_failed_ex_code:
                body = kwargs.get("body")
                if retry_body is None and not (body is None or isinstance(body, (bytes, bytearray, str))):
                    self.error_handler(response)  # non-replayable body, surface the auth error instead of retrying
                auth_retried = True
                self.set_access_token(self.token_provider())
                headers["Authorization"] = self.headers["Authorization"]
                if retry_body is not None:
                    kwargs["body"] = retry_body()
                response.close()
                logger.debug("Refreshing access token after authentication failure")
            elif error_handler is not None:
                error_handler(response)
            else:
                self.error_handler(response)

    def ping(self) -> bool:
        try:
            headers = dict_copy(self.headers)
            kwargs: dict[str, Any] = {"headers": headers, "timeout": 3, "preload_content": True}
            if self.server_host_name:
                kwargs["assert_same_host"] = False
                headers["Host"] = self.server_host_name
            response = cast(PoolManager, self.http).request("GET", f"{self.url}/ping", **kwargs)
            return 200 <= response.status < 300
        except HTTPError:
            logger.debug("ping failed", exc_info=True)
            return False

    def close_connections(self) -> None:
        cast(PoolManager, self.http).clear()

    def close(self) -> None:
        if self.owns_pool_manager:
            cast(PoolManager, self.http).clear()
            all_managers.pop(cast(PoolManager, self.http), None)


if TYPE_CHECKING:

    def _contract_conformance(backend: HttpSyncBackend) -> SyncBackend:
        return backend
