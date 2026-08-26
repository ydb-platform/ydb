"""HTTP semantics shared by the sync (urllib3) and async (aiohttp) transports.

Everything here is transport-library neutral: pure functions over response
headers, bodies, and client configuration that were previously duplicated
between httpclient.py and asyncclient.py.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import version as dist_version
from typing import TYPE_CHECKING, Any, Protocol

import lz4.frame

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client
    from clickhouse_connect.driver.external import ExternalData
    from clickhouse_connect.driver.insert import InsertContext
    from clickhouse_connect.driver.query import QueryContext

from clickhouse_connect import common
from clickhouse_connect.driver._backend.models import QueryRuntime
from clickhouse_connect.driver.binding import quote_identifier, use_form_encoding
from clickhouse_connect.driver.common import coerce_bool, dict_copy
from clickhouse_connect.driver.compression import _zstd_decompress, available_compression
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    OperationalError,
    ProgrammingError,
    error_code_from_header,
    error_name_from_body,
)

logger = logging.getLogger(__name__)

ex_header = "X-ClickHouse-Exception-Code"
ex_tag_header = "X-ClickHouse-Exception-Tag"
auth_failed_ex_code = "516"  # ClickHouse AUTHENTICATION_FAILED
retryable_http_statuses = (429, 503, 504)

columns_only_re = re.compile(r"LIMIT 0\s*$", re.IGNORECASE)

if "br" in available_compression:
    import brotli
else:
    brotli = None


def summary_from_headers(headers: Mapping[str, str]) -> dict[str, Any]:
    """Extract the query summary from ClickHouse response headers."""
    summary = {}
    if "X-ClickHouse-Summary" in headers:
        try:
            summary = json.loads(headers["X-ClickHouse-Summary"])
        except json.JSONDecodeError:
            pass
    summary["query_id"] = headers.get("X-ClickHouse-Query-Id", "")
    return summary


def build_http_error(
    status: int,
    err_code: str | None,
    full_body: str,
    show_clickhouse_errors: bool,
    url: str,
    retried: bool,
) -> DatabaseError:
    """Build the exception for a failed HTTP response from its already-read body."""
    code = error_code_from_header(err_code)
    name = error_name_from_body(full_body) if show_clickhouse_errors else None
    body = ""
    try:
        body = common.format_error(full_body).strip()
    except Exception:
        logger.warning("Failed to format error response body", exc_info=True)

    if show_clickhouse_errors:
        if err_code:
            err_str = f"Received ClickHouse exception, code: {err_code}"
        else:
            err_str = f"HTTP driver received HTTP status {status}"
        if body:
            err_str = f"{err_str}, server response: {body}"
    else:
        err_str = "The ClickHouse server returned an error"

    err_str = f"{err_str} (for url {url})"
    err_type = OperationalError if retried else DatabaseError
    return err_type(err_str, code=code, name=name)


def parse_command_body(body: bytes) -> str | int | Sequence[str]:
    """Convert a non-empty command response body to the command return value."""
    try:
        result = body.decode()[:-1].split("\t")
        if len(result) == 1:
            try:
                return int(result[0])
            except ValueError:
                return result[0]
        return result
    except UnicodeDecodeError:
        return str(body)


def negotiate_compression(compress: bool | str) -> tuple[str | None, str | None]:
    """Resolve the compress constructor param to (accept_encoding, write_compression)."""
    if coerce_bool(compress):
        return ",".join(available_compression), available_compression[0]
    if compress and compress not in ("False", "false", "0"):
        if compress not in available_compression:
            raise ProgrammingError(f"Unsupported compression method {compress}")
        return compress, compress
    return None, None


def decompress_response(data: bytes, encoding: str | None) -> bytes:
    """Decompress a fully-read response body based on its Content-Encoding header."""
    if not encoding or encoding == "identity":
        return data

    if encoding == "lz4":
        lz4_decom = lz4.frame.LZ4FrameDecompressor()
        return lz4_decom.decompress(data, len(data))
    if encoding == "zstd":
        return _zstd_decompress(data)
    if encoding == "br":
        if brotli is not None:
            return brotli.decompress(data)
        raise OperationalError("Brotli compression requested but not installed.")
    if encoding == "gzip":
        return gzip.decompress(data)
    if encoding == "deflate":
        return zlib.decompress(data)
    raise OperationalError(f"Unsupported compression type: '{encoding}'. Supported compression: {', '.join(available_compression)}")


def embed_insert_query(
    table: str, column_names: Sequence[str] | None, fmt: str, compression: str | None, insert_block: Any
) -> tuple[Any, str | None]:
    """Combine a raw insert query with its data block.

    Returns (body, query_param). String and bytes blocks get the INSERT
    statement prepended; generators, file-like objects, and compressed data
    keep the statement as a URL parameter and stream the body as-is.
    """
    cols = f" ({', '.join([quote_identifier(x) for x in column_names])})" if column_names is not None else ""
    query = f"INSERT INTO {table}{cols} FORMAT {fmt}"
    if not compression and isinstance(insert_block, str):
        return query + "\n" + insert_block, None
    if not compression and isinstance(insert_block, (bytes, bytearray)):
        return (query + "\n").encode() + insert_block, None
    return insert_block, query


class HttpTransportState(Protocol):
    """Transport slots for server-negotiated HTTP behavior."""

    compression: str | None
    send_comp_setting: bool
    send_progress: bool | None
    progress_interval: str | None


def apply_http_server_settings(client: Client, transport: HttpTransportState, compression: str | None, send_receive_timeout: int) -> None:
    """Apply HTTP-specific client setting defaults after server settings discovery.

    Sets the readonly-query cancel default (unless user-supplied), response
    compression, and the progress-header keep-alive parameters.
    """
    cancel_setting = client._setting_status("cancel_http_readonly_queries_on_client_close")
    if (
        cancel_setting.is_writable
        and not cancel_setting.is_set
        and "cancel_http_readonly_queries_on_client_close" not in (client._initial_settings or {})
    ):
        client.set_client_setting("cancel_http_readonly_queries_on_client_close", "1")
    comp_setting = client._setting_status("enable_http_compression")
    transport.send_comp_setting = not comp_setting.is_set and comp_setting.is_writable
    if comp_setting.is_set or comp_setting.is_writable:
        transport.compression = compression
    send_setting = client._setting_status("send_progress_in_http_headers")
    transport.send_progress = not send_setting.is_set and send_setting.is_writable
    if (send_setting.is_set or send_setting.is_writable) and client._setting_status("http_headers_progress_interval_ms").is_writable:
        transport.progress_interval = str(min(120000, max(10000, (send_receive_timeout - 5) * 1000)))


@dataclass
class QueryRequestPlan:
    """A shaped HTTP query request, ready for a transport to send.

    form_values holds plain text form fields (query and bind parameters);
    form_files holds external-data file fields. Transports merge the two in
    their historical part order. body applies only when both are None.
    """

    columns_only: bool
    params: dict[str, str]
    headers: dict[str, Any]
    body: str | bytes | None = None
    form_values: dict[str, Any] | None = None
    form_files: dict[str, Any] | None = None


def plan_query_request(
    context: QueryContext,
    runtime: QueryRuntime,
    *,
    form_encode_query_params: bool,
    compression: str | None,
    send_comp_setting: bool,
    read_format: str,
    prepped_query: str | bytes,
) -> QueryRequestPlan:
    """Shape a QueryContext into an HTTP request plan.

    Columns-only (LIMIT 0) probes are planned as FORMAT JSON metadata
    requests built from context.final_query; prepped_query (the limit-applied
    query) is used only on the non-probe path, where the read format is
    appended and the response streams.
    """
    params: dict[str, str] = {}
    if runtime.database:
        params["database"] = runtime.database
    if runtime.protocol_version:
        params["client_protocol_version"] = str(runtime.protocol_version)
    params.update(runtime.settings)
    headers: dict[str, Any] = {}
    use_form = use_form_encoding(context.final_query, context.bind_params, form_encode_query_params)

    if not context.is_insert and columns_only_re.search(context.uncommented_query):
        fmt_json_query = f"{context.final_query}\n FORMAT JSON"
        if use_form:
            form_values: dict[str, Any] = {"query": fmt_json_query}
            form_values.update(context.bind_params)
            form_files: dict[str, Any] = {}
            if context.external_data:
                params.update(context.external_data.query_params)
                form_files = context.external_data.form_data
            return QueryRequestPlan(True, params, headers, form_values=form_values, form_files=form_files)
        if context.external_data:
            params.update(context.bind_params)
            params.update(context.external_data.query_params)
            params["query"] = fmt_json_query
            return QueryRequestPlan(True, params, headers, form_files=context.external_data.form_data)
        params.update(context.bind_params)
        return QueryRequestPlan(True, params, headers, body=fmt_json_query)

    if compression:
        headers["Accept-Encoding"] = compression
        if send_comp_setting:
            params["enable_http_compression"] = "1"
    final_query: Any = prepped_query
    if not context.is_insert:
        fmt = f"\n FORMAT {read_format}"
        final_query = prepped_query + fmt.encode() if isinstance(prepped_query, bytes) else prepped_query + fmt
    if use_form:
        form_values = {"query": final_query}
        form_values.update(context.bind_params)
        form_files = {}
        if context.external_data:
            params.update(context.external_data.query_params)
            form_files = context.external_data.form_data
        return QueryRequestPlan(False, params, headers, form_values=form_values, form_files=form_files)
    if context.external_data:
        params.update(context.bind_params)
        params["query"] = final_query
        params.update(context.external_data.query_params)
        return QueryRequestPlan(False, params, headers, form_files=context.external_data.form_data)
    params.update(context.bind_params)
    headers["Content-Type"] = "text/plain; charset=utf-8"
    return QueryRequestPlan(False, params, headers, body=final_query)


def plan_raw_query_request(
    final_query: str | bytes,
    bind_params: dict[str, str],
    external_data: ExternalData | None,
    runtime: QueryRuntime,
    form_encode_query_params: bool,
    transport_settings: dict[str, str] | None,
) -> QueryRequestPlan:
    """Shape an already-bound raw query into an HTTP request plan.

    Unlike plan_query_request, raw queries carry no probe, compression, or
    FORMAT handling, and settings precede the database in the params order.
    """
    params: dict[str, str] = dict(runtime.settings)
    if runtime.database:
        params["database"] = runtime.database
    headers: dict[str, Any] = dict_copy(transport_settings or {})
    use_form = use_form_encoding(final_query, bind_params, form_encode_query_params)
    if external_data and not use_form and isinstance(final_query, bytes):
        raise ProgrammingError("Binary query cannot be placed in URL when using External Data; enable form encoding.")
    if use_form:
        form_values: dict[str, Any] = {"query": final_query}
        form_values.update(bind_params)
        form_files: dict[str, Any] = {}
        if external_data:
            params.update(external_data.query_params)
            form_files = external_data.form_data
        return QueryRequestPlan(False, params, headers, form_values=form_values, form_files=form_files)
    if external_data:
        params.update(bind_params)
        assert isinstance(final_query, str)  # the guard above rejects bytes
        params["query"] = final_query
        params.update(external_data.query_params)
        return QueryRequestPlan(False, params, headers, form_files=external_data.form_data)
    params.update(bind_params)
    return QueryRequestPlan(False, params, headers, body=final_query)


@dataclass
class InsertRequestPlan:
    """A shaped HTTP insert request. body is set only by the raw-insert
    planner; context inserts stream a transport-built body instead."""

    params: dict[str, str]
    headers: dict[str, Any]
    body: Any = None


def plan_data_insert_request(context: InsertContext, runtime: QueryRuntime) -> InsertRequestPlan:
    """Shape an InsertContext into an HTTP request plan. The insert payload
    itself is built and streamed by the transport."""
    headers: dict[str, Any] = {"Content-Type": "application/octet-stream"}
    if isinstance(context.compression, str):
        headers["Content-Encoding"] = context.compression
    params: dict[str, str] = {}
    if runtime.database:
        params["database"] = runtime.database
    params.update(runtime.settings)
    headers = dict_copy(headers, context.transport_settings)
    return InsertRequestPlan(params, headers)


def plan_raw_insert_request(
    table: str | None,
    column_names: Sequence[str] | None,
    insert_block: Any,
    fmt: str,
    compression: str | None,
    runtime: QueryRuntime,
    transport_settings: dict[str, str] | None,
) -> InsertRequestPlan:
    """Shape a raw insert into an HTTP request plan, embedding the INSERT
    statement into the body or the query URL parameter per block type."""
    params: dict[str, str] = {}
    headers: dict[str, Any] = {"Content-Type": "application/octet-stream"}
    if compression:
        headers["Content-Encoding"] = compression
    body = insert_block
    if table:
        body, query_param = embed_insert_query(table, column_names, fmt, compression, insert_block)
        if query_param:
            params["query"] = query_param
    if runtime.database:
        params["database"] = runtime.database
    params.update(runtime.settings)
    headers = dict_copy(headers, transport_settings)
    return InsertRequestPlan(params, headers, body)


@dataclass
class CommandRequestPlan:
    """A shaped HTTP command request, ready for a transport to send.

    payload is the request body (the bound command itself, or user data with
    the command moved to the query URL parameter); form_files holds
    external-data file fields.
    """

    params: dict[str, str]
    headers: dict[str, Any]
    method: str
    payload: str | bytes | None = None
    form_files: dict[str, Any] | None = None


def plan_command_request(
    bound_cmd: str | bytes,
    bind_params: dict[str, str],
    data: str | bytes | None,
    external_data: ExternalData | None,
    runtime: QueryRuntime,
    transport_settings: dict[str, str] | None,
) -> CommandRequestPlan:
    """Shape an already-bound command into an HTTP request plan."""
    params = dict(bind_params)
    headers: dict[str, Any] = {}
    payload: str | bytes | None = None
    form_files = None
    if external_data:
        if data:
            raise ProgrammingError("Cannot combine command data with external data") from None
        form_files = external_data.form_data
        params.update(external_data.query_params)
    elif isinstance(data, str):
        headers["Content-Type"] = "text/plain; charset=utf-8"
        payload = data.encode()
    elif isinstance(data, bytes):
        headers["Content-Type"] = "application/octet-stream"
        payload = data
    if payload is None and not bound_cmd:
        raise ProgrammingError("Command sent without query or recognized data") from None
    if payload or form_files:
        if isinstance(bound_cmd, bytes):
            raise ProgrammingError("Binary parameter bind cannot be combined with command data or external data") from None
        params["query"] = bound_cmd
    else:
        payload = bound_cmd
    if runtime.database:
        params["database"] = runtime.database
    params.update(runtime.settings)
    headers = dict_copy(headers, transport_settings)
    method = "POST" if payload or form_files else "GET"
    return CommandRequestPlan(params, headers, method, payload=payload, form_files=form_files)


def add_integration_tag(headers: dict[str, str], reported_libs: set[str], name: str) -> str | None:
    """Add a product (like pandas or sqlalchemy) to the User-Agent details section.

    Mutates headers in place and returns the new User-Agent string when it changed.
    """
    if not common.get_setting("send_integration_tags") or name in reported_libs:
        return None

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

        ua = headers.get("User-Agent", "")
        start = ua.find("(")
        if start == -1:
            return None
        end = ua.find(")", start + 1)
        if end == -1:
            return None

        details = ua[start + 1 : end].strip()

        if product_info in details:
            reported_libs.add(name)
            return None

        new_details = f"{product_info}; {details}" if details else product_info
        new_ua = f"{ua[: start + 1]}{new_details}{ua[end:]}".strip()
        headers["User-Agent"] = new_ua

        reported_libs.add(name)
        logger.debug("Added '%s' to User-Agent", product_info)
        return new_ua

    except Exception as e:
        logger.debug("Problem adding '%s' to User-Agent: %s", name, e)
        return None
