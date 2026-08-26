"""In-process chDB execution backend.

Implements the `SyncBackend` contract against chdb's embedded engine: queries
run through `chdb.Connection.query`/`send_query` in Native format and stream
into the shared codec exactly like HTTP response bytes, and inserts write the
built Native payload to a temp file ingested with `INSERT ... FROM INFILE`.

Each backend owns its own `chdb.Connection` handle. chdb allows any number of
handles to the one engine a process can host, and enforces the single engine
path itself at connect time. Session state (`USE`, `SET`) is per handle, so
clients stay isolated from each other the way HTTP clients are. A per-client
lock still serializes calls on the handle: a query issued on a handle whose
stream is open silently returns an empty result, and the `SET` apply/restore
dance must not interleave with another thread's call on the same client.

Per-call settings ride as a `SETTINGS` clause when the backend controls where
the `FORMAT` clause lands (selects, inserts). The command, raw, and
insert-through-query paths cannot take a trailing clause, so their settings
are applied with `SET` and restored afterwards in a single lock hold.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import tempfile
import threading
import weakref
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import chdb

from clickhouse_connect.driver._backend.httpcommon import columns_only_re
from clickhouse_connect.driver._backend.models import Capabilities, CommandExecution, QueryExecution, QueryRuntime
from clickhouse_connect.driver.binding import quote_identifier
from clickhouse_connect.driver.exceptions import (
    DatabaseError,
    NotSupportedError,
    ProgrammingError,
    StreamFailureError,
    error_name_from_body,
)

if TYPE_CHECKING:
    from clickhouse_connect.driver._backend.contracts import SyncBackend
    from clickhouse_connect.driver.external import ExternalData
    from clickhouse_connect.driver.insert import InsertContext
    from clickhouse_connect.driver.query import QueryContext

logger = logging.getLogger(__name__)

# Settings the HTTP transport consumes itself. They pass client-side setting
# validation for drop-in compatibility but must never reach the chdb engine,
# which would reject them as unknown settings.
CHDB_TRANSPORT_SETTINGS = frozenset(
    {
        "database",
        "buffer_size",
        "session_id",
        "session_timeout",
        "session_check",
        "query_id",
        "quota_key",
        "compress",
        "decompress",
        "wait_end_of_query",
        "client_protocol_version",
        "role",
        "send_progress_in_http_headers",
        "http_headers_progress_interval_ms",
        "enable_http_compression",
    }
)

_SETTING_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# chdb's send_query emits each ClickHouse block as a self-contained encoding in
# the requested format. Concatenated chunks form a valid stream only for
# formats without a global header/footer; anything else (Arrow, Parquet, JSON,
# *WithNames) is materialized with a single non-streaming query instead.
_STREAM_SAFE_FORMATS = frozenset({"Native", "TabSeparated", "TSV", "CSV", "RowBinary", "JSONEachRow"})

# The facade appends "\n FORMAT <fmt>" to raw queries; commands and internal
# probes may also end with an explicit FORMAT clause.
_TRAILING_FORMAT_RE = re.compile(rb"\bFORMAT\s+(\w+)\s*;?\s*$", re.IGNORECASE)

_STREAM_OPEN_MESSAGE = (
    "The chdb connection is streaming a query result on this thread. Close or fully consume the stream before another operation."
)

_ERROR_CODE_RE = re.compile(r"\bCode:\s*(\d+)")


def _quote_sql_string(text: str) -> str:
    """Single-quote a string literal (e.g. an INFILE path, which can contain
    apostrophes on macOS TMPDIRs), escaping backslashes and quotes."""
    escaped = text.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _validate_setting_name(key: str) -> str:
    """Reject setting names that are not ClickHouse identifiers. Names are
    interpolated into SETTINGS clauses and SET statements, and the permissive
    invalid_setting_action lets arbitrary keys through client validation."""
    if not isinstance(key, str) or not _SETTING_NAME_RE.match(key):
        raise ProgrammingError(f"Invalid setting name {key!r}: must match {_SETTING_NAME_RE.pattern}")
    return key


def _quote_setting_value(value: str) -> str:
    """SQL-quote a setting value. Bare numeric-looking strings parse as UInt64
    and break String-typed settings; ClickHouse coerces quoted literals back to
    numeric types where needed, so quoting unconditionally is safe."""
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _settings_clause(settings: Mapping[str, str]) -> str:
    return ", ".join(f"{_validate_setting_name(k)} = {_quote_setting_value(v)}" for k, v in settings.items())


def _format_error_message(message: str) -> str:
    """Extract the ClickHouse exception message from a chdb error string."""
    if not message:
        return ""
    idx = message.find("Code: ")
    if idx > 0:
        return message[idx:].strip()
    return message.strip()


def _strip_param_prefix(bind_params: Mapping[str, Any] | None) -> dict[str, Any]:
    """chdb's params kwarg expects bare names; bind_query produces param_x keys."""
    if not bind_params:
        return {}
    return {(k[6:] if k.startswith("param_") else k): v for k, v in bind_params.items()}


def _trailing_format(sql: str | bytes) -> str | None:
    raw = sql if isinstance(sql, bytes) else sql.encode()
    match = _TRAILING_FORMAT_RE.search(raw)
    return match.group(1).decode() if match else None


def _write_block_to_file(block: Any, file: Any) -> None:
    """Write any supported insert_block shape to an open binary file."""
    if isinstance(block, (bytes, bytearray, memoryview)):
        file.write(block)
    elif isinstance(block, str):
        file.write(block.encode())
    elif hasattr(block, "to_pybytes"):
        file.write(block.to_pybytes())
    elif hasattr(block, "read"):
        while True:
            chunk = block.read(1024 * 1024)
            if not chunk:
                break
            file.write(chunk)
    else:
        for chunk in block:
            file.write(chunk if isinstance(chunk, (bytes, bytearray)) else chunk.encode())


def _decompress(data: bytes, encoding: str) -> bytes:
    if encoding == "lz4":
        import lz4.frame

        return lz4.frame.decompress(data)
    if encoding == "zstd":
        from clickhouse_connect.driver.compression import _zstd_decompress

        return _zstd_decompress(data)
    if encoding == "gzip":
        import gzip

        return gzip.decompress(data)
    if encoding == "deflate":
        import zlib

        return zlib.decompress(data)
    if encoding == "br":
        try:
            import brotli
        except ImportError as ex:
            raise NotSupportedError("brotli is required to decompress 'br' for a chdb raw insert") from ex
        return brotli.decompress(data)
    raise NotSupportedError(f"Unsupported compression {encoding!r} for a chdb raw insert")


class _EngineHandle:
    """Per-client engine state: the chdb connection handle, the lock
    serializing calls on it, the handle-level USE state, the thread currently
    holding the lock for an open stream, and a deferred-close flag set when
    _close_handle could not take the lock."""

    __slots__ = ("conn", "lock", "active_database", "stream_owner", "close_pending")

    def __init__(self, conn: Any):
        self.conn = conn
        self.lock = threading.Lock()
        self.active_database: str | None = None
        self.stream_owner: int | None = None
        self.close_pending = False


def _open_handle(conn_str: str) -> _EngineHandle:
    """Open a dedicated chdb connection handle. chdb allows any number of
    handles to one engine but only one engine path per process; it enforces
    that itself, so a conflicting path surfaces as a RuntimeError here."""
    try:
        return _EngineHandle(chdb.connect(conn_str))
    except RuntimeError as ex:
        raise ProgrammingError(f"Unable to open the chdb engine at {conn_str!r}: {ex}") from ex


def _close_handle(handle: _EngineHandle, blocking: bool = False) -> None:
    """Close a handle's chdb connection. An unclosed handle keeps the engine
    alive, and chdb rejects a different path while any handle is open, so a
    handle that outlives its backend must still get closed. The explicit
    close() path blocks (its streams are already closed, so it only waits out
    an in-flight cross-thread call); the weakref.finalize leak path must not
    block, so when a stream still holds the lock the close is deferred via
    close_pending and _finalize_stream retries it."""
    handle.close_pending = True
    if not handle.lock.acquire(blocking=blocking):
        logger.debug("Deferring chdb connection close: its handle lock is still held")
        return
    try:
        if handle.conn is not None:
            try:
                handle.conn.close()
            except Exception:
                logger.debug("Error closing chdb connection", exc_info=True)
            handle.conn = None
        handle.close_pending = False
    finally:
        handle.lock.release()


def _finalize_stream(streaming_result: Any, handle: _EngineHandle, released_box: list[bool]) -> None:
    """Idempotently close a chdb StreamingResult and release the handle lock.
    Registered as a weakref.finalize so a stream that is GC'd without being
    closed still releases the lock; a leaked lock would deadlock every later
    call on the client's connection."""
    if released_box[0]:
        return
    released_box[0] = True
    try:
        close = getattr(streaming_result, "close", None)
        if close is not None:
            close()
    except Exception:
        logger.debug("Error closing chdb StreamingResult during finalize", exc_info=True)
    handle.stream_owner = None
    try:
        handle.lock.release()
    except RuntimeError:
        pass
    if handle.close_pending:
        # The backend was closed or leaked while this stream held the lock
        _close_handle(handle)


class _BytesSource:
    """Stand-in for the HTTP ResponseSource: a single-chunk byte source with
    the attributes the response buffer and transform layer read."""

    __slots__ = ("data", "last_message", "exception_tag")

    def __init__(self, data: bytes):
        self.data = data
        self.last_message: bytes | None = None
        self.exception_tag: str | None = None

    @property
    def gen(self):
        def _gen():
            yield self.data

        return _gen()

    def close(self) -> None:
        return None


class _ChdbStreamSource:
    """Response-buffer source backed by a chdb StreamingResult. Yields each
    block's bytes and surfaces mid-stream engine errors as StreamFailureError,
    matching the HTTP backend's mid-stream failure type."""

    __slots__ = ("_sr", "_released", "_finalizer", "last_message", "exception_tag", "__weakref__")

    def __init__(self, streaming_result: Any, handle: _EngineHandle):
        self._sr = streaming_result
        self._released = [False]
        self._finalizer = weakref.finalize(self, _finalize_stream, streaming_result, handle, self._released)
        self.last_message: bytes | None = None
        self.exception_tag: str | None = None

    @property
    def gen(self):
        def _gen():
            try:
                while True:
                    try:
                        chunk = next(self._sr)
                    except StopIteration:
                        return
                    except Exception as ex:
                        raise StreamFailureError(_format_error_message(str(ex))) from ex
                    payload = chunk.bytes() if hasattr(chunk, "bytes") else bytes(chunk)
                    if payload:
                        yield payload
            finally:
                self.close()

        return _gen()

    def close(self) -> None:
        self._finalizer()


class _ChdbStreamFile(io.RawIOBase):
    """io.IOBase adapter over a chdb StreamingResult for raw_stream callers.
    Holds the handle lock for its lifetime."""

    def __init__(self, streaming_result: Any, handle: _EngineHandle):
        super().__init__()
        self._sr = streaming_result
        self._buf = bytearray()
        self._eof = False
        self._released = [False]
        self._finalizer = weakref.finalize(self, _finalize_stream, streaming_result, handle, self._released)

    def readable(self) -> bool:
        return True

    def _pull(self) -> bytes:
        while True:
            try:
                chunk = next(self._sr)
            except StopIteration:
                self._eof = True
                return b""
            except Exception as ex:
                self._eof = True
                raise StreamFailureError(_format_error_message(str(ex))) from ex
            payload = chunk.bytes() if hasattr(chunk, "bytes") else bytes(chunk)
            if payload:
                return payload

    def read(self, size: int | None = -1) -> bytes:
        if self.closed or self._released[0]:
            raise ValueError("I/O operation on closed file")
        if size is None or size < 0:
            parts = [bytes(self._buf)]
            self._buf.clear()
            while not self._eof:
                chunk = self._pull()
                if not chunk:
                    break
                parts.append(chunk)
            return b"".join(parts)
        while len(self._buf) < size and not self._eof:
            chunk = self._pull()
            if not chunk:
                break
            self._buf.extend(chunk)
        if not self._buf:
            return b""
        out = bytes(self._buf[:size])
        del self._buf[:size]
        return out

    def readinto(self, buf) -> int:
        data = self.read(len(buf))
        n = len(data)
        if n:
            buf[:n] = data
        return n

    def close(self) -> None:
        self._finalizer()
        super().close()


class ChdbBackend:
    capabilities = Capabilities(native_async=False, sessions=False)

    def __init__(self, *, connection_string: str):
        self.connection_string = connection_string
        self.show_clickhouse_errors = True
        self._handle = _open_handle(connection_string)
        self._closed = False
        self._streams: weakref.WeakSet = weakref.WeakSet()
        # A backend leaked without close() still releases its connection handle
        self._release = weakref.finalize(self, _close_handle, self._handle)

    # ---- engine access -------------------------------------------------

    def _wrap_exception(self, ex: Exception) -> DatabaseError:
        message = _format_error_message(str(ex))
        code_match = _ERROR_CODE_RE.search(message)
        code = int(code_match.group(1)) if code_match else None
        if not self.show_clickhouse_errors or not message:
            # The numeric code is always populated, matching the HTTP path
            return DatabaseError("The ClickHouse server returned an error.", code=code)
        return DatabaseError(message, code=code, name=error_name_from_body(message))

    def _guard(self) -> None:
        if self._closed:
            raise ProgrammingError("The client has been closed")
        if self._handle.stream_owner == threading.get_ident():
            # Blocking on the handle lock here would deadlock: this thread
            # holds it through the open stream.
            raise ProgrammingError(_STREAM_OPEN_MESSAGE)

    def _guard_locked(self) -> None:
        """Re-check under the lock: a cross-thread close() may have won the
        race after _guard and closed the connection."""
        if self._closed or self._handle.conn is None:
            raise ProgrammingError("The client has been closed")

    def _use_database_locked(self, database: str | None) -> None:
        """Rebind this client's handle with USE when the requested database
        differs. USE state is per handle, so other clients are unaffected."""
        if not database or database == self._handle.active_database:
            return
        try:
            self._handle.conn.query(f"USE {quote_identifier(database)}", "TabSeparated")
        except Exception as ex:
            raise self._wrap_exception(ex) from ex
        self._handle.active_database = database

    def _query_locked(self, sql: str | bytes, fmt: str, params: dict[str, Any] | None = None) -> Any:
        try:
            return self._handle.conn.query(sql, fmt, params=params or {})
        except Exception as ex:
            raise self._wrap_exception(ex) from ex

    def _run(self, sql: str | bytes, fmt: str, params: dict[str, Any] | None = None, database: str | None = None) -> Any:
        """Run one engine call, with the USE rebind atomic under the lock."""
        self._guard()
        with self._handle.lock:
            self._guard_locked()
            self._use_database_locked(database)
            return self._query_locked(sql, fmt, params)

    def _run_with_settings(
        self,
        sql: str | bytes,
        fmt: str,
        settings: Mapping[str, str],
        params: dict[str, Any] | None = None,
        database: str | None = None,
    ) -> Any:
        """Run one engine call with per-call settings applied via SET and
        restored afterwards, all in a single lock hold so no other caller can
        observe the temporary values."""
        if not settings:
            return self._run(sql, fmt, params=params, database=database)
        self._guard()
        with self._handle.lock:
            self._guard_locked()
            self._use_database_locked(database)
            snapshot = self._snapshot_settings_locked(settings)
            try:
                # Applying inside the try keeps a mid-apply SET failure from
                # leaking the already-applied settings into later calls
                self._apply_settings_locked(settings)
                return self._query_locked(sql, fmt, params)
            finally:
                self._restore_settings_locked(snapshot)

    def _open_stream(self, sql: str | bytes, fmt: str, params: dict[str, Any] | None, database: str | None) -> Any:
        """Start a streaming query, leaving the handle lock held and owned by
        the returned stream until it closes."""
        self._guard()
        self._handle.lock.acquire()
        try:
            self._guard_locked()
            self._use_database_locked(database)
            streaming = self._handle.conn.send_query(sql, fmt, params=params or {})
        except DatabaseError:
            self._handle.lock.release()
            raise
        except Exception as ex:
            self._handle.lock.release()
            raise self._wrap_exception(ex) from ex
        self._handle.stream_owner = threading.get_ident()
        return streaming

    @staticmethod
    def _engine_settings(settings: Mapping[str, str]) -> dict[str, str]:
        """Drop transport-only settings that chdb would reject as unknown."""
        return {k: v for k, v in settings.items() if k not in CHDB_TRANSPORT_SETTINGS}

    @staticmethod
    def _summary(result: Any) -> dict[str, Any]:
        """Map chdb result statistics onto the HTTP summary key shapes."""
        try:
            return {
                "read_rows": str(result.rows_read()),
                "read_bytes": str(result.bytes_read()),
                "elapsed_ns": str(int(result.elapsed() * 1_000_000_000)),
            }
        except Exception:
            return {}

    @staticmethod
    def _insert_summary(result: Any) -> dict[str, Any]:
        """An INFILE insert reads exactly the rows it writes, so its read
        statistics are the written counts."""
        try:
            return {
                "written_rows": str(result.rows_read()),
                "written_bytes": str(result.bytes_read()),
                "elapsed_ns": str(int(result.elapsed() * 1_000_000_000)),
            }
        except Exception:
            return {}

    def _snapshot_settings_locked(self, settings: Mapping[str, str]) -> dict[str, tuple[str, bool]]:
        """Read the current value and changed flag of each setting from
        system.settings for `_restore_settings_locked`."""
        names = ", ".join(_quote_setting_value(_validate_setting_name(k)) for k in settings)
        # JSONEachRow keeps values byte-faithful; TabSeparated escaping would
        # mangle expression defaults such as max_threads = 'auto(14)'
        result = self._query_locked(f"SELECT name, value, changed FROM system.settings WHERE name IN ({names})", "JSONEachRow")
        snapshot: dict[str, tuple[str, bool]] = {}
        for line in result.bytes().splitlines():
            row = json.loads(line)
            snapshot[row["name"]] = (row["value"], bool(row["changed"]))
        for key in settings:
            # A name missing from system.settings (e.g. a custom setting) is
            # restored to DEFAULT so it cannot leak into later calls
            snapshot.setdefault(key, ("", False))
        return snapshot

    def _apply_settings_locked(self, settings: Mapping[str, str]) -> None:
        for key, value in settings.items():
            self._query_locked(f"SET {_validate_setting_name(key)} = {_quote_setting_value(value)}", "TabSeparated")

    def _restore_settings_locked(self, snapshot: dict[str, tuple[str, bool]]) -> None:
        for name, (value, was_changed) in snapshot.items():
            try:
                if was_changed:
                    # A value that is already a quoted literal is an expression
                    # repr (e.g. 'auto(14)') and must be re-applied verbatim
                    literal = value if len(value) >= 2 and value.startswith("'") and value.endswith("'") else _quote_setting_value(value)
                    self._query_locked(f"SET {_validate_setting_name(name)} = {literal}", "TabSeparated")
                else:
                    self._query_locked(f"SET {_validate_setting_name(name)} = DEFAULT", "TabSeparated")
            except Exception:
                logger.debug("Failed to restore setting %s", name, exc_info=True)

    @staticmethod
    def _reject_external_data(external_data: ExternalData | None) -> None:
        if external_data is not None:
            raise NotSupportedError("external_data is not supported by the chdb backend")

    # ---- contract methods ----------------------------------------------

    def execute_query(self, context: QueryContext, runtime: QueryRuntime, prepped_query: str | bytes) -> QueryExecution:
        self._reject_external_data(context.external_data)
        params = _strip_param_prefix(context.bind_params)
        settings = self._engine_settings(runtime.settings)

        if not context.is_insert and columns_only_re.search(context.uncommented_query):
            # chdb emits zero Native bytes for LIMIT 0, so probe the column
            # metadata with FORMAT JSON like the HTTP backend does.
            probe_sql = context.final_query
            if settings:
                probe_sql = f"{probe_sql}\n SETTINGS {_settings_clause(settings)}"
            result = self._run(f"{probe_sql}\n FORMAT JSON", "JSON", params=params, database=runtime.database)
            return QueryExecution(columns=json.loads(result.bytes())["meta"])

        if context.is_insert:
            # Inline VALUES data must stay the final clause, so settings go
            # through the SET dance rather than a trailing SETTINGS clause
            result = self._run_with_settings(prepped_query, "TabSeparated", settings, params=params, database=runtime.database)
            return QueryExecution(source=_BytesSource(b""), summary=self._summary(result))

        final_query: Any = prepped_query
        if settings:
            clause = f"\n SETTINGS {_settings_clause(settings)}"
            final_query = final_query + clause.encode() if isinstance(final_query, bytes) else final_query + clause
        final_query = final_query + b"\n FORMAT Native" if isinstance(final_query, bytes) else final_query + "\n FORMAT Native"

        if context.streaming:
            streaming = self._open_stream(final_query, "Native", params, runtime.database)
            source = _ChdbStreamSource(streaming, self._handle)
            self._streams.add(source)
            return QueryExecution(source=source)

        result = self._run(final_query, "Native", params=params, database=runtime.database)
        return QueryExecution(source=_BytesSource(result.bytes()), summary=self._summary(result))

    def execute_command(
        self,
        bound_cmd: str | bytes,
        bind_params: dict[str, str],
        data: str | bytes | None,
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> CommandExecution:
        self._reject_external_data(external_data)
        cmd: str | bytes = bound_cmd
        if data is not None:
            data_str = data.decode() if isinstance(data, bytes) else data
            cmd = cmd.decode() if isinstance(cmd, bytes) else cmd
            cmd = f"{cmd}\n{data_str}"
        params = _strip_param_prefix(bind_params)
        # DDL rejects a SETTINGS clause, so command settings use the SET dance
        settings = self._engine_settings(runtime.settings)
        embedded_fmt = _trailing_format(cmd)
        if embedded_fmt is not None:
            # An embedded FORMAT clause wins over the format argument, and a
            # statement that can carry one produces a result set, so report it
            result = self._run_with_settings(cmd, "TabSeparated", settings, params=params, database=runtime.database)
            return CommandExecution(body=result.bytes() or b"", summary=self._summary(result), result_format=embedded_fmt)
        # HTTP reports a result set via the X-ClickHouse-Format header even when
        # it has zero rows. chdb has no headers, so run the command with names:
        # a result-producing statement always emits at least the names line,
        # while a control statement emits nothing in any format. Strip the
        # names line to keep the TabSeparated body parse_command_body expects.
        result = self._run_with_settings(cmd, "TabSeparatedWithNames", settings, params=params, database=runtime.database)
        body = result.bytes() or b""
        if not body:
            return CommandExecution(body=b"", summary=self._summary(result))
        newline = body.find(b"\n")
        body = body[newline + 1 :] if newline >= 0 else b""
        return CommandExecution(body=body, summary=self._summary(result), result_format="TabSeparated")

    def execute_data_insert(
        self,
        context: InsertContext,
        runtime: QueryRuntime,
        body: Any,
        retry_body: Callable[[], Any],
    ) -> dict[str, Any]:
        if isinstance(context.compression, str):
            raise NotSupportedError("Insert compression is not supported by the chdb backend")
        cols = ", ".join(quote_identifier(name) for name in context.column_names)
        # build_insert prepends this exact statement to its first chunk for
        # the HTTP request body; INFILE wants only the Native bytes
        body_prefix = f"INSERT INTO {context.table} ({cols}) FORMAT Native\n".encode()
        tmp = tempfile.NamedTemporaryFile(suffix=".native", delete=False)  # noqa: SIM115
        try:
            try:
                first_chunk = True
                for chunk in body:
                    if context.insert_exception is not None:
                        ex = context.insert_exception
                        context.insert_exception = None
                        raise ex  # noqa: TRY301
                    if first_chunk:
                        if chunk.startswith(body_prefix):
                            chunk = chunk[len(body_prefix) :]
                        else:
                            newline = chunk.find(b"\n")
                            if newline >= 0:
                                chunk = chunk[newline + 1 :]
                        first_chunk = False
                    tmp.write(chunk)
            finally:
                tmp.close()
            settings = self._engine_settings(runtime.settings)
            clause = f" SETTINGS {_settings_clause(settings)}" if settings else ""
            sql = f"INSERT INTO {context.table} ({cols}) FROM INFILE {_quote_sql_string(tmp.name)}{clause} FORMAT Native"
            result = self._run(sql, "TabSeparated", database=runtime.database)
            return self._insert_summary(result)
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

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
        if not table:
            raise ProgrammingError("The chdb backend requires a table name for raw_insert")
        if insert_block is None:
            raise ProgrammingError("No insert block provided for raw_insert")
        tmp = tempfile.NamedTemporaryFile(suffix=".raw", delete=False)  # noqa: SIM115
        try:
            if compression and compression != "identity":
                # chdb has no Content-Encoding input stage; the payload must
                # be fully materialized to decompress it client-side
                buffer = io.BytesIO()
                _write_block_to_file(insert_block, buffer)
                tmp.write(_decompress(buffer.getvalue(), compression))
            else:
                _write_block_to_file(insert_block, tmp)
            tmp.close()
            cols = f" ({', '.join(quote_identifier(name) for name in column_names)})" if column_names else ""
            settings = self._engine_settings(runtime.settings)
            clause = f" SETTINGS {_settings_clause(settings)}" if settings else ""
            sql = f"INSERT INTO {table}{cols} FROM INFILE {_quote_sql_string(tmp.name)}{clause} FORMAT {fmt}"
            result = self._run(sql, "TabSeparated", database=runtime.database)
            return self._insert_summary(result)
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    def execute_raw_query(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> bytes:
        self._reject_external_data(external_data)
        params = _strip_param_prefix(bind_params)
        # The facade has already appended any FORMAT clause, so settings
        # cannot ride as a trailing SETTINGS clause; apply them with SET.
        # An embedded FORMAT clause wins over the format argument; the
        # argument only supplies the HTTP default for format-less SQL.
        settings = self._engine_settings(runtime.settings)
        result = self._run_with_settings(final_query, "TabSeparated", settings, params=params, database=runtime.database)
        return result.bytes() or b""

    def execute_raw_stream(
        self,
        final_query: str | bytes,
        bind_params: dict[str, str],
        external_data: ExternalData | None,
        runtime: QueryRuntime,
        transport_settings: dict[str, str] | None,
    ) -> io.IOBase:
        self._reject_external_data(external_data)
        params = _strip_param_prefix(bind_params)
        settings = self._engine_settings(runtime.settings)
        fmt = _trailing_format(final_query)
        if fmt not in _STREAM_SAFE_FORMATS or settings:
            # Formats with a global header/footer cannot be streamed as
            # concatenated per-block chunks. Per-call settings need the SET
            # apply/restore dance, and the restore cannot run while a stream
            # holds the handle lock. Both cases materialize in one call.
            return io.BytesIO(self.execute_raw_query(final_query, bind_params, external_data, runtime, transport_settings))
        streaming = self._open_stream(final_query, fmt, params, runtime.database)
        stream = _ChdbStreamFile(streaming, self._handle)
        self._streams.add(stream)
        return stream

    def set_client_setting(self, key: str, value: str) -> None:
        """Persist a client-level setting on this client's handle with SET.
        Raises when the engine rejects it, the analogue of HTTP failing the
        next request when an invalid setting rides in the params."""
        self._run(f"SET {_validate_setting_name(key)} = {_quote_setting_value(value)}", "TabSeparated")

    def ping(self) -> bool:
        try:
            result = self._run("SELECT 1", "TabSeparated")
            return result.bytes().strip() == b"1"
        except Exception:
            logger.debug("chdb ping failed", exc_info=True)
            return False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Close this backend's open streams first: they hold the handle lock,
        # and _close_handle will not close a connection under a live stream
        for stream in list(self._streams):
            try:
                stream.close()
            except Exception:
                logger.debug("Error closing chdb stream during client close", exc_info=True)
        # detach() disarms the leak finalizer; explicit close can then block,
        # which only waits out an in-flight cross-thread call
        if self._release.detach() is not None:
            _close_handle(self._handle, blocking=True)

    def close_connections(self) -> None:
        # Each client owns a single embedded connection; there is no pool to
        # recycle.
        return


if TYPE_CHECKING:

    def _contract_conformance(backend: ChdbBackend) -> SyncBackend:
        return backend
