import logging
from collections.abc import Generator
from typing import Any, Literal

from clickhouse_connect import common
from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType, ch_read_formats, ch_write_formats
from clickhouse_connect.datatypes.container import Tuple
from clickhouse_connect.datatypes.network import IPv4
from clickhouse_connect.datatypes.special import UUID, SimpleAggregateFunction
from clickhouse_connect.datatypes.string import FixedString, String
from clickhouse_connect.datatypes.temporal import Date, DateTimeBase
from clickhouse_connect.driver import options
from clickhouse_connect.driver.compression import get_compressor
from clickhouse_connect.driver.exceptions import DataError, Error, NotSupportedError, ProgrammingError, StreamFailureError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.npquery import NumpyResult
from clickhouse_connect.driver.query import QueryContext, QueryResult
from clickhouse_connect.driver.rustnumpy import (
    _build_converters,
    _contains_json_type,
    _convert_block,
    _normalize_json_shared_column,
)
from clickhouse_connect.driver.streaming import ReadAheadSource
from clickhouse_connect.driver.transform import (
    NativeTransform,
    Transform,
    extract_error_message,
    extract_exception_with_tag,
    format_stream_error,
)
from clickhouse_connect.driver.types import ByteSource, Closable

logger: logging.Logger = logging.getLogger(__name__)

NativeCodec = Literal["python", "rust", "rust_strict"]

_VALID_CODECS = ("python", "rust", "rust_strict")

REQUIRED_BINDING_API_VERSION = 3

_versions_logged = False


def _ch_core_module() -> Any:
    """Return the imported _ch_core module, or None if it is not available."""
    try:
        import _ch_core
    except ImportError:
        return None
    return _ch_core


def _log_versions_once(resolved: str, core: Any) -> None:
    global _versions_logged
    if _versions_logged:
        return
    _versions_logged = True
    logger.info(
        "native_codec=%s using clickhouse-connect-core %s (binding API %s), clickhouse-connect %s",
        resolved,
        getattr(core, "__version__", "unknown"),
        getattr(core, "BINDING_API_VERSION", 0),
        common.version(),
    )


def resolve_native_codec(native_codec: str | None) -> str:
    """Resolve the effective codec name and verify a compatible compiled _ch_core module is available."""
    if native_codec is None:
        resolved = common.get_setting("native_codec")
    else:
        resolved = native_codec.strip().lower()
    if resolved not in _VALID_CODECS:
        raise ProgrammingError(f"Invalid native_codec {native_codec!r}; expected one of {', '.join(_VALID_CODECS)}")
    if resolved == "python":
        return resolved
    core = _ch_core_module()
    if core is None:
        raise NotSupportedError(
            f'native_codec="{resolved}" requires the compiled _ch_core extension module, which ships separately as '
            "the clickhouse-connect-core wheel and is not installed in this environment. Install it with "
            'pip install "clickhouse-connect[rust]", or use native_codec="python" to run without it.'
        )
    api_version = getattr(core, "BINDING_API_VERSION", 0)
    if api_version < REQUIRED_BINDING_API_VERSION:
        raise NotSupportedError(
            f"The installed clickhouse-connect-core version {getattr(core, '__version__', 'unknown')} provides "
            f"binding API {api_version}, but this version of clickhouse-connect requires binding API "
            f"{REQUIRED_BINDING_API_VERSION} or newer. Upgrade it with pip install --upgrade clickhouse-connect-core."
        )
    _log_versions_once(resolved, core)
    return resolved


def _make_native_transform(native_codec: NativeCodec | None = None) -> Transform:
    """Build the Transform for the resolved codec."""
    resolved = resolve_native_codec(native_codec)
    if resolved == "python":
        return NativeTransform()
    return _RustNativeTransform(strict=(resolved == "rust_strict"))


def _rust_query_ineligible_reason(context: QueryContext) -> str | None:
    """Return a short reason the rust decoder cannot serve this query, or None if it can.

    column_oriented, streaming, block_info, column_renamer, numpy/pandas output, and
    settings/transport_settings/external_data are honored and not listed here. The columns-only LIMIT 0 branch is
    answered from FORMAT JSON metadata in both clients before any transform runs.
    """
    if context.use_numpy and options.arrow is None:
        # numpy and pandas output route through the zero-copy Arrow exit, which needs pyarrow.
        return "pyarrow not installed"
    if context.query_formats:
        return "query_formats"
    if context.column_formats:
        return "column_formats"
    if ch_read_formats:
        return "global read format override"
    if not context.use_none:
        return "use_none=False"
    if context.encoding:
        return "custom encoding"
    if context.query_tz is not None:
        return "query_tz"
    if context.column_tzs:
        return "column_tzs"
    if context.response_tz is not None:
        return "server timezone header"
    if context.tz_mode != "naive_utc":
        return "tz_mode"
    # With the checks above excluded, active_tz(None) is None exactly when bare DateTime renders naive-UTC
    # (query.py active_tz), the only ambient timezone behavior the rust decoder reproduces.
    if context.active_tz(None) is not None:
        return "ambient timezone"
    return None


class _ExceptionTagScanner:
    """Scans raw stream chunks for a complete tagged exception block, mirroring ResponseBuffer._check_for_exception."""

    def __init__(self, exception_tag: str):
        tag_bytes = exception_tag.encode()
        self._open_marker = b"__exception__\r\n" + tag_bytes
        self._close_marker = tag_bytes + b"\r\n__exception__"
        self._carryover = b""
        self._exception_buf: bytearray | None = None

    def push(self, chunk: bytes) -> bytes | None:
        if self._exception_buf is not None:
            self._exception_buf += chunk
            if self._close_marker in self._exception_buf:
                return bytes(self._exception_buf)
            return None
        search_data = self._carryover + chunk
        marker_pos = search_data.find(self._open_marker)
        if marker_pos != -1:
            self._exception_buf = bytearray(search_data[marker_pos:])
            if self._close_marker in self._exception_buf:
                return bytes(self._exception_buf)
            return None
        carry_size = len(self._open_marker) - 1
        if len(search_data) >= carry_size:
            self._carryover = search_data[-carry_size:]
        else:
            self._carryover = search_data
        return None


def _unsupported_decode_error(ex: Exception) -> NotSupportedError:
    return NotSupportedError(
        f'The rust native codec cannot decode this response: {ex}. Use native_codec="python" to fall back to the Python codec'
    )


def _binding_value_error(ex: ValueError) -> Error:
    # The binding raises NotImplementedError for unsupported input (handled separately at
    # each call site), so any ValueError from it is malformed data.
    return DataError(str(ex))


def _contains_datetime_type(ch_type: object) -> bool:
    """Return whether a ClickHouse type contains DateTime or DateTime64."""
    if not isinstance(ch_type, ClickHouseType):
        return False
    if isinstance(ch_type, DateTimeBase):
        return True
    for attr in ("element_type", "key_type", "value_type"):
        child = getattr(ch_type, attr, None)
        if child is not None and _contains_datetime_type(child):
            return True
    return any(_contains_datetime_type(child) for child in getattr(ch_type, "element_types", ()))


def _python_codec_tuple_container(ch_type: ClickHouseType) -> bool:
    """Whether the Python codec's C-accelerated column readers return this column as a tuple.

    Streamed column blocks match those containers exactly: read_str_col renders String (nullable
    included) and FixedString as tuples, the dataconv date/datetime/uuid/ipv4 readers render their
    non-nullable columns as tuples, and unnamed Tuple columns are tuples of row tuples.
    """
    if isinstance(ch_type, SimpleAggregateFunction):
        ch_type = ch_type.element_type
    if ch_type.low_card:
        return False
    if isinstance(ch_type, String):
        return True
    if isinstance(ch_type, Tuple):
        return not ch_type.element_names
    if ch_type.nullable:
        return False
    return isinstance(ch_type, (FixedString, Date, DateTimeBase, UUID, IPv4))


def _chunk_has_server_error(chunk: bytes, exception_tag: str | None) -> bool:
    if not chunk:
        return False
    # On tag-capable servers (v25.11+) a real server error is caught by the scanner before decoding, so a
    # tagged block reaching here has no error. Only apply the byte heuristic for untagged servers, otherwise
    # block data containing b"Code: " would misclassify an unsupported-type error as a stream failure.
    if exception_tag:
        return bool(extract_exception_with_tag(chunk, exception_tag))
    return b"Code: " in chunk


class _BufferedQueryResult(QueryResult):
    """QueryResult over per-batch Python column lists materialized at parse time.

    The stream is fully consumed, decoded, and closed inside parse_response, so decode errors raise from
    the query call itself. result_rows chains zip(*batch) per batch, exactly how the Python codec generates
    rows from its column blocks, and result_columns concatenates per column across batches.
    """

    def __init__(self, batch_columns: list, names: tuple, col_types: tuple, column_oriented: bool, source: Closable):
        super().__init__(None, None, names, col_types, column_oriented, source)
        self._block_gen = None
        self._batch_columns = batch_columns

    @property
    def result_rows(self) -> Any:
        if self._result_rows is None:
            rows: list = []
            for columns in self._batch_columns:
                rows.extend(zip(*columns))
            self._result_rows = rows
        return self._result_rows

    @property
    def result_columns(self) -> Any:
        if self._result_columns is None:
            columns = self._batch_columns[0]
            for batch in self._batch_columns[1:]:
                for base, added in zip(columns, batch):
                    base.extend(added)
            # The first batch now holds the concatenated columns; collapse so a later
            # result_rows access does not see the trailing batches twice.
            self._batch_columns = [columns]
            self._result_columns = columns
        return self._result_columns


class _RustNativeTransform:
    """FORMAT Native codec backed by the compiled _ch_core module."""

    threaded_insert = True

    def __init__(self, strict: bool = False):
        self.strict = strict

    def parse_response(self, source: ByteSource, context: QueryContext) -> NumpyResult | QueryResult:
        if context.internal:
            # Driver-internal metadata queries pin read formats and always use the Python codec, even under strict.
            return NativeTransform.parse_response(source, context)
        reason = _rust_query_ineligible_reason(context)
        if reason is not None:
            if self.strict:
                source.close()
                raise NotSupportedError(f'native_codec="rust_strict" does not support {reason}; use native_codec="python" or "rust"')
            if reason == "pyarrow not installed":
                logger.warning("Native codec fallback to Python for query: %s", reason)
            else:
                logger.info("Native codec fallback to Python for query: %s", reason)
            return NativeTransform.parse_response(source, context)

        core = _ch_core_module()
        if core is None:
            source.close()
            raise NotSupportedError('The rust native codec is unavailable (_ch_core not importable); use native_codec="python"')

        # Read-ahead: a daemon thread pulls transport chunks into a bounded queue so the network read overlaps
        # decode. The consumer generator re-raises producer errors verbatim, in stream order, so the exception
        # mapping, tag scanning, and last-chunk heuristics below run unchanged on the consuming thread.
        read_source = ReadAheadSource(source)
        decoder = core.StreamDecoder(has_block_info=context.block_info)
        exception_tag = read_source.exception_tag
        scanner = _ExceptionTagScanner(exception_tag) if exception_tag else None
        last_chunk = b""
        show_clickhouse_errors = context.show_clickhouse_errors

        def server_error(chunk: bytes) -> StreamFailureError:
            # Tagged text first, as in the Python codec, then the client's show_clickhouse_errors policy.
            message = extract_exception_with_tag(chunk, exception_tag or "") or extract_error_message(chunk)
            return StreamFailureError(format_stream_error(message, show_clickhouse_errors))

        def raw_blocks() -> Generator[Any, None, None]:
            # Decode blocks lazily so streaming queries keep bounded memory. Errors surface here on the
            # consuming thread with the same mapping the Python codec uses; the source is closed before every raise.
            nonlocal last_chunk
            try:
                for chunk in read_source.gen:
                    last_chunk = chunk
                    if scanner is not None:
                        hit = scanner.push(chunk)
                        if hit is not None:
                            read_source.close()
                            raise server_error(hit)
                    yield from decoder.feed(chunk)
                yield from decoder.finish()
            except StreamFailureError:
                raise
            except EOFError as ex:
                read_source.close()
                if _chunk_has_server_error(last_chunk, exception_tag):
                    raise server_error(last_chunk) from ex
                raise StreamFailureError("Stream ended unexpectedly (connection closed by server)") from ex
            except NotImplementedError as ex:
                read_source.close()
                if _chunk_has_server_error(last_chunk, exception_tag):
                    raise server_error(last_chunk) from ex
                raise _unsupported_decode_error(ex) from ex
            except ValueError as ex:
                read_source.close()
                if _chunk_has_server_error(last_chunk, exception_tag):
                    raise server_error(last_chunk) from ex
                raise _binding_value_error(ex) from ex
            except Exception as ex:
                read_source.close()
                if _chunk_has_server_error(last_chunk, exception_tag):
                    raise server_error(last_chunk) from ex
                if ex.__class__.__name__ == "ClientPayloadError":
                    raise StreamFailureError("Stream failed during read (connection closed by server)") from ex
                raise

        blocks = raw_blocks()
        try:
            first = next(blocks)
        except StopIteration:
            read_source.close()
            return NumpyResult() if context.use_numpy else QueryResult([])

        renamer = context.column_renamer
        try:
            names = tuple(renamer(name) if renamer is not None else name for name in first.column_names)
            col_types = tuple(registry.get_from_name(type_name) for type_name in first.column_type_names)
        except Exception:
            read_source.close()
            raise

        json_flags = [_contains_json_type(col_type) for col_type in col_types]
        needs_post = any(json_flags)

        def normalize_json_columns(columns: list) -> list:
            return [_normalize_json_shared_column(col_type, column, context) for col_type, column in zip(col_types, columns)]

        if not context.use_numpy and not context.streaming:
            # Buffered query: materialize each batch to Python columns as it arrives so object building
            # on this thread overlaps the producer's transport drain, and any decode error surfaces here
            # rather than on a later result access.
            try:
                if needs_post:
                    batch_columns = [normalize_json_columns(first.to_python_columns())]
                    for batch in blocks:
                        batch_columns.append(normalize_json_columns(batch.to_python_columns()))
                else:
                    batch_columns = [first.to_python_columns()]
                    for batch in blocks:
                        batch_columns.append(batch.to_python_columns())
            except NotImplementedError as ex:
                read_source.close()
                raise _unsupported_decode_error(ex) from ex
            except ValueError as ex:
                read_source.close()
                raise _binding_value_error(ex) from ex
            except Exception:
                read_source.close()
                raise
            read_source.close()
            return _BufferedQueryResult(batch_columns, names, col_types, context.column_oriented, read_source)

        tuple_flags = [_python_codec_tuple_container(col_type) for col_type in col_types]
        # JSON-containing columns normalize in Python below, so their tuple wrap stays there too.
        # None when no flag is set skips the per-call flag extraction in the binding.
        flags = [tf and not jf for tf, jf in zip(tuple_flags, json_flags)]
        rust_tuple_flags = flags if any(flags) else None

        def post_process_json(columns: list) -> list:
            # Streamed blocks match the Python codec's per-column containers.
            for i, json_flag in enumerate(json_flags):
                if json_flag:
                    normalized = _normalize_json_shared_column(col_types[i], columns[i], context)
                    columns[i] = tuple(normalized) if tuple_flags[i] else normalized
            return columns

        try:
            if context.use_numpy:
                converters = _build_converters(col_types, context)
                first_columns = _convert_block(first, converters)
            else:
                first_columns = first.to_python_columns(typed_numeric=True, tuple_columns=rust_tuple_flags)
                if needs_post:
                    first_columns = post_process_json(first_columns)
        except NotImplementedError as ex:
            read_source.close()
            raise _unsupported_decode_error(ex) from ex
        except ValueError as ex:
            read_source.close()
            raise _binding_value_error(ex) from ex
        except Exception:
            read_source.close()
            raise

        if context.use_numpy:
            d_types = [col.dtype if hasattr(col, "dtype") else "O" for col in first_columns]

            def np_block_gen() -> Generator[list, None, None]:
                yield first_columns
                for batch in blocks:
                    try:
                        columns = _convert_block(batch, converters)
                    except NotImplementedError as ex:
                        read_source.close()
                        raise _unsupported_decode_error(ex) from ex
                    except ValueError as ex:
                        read_source.close()
                        raise _binding_value_error(ex) from ex
                    except Exception:
                        read_source.close()
                        raise
                    yield columns

            return NumpyResult(np_block_gen(), names, col_types, d_types, read_source)

        def block_gen() -> Generator[list, None, None]:
            yield first_columns
            for batch in blocks:
                try:
                    columns = batch.to_python_columns(typed_numeric=True, tuple_columns=rust_tuple_flags)
                    if needs_post:
                        columns = post_process_json(columns)
                except NotImplementedError as ex:
                    read_source.close()
                    raise _unsupported_decode_error(ex) from ex
                except ValueError as ex:
                    read_source.close()
                    raise _binding_value_error(ex) from ex
                except Exception:
                    read_source.close()
                    raise
                yield columns

        return QueryResult(None, block_gen(), names, col_types, context.column_oriented, read_source)

    def build_insert(self, context: InsertContext) -> Generator[bytes, None, None]:
        core = _ch_core_module()
        if core is None:
            raise NotSupportedError('The rust native codec is unavailable (_ch_core not importable); use native_codec="python"')

        if common.get_setting("naive_datetime_insert") == "server" and any(
            _contains_datetime_type(ch_type) for ch_type in context.column_types
        ):
            if self.strict:
                raise NotSupportedError(
                    'native_codec="rust_strict" does not support naive_datetime_insert="server" for DateTime inserts; '
                    'use native_codec="python" or "rust"'
                )
            logger.info('Native codec fallback to Python for insert: naive_datetime_insert="server"')
            return NativeTransform.build_insert(context)

        if ch_write_formats:
            # The rust encoder does not consult the global write-format registry, so per-value conversions
            # (e.g. set_write_format) would be ignored. Route these to the Python encoder.
            if self.strict:
                raise NotSupportedError(
                    'native_codec="rust_strict" does not support global write format overrides; use native_codec="python" or "rust"'
                )
            logger.info("Native codec fallback to Python for insert: global write format override")
            return NativeTransform.build_insert(context)

        if context.col_simple_formats or context.col_type_formats or context.type_formats:
            # The rust encoder ignores user column/query formats. Gate on the compiled format dicts, which are
            # built from the user dict at init. _convert_pandas injects a harmless column_formats["int"] hint
            # for datetime columns post-init, and the rust encoder already accepts the raw int values it feeds.
            if self.strict:
                raise NotSupportedError(
                    'native_codec="rust_strict" does not support per-column or per-type write formats; use native_codec="python" or "rust"'
                )
            logger.info("Native codec fallback to Python for insert: column/type write format")
            return NativeTransform.build_insert(context)

        column_names = list(context.column_names)
        type_names = [col_type.name for col_type in context.column_types]
        try:
            core.encode_native_block(column_names, type_names, [[] for _ in column_names], 0, None)
        except NotImplementedError as ex:
            if self.strict:
                raise NotSupportedError(
                    f'native_codec="rust_strict" cannot insert unsupported column type: {ex}; use native_codec="python" or "rust"'
                ) from ex
            logger.info("Native codec fallback to Python for insert: unsupported type (%s)", ex)
            return NativeTransform.build_insert(context)

        compression = context.compression if isinstance(context.compression, str) else None
        compressor = get_compressor(compression)

        def chunk_gen():
            for block in context.next_block():
                try:
                    output = core.encode_native_block(
                        list(block.column_names),
                        [col_type.name for col_type in block.column_types],
                        list(block.column_data),
                        block.row_count,
                        block.prefix,
                    )
                except Exception as ex:
                    logger.error("Error serializing insert with Rust Native encoder", exc_info=True)
                    if not isinstance(ex, Error):
                        wrapped = DataError(str(ex))
                        wrapped.__cause__ = ex
                        ex = wrapped
                    context.insert_exception = ex
                    yield b"INTERNAL EXCEPTION WHILE SERIALIZING"
                    return
                yield compressor.compress_block(output)
            footer = compressor.flush()
            if footer:
                yield footer

        return chunk_gen()
