import logging
from typing import Union

from clickhouse_connect.datatypes import registry
from clickhouse_connect.driver.common import write_leb128
from clickhouse_connect.driver.exceptions import StreamCompleteException, StreamFailureError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.npquery import NumpyResult
from clickhouse_connect.driver.query import QueryResult, QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.compression import get_compressor

_EMPTY_CTX = QueryContext()

logger = logging.getLogger(__name__)


class NativeTransform:
    # pylint: disable=too-many-locals, too-many-statements, too-many-branches
    @staticmethod
    def parse_response(source: ByteSource, context: QueryContext = _EMPTY_CTX) -> Union[NumpyResult, QueryResult]:
        names = []
        col_types = []
        block_num = 0
        renamer = context.column_renamer

        def get_block():
            nonlocal block_num
            result_block = []
            try:
                try:
                    if context.block_info:
                        source.read_bytes(8)
                    num_cols = source.read_leb128()
                except StreamCompleteException:
                    if source.last_message:
                        error_msg = None
                        exception_tag = getattr(source, "exception_tag", None)
                        if exception_tag:
                            error_msg = extract_exception_with_tag(source.last_message, exception_tag)
                        if error_msg:
                            raise StreamFailureError(error_msg) from None
                    return None
                num_rows = source.read_leb128()
                for col_num in range(num_cols):
                    orig_name = source.read_leb128_str()
                    type_name = source.read_leb128_str()
                    if block_num == 0:
                        disp_name = renamer(orig_name) if renamer is not None else orig_name
                        names.append(disp_name)
                        col_type = registry.get_from_name(type_name)
                        col_types.append(col_type)
                    else:
                        col_type = col_types[col_num]
                    if num_rows == 0:
                        result_block.append(tuple())
                    else:
                        context.start_column(orig_name)
                        column = col_type.read_column(source, num_rows, context)
                        result_block.append(column)
            except Exception as ex:
                source.close()
                if isinstance(ex, StreamCompleteException):
                    # We ran out of data before it was expected, this could be ClickHouse reporting an error
                    # in the response
                    if source.last_message:
                        error_msg = None
                        exception_tag = getattr(source, "exception_tag", None)
                        if exception_tag:
                            error_msg = extract_exception_with_tag(source.last_message, exception_tag)
                        if not error_msg:
                            error_msg = extract_error_message(source.last_message)
                        raise StreamFailureError(error_msg) from None
                raise
            block_num += 1
            return result_block

        first_block = get_block()
        if first_block is None:
            return NumpyResult() if context.use_numpy else QueryResult([])

        def gen():
            yield first_block
            while True:
                next_block = get_block()
                if next_block is None:
                    return
                yield next_block

        if context.use_numpy:
            res_types = [col.dtype if hasattr(col, 'dtype') else 'O' for col in first_block]
            return NumpyResult(gen(), tuple(names), tuple(col_types), res_types, source)
        return QueryResult(None, gen(), tuple(names), tuple(col_types), context.column_oriented, source)

    @staticmethod
    def build_insert(context: InsertContext):
        compressor = get_compressor(context.compression)

        def chunk_gen():
            for block in context.next_block():
                output = bytearray()
                output += block.prefix
                write_leb128(block.column_count, output)
                write_leb128(block.row_count, output)
                for col_name, col_type, data in zip(block.column_names, block.column_types, block.column_data):
                    col_enc = col_name.encode()
                    write_leb128(len(col_enc), output)
                    output += col_enc
                    col_enc = col_type.insert_name.encode()
                    write_leb128(len(col_enc), output)
                    output += col_enc
                    context.start_column(col_name)
                    try:
                        col_type.write_column(data, output, context)
                    except Exception as ex:  # pylint: disable=broad-except
                        # This is hideous, but some low level serializations can fail while streaming
                        # the insert if the user has included bad data in the column.  We need to ensure that the
                        # insert fails (using garbage data) to avoid a partial insert, and use the context to
                        # propagate the correct exception to the user
                        logger.error('Error serializing column `%s` into data type `%s`',
                                     col_name, col_type.name, exc_info=True)
                        context.insert_exception = ex
                        yield 'INTERNAL EXCEPTION WHILE SERIALIZING'.encode()
                        return
                yield compressor.compress_block(output)
            footer = compressor.flush()
            if footer:
                yield footer

        return chunk_gen()


# pylint: disable=too-many-return-statements,too-many-branches
def extract_exception_with_tag(message: bytes, exception_tag: str) -> Union[str, None]:
    """Extract exception message from the new format with exception tag. Server v25.11+.

    Format: __exception__<TAG>\\r\\n<error message>\\r\\n<message_length> <TAG>__exception__\\r\\n
    """
    if not exception_tag:
        return None

    marker = b"__exception__"
    marker_pos = message.find(marker)
    if marker_pos == -1:
        return None

    pos = marker_pos + len(marker)
    while pos < len(message) and message[pos : pos + 1] in (b"\r", b"\n"):
        pos += 1

    tag_end = message.find(b"\r", pos)
    if tag_end == -1:
        tag_end = message.find(b"\n", pos)
    if tag_end == -1:
        return None

    found_tag = message[pos:tag_end].decode("ascii", errors="ignore").strip()
    if found_tag != exception_tag:
        return None

    pos = tag_end
    while pos < len(message) and message[pos : pos + 1] in (b"\r", b"\n"):
        pos += 1

    # Find the footer pattern: <message_length> <TAG>\r\n__exception__
    footer_pattern = f" {exception_tag}".encode()
    footer_pos = message.rfind(footer_pattern)
    if footer_pos == -1 or footer_pos < pos:
        return None

    suffix = message[footer_pos + len(footer_pattern) :]
    if b"__exception__" not in suffix:
        return None

    search_start = max(pos, footer_pos - 100)  # Search last 100 bytes for the newline
    last_newline = message.rfind(b"\n", search_start, footer_pos)
    if last_newline != -1:
        error_end = last_newline
        if error_end > 0 and message[error_end - 1 : error_end] == b"\r":
            error_end -= 1
    else:
        error_end = footer_pos

    error_message = message[pos:error_end]

    try:
        return error_message.decode("utf-8", errors="replace").strip()
    except Exception:  # pylint: disable=broad-except
        return error_message.decode("latin-1", errors="replace").strip()


def extract_error_message(message: bytes) -> str:
    if len(message) > 1024:
        message = message[-1024:]
    error_start = message.find('Code: '.encode())
    if error_start != -1:
        message = message[error_start:]
    try:
        message_str = message.decode()
    except UnicodeError:
        message_str = f'unrecognized data found in stream: `{message.hex()[128:]}`'
    return message_str
