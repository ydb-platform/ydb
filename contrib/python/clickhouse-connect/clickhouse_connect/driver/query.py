import logging
import re
from collections.abc import Generator, Sequence
from datetime import timezone, tzinfo
from io import IOBase
from typing import TYPE_CHECKING, Any, BinaryIO, Literal
from zoneinfo import ZoneInfoNotFoundError

from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.binding import bind_query
from clickhouse_connect.driver.common import StreamContext, dict_copy, empty_gen, get_rename_method
from clickhouse_connect.driver.context import BaseQueryContext
from clickhouse_connect.driver.exceptions import ProgrammingError, StreamClosedError
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.options import check_arrow
from clickhouse_connect.driver.types import Closable, Matrix

if TYPE_CHECKING:
    from clickhouse_connect.datatypes.base import ClickHouseType

logger = logging.getLogger(__name__)

TzMode = Literal["naive_utc", "aware", "schema"]
TzSource = Literal["auto", "server", "local"]

_VALID_TZ_MODES = {"naive_utc", "aware", "schema"}

_VALID_TZ_SOURCES = {"auto", "server", "local"}


commands = "CREATE|ALTER|SYSTEM|GRANT|REVOKE|CHECK|DETACH|ATTACH|DROP|DELETE|KILL|OPTIMIZE|SET|RENAME|TRUNCATE|USE|UPDATE"

limit_re = re.compile(r"\s+LIMIT($|\s)", re.IGNORECASE)
select_re = re.compile(r"(^|\s)SELECT\s", re.IGNORECASE)
insert_re = re.compile(r"(^|\s)INSERT\s*INTO", re.IGNORECASE)
command_re = re.compile(r"(^\s*)(" + commands + r")\s", re.IGNORECASE)


class QueryContext(BaseQueryContext):
    """
    Argument/parameter object for queries.  This context is used to set thread/query specific formats
    """

    def __init__(
        self,
        query: str | bytes = "",
        parameters: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        server_tz: tzinfo = timezone.utc,
        use_none: bool | None = None,
        column_oriented: bool | None = None,
        use_numpy: bool | None = None,
        max_str_len: int | None = 0,
        query_tz: str | tzinfo | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        use_extended_dtypes: bool | None = None,
        as_pandas: bool = False,
        streaming: bool = False,
        apply_server_tz: bool = False,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        rename_response_column: str | None = None,
        tz_mode: TzMode | None = None,
    ):
        """
        Initializes various configuration settings for the query context

        :param query:  Query string with Python style format value replacements
        :param parameters: Optional dictionary of substitution values
        :param settings: Optional ClickHouse settings for the query
        :param query_formats: Optional dictionary of query formats with the key of a ClickHouse type name
          (with * wildcards) and a value of valid query formats for those types.
          The value 'encoding' can be sent to change the expected encoding for this query, with a value of
          the desired encoding such as `latin-1`
        :param column_formats: Optional dictionary of column specific formats.  The key is the column name,
          The value is either the format for the data column (such as 'string' for a UUID column) or a
          second level "format" dictionary of a ClickHouse type name and a value of query formats.  This
          secondary dictionary can be used for nested column types such as Tuples or Maps
        :param encoding: Optional string encoding for this query, such as 'latin-1'
        :param column_formats: Optional dictionary
        :param use_none: Use a Python None for ClickHouse NULL values in nullable columns.  Otherwise the default
          value of the column (such as 0 for numbers) will be returned in the result_set
        :param max_str_len Limit returned ClickHouse String values to this length, which allows a Numpy
          structured array even with ClickHouse variable length String columns.  If 0, Numpy arrays for
          String columns will always be object arrays
        :param query_tz  Either a string IANA timezone name or a tzinfo object (strings are resolved via zoneinfo).
          Values for any DateTime or DateTime64 column in the query will be converted to Python datetime.datetime
          objects with the selected timezone
        :param column_tzs A dictionary of column names to tzinfo objects (or strings that will be converted to
          tzinfo objects).  The timezone will be applied to datetime objects returned in the query
        :param tz_mode Controls timezone-aware behavior for UTC DateTime columns. "naive_utc" (default) returns
          naive UTC timestamps. "aware" forces timezone-aware UTC datetimes. "schema" returns datetimes that
          match the server's column definition which means timezone-aware when the column schema defines a timezone
          (e.g. DateTime('UTC')) and naive for bare DateTime columns.
        """
        super().__init__(
            settings,
            query_formats,
            column_formats,
            encoding,
            use_extended_dtypes if use_extended_dtypes is not None else False,
            use_numpy if use_numpy is not None else False,
            transport_settings=transport_settings,
        )
        self.query = query
        self.parameters = parameters or {}
        self.use_none = True if use_none is None else use_none
        self.column_oriented = False if column_oriented is None else column_oriented
        self.use_numpy = use_numpy
        self.max_str_len = 0 if max_str_len is None else max_str_len
        self.server_tz = server_tz
        self.apply_server_tz = apply_server_tz
        self.external_data = external_data
        self.tz_mode = tz_mode if tz_mode is not None else "naive_utc"
        if self.tz_mode not in _VALID_TZ_MODES:
            raise ProgrammingError(f'tz_mode must be "naive_utc", "aware", or "schema", got "{self.tz_mode}"')
        if isinstance(query_tz, str):
            try:
                query_tz = tzutil.resolve_zone(query_tz)
            except ZoneInfoNotFoundError as ex:
                raise ProgrammingError(f"query_tz {query_tz} is not recognized; {tzutil.TZDATA_HINT}") from ex
        self.query_tz = query_tz
        if column_tzs is not None:
            resolved_column_tzs = {}
            for col_name, col_tz in column_tzs.items():
                if isinstance(col_tz, str):
                    try:
                        resolved_column_tzs[col_name] = tzutil.resolve_zone(col_tz)
                    except ZoneInfoNotFoundError as ex:
                        raise ProgrammingError(f"column_tz {col_tz} is not recognized; {tzutil.TZDATA_HINT}") from ex
                else:
                    resolved_column_tzs[col_name] = col_tz
            column_tzs = resolved_column_tzs
        self.column_tzs = column_tzs
        self.column_tz = None
        self.response_tz = None
        self.block_info = False
        self.as_pandas = as_pandas
        self.streaming = streaming
        self._rename_response_column: str | None = rename_response_column
        self.column_renamer = get_rename_method(rename_response_column)
        self._update_query()

    @property
    def rename_response_column(self) -> str | None:
        return self._rename_response_column

    @rename_response_column.setter
    def rename_response_column(self, method: str | None):
        self._rename_response_column = method
        self.column_renamer = get_rename_method(method)

    @property
    def is_select(self) -> bool:
        return select_re.search(self.uncommented_query) is not None

    @property
    def has_limit(self) -> bool:
        return limit_re.search(self.uncommented_query) is not None

    @property
    def is_insert(self) -> bool:
        return insert_re.search(self.uncommented_query) is not None

    @property
    def is_command(self) -> bool:
        return command_re.search(self.uncommented_query) is not None

    def set_parameters(self, parameters: dict[str, Any]):
        self.parameters = parameters
        self._update_query()

    def set_parameter(self, key: str, value: Any):
        if not self.parameters:
            self.parameters = {}
        self.parameters[key] = value
        self._update_query()

    def set_response_tz(self, response_tz: tzinfo):
        self.response_tz = response_tz

    def start_column(self, name: str):
        super().start_column(name)
        if self.column_tzs and name in self.column_tzs:
            self.column_tz = self.column_tzs[name]
        else:
            self.column_tz = None

    def active_tz(self, datatype_tz: tzinfo | None):
        if self.tz_mode == "schema":
            return self.column_tz or datatype_tz
        if self.column_tz:
            active_tz = self.column_tz
        elif datatype_tz:
            active_tz = datatype_tz
        elif self.query_tz:
            active_tz = self.query_tz
        elif self.response_tz:
            active_tz = self.response_tz
        elif self.apply_server_tz:
            active_tz = self.server_tz
        else:
            active_tz = tzutil.local_tz
        if tzutil.is_utc_timezone(active_tz) and self.tz_mode == "naive_utc":
            return None
        return active_tz

    def updated_copy(
        self,
        query: str | bytes | None = None,
        parameters: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        server_tz: tzinfo | None = None,
        use_none: bool | None = None,
        column_oriented: bool | None = None,
        use_numpy: bool | None = None,
        max_str_len: int | None = None,
        query_tz: str | tzinfo | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        use_extended_dtypes: bool | None = None,
        as_pandas: bool = False,
        streaming: bool = False,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        rename_response_column: str | None = None,
        tz_mode: TzMode | None = None,
    ) -> "QueryContext":
        """
        Creates Query context copy with parameters overridden/updated as appropriate.
        """
        resolved_tz_mode = tz_mode if tz_mode is not None else self.tz_mode
        return QueryContext(
            query=query or self.query,
            parameters=dict_copy(self.parameters, parameters),
            settings=dict_copy(self.settings, settings),
            query_formats=dict_copy(self.query_formats, query_formats),
            column_formats=dict_copy(self.column_formats, column_formats),
            encoding=encoding if encoding else self.encoding,
            server_tz=server_tz if server_tz else self.server_tz,
            use_none=self.use_none if use_none is None else use_none,
            column_oriented=self.column_oriented if column_oriented is None else column_oriented,
            use_numpy=self.use_numpy if use_numpy is None else use_numpy,
            max_str_len=self.max_str_len if max_str_len is None else max_str_len,
            query_tz=self.query_tz if query_tz is None else query_tz,
            column_tzs=self.column_tzs if column_tzs is None else column_tzs,
            tz_mode=resolved_tz_mode,
            use_extended_dtypes=self.use_extended_dtypes if use_extended_dtypes is None else use_extended_dtypes,
            as_pandas=as_pandas,
            streaming=streaming,
            apply_server_tz=self.apply_server_tz,
            external_data=self.external_data if external_data is None else external_data,
            transport_settings=self.transport_settings if transport_settings is None else transport_settings,
            rename_response_column=self.rename_response_column if rename_response_column is None else rename_response_column,
        )

    def _update_query(self):
        self.final_query, self.bind_params = bind_query(self.query, self.parameters, self.server_tz)
        if isinstance(self.final_query, bytes):
            # If we've embedded binary data in the query, all bets are off, and we check the original query for comments
            self.uncommented_query = remove_sql_comments(self.query)
        else:
            self.uncommented_query = remove_sql_comments(self.final_query)


class QueryResult(Closable):
    """
    Wrapper class for query return values and metadata
    """

    def __init__(
        self,
        result_set: Matrix = None,
        block_gen: Generator[Matrix, None, None] = None,
        column_names: tuple[str, ...] = (),
        column_types: tuple["ClickHouseType", ...] = (),
        column_oriented: bool = False,
        source: Closable = None,
        query_id: str = None,
        summary: dict[str, Any] = None,
    ):
        self._result_rows = result_set
        self._result_columns = None
        self._block_gen = block_gen or empty_gen()
        self._in_context = False
        self._query_id = query_id
        self.column_names = column_names
        self.column_types = column_types
        self.column_oriented = column_oriented
        self.source = source
        self.summary = {} if summary is None else summary

    @property
    def result_set(self) -> Matrix:
        if self.column_oriented:
            return self.result_columns
        return self.result_rows

    @property
    def result_columns(self) -> Matrix:
        if self._result_columns is None:
            # If rows are already materialized and stream is closed, transpose from rows
            # This happens when async client eagerly materializes result_rows
            if self._result_rows is not None and self._block_gen is None:
                if self._result_rows:
                    self._result_columns = list(map(list, zip(*self._result_rows)))
                else:
                    self._result_columns = [[] for _ in range(len(self.column_names))]
            else:
                result = [[] for _ in range(len(self.column_names))]
                with self.column_block_stream as stream:
                    for block in stream:
                        for base, added in zip(result, block):
                            base.extend(added)
                self._result_columns = result
        return self._result_columns

    @property
    def result_rows(self) -> Matrix:
        if self._result_rows is None:
            result = []
            with self.row_block_stream as stream:
                for block in stream:
                    result.extend(block)
            self._result_rows = result
        return self._result_rows

    @property
    def query_id(self) -> str:
        query_id = self.summary.get("query_id")
        if query_id:
            return query_id
        return self._query_id

    def _column_block_stream(self):
        if self._block_gen is None:
            raise StreamClosedError
        block_stream = self._block_gen
        self._block_gen = None
        return block_stream

    def _row_block_stream(self):
        for block in self._column_block_stream():
            yield list(zip(*block))

    @property
    def column_block_stream(self) -> StreamContext:
        return StreamContext(self, self._column_block_stream())

    @property
    def row_block_stream(self) -> StreamContext:
        return StreamContext(self, self._row_block_stream())

    @property
    def rows_stream(self) -> StreamContext:
        def stream():
            for block in self._row_block_stream():
                yield from block

        return StreamContext(self, stream())

    def named_results(self) -> Generator[dict, None, None]:
        for row in zip(*self.result_set) if self.column_oriented else self.result_set:
            yield dict(zip(self.column_names, row))

    @property
    def row_count(self) -> int:
        if self.column_oriented:
            return 0 if len(self.result_set) == 0 else len(self.result_set[0])
        return len(self.result_set)

    @property
    def first_item(self) -> dict[str, Any]:
        if self.column_oriented:
            return {name: col[0] for name, col in zip(self.column_names, self.result_set)}
        return dict(zip(self.column_names, self.result_set[0]))

    @property
    def first_row(self) -> Sequence[Any]:
        if self.column_oriented:
            return [col[0] for col in self.result_set]
        return self.result_set[0]

    def close(self) -> None:
        if self.source:
            self.source.close()
            self.source = None
        if self._block_gen is not None:
            self._block_gen.close()
            self._block_gen = None


comment_re = re.compile(r"(\".*?\"|\'.*?\')|(/\*.*?\*/|(--\s)[^\n]*$)", re.MULTILINE | re.DOTALL)


def remove_sql_comments(sql: str) -> str:
    """
    Remove SQL comments.  This is useful to determine the type of SQL query, such as SELECT or INSERT, but we
    don't fully trust it to correctly ignore weird quoted strings, and other edge cases, so we always pass the
    original SQL to ClickHouse (which uses a full-fledged AST/ token parser)
    :param sql:  SQL query
    :return: SQL Query without SQL comments
    """

    def replacer(match):
        # if the 2nd group (capturing comments) is not None, it means we have captured a
        # non-quoted, actual comment string, so return nothing to remove the comment
        if match.group(2):
            return ""
        # Otherwise we've actually captured a quoted string, so return it
        return match.group(1)

    return comment_re.sub(replacer, sql)


def to_arrow(content: bytes):
    pyarrow = check_arrow()
    reader = pyarrow.ipc.RecordBatchFileReader(content)
    return reader.read_all()


def to_arrow_batches(buffer: IOBase) -> StreamContext:
    pyarrow = check_arrow()
    reader = pyarrow.ipc.open_stream(buffer)
    return StreamContext(buffer, reader)


def arrow_buffer(table, compression: str | None = None) -> tuple[Sequence[str], bytes | BinaryIO]:
    pyarrow = check_arrow()
    write_options = None
    if compression in ("zstd", "lz4"):
        write_options = pyarrow.ipc.IpcWriteOptions(compression=pyarrow.Codec(compression=compression))
    sink = pyarrow.BufferOutputStream()
    with pyarrow.RecordBatchFileWriter(sink, table.schema, options=write_options) as writer:
        writer.write(table)
    return table.schema.names, sink.getvalue()
