import logging
import re
import warnings
import pytz

from io import IOBase
from typing import Any, Literal, Tuple, Dict, Sequence, Optional, Union, Generator, BinaryIO, TYPE_CHECKING
from datetime import tzinfo

from pytz.exceptions import UnknownTimeZoneError

from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.binding import bind_query
from clickhouse_connect.driver.common import dict_copy, empty_gen, StreamContext, get_rename_method
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.types import Matrix, Closable
from clickhouse_connect.driver.exceptions import StreamClosedError, ProgrammingError
from clickhouse_connect.driver import options
from clickhouse_connect.driver.options import check_arrow
from clickhouse_connect.driver.context import BaseQueryContext

if TYPE_CHECKING:
    from clickhouse_connect.datatypes.base import ClickHouseType

logger = logging.getLogger(__name__)

TzMode = Literal["naive_utc", "aware", "schema"]
TzSource = Literal["auto", "server", "local"]

_UTC_TZ_AWARE_TO_TZ_MODE: Dict[Union[bool, str], TzMode] = {
    False: "naive_utc",
    True: "aware",
    "schema": "schema",
}

_VALID_TZ_MODES = {"naive_utc", "aware", "schema"}

_TZ_MODE_TO_UTC_TZ_AWARE: Dict[str, Union[bool, Literal["schema"]]] = {
    "naive_utc": False,
    "aware": True,
    "schema": "schema",
}

_APPLY_SERVER_TZ_TO_TZ_SOURCE: Dict[Union[bool, str, None], TzSource] = {
    None: "auto",
    True: "server",
    False: "local",
    "always": "server",
}

_TZ_SOURCE_TO_APPLY_SERVER_TZ: Dict[str, Optional[Union[bool, str]]] = {
    "auto": None,
    "server": True,
    "local": False,
}

_VALID_TZ_SOURCES = {"auto", "server", "local"}

# Mapping for string booleans that may arrive via URL params
_STR_BOOL_MAP = {"true": True, "false": False, "1": True, "0": False}


def _resolve_tz_mode(
    tz_mode: Optional[TzMode] = None,
    utc_tz_aware: Optional[Union[bool, Literal["schema"]]] = None,
) -> TzMode:
    """Resolve tz_mode from either the new ``tz_mode`` or deprecated ``utc_tz_aware`` parameter.

    Returns the canonical TzMode string.  Raises ``ProgrammingError`` on conflicts or
    invalid values.
    """
    if tz_mode is not None and utc_tz_aware is not None:
        raise ProgrammingError(
            "Cannot specify both 'tz_mode' and 'utc_tz_aware'. "
            "Use 'tz_mode' only; 'utc_tz_aware' is deprecated."
        )

    if utc_tz_aware is not None:
        # Coerce string booleans from URL params (e.g. "true" -> True)
        if isinstance(utc_tz_aware, str) and utc_tz_aware.lower() in _STR_BOOL_MAP:
            utc_tz_aware = _STR_BOOL_MAP[utc_tz_aware.lower()]

        if utc_tz_aware not in _UTC_TZ_AWARE_TO_TZ_MODE:
            raise ProgrammingError(
                f'utc_tz_aware must be True, False, or "schema", got "{utc_tz_aware}"'
            )
        warnings.warn(
            "utc_tz_aware is deprecated and will be removed in 1.0. "
            "Use tz_mode='naive_utc' | 'aware' | 'schema' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return _UTC_TZ_AWARE_TO_TZ_MODE[utc_tz_aware]

    if tz_mode is not None:
        if tz_mode not in _VALID_TZ_MODES:
            raise ProgrammingError(
                f'tz_mode must be "naive_utc", "aware", or "schema", got "{tz_mode}"'
            )
        return tz_mode

    return "naive_utc"


def _resolve_tz_source(
    tz_source: Optional[TzSource] = None,
    apply_server_timezone: Optional[Union[str, bool]] = None,
) -> TzSource:
    """Resolve tz_source from either the new ``tz_source`` or deprecated ``apply_server_timezone`` parameter.

    Returns the canonical TzSource string.  Raises ``ProgrammingError`` on conflicts or
    invalid values.
    """
    if tz_source is not None and apply_server_timezone is not None:
        raise ProgrammingError(
            "Cannot specify both 'tz_source' and 'apply_server_timezone'. "
            "Use 'tz_source' only; 'apply_server_timezone' is deprecated."
        )

    if apply_server_timezone is not None:
        # Coerce string booleans from URL params (e.g. "true" -> True)
        if isinstance(apply_server_timezone, str) and apply_server_timezone.lower() in _STR_BOOL_MAP:
            apply_server_timezone = _STR_BOOL_MAP[apply_server_timezone.lower()]

        if apply_server_timezone not in _APPLY_SERVER_TZ_TO_TZ_SOURCE:
            raise ProgrammingError(
                f"apply_server_timezone must be None, True, False, or 'always', "
                f'got "{apply_server_timezone}"'
            )
        warnings.warn(
            "apply_server_timezone is deprecated and will be removed in 1.0. "
            "Use tz_source='auto' | 'server' | 'local' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return _APPLY_SERVER_TZ_TO_TZ_SOURCE[apply_server_timezone]

    if tz_source is not None:
        if tz_source not in _VALID_TZ_SOURCES:
            raise ProgrammingError(
                f'tz_source must be "auto", "server", or "local", got "{tz_source}"'
            )
        return tz_source

    return "auto"


commands = 'CREATE|ALTER|SYSTEM|GRANT|REVOKE|CHECK|DETACH|ATTACH|DROP|DELETE|KILL|' + \
           'OPTIMIZE|SET|RENAME|TRUNCATE|USE|UPDATE'

limit_re = re.compile(r'\s+LIMIT($|\s)', re.IGNORECASE)
select_re = re.compile(r'(^|\s)SELECT\s', re.IGNORECASE)
insert_re = re.compile(r'(^|\s)INSERT\s*INTO', re.IGNORECASE)
command_re = re.compile(r'(^\s*)(' + commands + r')\s', re.IGNORECASE)


# pylint: disable=too-many-instance-attributes
class QueryContext(BaseQueryContext):
    """
    Argument/parameter object for queries.  This context is used to set thread/query specific formats
    """

    # pylint: disable=duplicate-code,too-many-arguments,too-many-positional-arguments,too-many-locals
    def __init__(self,
                 query: Union[str, bytes] = '',
                 parameters: Optional[Dict[str, Any]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                 encoding: Optional[str] = None,
                 server_tz: tzinfo = pytz.UTC,
                 use_none: Optional[bool] = None,
                 column_oriented: Optional[bool] = None,
                 use_numpy: Optional[bool] = None,
                 max_str_len: Optional[int] = 0,
                 query_tz: Optional[Union[str, tzinfo]] = None,
                 column_tzs: Optional[Dict[str, Union[str, tzinfo]]] = None,
                 utc_tz_aware: Optional[Union[bool, Literal["schema"]]] = None,
                 use_extended_dtypes: Optional[bool] = None,
                 as_pandas: bool = False,
                 streaming: bool = False,
                 apply_server_tz: bool = False,
                 external_data: Optional[ExternalData] = None,
                 transport_settings: Optional[Dict[str, str]] = None,
                 rename_response_column: Optional[str] = None,
                 tz_mode: Optional[TzMode] = None):
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
        :param query_tz  Either a string or a pytz tzinfo object.  (Strings will be converted to tzinfo objects).
          Values for any DateTime or DateTime64 column in the query will be converted to Python datetime.datetime
          objects with the selected timezone
        :param column_tzs A dictionary of column names to tzinfo objects (or strings that will be converted to
          tzinfo objects).  The timezone will be applied to datetime objects returned in the query
        :param tz_mode Controls timezone-aware behavior for UTC DateTime columns. "naive_utc" (default) returns
          naive UTC timestamps. "aware" forces timezone-aware UTC datetimes. "schema" returns datetimes that
          match the server's column definition which means timezone-aware when the column schema defines a timezone
          (e.g. DateTime('UTC')) and naive for bare DateTime columns.
        :param utc_tz_aware Deprecated. Use tz_mode instead.
        """
        super().__init__(settings,
                         query_formats,
                         column_formats,
                         encoding,
                         use_extended_dtypes if use_extended_dtypes is not None else False,
                         use_numpy if use_numpy is not None else False,
                         transport_settings=transport_settings)
        self.query = query
        self.parameters = parameters or {}
        self.use_none = True if use_none is None else use_none
        self.column_oriented = False if column_oriented is None else column_oriented
        self.use_numpy = use_numpy
        self.max_str_len = 0 if max_str_len is None else max_str_len
        self.server_tz = server_tz
        self.apply_server_tz = apply_server_tz
        self.external_data = external_data
        self.tz_mode = _resolve_tz_mode(tz_mode, utc_tz_aware)
        if isinstance(query_tz, str):
            try:
                query_tz = pytz.timezone(query_tz)
            except UnknownTimeZoneError as ex:
                raise ProgrammingError(f'query_tz {query_tz} is not recognized') from ex
        self.query_tz = query_tz
        if column_tzs is not None:
            for col_name, timezone in column_tzs.items():
                if isinstance(timezone, str):
                    try:
                        timezone = pytz.timezone(timezone)
                        column_tzs[col_name] = timezone
                    except UnknownTimeZoneError as ex:
                        raise ProgrammingError(f'column_tz {timezone} is not recognized') from ex
        self.column_tzs = column_tzs
        self.column_tz = None
        self.response_tz = None
        self.block_info = False
        self.as_pandas = as_pandas
        self.use_pandas_na = as_pandas and options.pd_extended_dtypes
        self.streaming = streaming
        self._rename_response_column: Optional[str] = rename_response_column
        self.column_renamer = get_rename_method(rename_response_column)
        self._update_query()

    @property
    def rename_response_column(self) -> Optional[str]:
        return self._rename_response_column

    @rename_response_column.setter
    def rename_response_column(self, method: Optional[str]):
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

    @property
    def utc_tz_aware(self) -> Union[bool, Literal["schema"]]:
        """Deprecated: use tz_mode instead."""
        warnings.warn(
            "utc_tz_aware is deprecated and will be removed in 1.0. "
            "Use tz_mode instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _TZ_MODE_TO_UTC_TZ_AWARE[self.tz_mode]

    def set_parameters(self, parameters: Dict[str, Any]):
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

    def active_tz(self, datatype_tz: Optional[tzinfo]):
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

    # pylint disable=too-many-positional-arguments
    def updated_copy(self,
                     query: Optional[Union[str, bytes]] = None,
                     parameters: Optional[Dict[str, Any]] = None,
                     settings: Optional[Dict[str, Any]] = None,
                     query_formats: Optional[Dict[str, str]] = None,
                     column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                     encoding: Optional[str] = None,
                     server_tz: Optional[tzinfo] = None,
                     use_none: Optional[bool] = None,
                     column_oriented: Optional[bool] = None,
                     use_numpy: Optional[bool] = None,
                     max_str_len: Optional[int] = None,
                     query_tz: Optional[Union[str, tzinfo]] = None,
                     column_tzs: Optional[Dict[str, Union[str, tzinfo]]] = None,
                     utc_tz_aware: Optional[Union[bool, Literal["schema"]]] = None,
                     use_extended_dtypes: Optional[bool] = None,
                     as_pandas: bool = False,
                     streaming: bool = False,
                     external_data: Optional[ExternalData] = None,
                     transport_settings: Optional[Dict[str, str]] = None,
                     rename_response_column: Optional[str] = None,
                     tz_mode: Optional[TzMode] = None) -> 'QueryContext':
        """
        Creates Query context copy with parameters overridden/updated as appropriate.
        """
        if tz_mode is not None or utc_tz_aware is not None:
            resolved_tz_mode = _resolve_tz_mode(tz_mode, utc_tz_aware)
        else:
            resolved_tz_mode = self.tz_mode
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

    # pylint: disable=too-many-arguments
    def __init__(self,
                 result_set: Matrix = None,
                 block_gen: Generator[Matrix, None, None] = None,
                 column_names: Tuple[str, ...] = (),
                 column_types: Tuple['ClickHouseType', ...] = (),
                 column_oriented: bool = False,
                 source: Closable = None,
                 query_id: str = None,
                 summary: Dict[str, Any] = None):
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
        query_id = self.summary.get('query_id')
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
    def first_item(self) -> Dict[str, Any]:
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
            return ''
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


def arrow_buffer(table, compression: Optional[str] = None) -> Tuple[Sequence[str], Union[bytes, BinaryIO]]:
    pyarrow = check_arrow()
    write_options = None
    if compression in ('zstd', 'lz4'):
        write_options = pyarrow.ipc.IpcWriteOptions(compression=pyarrow.Codec(compression=compression))
    sink = pyarrow.BufferOutputStream()
    with pyarrow.RecordBatchFileWriter(sink, table.schema, options=write_options) as writer:
        writer.write(table)
    return table.schema.names, sink.getvalue()
