from typing import Optional, Any
from chdb import _chdb

# try import pyarrow if failed, raise ImportError with suggestion
try:
    import pyarrow as pa  # noqa
except ImportError as e:
    print(f"ImportError: {e}")
    print('Please install pyarrow via "pip install pyarrow"')
    raise ImportError("Failed to import pyarrow") from None


_arrow_format = set({"dataframe", "arrowtable"})
_process_result_format_funs = {
    "dataframe": lambda x: to_df(x),
    "arrowtable": lambda x: to_arrowTable(x),
}


# return pyarrow table
def to_arrowTable(res):
    """convert res to arrow table"""
    # try import pyarrow and pandas, if failed, raise ImportError with suggestion
    try:
        import pyarrow as pa  # noqa
        import pandas as pd  # noqa
    except ImportError as e:
        print(f"ImportError: {e}")
        print('Please install pyarrow and pandas via "pip install pyarrow pandas"')
        raise ImportError("Failed to import pyarrow or pandas") from None
    if len(res) == 0:
        return pa.Table.from_batches([], schema=pa.schema([]))
    return pa.RecordBatchFileReader(res.bytes()).read_all()


# return pandas dataframe
def to_df(r):
    """convert arrow table to Dataframe"""
    t = to_arrowTable(r)
    return t.to_pandas(use_threads=True)


class StreamingResult:
    def __init__(self, c_result, conn, result_func):
        self._result = c_result
        self._result_func = result_func
        self._conn = conn
        self._exhausted = False

    def fetch(self):
        """Fetch next chunk of streaming results"""
        if self._exhausted:
            return None

        try:
            result = self._conn.streaming_fetch_result(self._result)
            if result is None or result.rows_read() == 0:
                self._exhausted = True
                return None
            return self._result_func(result)
        except Exception as e:
            self._exhausted = True
            raise RuntimeError(f"Streaming query failed: {str(e)}") from e

    def __iter__(self):
        return self

    def __next__(self):
        if self._exhausted:
            raise StopIteration

        chunk = self.fetch()
        if chunk is None:
            self._exhausted = True
            raise StopIteration

        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def cancel(self):
        self._exhausted = True

        try:
            self._conn.streaming_cancel_query(self._result)
        except Exception as e:
            raise RuntimeError(f"Failed to cancel streaming query: {str(e)}") from e


class Connection:
    def __init__(self, connection_string: str):
        # print("Connection", connection_string)
        self._cursor: Optional[Cursor] = None
        self._conn = _chdb.connect(connection_string)

    def cursor(self) -> "Cursor":
        self._cursor = Cursor(self._conn)
        return self._cursor

    def query(self, query: str, format: str = "CSV") -> Any:
        lower_output_format = format.lower()
        result_func = _process_result_format_funs.get(lower_output_format, lambda x: x)
        if lower_output_format in _arrow_format:
            format = "Arrow"

        result = self._conn.query(query, format)
        return result_func(result)

    def send_query(self, query: str, format: str = "CSV") -> StreamingResult:
        lower_output_format = format.lower()
        result_func = _process_result_format_funs.get(lower_output_format, lambda x: x)
        if lower_output_format in _arrow_format:
            format = "Arrow"

        c_stream_result = self._conn.send_query(query, format)
        return StreamingResult(c_stream_result, self._conn, result_func)

    def close(self) -> None:
        # print("close")
        if self._cursor:
            self._cursor.close()
        self._conn.close()


class Cursor:
    def __init__(self, connection):
        self._conn = connection
        self._cursor = self._conn.cursor()
        self._current_table: Optional[pa.Table] = None
        self._current_row: int = 0

    def execute(self, query: str) -> None:
        self._cursor.execute(query)
        result_mv = self._cursor.get_memview()
        if self._cursor.has_error():
            raise Exception(self._cursor.error_message())
        if self._cursor.data_size() == 0:
            self._current_table = None
            self._current_row = 0
            self._column_names = []
            self._column_types = []
            return

        # Parse JSON data
        json_data = result_mv.tobytes().decode("utf-8")
        import json

        try:
            # First line contains column names
            # Second line contains column types
            # Following lines contain data
            lines = json_data.strip().split("\n")
            if len(lines) < 2:
                self._current_table = None
                self._current_row = 0
                self._column_names = []
                self._column_types = []
                return

            self._column_names = json.loads(lines[0])
            self._column_types = json.loads(lines[1])

            # Convert data rows
            rows = []
            for line in lines[2:]:
                if not line.strip():
                    continue
                row_data = json.loads(line)
                converted_row = []
                for val, type_info in zip(row_data, self._column_types):
                    # Handle NULL values first
                    if val is None:
                        converted_row.append(None)
                        continue

                    # Basic type conversion
                    try:
                        if type_info.startswith("Int") or type_info.startswith("UInt"):
                            converted_row.append(int(val))
                        elif type_info.startswith("Float"):
                            converted_row.append(float(val))
                        elif type_info == "Bool":
                            converted_row.append(bool(val))
                        elif type_info == "String" or type_info == "FixedString":
                            converted_row.append(str(val))
                        elif type_info.startswith("DateTime"):
                            from datetime import datetime

                            # Check if the value is numeric (timestamp)
                            val_str = str(val)
                            if val_str.replace(".", "").isdigit():
                                converted_row.append(datetime.fromtimestamp(float(val)))
                            else:
                                # Handle datetime string formats
                                if "." in val_str:  # Has microseconds
                                    converted_row.append(
                                        datetime.strptime(
                                            val_str, "%Y-%m-%d %H:%M:%S.%f"
                                        )
                                    )
                                else:  # No microseconds
                                    converted_row.append(
                                        datetime.strptime(val_str, "%Y-%m-%d %H:%M:%S")
                                    )
                        elif type_info.startswith("Date"):
                            from datetime import date, datetime

                            # Check if the value is numeric (days since epoch)
                            val_str = str(val)
                            if val_str.isdigit():
                                converted_row.append(
                                    date.fromtimestamp(float(val) * 86400)
                                )
                            else:
                                # Handle date string format
                                converted_row.append(
                                    datetime.strptime(val_str, "%Y-%m-%d").date()
                                )
                        else:
                            # For unsupported types, keep as string
                            converted_row.append(str(val))
                    except (ValueError, TypeError):
                        # If conversion fails, keep original value as string
                        converted_row.append(str(val))
                rows.append(tuple(converted_row))

            self._current_table = rows
            self._current_row = 0

        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse JSON data: {e}")

    def commit(self) -> None:
        self._cursor.commit()

    def fetchone(self) -> Optional[tuple]:
        if not self._current_table or self._current_row >= len(self._current_table):
            return None

        # Now self._current_table is a list of row tuples
        row = self._current_table[self._current_row]
        self._current_row += 1
        return row

    def fetchmany(self, size: int = 1) -> tuple:
        if not self._current_table:
            return tuple()

        rows = []
        for _ in range(size):
            if (row := self.fetchone()) is None:
                break
            rows.append(row)
        return tuple(rows)

    def fetchall(self) -> tuple:
        if not self._current_table:
            return tuple()

        remaining_rows = []
        while (row := self.fetchone()) is not None:
            remaining_rows.append(row)
        return tuple(remaining_rows)

    def close(self) -> None:
        self._cursor.close()

    def __iter__(self):
        return self

    def __next__(self) -> tuple:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def column_names(self) -> list:
        """Return a list of column names from the last executed query"""
        return self._column_names if hasattr(self, "_column_names") else []

    def column_types(self) -> list:
        """Return a list of column types from the last executed query"""
        return self._column_types if hasattr(self, "_column_types") else []

    @property
    def description(self) -> list:
        """
        Return a description of the columns as per DB-API 2.0
        Returns a list of 7-item tuples, each containing:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)
        where only name and type_code are provided
        """
        if not hasattr(self, "_column_names") or not self._column_names:
            return []

        return [
            (name, type_info, None, None, None, None, None)
            for name, type_info in zip(self._column_names, self._column_types)
        ]


def connect(connection_string: str = ":memory:") -> Connection:
    """
    Create a connection to chDB backgroud server.
    Only one open connection is allowed per process. Use `close` to close the connection.
    If called with the same connection string, the same connection object will be returned.
    You can use the connection object to create cursor object. `cursor` method will return a cursor object.

    Args:
        connection_string (str, optional): Connection string. Defaults to ":memory:".
        Also support file path like:
          - ":memory:" (for in-memory database)
          - "test.db" (for relative path)
          - "file:test.db" (same as above)
          - "/path/to/test.db" (for absolute path)
          - "file:/path/to/test.db" (same as above)
          - "file:test.db?param1=value1&param2=value2" (for relative path with query params)
          - "file::memory:?verbose&log-level=test" (for in-memory database with query params)
          - "///path/to/test.db?param1=value1&param2=value2" (for absolute path)

        Connection string args handling:
          Connection string can contain query params like "file:test.db?param1=value1&param2=value2"
          "param1=value1" will be passed to ClickHouse engine as start up args.

          For more details, see `clickhouse local --help --verbose`
          Some special args handling:
            - "mode=ro" would be "--readonly=1" for clickhouse (read-only mode)

    Returns:
        Connection: Connection object
    """
    return Connection(connection_string)
