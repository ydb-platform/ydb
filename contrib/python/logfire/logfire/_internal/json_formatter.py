from __future__ import annotations

import io
from datetime import date, datetime, time, timedelta
from functools import partial
from typing import Any, Callable, cast

from .json_types import ArraySchema, DataType, JSONSchema
from .utils import safe_repr


class JsonArgsValueFormatter:
    """Format values recursively based on the information provided in value dict.

    When a custom format is identified, the `$__datatype__` key is always present.
    """

    def __init__(self, *, indent: int):
        self._indent_step = indent
        self._newlines = indent != 0
        self._data_type_map: dict[DataType, Callable[[int, Any, JSONSchema | None], None]] = {
            'str': self._format_string,
            'int': self._format_number,
            'float': self._format_number,
            'PydanticModel': partial(self._format_items, '(', '=', ')', False),
            'dataclass': partial(self._format_items, '(', '=', ')', False),
            'Mapping': partial(self._format_items, '({', ': ', '})', True),
            'tuple': partial(self._format_list_like, '(', ')'),
            'Sequence': partial(self._format_sequence, '([', '])'),
            'set': partial(self._format_list_like, '{', '}'),
            'frozenset': partial(self._format_list_like, 'frozenset({', '})'),
            'deque': partial(self._format_list_like, 'deque([', '])'),
            'generator': partial(self._format_list_like, 'generator((', '))'),
            'bytes': self._format_bytes,
            'Decimal': partial(self._write, 'Decimal(', ')', True),
            'date': self._format_date,
            'datetime': self._format_datetime,
            'time': self._format_time,
            'timedelta': self._format_timedelta,
            'Enum': partial(self._write, '(', ')', True),
            'IPv4Address': partial(self._write, 'IPv4Address(', ')', True),
            'AnyUrl': partial(self._write, 'AnyUrl(', ')', True),
            'Url': partial(self._write, 'Url(', ')', True),
            'IPv4Interface': partial(self._write, 'IPv4Interface(', ')', True),
            'IPv4Network': partial(self._write, 'IPv4Network(', ')', True),
            'IPv6Address': partial(self._write, 'IPv6Address(', ')', True),
            'IPv6Interface': partial(self._write, 'IPv6Interface(', ')', True),
            'IPv6Network': partial(self._write, 'IPv6Network(', ')', True),
            'PosixPath': partial(self._write, 'PosixPath(', ')', True),
            'Pattern': partial(self._write, 're.compile(', ')', True),
            'SecretBytes': partial(self._write, 'SecretBytes(', ')', False),
            'SecretStr': partial(self._write, 'SecretStr(', ')', True),
            'NameEmail': partial(self._write, '', '', False),
            'UUID': partial(self._write, "UUID('", "')", False),
            'Exception': partial(self._write, '(', ')', True),
            'ndarray': partial(self._format_list_like, 'array([', '])'),
            'DataFrame': self._format_data_frame,
            'attrs': partial(self._format_items, '(', '=', ')', False),
            'sqlalchemy': partial(self._format_items, '(', '=', ')', False),
            'unknown': partial(self._write, '', '', False),
        }

    def __call__(self, value: Any, *, schema: JSONSchema | None = None, indent_current: int = 0):
        self._stream = io.StringIO()
        self._format(indent_current, True, value, schema)
        return self._stream.getvalue()

    def _format(
        self, indent_current: int, use_repr: bool, value: dict[str, Any] | list[Any] | Any, schema: JSONSchema | None
    ) -> None:
        if schema is not None:
            if 'type' in schema:
                if (data_type := schema.get('x-python-datatype')) is None:
                    if schema['type'] == 'object' and isinstance(value, dict):
                        self._format_items('{', ': ', '}', True, indent_current, value, None)
                    elif schema['type'] == 'array' and isinstance(value, list):
                        self._format_list_like('[', ']', indent_current, value, schema)
                    else:
                        # e.g. {'type': 'string', 'format': 'date-time'}
                        # or {'type': 'null'}
                        self._write('', '', False, 0, value, None)
                else:
                    func = self._data_type_map.get(data_type)
                    assert func is not None, f'Unknown data type {data_type}'
                    func(indent_current, value, schema)
            else:
                self._write('', '', False, 0, safe_repr(value), None)
        else:
            if use_repr:
                value = safe_repr(value)
            self._write('', '', False, 0, value, None)

    def _write(
        self,
        open_: str,
        after_: str,
        use_repr: bool,
        _indent_current: int,
        value: Any,
        schema: JSONSchema | None,
    ) -> None:
        if schema is not None and (cls := schema.get('title')):
            open_ = f'{cls}{open_}'

        if use_repr:
            value = safe_repr(value)

        self._stream.write(f'{open_}{value}{after_}')

    def _format_timedelta(self, _indent_current: int, value: Any, _schema: JSONSchema | None) -> None:
        self._write('', '', True, 0, timedelta(seconds=value), None)

    def _format_sequence(
        self, open_: str, close_: str, indent_current: int, value: Any, schema: JSONSchema | None
    ) -> None:
        schema = schema or {}
        if (cls := schema.get('title')) == 'range':
            self._write('(', ')', False, 0, f'{value[0]}, {value[-1] + 1}', schema)
        else:
            self._format_list_like(f'{cls}{open_}', close_, indent_current, value, None)

    def _format_list_like(
        self, open_: str, close_: str, indent_current: int, value: Any, schema: JSONSchema | None
    ) -> None:
        schema = cast(ArraySchema, schema or {})
        indent_new = indent_current + self._indent_step
        before = indent_new * ' '
        comma = ',\n' if self._newlines else ', '

        self._stream.write(open_)

        first = True
        items_schema = (schema or {}).get('items')
        prefix_items = (schema or {}).get('prefixItems')
        if shape := (schema or {}).get('x-shape'):
            if len(shape) > 1:
                # We convert to list because we don't want the "array(" prefix to be added
                # when we call _format_list_like recursively.
                items_schema = {'x-shape': shape[1:], 'type': 'array'}
        for i, v in enumerate(value):
            if first:
                first = False
                if self._newlines:
                    self._stream.write('\n')
            else:
                # write comma here not after so that we don't have a trailing comma
                self._stream.write(comma)
            self._stream.write(before)

            item_schema = prefix_items[i] if prefix_items and len(prefix_items) > i else items_schema
            self._format(indent_new, True, v, item_schema)  # type: ignore

        if self._newlines and not first:
            self._stream.write(comma)
        self._stream.write(indent_current * ' ' + close_)

    def _format_items(
        self,
        open_: str,
        split_: str,
        close_: str,
        repr_key: bool,
        indent_current: int,
        value: Any,
        schema: JSONSchema | None,
    ) -> None:
        indent_new = indent_current + self._indent_step
        before_ = indent_new * ' '
        comma = ',\n' if self._newlines else ', '

        schema = schema or {}
        if cls := schema.get('title'):
            open_ = f'{cls}{open_}'
        self._stream.write(open_)
        first = True
        properties = schema.get('properties', {})
        for k, v in value.items():
            if first:
                if self._newlines:
                    self._stream.write('\n')
                first = False
            else:
                # write comma here not after so that we don't have a trailing comma
                self._stream.write(comma)
            self._stream.write(before_)
            self._format(indent_new, repr_key, k, None)
            self._stream.write(split_)
            self._format(indent_new, True, v, properties.get(k, None))

        if self._newlines and not first:
            self._stream.write(',\n')
        self._stream.write(indent_current * ' ' + close_)

    def _format_bytes(self, _indent_current: int, value: Any, schema: JSONSchema | None) -> None:
        """Format bytes value.

        Examples:
            >>> value = b'hello'
            >>> schema = {'type': 'string', 'x-python-datatype': 'bytes'}
            >>> _format_bytes(0, value, schema)
            "hello"
            >>> schema = {'type': 'string', 'x-python-datatype': 'bytes', 'title': 'MyBytes'}
            >>> _format_bytes(0, value, schema)
            MyBytes("hello")
        """
        cls = schema and schema.get('title')
        output = f'{cls}({value})' if cls else value
        self._stream.write(output)

    def _format_string(self, _indent_current: int, value: Any, schema: JSONSchema | None) -> None:
        """Format string value.

        Examples:
            >>> value = 'hello'
            >>> schema = {'type': 'string', 'x-python-datatype': 'str'}
            >>> _format_string(0, value, schema)
            "hello"
            >>> schema = {'type': 'string', 'x-python-datatype': 'str', 'title': 'MyString'}
            >>> _format_string(0, value, schema)
            MyString("hello")
        """
        cls = schema and schema.get('title')
        output = f'{cls}({repr(value)})' if cls else repr(value)
        self._stream.write(output)

    def _format_number(self, _indent_current: int, value: Any, schema: JSONSchema | None) -> None:
        """Format number value. Supports both integer and float types.

        Examples:
            >>> value = 42
            >>> schema = {'type': 'integer', 'x-python-datatype': 'int'}
            >>> _format_number(0, value, schema)
            42
            >>> schema = {'type': 'number', 'x-python-datatype': 'float', 'title': 'MyNumber'}
            >>> _format_number(0, value, schema)
            MyNumber(42)
        """
        cls = schema and schema.get('title')
        output = f'{cls}({value})' if cls else str(value)
        self._stream.write(output)

    def _format_table(
        self, columns: list[Any], indices: list[Any], rows: list[Any], real_column_count: int, real_row_count: int
    ) -> None:
        """Inspired by https://gist.github.com/lonetwin/4721748.

        >>> columns = ['col1', 'col2', 'col4', 'col5']
        >>> indices = ['a', 'b', 'd', 'e']
        >>> rows = [[1, 2, 4, 5], [2, 4, 8, 10], [4, 8, 16, 20], [5, 10, 20, 25]]
        >>> real_column_count = 5
        >>> real_rows_count = 5
        >>> _format_table(columns, indices, rows, real_column_count, real_rows_count)

            | col1 | col2 | ... | col4 | col5
        ----+------+------+-----+------+-----
        a   | 1    | 2    | ... | 4    | 5
        b   | 2    | 4    | ... | 8    | 10
        ... | ...  | ...  | ... | ...  | ...
        d   | 4    | 8    | ... | 16   | 20
        e   | 5    | 10   | ... | 20   | 25

        [5 rows x 5 columns]
        """

        def insert_into_list_middle(items: list[Any], item: str | list[str]) -> list[Any]:
            midpoint = len(items) // 2
            return items[0:midpoint] + [item] + items[midpoint:]

        # add a column at the begging for index
        column_count = len(columns)
        if column_count < real_column_count:
            columns = insert_into_list_middle(columns, '...')
        columns = [''] + [str(x) for x in columns]

        converted_rows: list[Any] = []
        for i, row in enumerate(rows):
            # add index at the beginning of row
            if column_count < real_column_count:
                row = insert_into_list_middle(row, '...')
            row = [indices[i]] + [str(x) for x in row]
            converted_rows.append(row)

        if len(rows) < real_row_count:
            converted_rows = insert_into_list_middle(converted_rows, ['...'] * len(columns))

        # figure out column widths
        widths = [len(max(cols, key=len)) for cols in zip(*([columns] + converted_rows))]

        # write the header
        self._stream.write(' | '.join(f'{title:{width}s}' for width, title in zip(widths, columns)) + '\n')

        # write the separator
        self._stream.write('-+-'.join('-' * width for width in widths) + '\n')

        # write the data
        for row in converted_rows:
            self._stream.write(' | '.join(f'{cdata:{width}s}' for width, cdata in zip(widths, row)) + '\n')

        # write summary
        self._stream.write(f'\n[{real_row_count} rows x {real_column_count} columns]')

    def _format_data_frame(self, _indent_current: int, value: list[Any], schema: JSONSchema | None) -> None:
        schema = schema or {}
        self._format_table(
            columns=schema.get('x-columns', []),
            indices=schema.get('x-indices', []),
            rows=value,
            real_column_count=schema.get('x-column-count', 0),
            real_row_count=schema.get('x-row-count', 0),
        )

    def _format_date(self, _indent_current: int, value: Any, schema: JSONSchema | None) -> None:
        self._write('', '', True, 0, date.fromisoformat(value) if isinstance(value, str) else value, schema)

    def _format_datetime(self, _indent_current: int, value: Any, schema: JSONSchema | None) -> None:
        self._write('', '', True, 0, datetime.fromisoformat(value) if isinstance(value, str) else value, schema)

    def _format_time(self, _indent_current: int, value: Any, schema: JSONSchema | None) -> None:
        self._write('', '', True, 0, time.fromisoformat(value) if isinstance(value, str) else value, schema)


json_args_value_formatter = JsonArgsValueFormatter(indent=4)
json_args_value_formatter_compact = JsonArgsValueFormatter(indent=0)
