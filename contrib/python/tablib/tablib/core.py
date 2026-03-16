"""
    tablib.core
    ~~~~~~~~~~~

    This module implements the central Tablib objects.

    :copyright: (c) 2016 by Kenneth Reitz. 2019 Jazzband.
    :license: MIT, see LICENSE for more details.
"""

from copy import copy
from operator import itemgetter

from .exceptions import (
    HeadersNeeded,
    InvalidDatasetIndex,
    InvalidDatasetType,
    InvalidDimensions,
    UnsupportedFormat,
)
from .formats import registry
from .utils import normalize_input

__title__ = 'tablib'
__author__ = 'Kenneth Reitz'
__license__ = 'MIT'
__copyright__ = 'Copyright 2017 Kenneth Reitz. 2019 Jazzband.'
__docformat__ = 'restructuredtext'


class Row:
    """Internal Row object. Mainly used for filtering."""

    __slots__ = ['_row', 'tags']

    def __init__(self, row=(), tags=()):
        self._row = list(row)
        self.tags = list(tags)

    def __iter__(self):
        return (col for col in self._row)

    def __len__(self):
        return len(self._row)

    def __repr__(self):
        return repr(self._row)

    def __getitem__(self, i):
        return self._row[i]

    def __setitem__(self, i, value):
        self._row[i] = value

    def __delitem__(self, i):
        del self._row[i]

    def __getstate__(self):
        return self._row, self.tags

    def __setstate__(self, state):
        self._row, self.tags = state

    def rpush(self, value):
        self.insert(len(self._row), value)

    def lpush(self, value):
        self.insert(0, value)

    def append(self, value):
        self.rpush(value)

    def insert(self, index, value):
        self._row.insert(index, value)

    def copy(self):
        return Row(self._row.copy(), self.tags.copy())

    def __contains__(self, item):
        return item in self._row

    @property
    def tuple(self):
        """Tuple representation of :class:`Row`."""
        return tuple(self._row)

    @property
    def list(self):
        """List representation of :class:`Row`."""
        return list(self._row)

    def has_tag(self, tag):
        """Returns true if current row contains tag."""

        if tag is None:
            return False
        elif isinstance(tag, str):
            return tag in self.tags
        else:
            return bool(len(set(tag) & set(self.tags)))


class Dataset:
    """The :class:`Dataset` object is the heart of Tablib. It provides all core
    functionality.

    Usually you create a :class:`Dataset` instance in your main module, and append
    rows as you collect data. ::

        data = tablib.Dataset()
        data.headers = ('name', 'age')

        for (name, age) in some_collector():
            data.append((name, age))


    Setting columns is similar. The column data length must equal the
    current height of the data and headers must be set. ::

        data = tablib.Dataset()
        data.headers = ('first_name', 'last_name')

        data.append(('John', 'Adams'))
        data.append(('George', 'Washington'))

        data.append_col((90, 67), header='age')


    You can also set rows and headers upon instantiation. This is useful if
    dealing with dozens or hundreds of :class:`Dataset` objects. ::

        headers = ('first_name', 'last_name')
        data = [('John', 'Adams'), ('George', 'Washington')]

        data = tablib.Dataset(*data, headers=headers)

    :param \\*args: (optional) list of rows to populate Dataset
    :param headers: (optional) list strings for Dataset header row
    :param title: (optional) string to use as title of the Dataset


    .. admonition:: Format Attributes Definition

    If you look at the code, the various output/import formats are not
    defined within the :class:`Dataset` object. To add support for a new format, see
    :ref:`Adding New Formats <newformats>`.

    """

    def __init__(self, *args, **kwargs):
        self._data = [Row(arg) for arg in args]
        self.__headers = None

        # ('title', index) tuples
        self._separators = []

        # (column, callback) tuples
        self._formatters = []

        # {col_index: col_func}
        self._dynamic_columns = {}

        self.headers = kwargs.get('headers')

        self.title = kwargs.get('title')

    def __len__(self):
        return self.height

    def __getitem__(self, key):
        if isinstance(key, str):
            if key in self.headers:
                pos = self.headers.index(key)  # get 'key' index from each data
                return [row[pos] for row in self._data]
            else:
                raise KeyError
        else:
            _results = self._data[key]
            if isinstance(_results, Row):
                return _results.tuple
            else:
                return [result.tuple for result in _results]

    def __setitem__(self, key, value):
        self._validate(value)
        self._data[key] = Row(value)

    def __delitem__(self, key):
        if isinstance(key, str):

            if key in self.headers:

                pos = self.headers.index(key)
                del self.headers[pos]
                if pos in self._dynamic_columns:
                    del self._dynamic_columns[pos]

                for i, row in enumerate(self._data):

                    del row[pos]
                    self._data[i] = row
            else:
                raise KeyError
        else:
            del self._data[key]

    def __repr__(self):
        try:
            return f'<{self.title.lower()} dataset>'
        except AttributeError:
            return '<dataset object>'

    def __str__(self):
        result = []

        # Add str representation of headers.
        if self.__headers:
            result.append([str(h) for h in self.__headers])

        # Add str representation of rows.
        result.extend(list(map(str, row)) for row in self._data)

        lens = [list(map(len, row)) for row in result]
        field_lens = list(map(max, zip(*lens)))

        # delimiter between header and data
        if self.__headers:
            result.insert(1, ['-' * length for length in field_lens])

        format_string = '|'.join('{{{}:{}}}'.format(*item) for item in enumerate(field_lens))

        return '\n'.join(format_string.format(*row) for row in result)

    # ---------
    # Internals
    # ---------

    def _get_in_format(self, fmt_key, **kwargs):
        return registry.get_format(fmt_key).export_set(self, **kwargs)

    def _set_in_format(self, fmt_key, in_stream, **kwargs):
        in_stream = normalize_input(in_stream)
        return registry.get_format(fmt_key).import_set(self, in_stream, **kwargs)

    def _validate(self, row=None, col=None, safety=False):
        """Assures size of every row in dataset is of proper proportions."""
        if row:
            if self.width:
                is_valid = (
                    len(row) == self.width or
                    len(row) == (self.width - len(self._dynamic_columns))
                )
            else:
                is_valid = True
        elif col:
            if len(col) < 1:
                is_valid = True
            else:
                is_valid = (len(col) == self.height) if self.height else True
        else:
            is_valid = all(len(x) == self.width for x in self._data)

        if is_valid:
            return True
        else:
            if not safety:
                raise InvalidDimensions
            return False

    def _package(self, dicts=True):
        """Packages Dataset into lists of dictionaries for transmission."""
        # TODO: Dicts default to false?

        _data = list(self._data)

        def format_row(row):
            # Execute formatters
            if self._formatters:
                row = row.copy()  # To not mutate internal data structure
                for col, callback in self._formatters:
                    if col is None:
                        # Apply formatter to all cells
                        row = [callback(cell) for cell in row]
                    else:
                        row[col] = callback(row[col])
            return list(row)

        if self.headers:
            if dicts:
                data = [dict(list(zip(self.headers, format_row(row)))) for row in _data]
            else:
                data = [list(self.headers)] + [format_row(row) for row in _data]
        else:
            data = [format_row(row) for row in _data]
        return data

    def _get_headers(self):
        """An *optional* list of strings to be used for header rows and attribute names.

        This must be set manually. The given list length must equal :attr:`Dataset.width`.

        """
        return self.__headers

    def _set_headers(self, collection):
        """Validating headers setter."""
        self._validate(collection)
        if collection:
            self.__headers = list(collection)
        else:
            self.__headers = None

    headers = property(_get_headers, _set_headers)

    def _get_dict(self):
        """A native Python representation of the :class:`Dataset` object. If headers have
        been set, a list of Python dictionaries will be returned. If no headers have been set,
        a list of tuples (rows) will be returned instead.

        A dataset object can also be imported by setting the `Dataset.dict` attribute: ::

            data = tablib.Dataset()
            data.dict = [{'age': 90, 'first_name': 'Kenneth', 'last_name': 'Reitz'}]

        """
        return self._package()

    def _set_dict(self, pickle):
        """A native Python representation of the Dataset object. If headers have been
        set, a list of Python dictionaries will be returned. If no headers have been
        set, a list of tuples (rows) will be returned instead.

        A dataset object can also be imported by setting the :attr:`Dataset.dict` attribute. ::

            data = tablib.Dataset()
            data.dict = [{'age': 90, 'first_name': 'Kenneth', 'last_name': 'Reitz'}]

        """
        error_details = (
            "Please check format documentation "
            "https://tablib.readthedocs.io/en/stable/formats.html#yaml"
        )

        if not pickle:
            return

        if not isinstance(pickle, list):
            # sometimes pickle is a dict and len(pickle) returns True.
            # since we access index 0 we should check if the type is list
            raise UnsupportedFormat(error_details)

        # if list of rows
        if isinstance(pickle[0], list):
            self.wipe()
            for row in pickle:
                self.append(Row(row))

        # if list of objects
        elif isinstance(pickle[0], dict):
            self.wipe()
            self.headers = list(pickle[0].keys())
            for row in pickle:
                self.append(Row(list(row.values())))
        else:
            raise UnsupportedFormat(error_details)

    dict = property(_get_dict, _set_dict)

    def _clean_col(self, col):
        """Prepares the given column for insert/append. `col` is not supposed to
           contain any header value.
        """

        col = list(col)

        if len(col) == 1 and hasattr(col[0], '__call__'):
            col = list(map(col[0], self._data))

        return col

    @property
    def height(self):
        """The number of rows currently in the :class:`Dataset`.
           Cannot be directly modified.
        """
        return len(self._data)

    @property
    def width(self):
        """The number of columns currently in the :class:`Dataset`.
           Cannot be directly modified.
        """

        try:
            return len(self._data[0])
        except IndexError:
            try:
                return len(self.headers)
            except TypeError:
                return 0

    def load(self, in_stream, format=None, **kwargs):
        """
        Import `in_stream` to the :class:`Dataset` object using the `format`.
        `in_stream` can be a file-like object, a string, or a bytestring.

        :param \\*\\*kwargs: (optional) custom configuration to the format `import_set`.
        """

        stream = normalize_input(in_stream)
        if not format:
            format = detect_format(stream)

        fmt = registry.get_format(format)
        if not hasattr(fmt, 'import_set'):
            raise UnsupportedFormat(f'Format {format} cannot be imported.')

        fmt.import_set(self, stream, **kwargs)
        return self

    def export(self, format, **kwargs):
        """
        Export :class:`Dataset` object to `format`.

        :param \\*\\*kwargs: (optional) custom configuration to the format `export_set`.
        """
        fmt = registry.get_format(format)
        if not hasattr(fmt, 'export_set'):
            raise UnsupportedFormat(f'Format {format} cannot be exported.')

        return fmt.export_set(self, **kwargs)

    # ----
    # Rows
    # ----

    def insert(self, index, row, tags=()):
        """Inserts a row to the :class:`Dataset` at the given index.

        Rows inserted must be the correct size (height or width).

        The default behaviour is to insert the given row to the :class:`Dataset`
        object at the given index.

        You can add :ref:`tags <tags>` to the row you are inserting.
        This gives you the ability to :method:`filter <Dataset.filter>` your
        :class:`Dataset` later.
        """

        self._validate(row)
        if len(row) < self.width:
            for pos, func in self._dynamic_columns.items():
                row = list(row)
                row.insert(pos, func(row))
        self._data.insert(index, Row(row, tags=tags))

    def rpush(self, row, tags=()):
        """Adds a row to the end of the :class:`Dataset`.
        See :method:`Dataset.insert` for additional documentation.
        """

        self.insert(self.height, row=row, tags=tags)

    def lpush(self, row, tags=()):
        """Adds a row to the top of the :class:`Dataset`.
        See :method:`Dataset.insert` for additional documentation.
        """

        self.insert(0, row=row, tags=tags)

    def append(self, row, tags=()):
        """Adds a row to the :class:`Dataset`.
        See :method:`Dataset.insert` for additional documentation.
        """

        self.rpush(row, tags)

    def extend(self, rows, tags=()):
        """Adds a list of rows to the :class:`Dataset` using
        :method:`Dataset.append`
        """

        for row in rows:
            self.append(row, tags)

    def lpop(self):
        """Removes and returns the first row of the :class:`Dataset`."""

        cache = self[0]
        del self[0]

        return cache

    def rpop(self):
        """Removes and returns the last row of the :class:`Dataset`."""

        cache = self[-1]
        del self[-1]

        return cache

    def pop(self):
        """Removes and returns the last row of the :class:`Dataset`."""

        return self.rpop()

    def get(self, index):
        """Returns the row from the :class:`Dataset` at the given index."""

        if isinstance(index, int):
            return self[index]

        raise TypeError('Row indices must be integers.')

    # -------
    # Columns
    # -------

    def insert_col(self, index, col=None, header=None):
        """Inserts a column to the :class:`Dataset` at the given index.

        Columns inserted must be the correct height.

        You can also insert a column of a single callable object, which will
        add a new column with the return values of the callable each as an
        item in the column. ::

            data.append_col(col=random.randint)

        If inserting a column, and :attr:`Dataset.headers` is set, the
        header attribute must be set, and will be considered the header for
        that row.

        See :ref:`dyncols` for an in-depth example.
        """

        if col is None:
            col = []

        # Callable Columns...
        if callable(col):
            self._dynamic_columns[self.width] = col
            col = list(map(col, self._data))

        col = self._clean_col(col)
        self._validate(col=col)

        if self.headers:
            # pop the first item off, add to headers
            if not header:
                raise HeadersNeeded()

            # corner case - if header is set without data
            elif header and self.height == 0 and len(col):
                raise InvalidDimensions

            self.headers.insert(index, header)

        if self.height and self.width:

            for i, row in enumerate(self._data):

                row.insert(index, col[i])
                self._data[i] = row
        else:
            self._data = [Row([row]) for row in col]

    def rpush_col(self, col, header=None):
        """Adds a column to the end of the :class:`Dataset`.
        See :method:`Dataset.insert` for additional documentation.
        """

        self.insert_col(self.width, col, header=header)

    def lpush_col(self, col, header=None):
        """Adds a column to the top of the :class:`Dataset`.
        See :method:`Dataset.insert` for additional documentation.
        """

        self.insert_col(0, col, header=header)

    def insert_separator(self, index, text='-'):
        """Adds a separator to :class:`Dataset` at given index."""

        sep = (index, text)
        self._separators.append(sep)

    def append_separator(self, text='-'):
        """Adds a :ref:`separator <separators>` to the :class:`Dataset`."""

        # change offsets if headers are or aren't defined
        if not self.headers:
            index = self.height if self.height else 0
        else:
            index = (self.height + 1) if self.height else 1

        self.insert_separator(index, text)

    def append_col(self, col, header=None):
        """Adds a column to the :class:`Dataset`.
        See :method:`Dataset.insert_col` for additional documentation.
        """

        self.rpush_col(col, header)

    def get_col(self, index):
        """Returns the column from the :class:`Dataset` at the given index."""

        return [row[index] for row in self._data]

    # ----
    # Misc
    # ----

    def add_formatter(self, col, handler):
        """Adds a formatter to the :class:`Dataset`.

        :param col: column to. Accepts index int, header str, or None to apply
                    the formatter to all columns.
        :param handler: reference to callback function to execute against
                        each cell value.
        """

        if isinstance(col, str):
            if col in self.headers:
                col = self.headers.index(col)  # get 'key' index from each data
            else:
                raise KeyError

        if col is None or col <= self.width:
            self._formatters.append((col, handler))
        else:
            raise InvalidDatasetIndex

        return True

    def filter(self, tag):
        """Returns a new instance of the :class:`Dataset`, excluding any rows
        that do not contain the given :ref:`tags <tags>`.
        """
        _dset = copy(self)
        _dset._data = [row for row in _dset._data if row.has_tag(tag)]

        return _dset

    def sort(self, col, reverse=False):
        """Sort a :class:`Dataset` by a specific column, given string (for
        header) or integer (for column index). The order can be reversed by
        setting ``reverse`` to ``True``.

        Returns a new :class:`Dataset` instance where columns have been
        sorted.
        """

        if isinstance(col, str):

            if not self.headers:
                raise HeadersNeeded

            _sorted = sorted(self.dict, key=itemgetter(col), reverse=reverse)
            _dset = Dataset(headers=self.headers, title=self.title)

            for item in _sorted:
                row = [item[key] for key in self.headers]
                _dset.append(row=row)

        else:
            if self.headers:
                col = self.headers[col]

            _sorted = sorted(self.dict, key=itemgetter(col), reverse=reverse)
            _dset = Dataset(headers=self.headers, title=self.title)

            for item in _sorted:
                if self.headers:
                    row = [item[key] for key in self.headers]
                else:
                    row = item
                _dset.append(row=row)

        return _dset

    def _transpose_with_headers(self):
        """Transpose a :class:`Dataset`, turning rows into columns and vice
        versa, returning a new ``Dataset`` instance. The first row of the
        original instance becomes the new header row."""

        _dset = Dataset()
        # The first element of the headers stays in the headers,
        # it is our "hinge" on which we rotate the data
        new_headers = [self.headers[0]] + self[self.headers[0]]

        _dset.headers = new_headers
        for index, column in enumerate(self.headers):

            if column == self.headers[0]:
                # It's in the headers, so skip it
                continue

            # Adding the column name as now they're a regular column
            # Use `get_col(index)` in case there are repeated values
            row_data = [column] + self.get_col(index)
            row_data = Row(row_data)
            _dset.append(row=row_data)
        return _dset

    def _transpose_without_headers(self):
        """Transpose a :class:`Dataset`, turning rows into columns and vice
        versa, returning a new ``Dataset`` instance. This instance should not
        have headers, or the dimension would be invalid."""

        _dset = Dataset()

        # Add columns as rows in new instance
        for index in range(len(self._data[0])):
            row_data = self.get_col(index)
            _dset.append(row=row_data)

        return _dset

    def transpose(self):
        """Transpose a :class:`Dataset`, turning rows into columns and vice
        versa, returning a new ``Dataset`` instance. If the instance has
        headers, the first row of the original instance becomes the new header
        row."""

        # Don't transpose if there is no data
        if not self:
            return

        if self.headers is None:
            return self._transpose_without_headers()
        else:
            return self._transpose_with_headers()

    def stack(self, other):
        """Stack two :class:`Dataset` instances together by
        joining at the row level, and return new combined
        ``Dataset`` instance."""

        if not isinstance(other, Dataset):
            return

        if self.width != other.width:
            raise InvalidDimensions

        # Copy the source data
        _dset = copy(self)

        rows_to_stack = list(_dset._data)
        other_rows = list(other._data)

        rows_to_stack.extend(other_rows)
        _dset._data = rows_to_stack

        return _dset

    def stack_cols(self, other):
        """Stack two :class:`Dataset` instances together by
        joining at the column level, and return a new
        combined ``Dataset`` instance. If either ``Dataset``
        has headers set, than the other must as well."""

        if not isinstance(other, Dataset):
            return

        if self.headers or other.headers:
            if not self.headers or not other.headers:
                raise HeadersNeeded

        if self.height != other.height:
            raise InvalidDimensions

        try:
            new_headers = self.headers + other.headers
        except TypeError:
            new_headers = None

        _dset = Dataset()

        for column in self.headers:
            _dset.append_col(col=self[column])

        for column in other.headers:
            _dset.append_col(col=other[column])

        _dset.headers = new_headers

        return _dset

    def remove_duplicates(self):
        """Removes all duplicate rows from the :class:`Dataset` object
        while maintaining the original order."""
        seen = set()
        self._data[:] = [
            row for row in self._data if not (tuple(row) in seen or seen.add(tuple(row)))
        ]

    def wipe(self):
        """Removes all content and headers from the :class:`Dataset` object."""
        self._data = []
        self.__headers = None

    def subset(self, rows=None, cols=None):
        """Returns a new instance of the :class:`Dataset`,
        including only specified rows and columns.
        """

        # Don't return if no data
        if not self:
            return

        if rows is None:
            rows = list(range(self.height))

        if cols is None:
            cols = list(self.headers)

        # filter out impossible rows and columns
        rows = [row for row in rows if row in range(self.height)]
        cols = [header for header in cols if header in self.headers]

        _dset = Dataset()

        # filtering rows and columns
        _dset.headers = list(cols)

        _dset._data = []
        for row_no, row in enumerate(self._data):
            data_row = []
            for key in _dset.headers:
                if key in self.headers:
                    pos = self.headers.index(key)
                    data_row.append(row[pos])
                else:
                    raise KeyError

            if row_no in rows:
                _dset.append(row=Row(data_row))

        return _dset


class Databook:
    """A book of :class:`Dataset` objects.
    """

    def __init__(self, sets=None):
        self._datasets = sets or []

    def __repr__(self):
        try:
            return f'<{self.title.lower()} databook>'
        except AttributeError:
            return '<databook object>'

    def wipe(self):
        """Removes all :class:`Dataset` objects from the :class:`Databook`."""
        self._datasets = []

    def sheets(self):
        return self._datasets

    def add_sheet(self, dataset):
        """Adds given :class:`Dataset` to the :class:`Databook`."""
        if isinstance(dataset, Dataset):
            self._datasets.append(dataset)
        else:
            raise InvalidDatasetType

    def _package(self):
        """Packages :class:`Databook` for delivery."""
        collector = []

        for dset in self._datasets:
            collector.append({
                'title': dset.title,
                'data': dset._package()
            })
        return collector

    @property
    def size(self):
        """The number of the :class:`Dataset` objects within :class:`Databook`."""
        return len(self._datasets)

    def load(self, in_stream, format, **kwargs):
        """
        Import `in_stream` to the :class:`Databook` object using the `format`.
        `in_stream` can be a file-like object, a string, or a bytestring.

        :param \\*\\*kwargs: (optional) custom configuration to the format `import_book`.
        """

        stream = normalize_input(in_stream)
        if not format:
            format = detect_format(stream)

        fmt = registry.get_format(format)
        if not hasattr(fmt, 'import_book'):
            raise UnsupportedFormat(f'Format {format} cannot be loaded.')

        fmt.import_book(self, stream, **kwargs)
        return self

    def export(self, format, **kwargs):
        """
        Export :class:`Databook` object to `format`.

        :param \\*\\*kwargs: (optional) custom configuration to the format `export_book`.
        """
        fmt = registry.get_format(format)
        if not hasattr(fmt, 'export_book'):
            raise UnsupportedFormat(f'Format {format} cannot be exported.')

        return fmt.export_book(self, **kwargs)


def detect_format(stream):
    """Return format name of given stream (file-like object, string, or bytestring)."""
    stream = normalize_input(stream)
    fmt_title = None
    for fmt in registry.formats():
        try:
            if fmt.detect(stream):
                fmt_title = fmt.title
                break
        except AttributeError:
            pass
        finally:
            if hasattr(stream, 'seek'):
                stream.seek(0)
    return fmt_title


def import_set(stream, format=None, **kwargs):
    """Return dataset of given stream (file-like object, string, or bytestring)."""

    return Dataset().load(stream, format, **kwargs)


def import_book(stream, format=None, **kwargs):
    """Return dataset of given stream (file-like object, string, or bytestring)."""

    return Databook().load(stream, format, **kwargs)


registry.register_builtins()
