from __future__ import absolute_import
# Copyright (c) 2010-2019 openpyxl

"""Manage individual cells in a spreadsheet.

The Cell class is required to know its value and type, display options,
and any other features of an Excel cell.  Utilities for referencing
cells using Excel's 'A1' column/row nomenclature are also provided.

"""

__docformat__ = "restructuredtext en"

# Python stdlib imports
from copy import copy
import datetime
import re

from itertools import islice, product

from openpyxl.compat import (
    unicode,
    basestring,
    bytes,
    NUMERIC_TYPES,
    deprecated,
)
from openpyxl.utils.units import (
    DEFAULT_ROW_HEIGHT,
    DEFAULT_COLUMN_WIDTH
)
from openpyxl.utils.datetime  import (
    to_excel,
    time_to_days,
    timedelta_to_days,
    from_excel
    )
from openpyxl.utils.exceptions import (
    IllegalCharacterError
)
from openpyxl.utils.units import points_to_pixels
from openpyxl.utils import (
    get_column_letter,
    column_index_from_string,
)
from openpyxl.utils.inference import (
    cast_numeric,
    cast_percentage,
    cast_percentage,
)
from openpyxl.styles import numbers, is_date_format
from openpyxl.styles.styleable import StyleableObject
from openpyxl.worksheet.hyperlink import Hyperlink

# constants

TIME_TYPES = (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)
TIME_FORMATS = {
    datetime.datetime:numbers.FORMAT_DATE_DATETIME,
    datetime.date:numbers.FORMAT_DATE_YYYYMMDD2,
    datetime.time:numbers.FORMAT_DATE_TIME6,
    datetime.timedelta:numbers.FORMAT_DATE_TIMEDELTA,
                }
try:
    from pandas import Timestamp
    TIME_TYPES = TIME_TYPES + (Timestamp,)
    TIME_FORMATS[Timestamp] = numbers.FORMAT_DATE_DATETIME
except ImportError:
    pass

STRING_TYPES = (basestring, unicode, bytes)
KNOWN_TYPES = NUMERIC_TYPES + TIME_TYPES + STRING_TYPES + (bool, type(None))

ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')
ERROR_CODES = ('#NULL!', '#DIV/0!', '#VALUE!', '#REF!', '#NAME?', '#NUM!',
               '#N/A')


ERROR_CODES = ERROR_CODES

TYPE_STRING = 's'
TYPE_FORMULA = 'f'
TYPE_NUMERIC = 'n'
TYPE_BOOL = 'b'
TYPE_NULL = 'n'
TYPE_INLINE = 'inlineStr'
TYPE_ERROR = 'e'
TYPE_FORMULA_CACHE_STRING = 'str'

VALID_TYPES = (TYPE_STRING, TYPE_FORMULA, TYPE_NUMERIC, TYPE_BOOL,
               TYPE_NULL, TYPE_INLINE, TYPE_ERROR, TYPE_FORMULA_CACHE_STRING)


_TYPES = {int:'n', float:'n', unicode:'s', basestring:'s', bool:'b'}


def get_type(t, value):
    if isinstance(value, NUMERIC_TYPES):
        dt = 'n'
    elif isinstance(value, STRING_TYPES):
        dt = 's'
    elif isinstance(value, TIME_TYPES):
        dt = 'd'
    else:
        return
    _TYPES[t] = dt
    return dt


class Cell(StyleableObject):
    """Describes cell associated properties.

    Properties of interest include style, type, value, and address.

    """
    __slots__ = (
        'row',
        'column',
        '_value',
        'data_type',
        'parent',
        '_hyperlink',
        '_comment',
                 )

    def __init__(self, worksheet, row=None, column=None, value=None, style_array=None):
        super(Cell, self).__init__(worksheet, style_array)
        self.row = row
        """Row number of this cell (1-based)"""
        self.column = column
        """Column number of this cell (1-based)"""
        # _value is the stored value, while value is the displayed value
        self._value = None
        self._hyperlink = None
        self.data_type = 'n'
        if value is not None:
            self.value = value
        self._comment = None


    @property
    def coordinate(self):
        """This cell's coordinate (ex. 'A5')"""
        col = get_column_letter(self.column)
        return "%s%d" % (col, self.row)


    @property
    def col_idx(self):
        """The numerical index of the column"""
        return self.column


    @property
    def column_letter(self):
        return get_column_letter(self.column)


    @property
    def encoding(self):
        return self.parent.encoding

    @property
    def base_date(self):
        return self.parent.parent.epoch

    @property
    def guess_types(self):
        return getattr(self.parent.parent, 'guess_types', False)

    def __repr__(self):
        return "<Cell {0!r}.{1}>".format(self.parent.title, self.coordinate)

    def check_string(self, value):
        """Check string coding, length, and line break character"""
        if value is None:
            return
        # convert to unicode string
        if not isinstance(value, unicode):
            value = unicode(value, self.encoding)
        value = unicode(value)
        # string must never be longer than 32,767 characters
        # truncate if necessary
        value = value[:32767]
        if next(ILLEGAL_CHARACTERS_RE.finditer(value), None):
            raise IllegalCharacterError
        return value

    def check_error(self, value):
        """Tries to convert Error" else N/A"""
        try:
            return unicode(value)
        except UnicodeDecodeError:
            return u'#N/A'

    @deprecated("Type coercion will no longer be supported")
    def set_explicit_value(self, value=None, data_type=TYPE_STRING):
        """Coerce values according to their explicit type"""
        if data_type not in VALID_TYPES:
            raise ValueError('Invalid data type: %s' % data_type)
        if isinstance(value, STRING_TYPES):
            value = self.check_string(value)
        self._value = value
        self.data_type = data_type


    def _bind_value(self, value):
        """Given a value, infer the correct data type"""

        self.data_type = "n"
        t = type(value)
        try:
            dt = _TYPES[t]
        except KeyError:
            dt = get_type(t, value)

        if dt is not None:
            self.data_type = dt

        if dt == 'n' or dt == 'b':
            pass

        elif dt == 'd':
            if not is_date_format(self.number_format):
                self.number_format = TIME_FORMATS[t]
            self.data_type = "d"

        elif dt == "s":
            value = self.check_string(value)
            if len(value) > 1 and value.startswith("="):
                self.data_type = 'f'
            elif value in ERROR_CODES:
                self.data_type = 'e'
            elif self.guess_types: # deprecated
                value = self._infer_value(value)

        elif value is not None:
            raise ValueError("Cannot convert {0!r} to Excel".format(value))

        self._value = value


    def _infer_value(self, value):
        """Given a string, infer type and formatting options."""
        if not isinstance(value, unicode):
            value = str(value)

        # number detection
        v = cast_numeric(value)
        if v is None:
            # percentage detection
            v = cast_percentage(value)
        if v is None:
            # time detection
            v = cast_percentage(value)

        return value


    @property
    def value(self):
        """Get or set the value held in the cell.

        :type: depends on the value (string, float, int or
            :class:`datetime.datetime`)
        """
        return self._value

    @value.setter
    def value(self, value):
        """Set the value and infer type and display options."""
        self._bind_value(value)

    @property
    def internal_value(self):
        """Always returns the value for excel."""
        return self._value

    @property
    def hyperlink(self):
        """Return the hyperlink target or an empty string"""
        return self._hyperlink


    @hyperlink.setter
    def hyperlink(self, val):
        """Set value and display for hyperlinks in a cell.
        Automatically sets the `value` of the cell with link text,
        but you can modify it afterwards by setting the `value`
        property, and the hyperlink will remain.
        Hyperlink is removed if set to ``None``."""
        if val is None:
            self._hyperlink = None
        else:
            if not isinstance(val, Hyperlink):
                val = Hyperlink(ref="", target=val)
            val.ref = self.coordinate
            self._hyperlink = val
            if self._value is None:
                self.value = val.target or val.location


    @property
    def is_date(self):
        """True if the value is formatted as a date

        :type: bool
        """
        return self.data_type == 'd' or (
            self.data_type == 'n' and is_date_format(self.number_format)
            )


    def offset(self, row=0, column=0):
        """Returns a cell location relative to this cell.

        :param row: number of rows to offset
        :type row: int

        :param column: number of columns to offset
        :type column: int

        :rtype: :class:`openpyxl.cell.Cell`
        """
        offset_column = self.col_idx + column
        offset_row = self.row + row
        return self.parent.cell(column=offset_column, row=offset_row)


    @property
    def comment(self):
        """ Returns the comment associated with this cell

            :type: :class:`openpyxl.comments.Comment`
        """
        return self._comment


    @comment.setter
    def comment(self, value):
        """
        Assign a comment to a cell
        """

        if value is not None:
            if value.parent:
                value = copy(value)
            value.bind(self)
        elif value is None and self._comment:
            self._comment.unbind()
        self._comment = value


class MergedCell(StyleableObject):

    """
    Describes the properties of a cell in a merged cell and helps to
    display the borders of the merged cell.

    The value of a MergedCell is always None.
    """

    __slots__ = ('row', 'column')

    _value = None
    data_type = "n"
    comment = None
    hyperlink = None


    def __init__(self, worksheet, row=None, column=None):
        super(MergedCell, self).__init__(worksheet)
        self.row = row
        self.column = column


    def __repr__(self):
        return "<MergedCell {0!r}.{1}>".format(self.parent.title, self.coordinate)

    coordinate = Cell.coordinate
    _comment = comment
    value = _value


def WriteOnlyCell(ws=None, value=None):
    return Cell(worksheet=ws, column=1, row=1, value=value)
