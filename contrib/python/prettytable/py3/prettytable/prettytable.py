#!/usr/bin/env python
#
# Copyright (c) 2009-2014, Luke Maurits <luke@maurits.id.au>
# All rights reserved.
# With contributions from:
#  * Chris Clark
#  * Klein Stephane
#  * John Filleau
#  * Vladimir Vrzić
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
#   derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import annotations

import io
import re
from enum import IntEnum
from functools import lru_cache
from html.parser import HTMLParser
from typing import Any, Literal, TypedDict, cast

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from sqlite3 import Cursor
    from typing import Final, TypeAlias

    from _typeshed import SupportsRichComparison
    from typing_extensions import Self


class HRuleStyle(IntEnum):
    FRAME = 0
    ALL = 1
    NONE = 2
    HEADER = 3


class VRuleStyle(IntEnum):
    FRAME = 0
    ALL = 1
    NONE = 2


class TableStyle(IntEnum):
    DEFAULT = 10
    MSWORD_FRIENDLY = 11
    PLAIN_COLUMNS = 12
    MARKDOWN = 13
    ORGMODE = 14
    DOUBLE_BORDER = 15
    SINGLE_BORDER = 16
    RANDOM = 20


# keep for backwards compatibility
_DEPRECATED_FRAME: Final = 0
_DEPRECATED_ALL: Final = 1
_DEPRECATED_NONE: Final = 2
_DEPRECATED_HEADER: Final = 3
_DEPRECATED_DEFAULT: Final = TableStyle.DEFAULT
_DEPRECATED_MSWORD_FRIENDLY: Final = TableStyle.MSWORD_FRIENDLY
_DEPRECATED_PLAIN_COLUMNS: Final = TableStyle.PLAIN_COLUMNS
_DEPRECATED_MARKDOWN: Final = TableStyle.MARKDOWN
_DEPRECATED_ORGMODE: Final = TableStyle.ORGMODE
_DEPRECATED_DOUBLE_BORDER: Final = TableStyle.DOUBLE_BORDER
_DEPRECATED_SINGLE_BORDER: Final = TableStyle.SINGLE_BORDER
_DEPRECATED_RANDOM: Final = TableStyle.RANDOM
# --------------------------------

BASE_ALIGN_VALUE: Final = "base_align_value"

RowType: TypeAlias = list[Any]
AlignType: TypeAlias = Literal["l", "c", "r"]
VAlignType: TypeAlias = Literal["t", "m", "b"]
HeaderStyleType: TypeAlias = Literal["cap", "title", "upper", "lower"] | None


class OptionsType(TypedDict):
    title: str | None
    start: int
    end: int | None
    fields: Sequence[str | None] | None
    header: bool
    use_header_width: bool
    border: bool
    preserve_internal_border: bool
    sortby: str | None
    reversesort: bool
    sort_key: Callable[[RowType], SupportsRichComparison]
    row_filter: Callable[[RowType], bool]
    attributes: dict[str, str]
    format: bool
    hrules: HRuleStyle
    vrules: VRuleStyle
    int_format: str | dict[str, str] | None
    float_format: str | dict[str, str] | None
    custom_format: (
        Callable[[str, Any], str] | dict[str, Callable[[str, Any], str]] | None
    )
    min_table_width: int | None
    max_table_width: int | None
    padding_width: int
    left_padding_width: int | None
    right_padding_width: int | None
    vertical_char: str
    horizontal_char: str
    horizontal_align_char: str
    junction_char: str
    header_style: HeaderStyleType
    xhtml: bool
    print_empty: bool
    oldsortslice: bool
    top_junction_char: str
    bottom_junction_char: str
    right_junction_char: str
    left_junction_char: str
    top_right_junction_char: str
    top_left_junction_char: str
    bottom_right_junction_char: str
    bottom_left_junction_char: str
    align: dict[str, AlignType]
    valign: dict[str, VAlignType]
    min_width: int | dict[str, int] | None
    max_width: int | dict[str, int] | None
    none_format: str | dict[str, str | None] | None
    escape_header: bool
    escape_data: bool
    break_on_hyphens: bool


# ANSI colour codes
_re = re.compile(r"\033\[[0-9;]*m|\033\(B")
# OSC 8 hyperlinks
_osc8_re = re.compile(r"\033\]8;;.*?\033\\(.*?)\033\]8;;\033\\")


@lru_cache
def _get_size(text: str) -> tuple[int, int]:
    lines = text.split("\n")
    height = len(lines)
    width = max(_str_block_width(line) for line in lines)
    return width, height


class PrettyTable:
    _xhtml: bool
    _align: dict[str, AlignType]
    _valign: dict[str, VAlignType]
    _min_width: dict[str, int]
    _max_width: dict[str, int]
    _min_table_width: int | None
    _max_table_width: int | None
    _fields: Sequence[str | None] | None
    _title: str | None
    _start: int
    _end: int | None
    _sortby: str | None
    _reversesort: bool
    _sort_key: Callable[[RowType], SupportsRichComparison]
    _row_filter: Callable[[RowType], bool]
    _header: bool
    _use_header_width: bool
    _header_style: HeaderStyleType
    _border: bool
    _preserve_internal_border: bool
    _hrules: HRuleStyle
    _vrules: VRuleStyle
    _int_format: dict[str, str]
    _float_format: dict[str, str]
    _custom_format: dict[str, Callable[[str, Any], str]]
    _padding_width: int
    _left_padding_width: int | None
    _right_padding_width: int | None
    _vertical_char: str
    _horizontal_char: str
    _horizontal_align_char: str | None
    _junction_char: str
    _top_junction_char: str | None
    _bottom_junction_char: str | None
    _right_junction_char: str | None
    _left_junction_char: str | None
    _top_right_junction_char: str | None
    _top_left_junction_char: str | None
    _bottom_right_junction_char: str | None
    _bottom_left_junction_char: str | None
    _format: bool
    _print_empty: bool
    _oldsortslice: bool
    _attributes: dict[str, str]
    _escape_header: bool
    _escape_data: bool
    _style: TableStyle | None
    orgmode: bool
    _widths: list[int]
    _hrule: str
    _break_on_hyphens: bool

    def __init__(self, field_names: Sequence[str] | None = None, **kwargs) -> None:
        """Return a new PrettyTable instance

        Arguments:

        encoding - Unicode encoding scheme used to decode any encoded input
        title - optional table title
        field_names - list or tuple of field names
        fields - list or tuple of field names to include in displays
        start - index of first data row to include in output
        end - index of last data row to include in output PLUS ONE (list slice style)
        header - print a header showing field names (True or False)
        use_header_width - reflect width of header (True or False)
        header_style - stylisation to apply to field names in header
            ("cap", "title", "upper", "lower" or None)
        border - print a border around the table (True or False)
        preserve_internal_border - print a border inside the table even if
            border is disabled (True or False)
        hrules - controls printing of horizontal rules after rows.
            Allowed values: HRuleStyle
        vrules - controls printing of vertical rules between columns.
            Allowed values: VRuleStyle
        int_format - controls formatting of integer data
        float_format - controls formatting of floating point data
        custom_format - controls formatting of any column using callable
        min_table_width - minimum desired table width, in characters
        max_table_width - maximum desired table width, in characters
        min_width - minimum desired field width, in characters
        max_width - maximum desired field width, in characters
        padding_width - number of spaces on either side of column data
            (only used if left and right paddings are None)
        left_padding_width - number of spaces on left hand side of column data
        right_padding_width - number of spaces on right hand side of column data
        vertical_char - single character string used to draw vertical lines
        horizontal_char - single character string used to draw horizontal lines
        horizontal_align_char - single character string used to indicate alignment
        junction_char - single character string used to draw line junctions
        top_junction_char - single character string used to draw top line junctions
        bottom_junction_char -
            single character string used to draw bottom line junctions
        right_junction_char - single character string used to draw right line junctions
        left_junction_char - single character string used to draw left line junctions
        top_right_junction_char -
            single character string used to draw top-right line junctions
        top_left_junction_char -
            single character string used to draw top-left line junctions
        bottom_right_junction_char -
            single character string used to draw bottom-right line junctions
        bottom_left_junction_char -
            single character string used to draw bottom-left line junctions
        sortby - name of field to sort rows by
        sort_key - sorting key function, applied to data points before sorting
        row_filter - filter function applied on rows
        align - default align for each column (None, "l", "c" or "r")
        valign - default valign for each row (None, "t", "m" or "b")
        reversesort - True or False to sort in descending or ascending order
        oldsortslice - Slice rows before sorting in the "old style"
        break_on_hyphens - Whether long lines are broken on hypens or not, default: True
        """
        self.encoding = kwargs.get("encoding", "UTF-8")

        # Data
        self._field_names: list[str] = []
        self._rows: list[RowType] = []
        self._dividers: list[bool] = []
        self.align = {}
        self.valign = {}
        self.max_width = {}
        self.min_width = {}
        self.int_format = {}
        self.float_format = {}
        self.custom_format = {}
        self._style = None

        # Options
        self._options = [
            "title",
            "start",
            "end",
            "fields",
            "header",
            "use_header_width",
            "border",
            "preserve_internal_border",
            "sortby",
            "reversesort",
            "sort_key",
            "row_filter",
            "attributes",
            "format",
            "hrules",
            "vrules",
            "int_format",
            "float_format",
            "custom_format",
            "min_table_width",
            "max_table_width",
            "padding_width",
            "left_padding_width",
            "right_padding_width",
            "vertical_char",
            "horizontal_char",
            "horizontal_align_char",
            "junction_char",
            "header_style",
            "xhtml",
            "print_empty",
            "oldsortslice",
            "top_junction_char",
            "bottom_junction_char",
            "right_junction_char",
            "left_junction_char",
            "top_right_junction_char",
            "top_left_junction_char",
            "bottom_right_junction_char",
            "bottom_left_junction_char",
            "align",
            "valign",
            "max_width",
            "min_width",
            "none_format",
            "escape_header",
            "escape_data",
            "break_on_hyphens",
        ]

        self._none_format: dict[str, str | None] = {}
        self._kwargs = {}
        if field_names:
            self.field_names = field_names
        else:
            self._widths: list[int] = []

        for option in self._options:
            if option in kwargs:
                self._validate_option(option, kwargs[option])
                self._kwargs[option] = kwargs[option]
            else:
                kwargs[option] = None
                self._kwargs[option] = None

        self._title = kwargs["title"] or None
        self._start = kwargs["start"] or 0
        self._end = kwargs["end"] or None
        self._fields = kwargs["fields"] or None

        if kwargs["header"] in (True, False):
            self._header = kwargs["header"]
        else:
            self._header = True
        if kwargs["use_header_width"] in (True, False):
            self._use_header_width = kwargs["use_header_width"]
        else:
            self._use_header_width = True
        self._header_style = kwargs["header_style"] or None
        if kwargs["border"] in (True, False):
            self._border = kwargs["border"]
        else:
            self._border = True
        if kwargs["preserve_internal_border"] in (True, False):
            self._preserve_internal_border = kwargs["preserve_internal_border"]
        else:
            self._preserve_internal_border = False
        self._hrules = kwargs["hrules"] or HRuleStyle.FRAME
        self._vrules = kwargs["vrules"] or VRuleStyle.ALL

        self._sortby = kwargs["sortby"] or None
        if kwargs["reversesort"] in (True, False):
            self._reversesort = kwargs["reversesort"]
        else:
            self._reversesort = False
        self._sort_key = kwargs["sort_key"] or (lambda x: x)
        self._row_filter = kwargs["row_filter"] or (lambda x: True)

        if kwargs["escape_data"] in (True, False):
            self._escape_data = kwargs["escape_data"]
        else:
            self._escape_data = True
        if kwargs["escape_header"] in (True, False):
            self._escape_header = kwargs["escape_header"]
        else:
            self._escape_header = True

        self._column_specific_args()

        self._min_table_width = kwargs["min_table_width"] or None
        self._max_table_width = kwargs["max_table_width"] or None
        if kwargs["padding_width"] is None:
            self._padding_width = 1
        else:
            self._padding_width = kwargs["padding_width"]
        self._left_padding_width = kwargs["left_padding_width"] or None
        self._right_padding_width = kwargs["right_padding_width"] or None

        self._vertical_char = kwargs["vertical_char"] or "|"
        self._horizontal_char = kwargs["horizontal_char"] or "-"
        self._horizontal_align_char = kwargs["horizontal_align_char"]
        self._junction_char = kwargs["junction_char"] or "+"
        self._top_junction_char = kwargs["top_junction_char"]
        self._bottom_junction_char = kwargs["bottom_junction_char"]
        self._right_junction_char = kwargs["right_junction_char"]
        self._left_junction_char = kwargs["left_junction_char"]
        self._top_right_junction_char = kwargs["top_right_junction_char"]
        self._top_left_junction_char = kwargs["top_left_junction_char"]
        self._bottom_right_junction_char = kwargs["bottom_right_junction_char"]
        self._bottom_left_junction_char = kwargs["bottom_left_junction_char"]

        if kwargs["print_empty"] in (True, False):
            self._print_empty = kwargs["print_empty"]
        else:
            self._print_empty = True
        if kwargs["oldsortslice"] in (True, False):
            self._oldsortslice = kwargs["oldsortslice"]
        else:
            self._oldsortslice = False
        self._format = kwargs["format"] or False
        self._xhtml = kwargs["xhtml"] or False
        self._attributes = kwargs["attributes"] or {}
        if kwargs["break_on_hyphens"] in (True, False):
            self._break_on_hyphens = kwargs["break_on_hyphens"]
        else:
            self._break_on_hyphens = True

    def _column_specific_args(self) -> None:
        # Column specific arguments, use property.setters
        for attr in (
            "align",
            "valign",
            "max_width",
            "min_width",
            "int_format",
            "float_format",
            "custom_format",
            "none_format",
        ):
            setattr(
                self, attr, (self._kwargs[attr] or {}) if attr in self._kwargs else {}
            )

    def _justify(self, text: str, width: int, align: AlignType) -> str:
        excess = width - _str_block_width(text)
        if align == "l":
            return text + excess * " "
        elif align == "r":
            return excess * " " + text
        else:
            if excess % 2:
                # Uneven padding
                # Put more space on right if text is of odd length...
                if _str_block_width(text) % 2:
                    return (excess // 2) * " " + text + (excess // 2 + 1) * " "
                # and more space on left if text is of even length
                else:
                    return (excess // 2 + 1) * " " + text + (excess // 2) * " "
                # Why distribute extra space this way?  To match the behaviour of
                # the inbuilt str.center() method.
            else:
                # Equal padding on either side
                return (excess // 2) * " " + text + (excess // 2) * " "

    def __getattr__(self, name):
        if name == "rowcount":
            return len(self._rows)
        elif name == "colcount":
            if self._field_names:
                return len(self._field_names)
            elif self._rows:
                return len(self._rows[0])
            else:
                return 0
        else:
            raise AttributeError(name)

    def __getitem__(self, index: int | slice) -> PrettyTable:
        new = PrettyTable()
        new.field_names = self.field_names
        for attr in self._options:
            setattr(new, "_" + attr, getattr(self, "_" + attr))
        setattr(new, "_align", getattr(self, "_align"))
        if isinstance(index, slice):
            for row in self._rows[index]:
                new.add_row(row)
        elif isinstance(index, int):
            new.add_row(self._rows[index])
        else:
            msg = f"Index {index} is invalid, must be an integer or slice"
            raise IndexError(msg)
        return new

    def __str__(self) -> str:
        return self.get_string()

    def __repr__(self) -> str:
        return self.get_string()

    def _repr_html_(self) -> str:
        """
        Returns get_html_string value by default
        as the repr call in Jupyter notebook environment
        """
        return self.get_html_string()

    ##############################
    # ATTRIBUTE VALIDATORS       #
    ##############################

    # The method _validate_option is all that should be used elsewhere in the code base
    # to validate options. It will call the appropriate validation method for that
    # option. The individual validation methods should never need to be called directly
    # (although nothing bad will happen if they *are*).
    # Validation happens in TWO places.
    # Firstly, in the property setters defined in the ATTRIBUTE MANAGEMENT section.
    # Secondly, in the _get_options method, where keyword arguments are mixed with
    # persistent settings

    def _validate_option(self, option, val) -> None:
        if option == "field_names":
            self._validate_field_names(val)
        elif option == "none_format":
            self._validate_none_format(val)
        elif option in (
            "start",
            "end",
            "max_width",
            "min_width",
            "min_table_width",
            "max_table_width",
            "padding_width",
            "left_padding_width",
            "right_padding_width",
        ):
            self._validate_nonnegative_int(option, val)
        elif option == "sortby":
            self._validate_field_name(option, val)
        elif option in ("sort_key", "row_filter"):
            self._validate_function(option, val)
        elif option == "hrules":
            self._validate_hrules(option, val)
        elif option == "vrules":
            self._validate_vrules(option, val)
        elif option == "fields":
            self._validate_all_field_names(option, val)
        elif option in (
            "header",
            "use_header_width",
            "border",
            "preserve_internal_border",
            "reversesort",
            "xhtml",
            "format",
            "print_empty",
            "oldsortslice",
            "escape_header",
            "escape_data",
            "break_on_hyphens",
        ):
            self._validate_true_or_false(option, val)
        elif option == "header_style":
            self._validate_header_style(val)
        elif option == "int_format":
            self._validate_int_format(option, val)
        elif option == "float_format":
            self._validate_float_format(option, val)
        elif option == "custom_format":
            for k, formatter in val.items():
                self._validate_function(f"{option}.{k}", formatter)
        elif option in (
            "vertical_char",
            "horizontal_char",
            "horizontal_align_char",
            "junction_char",
            "top_junction_char",
            "bottom_junction_char",
            "right_junction_char",
            "left_junction_char",
            "top_right_junction_char",
            "top_left_junction_char",
            "bottom_right_junction_char",
            "bottom_left_junction_char",
        ):
            self._validate_single_char(option, val)
        elif option == "attributes":
            self._validate_attributes(option, val)

    def _validate_field_names(self, val):
        # Check for appropriate length
        if self._field_names:
            try:
                assert len(val) == len(self._field_names)
            except AssertionError:
                msg = (
                    "Field name list has incorrect number of values, "
                    f"(actual) {len(val)}!={len(self._field_names)} (expected)"
                )
                raise ValueError(msg)
        if self._rows:
            try:
                assert len(val) == len(self._rows[0])
            except AssertionError:
                msg = (
                    "Field name list has incorrect number of values, "
                    f"(actual) {len(val)}!={len(self._rows[0])} (expected)"
                )
                raise ValueError(msg)
        # Check for uniqueness
        try:
            assert len(val) == len(set(val))
        except AssertionError:
            msg = "Field names must be unique"
            raise ValueError(msg)

    def _validate_none_format(self, val):
        try:
            if val is not None:
                assert isinstance(val, str)
        except AssertionError:
            msg = "Replacement for None value must be a string if being supplied."
            raise TypeError(msg)

    def _validate_header_style(self, val):
        try:
            assert val in ("cap", "title", "upper", "lower", None)
        except AssertionError:
            msg = "Invalid header style, use cap, title, upper, lower or None"
            raise ValueError(msg)

    def _validate_align(self, val):
        try:
            assert val in ["l", "c", "r"]
        except AssertionError:
            msg = f"Alignment {val} is invalid, use l, c or r"
            raise ValueError(msg)

    def _validate_valign(self, val):
        try:
            assert val in ["t", "m", "b"]
        except AssertionError:
            msg = f"Alignment {val} is invalid, use t, m, b"
            raise ValueError(msg)

    def _validate_nonnegative_int(self, name, val):
        try:
            assert int(val) >= 0
        except AssertionError:
            msg = f"Invalid value for {name}: {val}"
            raise ValueError(msg)

    def _validate_true_or_false(self, name, val):
        try:
            assert val in (True, False)
        except AssertionError:
            msg = f"Invalid value for {name}. Must be True or False."
            raise ValueError(msg)

    def _validate_int_format(self, name, val):
        if val == "":
            return
        try:
            assert isinstance(val, str)
            assert val.isdigit()
        except AssertionError:
            msg = f"Invalid value for {name}. Must be an integer format string."
            raise ValueError(msg)

    def _validate_float_format(self, name, val):
        if val == "":
            return
        try:
            assert isinstance(val, str)
            assert "." in val
            bits = val.split(".")
            assert len(bits) <= 2
            assert bits[0] == "" or bits[0].isdigit()
            assert (
                bits[1] == ""
                or bits[1].isdigit()
                or (bits[1][-1] == "f" and bits[1].rstrip("f").isdigit())
            )
        except AssertionError:
            msg = f"Invalid value for {name}. Must be a float format string."
            raise ValueError(msg)

    def _validate_function(self, name, val):
        try:
            assert hasattr(val, "__call__")
        except AssertionError:
            msg = f"Invalid value for {name}. Must be a function."
            raise ValueError(msg)

    def _validate_hrules(self, name, val):
        try:
            assert val in list(HRuleStyle)
        except AssertionError:
            msg = f"Invalid value for {name}. Must be HRuleStyle."
            raise ValueError(msg)

    def _validate_vrules(self, name, val):
        try:
            assert val in list(VRuleStyle)
        except AssertionError:
            msg = f"Invalid value for {name}. Must be VRuleStyle."
            raise ValueError(msg)

    def _validate_field_name(self, name, val):
        try:
            assert (val in self._field_names) or (val is None)
        except AssertionError:
            msg = f"Invalid field name: {val}"
            raise ValueError(msg)

    def _validate_all_field_names(self, name, val):
        try:
            for x in val:
                self._validate_field_name(name, x)
        except AssertionError:
            msg = "Fields must be a sequence of field names"
            raise ValueError(msg)

    def _validate_single_char(self, name, val):
        try:
            assert _str_block_width(val) == 1
        except AssertionError:
            msg = f"Invalid value for {name}. Must be a string of length 1."
            raise ValueError(msg)

    def _validate_attributes(self, name, val):
        try:
            assert isinstance(val, dict)
        except AssertionError:
            msg = "Attributes must be a dictionary of name/value pairs"
            raise TypeError(msg)

    ##############################
    # ATTRIBUTE MANAGEMENT       #
    ##############################
    @property
    def rows(self) -> list[RowType]:
        return self._rows[:]

    @property
    def dividers(self) -> list[bool]:
        return self._dividers[:]

    @property
    def xhtml(self) -> bool:
        """Print <br/> tags if True, <br> tags if False"""
        return self._xhtml

    @xhtml.setter
    def xhtml(self, val: bool) -> None:
        self._validate_option("xhtml", val)
        self._xhtml = val

    @property
    def none_format(self) -> dict[str, str | None]:
        return self._none_format

    @none_format.setter
    def none_format(self, val: str | dict[str, str | None] | None):
        """Representation of None values:

        Arguments:

        val - The alternative representation to be used for None values
        """
        if not self._field_names:
            self._none_format = {}
        elif isinstance(val, str):
            for field in self._field_names:
                self._none_format[field] = None
            self._validate_none_format(val)
            for field in self._field_names:
                self._none_format[field] = val
        elif isinstance(val, dict) and val:
            for field, fval in val.items():
                self._validate_none_format(fval)
                self._none_format[field] = fval
        else:
            for field in self._field_names:
                self._none_format[field] = None

    @property
    def field_names(self) -> list[str]:
        """List or tuple of field names

        When setting field_names, if there are already field names the new list
        of field names must be the same length. Columns are renamed and row data
        remains unchanged."""
        return self._field_names

    @field_names.setter
    def field_names(self, val: Sequence[Any]) -> None:
        val = cast("list[str]", [str(x) for x in val])
        self._validate_option("field_names", val)
        old_names = None
        if self._field_names:
            old_names = self._field_names[:]
        self._field_names = val

        self._column_specific_args()

        if self._align and old_names:
            for old_name, new_name in zip(old_names, val):
                self._align[new_name] = self._align[old_name]
            for old_name in old_names:
                if old_name not in self._align:
                    self._align.pop(old_name)
        elif self._align:
            for field_name in self._field_names:
                self._align[field_name] = self._align[BASE_ALIGN_VALUE]
        else:
            self.align = "c"
        if self._valign and old_names:
            for old_name, new_name in zip(old_names, val):
                self._valign[new_name] = self._valign[old_name]
            for old_name in old_names:
                if old_name not in self._valign:
                    self._valign.pop(old_name)
        else:
            self.valign = "t"

    @property
    def align(self) -> dict[str, AlignType]:
        """Controls alignment of fields
        Arguments:

        align - alignment, one of "l", "c", or "r" """
        return self._align

    @align.setter
    def align(self, val: AlignType | dict[str, AlignType] | None) -> None:
        if isinstance(val, str):
            self._validate_align(val)
            if not self._field_names:
                self._align = {BASE_ALIGN_VALUE: val}
            else:
                for field in self._field_names:
                    self._align[field] = val
        elif isinstance(val, dict) and val:
            for field, fval in val.items():
                self._validate_align(fval)
                self._align[field] = fval
        else:
            if not self._field_names:
                self._align = {BASE_ALIGN_VALUE: "c"}
            else:
                for field in self._field_names:
                    self._align[field] = "c"

    @property
    def valign(self) -> dict[str, VAlignType]:
        """Controls vertical alignment of fields
        Arguments:

        valign - vertical alignment, one of "t", "m", or "b" """
        return self._valign

    @valign.setter
    def valign(self, val: VAlignType | dict[str, VAlignType] | None) -> None:
        if not self._field_names:
            self._valign = {}
        if isinstance(val, str):
            self._validate_valign(val)
            for field in self._field_names:
                self._valign[field] = val
        elif isinstance(val, dict) and val:
            for field, fval in val.items():
                self._validate_valign(fval)
                self._valign[field] = fval
        else:
            for field in self._field_names:
                self._valign[field] = "t"

    @property
    def max_width(self) -> dict[str, int]:
        """Controls maximum width of fields
        Arguments:

        max_width - maximum width integer"""
        return self._max_width

    @max_width.setter
    def max_width(self, val: int | dict[str, int] | None) -> None:
        if isinstance(val, int):
            self._validate_option("max_width", val)
            for field in self._field_names:
                self._max_width[field] = val
        elif isinstance(val, dict) and val:
            for field, fval in val.items():
                self._validate_option("max_width", fval)
                self._max_width[field] = fval
        else:
            self._max_width = {}

    @property
    def min_width(self) -> dict[str, int]:
        """Controls minimum width of fields
        Arguments:

        min_width - minimum width integer"""
        return self._min_width

    @min_width.setter
    def min_width(self, val: int | dict[str, int] | None) -> None:
        if isinstance(val, int):
            self._validate_option("min_width", val)
            for field in self._field_names:
                self._min_width[field] = val
        elif isinstance(val, dict) and val:
            for field, fval in val.items():
                self._validate_option("min_width", fval)
                self._min_width[field] = fval
        else:
            self._min_width = {}

    @property
    def min_table_width(self) -> int | None:
        return self._min_table_width

    @min_table_width.setter
    def min_table_width(self, val: int) -> None:
        self._validate_option("min_table_width", val)
        self._min_table_width = val

    @property
    def max_table_width(self) -> int | None:
        return self._max_table_width

    @max_table_width.setter
    def max_table_width(self, val: int) -> None:
        self._validate_option("max_table_width", val)
        self._max_table_width = val

    @property
    def fields(self) -> Sequence[str | None] | None:
        """List or tuple of field names to include in displays"""
        return self._fields

    @fields.setter
    def fields(self, val: Sequence[str | None]) -> None:
        self._validate_option("fields", val)
        self._fields = val

    @property
    def title(self) -> str | None:
        """Optional table title

        Arguments:

        title - table title"""
        return self._title

    @title.setter
    def title(self, val: str) -> None:
        self._title = str(val)

    @property
    def start(self) -> int:
        """Start index of the range of rows to print

        Arguments:

        start - index of first data row to include in output"""
        return self._start

    @start.setter
    def start(self, val: int) -> None:
        self._validate_option("start", val)
        self._start = val

    @property
    def end(self) -> int | None:
        """End index of the range of rows to print

        Arguments:

        end - index of last data row to include in output PLUS ONE (list slice style)"""
        return self._end

    @end.setter
    def end(self, val: int) -> None:
        self._validate_option("end", val)
        self._end = val

    @property
    def sortby(self) -> str | None:
        """Name of field by which to sort rows

        Arguments:

        sortby - field name to sort by"""
        return self._sortby

    @sortby.setter
    def sortby(self, val: str | None) -> None:
        self._validate_option("sortby", val)
        self._sortby = val

    @property
    def reversesort(self) -> bool:
        """Controls direction of sorting (ascending vs descending)

        Arguments:

        reveresort - set to True to sort by descending order, or False to sort by
            ascending order"""
        return self._reversesort

    @reversesort.setter
    def reversesort(self, val: bool) -> None:
        self._validate_option("reversesort", val)
        self._reversesort = val

    @property
    def sort_key(self) -> Callable[[RowType], SupportsRichComparison]:
        """Sorting key function, applied to data points before sorting

        Arguments:

        sort_key - a function which takes one argument and returns something to be
        sorted"""
        return self._sort_key

    @sort_key.setter
    def sort_key(self, val: Callable[[RowType], SupportsRichComparison]) -> None:
        self._validate_option("sort_key", val)
        self._sort_key = val

    @property
    def row_filter(self) -> Callable[[RowType], bool]:
        """Filter function, applied to data points

        Arguments:

        row_filter - a function which takes one argument and returns a Boolean"""
        return self._row_filter

    @row_filter.setter
    def row_filter(self, val: Callable[[RowType], bool]) -> None:
        self._validate_option("row_filter", val)
        self._row_filter = val

    @property
    def header(self) -> bool:
        """Controls printing of table header with field names

        Arguments:

        header - print a header showing field names (True or False)"""
        return self._header

    @header.setter
    def header(self, val: bool) -> None:
        self._validate_option("header", val)
        self._header = val

    @property
    def use_header_width(self) -> bool:
        """Controls whether header is included in computing width

        Arguments:

        use_header_width - respect width of fieldname in header to calculate column
            width (True or False)
        """
        return self._use_header_width

    @use_header_width.setter
    def use_header_width(self, val: bool) -> None:
        self._validate_option("use_header_width", val)
        self._use_header_width = val

    @property
    def header_style(self) -> HeaderStyleType:
        """Controls stylisation applied to field names in header

        Arguments:

        header_style - stylisation to apply to field names in header
            ("cap", "title", "upper", "lower" or None)"""
        return self._header_style

    @header_style.setter
    def header_style(self, val: HeaderStyleType) -> None:
        self._validate_header_style(val)
        self._header_style = val

    @property
    def border(self) -> bool:
        """Controls printing of border around table

        Arguments:

        border - print a border around the table (True or False)"""
        return self._border

    @border.setter
    def border(self, val: bool) -> None:
        self._validate_option("border", val)
        self._border = val

    @property
    def preserve_internal_border(self) -> bool:
        """Controls printing of border inside table

        Arguments:

        preserve_internal_border - print a border inside the table even if
            border is disabled (True or False)"""
        return self._preserve_internal_border

    @preserve_internal_border.setter
    def preserve_internal_border(self, val: bool) -> None:
        self._validate_option("preserve_internal_border", val)
        self._preserve_internal_border = val

    @property
    def hrules(self) -> HRuleStyle:
        """Controls printing of horizontal rules after rows

        Arguments:

        hrules - horizontal rules style.  Allowed values: HRuleStyle"""
        return self._hrules

    @hrules.setter
    def hrules(self, val: HRuleStyle) -> None:
        self._validate_option("hrules", val)
        self._hrules = val

    @property
    def vrules(self) -> VRuleStyle:
        """Controls printing of vertical rules between columns

        Arguments:

        vrules - vertical rules style.  Allowed values: VRuleStyle"""
        return self._vrules

    @vrules.setter
    def vrules(self, val: VRuleStyle) -> None:
        self._validate_option("vrules", val)
        self._vrules = val

    @property
    def int_format(self) -> dict[str, str]:
        """Controls formatting of integer data
        Arguments:

        int_format - integer format string"""
        return self._int_format

    @int_format.setter
    def int_format(self, val: str | dict[str, str] | None) -> None:
        if isinstance(val, str):
            self._validate_option("int_format", val)
            for field in self._field_names:
                self._int_format[field] = val
        elif isinstance(val, dict) and val:
            for field, fval in val.items():
                self._validate_option("int_format", fval)
                self._int_format[field] = fval
        else:
            self._int_format = {}

    @property
    def float_format(self) -> dict[str, str]:
        """Controls formatting of floating point data
        Arguments:

        float_format - floating point format string"""
        return self._float_format

    @float_format.setter
    def float_format(self, val: str | dict[str, str] | None) -> None:
        if isinstance(val, str):
            self._validate_option("float_format", val)
            for field in self._field_names:
                self._float_format[field] = val
        elif isinstance(val, dict) and val:
            for field, fval in val.items():
                self._validate_option("float_format", fval)
                self._float_format[field] = fval
        else:
            self._float_format = {}

    @property
    def custom_format(self) -> dict[str, Callable[[str, Any], str]]:
        """Controls formatting of any column using callable
        Arguments:

        custom_format - Dictionary of field_name and callable"""
        return self._custom_format

    @custom_format.setter
    def custom_format(
        self,
        val: Callable[[str, Any], str] | dict[str, Callable[[str, Any], str]] | None,
    ):
        if val is None:
            self._custom_format = {}
        elif isinstance(val, dict):
            for k, v in val.items():
                self._validate_function(f"custom_value.{k}", v)
            self._custom_format = val
        elif hasattr(val, "__call__"):
            self._validate_function("custom_value", val)
            for field in self._field_names:
                self._custom_format[field] = val
        else:
            msg = "The custom_format property need to be a dictionary or callable"
            raise TypeError(msg)

    @property
    def padding_width(self) -> int:
        """The number of empty spaces between a column's edge and its content

        Arguments:

        padding_width - number of spaces, must be a positive integer"""
        return self._padding_width

    @padding_width.setter
    def padding_width(self, val: int) -> None:
        self._validate_option("padding_width", val)
        self._padding_width = val

    @property
    def left_padding_width(self) -> int | None:
        """The number of empty spaces between a column's left edge and its content

        Arguments:

        left_padding - number of spaces, must be a positive integer"""
        return self._left_padding_width

    @left_padding_width.setter
    def left_padding_width(self, val: int) -> None:
        self._validate_option("left_padding_width", val)
        self._left_padding_width = val

    @property
    def right_padding_width(self) -> int | None:
        """The number of empty spaces between a column's right edge and its content

        Arguments:

        right_padding - number of spaces, must be a positive integer"""
        return self._right_padding_width

    @right_padding_width.setter
    def right_padding_width(self, val: int) -> None:
        self._validate_option("right_padding_width", val)
        self._right_padding_width = val

    @property
    def vertical_char(self) -> str:
        """The character used when printing table borders to draw vertical lines

        Arguments:

        vertical_char - single character string used to draw vertical lines"""
        return self._vertical_char

    @vertical_char.setter
    def vertical_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("vertical_char", val)
        self._vertical_char = val

    @property
    def horizontal_char(self) -> str:
        """The character used when printing table borders to draw horizontal lines

        Arguments:

        horizontal_char - single character string used to draw horizontal lines"""
        return self._horizontal_char

    @horizontal_char.setter
    def horizontal_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("horizontal_char", val)
        self._horizontal_char = val

    @property
    def horizontal_align_char(self) -> str:
        """The character used to indicate column alignment in horizontal lines

        Arguments:

        horizontal_align_char - single character string used to indicate alignment"""
        return self._bottom_left_junction_char or self.junction_char

    @horizontal_align_char.setter
    def horizontal_align_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("horizontal_align_char", val)
        self._horizontal_align_char = val

    @property
    def junction_char(self) -> str:
        """The character used when printing table borders to draw line junctions

        Arguments:

        junction_char - single character string used to draw line junctions"""
        return self._junction_char

    @junction_char.setter
    def junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("junction_char", val)
        self._junction_char = val

    @property
    def top_junction_char(self) -> str:
        """The character used when printing table borders to draw top line junctions

        Arguments:

        top_junction_char - single character string used to draw top line junctions"""
        return self._top_junction_char or self.junction_char

    @top_junction_char.setter
    def top_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("top_junction_char", val)
        self._top_junction_char = val

    @property
    def bottom_junction_char(self) -> str:
        """The character used when printing table borders to draw bottom line junctions

        Arguments:

        bottom_junction_char -
            single character string used to draw bottom line junctions"""
        return self._bottom_junction_char or self.junction_char

    @bottom_junction_char.setter
    def bottom_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("bottom_junction_char", val)
        self._bottom_junction_char = val

    @property
    def right_junction_char(self) -> str:
        """The character used when printing table borders to draw right line junctions

        Arguments:

        right_junction_char -
            single character string used to draw right line junctions"""
        return self._right_junction_char or self.junction_char

    @right_junction_char.setter
    def right_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("right_junction_char", val)
        self._right_junction_char = val

    @property
    def left_junction_char(self) -> str:
        """The character used when printing table borders to draw left line junctions

        Arguments:

        left_junction_char - single character string used to draw left line junctions"""
        return self._left_junction_char or self.junction_char

    @left_junction_char.setter
    def left_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("left_junction_char", val)
        self._left_junction_char = val

    @property
    def top_right_junction_char(self) -> str:
        """
        The character used when printing table borders to draw top-right line junctions

        Arguments:

        top_right_junction_char -
            single character string used to draw top-right line junctions"""
        return self._top_right_junction_char or self.junction_char

    @top_right_junction_char.setter
    def top_right_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("top_right_junction_char", val)
        self._top_right_junction_char = val

    @property
    def top_left_junction_char(self) -> str:
        """
        The character used when printing table borders to draw top-left line junctions

        Arguments:

        top_left_junction_char -
            single character string used to draw top-left line junctions"""
        return self._top_left_junction_char or self.junction_char

    @top_left_junction_char.setter
    def top_left_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("top_left_junction_char", val)
        self._top_left_junction_char = val

    @property
    def bottom_right_junction_char(self) -> str:
        """The character used when printing table borders
           to draw bottom-right line junctions

        Arguments:

        bottom_right_junction_char -
            single character string used to draw bottom-right line junctions"""
        return self._bottom_right_junction_char or self.junction_char

    @bottom_right_junction_char.setter
    def bottom_right_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("bottom_right_junction_char", val)
        self._bottom_right_junction_char = val

    @property
    def bottom_left_junction_char(self) -> str:
        """The character used when printing table borders
           to draw bottom-left line junctions

        Arguments:

        bottom_left_junction_char -
            single character string used to draw bottom-left line junctions"""
        return self._bottom_left_junction_char or self.junction_char

    @bottom_left_junction_char.setter
    def bottom_left_junction_char(self, val: str) -> None:
        val = str(val)
        self._validate_option("bottom_left_junction_char", val)
        self._bottom_left_junction_char = val

    @property
    def format(self) -> bool:
        """Controls whether or not HTML tables are formatted to match styling options

        Arguments:

        format - True or False"""
        return self._format

    @format.setter
    def format(self, val: bool) -> None:
        self._validate_option("format", val)
        self._format = val

    @property
    def print_empty(self) -> bool:
        """Controls whether or not empty tables produce a header and frame or just an
        empty string

        Arguments:

        print_empty - True or False"""
        return self._print_empty

    @print_empty.setter
    def print_empty(self, val: bool) -> None:
        self._validate_option("print_empty", val)
        self._print_empty = val

    @property
    def attributes(self) -> dict[str, str]:
        """A dictionary of HTML attribute name/value pairs to be included in the
        <table> tag when printing HTML

        Arguments:

        attributes - dictionary of attributes"""
        return self._attributes

    @attributes.setter
    def attributes(self, val: dict[str, str]) -> None:
        self._validate_option("attributes", val)
        self._attributes = val

    @property
    def oldsortslice(self) -> bool:
        """oldsortslice - Slice rows before sorting in the "old style" """
        return self._oldsortslice

    @oldsortslice.setter
    def oldsortslice(self, val: bool) -> None:
        self._validate_option("oldsortslice", val)
        self._oldsortslice = val

    @property
    def escape_header(self) -> bool:
        """Escapes the text within a header (True or False)"""
        return self._escape_header

    @escape_header.setter
    def escape_header(self, val: bool) -> None:
        self._validate_option("escape_header", val)
        self._escape_header = val

    @property
    def escape_data(self) -> bool:
        """Escapes the text within a data field (True or False)"""
        return self._escape_data

    @escape_data.setter
    def escape_data(self, val: bool) -> None:
        self._validate_option("escape_data", val)
        self._escape_data = val

    @property
    def break_on_hyphens(self) -> bool:
        """Break longlines on hyphens (True or False)"""
        return self._break_on_hyphens

    @break_on_hyphens.setter
    def break_on_hyphens(self, val: bool) -> None:
        self._validate_option("break_on_hyphens", val)
        self._break_on_hyphens = val

    ##############################
    # OPTION MIXER               #
    ##############################

    def _get_options(self, kwargs: Mapping[str, Any]) -> OptionsType:
        options: dict[str, Any] = {}
        for option in self._options:
            if option in kwargs:
                self._validate_option(option, kwargs[option])
                options[option] = kwargs[option]
            else:
                options[option] = getattr(self, option)
        return cast(OptionsType, options)

    ##############################
    # PRESET STYLE LOGIC         #
    ##############################

    def set_style(self, style: TableStyle) -> None:
        self._set_default_style()
        self._style = style
        if style == TableStyle.MSWORD_FRIENDLY:
            self._set_msword_style()
        elif style == TableStyle.PLAIN_COLUMNS:
            self._set_columns_style()
        elif style == TableStyle.MARKDOWN:
            self._set_markdown_style()
        elif style == TableStyle.ORGMODE:
            self._set_orgmode_style()
        elif style == TableStyle.DOUBLE_BORDER:
            self._set_double_border_style()
        elif style == TableStyle.SINGLE_BORDER:
            self._set_single_border_style()
        elif style == TableStyle.RANDOM:
            self._set_random_style()
        elif style != TableStyle.DEFAULT:
            msg = "Invalid pre-set style"
            raise ValueError(msg)

    def _set_orgmode_style(self) -> None:
        self.orgmode = True

    def _set_markdown_style(self) -> None:
        self.header = True
        self.border = True
        self._hrules = HRuleStyle.HEADER
        self.padding_width = 1
        self.left_padding_width = 1
        self.right_padding_width = 1
        self.vertical_char = "|"
        self.junction_char = "|"
        self._horizontal_align_char = ":"

    def _set_default_style(self) -> None:
        self.header = True
        self.border = True
        self._hrules = HRuleStyle.FRAME
        self._vrules = VRuleStyle.ALL
        self.padding_width = 1
        self.left_padding_width = 1
        self.right_padding_width = 1
        self.vertical_char = "|"
        self.horizontal_char = "-"
        self._horizontal_align_char = None
        self.junction_char = "+"
        self._top_junction_char = None
        self._bottom_junction_char = None
        self._right_junction_char = None
        self._left_junction_char = None
        self._top_right_junction_char = None
        self._top_left_junction_char = None
        self._bottom_right_junction_char = None
        self._bottom_left_junction_char = None

    def _set_msword_style(self) -> None:
        self.header = True
        self.border = True
        self._hrules = HRuleStyle.NONE
        self.padding_width = 1
        self.left_padding_width = 1
        self.right_padding_width = 1
        self.vertical_char = "|"

    def _set_columns_style(self) -> None:
        self.header = True
        self.border = False
        self.padding_width = 1
        self.left_padding_width = 0
        self.right_padding_width = 8

    def _set_double_border_style(self) -> None:
        self.horizontal_char = "═"
        self.vertical_char = "║"
        self.junction_char = "╬"
        self.top_junction_char = "╦"
        self.bottom_junction_char = "╩"
        self.right_junction_char = "╣"
        self.left_junction_char = "╠"
        self.top_right_junction_char = "╗"
        self.top_left_junction_char = "╔"
        self.bottom_right_junction_char = "╝"
        self.bottom_left_junction_char = "╚"

    def _set_single_border_style(self) -> None:
        self.horizontal_char = "─"
        self.vertical_char = "│"
        self.junction_char = "┼"
        self.top_junction_char = "┬"
        self.bottom_junction_char = "┴"
        self.right_junction_char = "┤"
        self.left_junction_char = "├"
        self.top_right_junction_char = "┐"
        self.top_left_junction_char = "┌"
        self.bottom_right_junction_char = "┘"
        self.bottom_left_junction_char = "└"

    def _set_random_style(self) -> None:
        # Just for fun!
        import random

        self.header = random.choice((True, False))
        self.border = random.choice((True, False))
        self._hrules = random.choice(list(HRuleStyle))
        self._vrules = random.choice(list(VRuleStyle))
        self.left_padding_width = random.randint(0, 5)
        self.right_padding_width = random.randint(0, 5)
        self.vertical_char = random.choice(r"~!@#$%^&*()_+|-=\{}[];':\",./;<>?")
        self.horizontal_char = random.choice(r"~!@#$%^&*()_+|-=\{}[];':\",./;<>?")
        self.junction_char = random.choice(r"~!@#$%^&*()_+|-=\{}[];':\",./;<>?")
        self.preserve_internal_border = random.choice((True, False))

    ##############################
    # DATA INPUT METHODS         #
    ##############################

    def add_rows(self, rows: Sequence[RowType], *, divider: bool = False) -> None:
        """Add rows to the table

        Arguments:

        rows - rows of data, should be an iterable of lists, each list with as many
        elements as the table has fields

        divider - add row divider after the row block
        """
        for row in rows[:-1]:
            self.add_row(row)

        if len(rows) > 0:
            self.add_row(rows[-1], divider=divider)

    def add_row(self, row: RowType, *, divider: bool = False) -> None:
        """Add a row to the table

        Arguments:

        row - row of data, should be a list with as many elements as the table
        has fields"""

        if self._field_names and len(row) != len(self._field_names):
            msg = (
                "Row has incorrect number of values, "
                f"(actual) {len(row)}!={len(self._field_names)} (expected)"
            )
            raise ValueError(msg)
        if not self._field_names:
            self.field_names = [f"Field {n + 1}" for n in range(len(row))]
        self._rows.append(list(row))
        self._dividers.append(divider)

    def del_row(self, row_index: int) -> None:
        """Delete a row from the table

        Arguments:

        row_index - The index of the row you want to delete.  Indexing starts at 0."""

        if row_index > len(self._rows) - 1:
            msg = (
                f"Can't delete row at index {row_index}, "
                f"table only has {len(self._rows)} rows"
            )
            raise IndexError(msg)
        del self._rows[row_index]
        del self._dividers[row_index]

    def add_divider(self) -> None:
        """Add a divider to the table"""
        if len(self._dividers) >= 1:
            self._dividers[-1] = True

    def add_column(
        self,
        fieldname: str,
        column: Sequence[Any],
        align: AlignType = "c",
        valign: VAlignType = "t",
    ) -> None:
        """Add a column to the table.

        Arguments:

        fieldname - name of the field to contain the new column of data
        column - column of data, should be a list with as many elements as the
        table has rows
        align - desired alignment for this column - "l" for left, "c" for centre and
            "r" for right
        valign - desired vertical alignment for new columns - "t" for top,
            "m" for middle and "b" for bottom"""

        if len(self._rows) in (0, len(column)):
            self._validate_align(align)
            self._validate_valign(valign)
            self._field_names.append(fieldname)
            self._align[fieldname] = align
            self._valign[fieldname] = valign
            for i in range(len(column)):
                if len(self._rows) < i + 1:
                    self._rows.append([])
                    self._dividers.append(False)
                self._rows[i].append(column[i])
        else:
            msg = (
                f"Column length {len(column)} does not match number of rows "
                f"{len(self._rows)}"
            )
            raise ValueError(msg)

    def add_autoindex(self, fieldname: str = "Index") -> None:
        """Add an auto-incrementing index column to the table.
        Arguments:
        fieldname - name of the field to contain the new column of data"""
        self._field_names.insert(0, fieldname)
        self._align[fieldname] = self._kwargs["align"] or "c"
        self._valign[fieldname] = self._kwargs["valign"] or "t"
        for i, row in enumerate(self._rows):
            row.insert(0, i + 1)

    def del_column(self, fieldname: str) -> None:
        """Delete a column from the table

        Arguments:

        fieldname - The field name of the column you want to delete."""

        if fieldname not in self._field_names:
            msg = (
                "Can't delete column {!r} which is not a field name of this table."
                " Field names are: {}".format(
                    fieldname, ", ".join(map(repr, self._field_names))
                )
            )
            raise ValueError(msg)

        col_index = self._field_names.index(fieldname)
        del self._field_names[col_index]
        for row in self._rows:
            del row[col_index]

    def clear_rows(self) -> None:
        """Delete all rows from the table but keep the current field names"""

        self._rows = []
        self._dividers = []

    def clear(self) -> None:
        """Delete all rows and field names from the table, maintaining nothing but
        styling options"""

        self._rows = []
        self._dividers = []
        self._field_names = []
        self._widths = []

    ##############################
    # MISC PUBLIC METHODS        #
    ##############################

    def copy(self) -> Self:
        import copy

        return copy.deepcopy(self)

    def get_formatted_string(self, out_format: str = "text", **kwargs) -> str:
        """Return string representation of specified format of table in current state.

        Arguments:
        out_format - resulting table format
        kwargs - passed through to function that performs formatting
        """
        if out_format == "text":
            return self.get_string(**kwargs)
        if out_format == "html":
            return self.get_html_string(**kwargs)
        if out_format == "json":
            return self.get_json_string(**kwargs)
        if out_format == "csv":
            return self.get_csv_string(**kwargs)
        if out_format == "latex":
            return self.get_latex_string(**kwargs)
        if out_format == "mediawiki":
            return self.get_mediawiki_string(**kwargs)

        msg = (
            f"Invalid format {out_format}. "
            "Must be one of: text, html, json, csv, latex or mediawiki"
        )
        raise ValueError(msg)

    ##############################
    # MISC PRIVATE METHODS       #
    ##############################

    def _format_value(self, field: str, value: Any) -> str:
        if isinstance(value, int) and field in self._int_format:
            return (f"%{self._int_format[field]}d") % value
        elif isinstance(value, float) and field in self._float_format:
            return (f"%{self._float_format[field]}f") % value

        formatter = self._custom_format.get(field, (lambda f, v: str(v)))
        return formatter(field, value)

    def _compute_table_width(self, options) -> int:
        if options["vrules"] == VRuleStyle.FRAME:
            table_width = 2
        if options["vrules"] == VRuleStyle.ALL:
            table_width = 1
        else:
            table_width = 0
        per_col_padding = sum(self._get_padding_widths(options))
        for index, fieldname in enumerate(self.field_names):
            if not options["fields"] or (
                options["fields"] and fieldname in options["fields"]
            ):
                table_width += self._widths[index] + per_col_padding + 1
        return table_width

    def _compute_widths(self, rows: list[list[str]], options: OptionsType) -> None:
        if options["header"] and options["use_header_width"]:
            widths = [_get_size(field)[0] for field in self._field_names]
        else:
            widths = len(self.field_names) * [0]

        for row in rows:
            for index, value in enumerate(row):
                fieldname = self.field_names[index]
                if (
                    value == "None"
                    and (none_val := self.none_format.get(fieldname)) is not None
                ):
                    value = none_val
                if fieldname in self.max_width:
                    widths[index] = max(
                        widths[index],
                        min(_get_size(value)[0], self.max_width[fieldname]),
                    )
                else:
                    widths[index] = max(widths[index], _get_size(value)[0])
                if fieldname in self.min_width:
                    widths[index] = max(widths[index], self.min_width[fieldname])

                if self._style == TableStyle.MARKDOWN:
                    # Markdown needs at least one hyphen in the divider
                    if self._align[fieldname] in ("l", "r"):
                        min_width = 1
                    else:  # "c"
                        min_width = 3
                    widths[index] = max(min_width, widths[index])

        self._widths = widths

        per_col_padding = sum(self._get_padding_widths(options))
        # Are we exceeding max_table_width?
        if self._max_table_width:
            table_width = self._compute_table_width(options)
            if table_width > self._max_table_width:
                # Shrink widths in proportion
                markup_chars = per_col_padding * len(widths) + len(widths) - 1
                scale = (self._max_table_width - markup_chars) / (
                    table_width - markup_chars
                )
                self._widths = [max(1, int(w * scale)) for w in widths]

        # Are we under min_table_width or title width?
        if self._min_table_width or options["title"]:
            if options["title"]:
                title_width = _str_block_width(options["title"]) + per_col_padding
                if options["vrules"] in (VRuleStyle.FRAME, VRuleStyle.ALL):
                    title_width += 2
            else:
                title_width = 0
            min_table_width = self.min_table_width or 0
            min_width = max(title_width, min_table_width)
            if options["border"]:
                borders = len(widths) + 1
            elif options["preserve_internal_border"]:
                borders = len(widths)
            else:
                borders = 0

            # Subtract padding for each column and borders
            min_width -= sum([per_col_padding for _ in widths]) + borders
            # What is being scaled is content so we sum column widths
            content_width = sum(widths) or 1

            if content_width < min_width:
                # Grow widths in proportion
                scale = 1.0 * min_width / content_width
                widths = [int(w * scale) for w in widths]
                if sum(widths) < min_width:
                    widths[-1] += min_width - sum(widths)
                self._widths = widths

    def _get_padding_widths(self, options: OptionsType) -> tuple[int, int]:
        if options["left_padding_width"] is not None:
            lpad = options["left_padding_width"]
        else:
            lpad = options["padding_width"]
        if options["right_padding_width"] is not None:
            rpad = options["right_padding_width"]
        else:
            rpad = options["padding_width"]
        return lpad, rpad

    def _get_rows(self, options: OptionsType) -> list[RowType]:
        """Return only those data rows that should be printed, based on slicing and
        sorting.

        Arguments:

        options - dictionary of option settings."""

        if options["oldsortslice"]:
            rows = self._rows[options["start"] : options["end"]]
        else:
            rows = self._rows

        rows = [row for row in rows if options["row_filter"](row)]

        # Sort
        if options["sortby"]:
            sortindex = self._field_names.index(options["sortby"])
            # Decorate
            rows = [[row[sortindex]] + row for row in rows]
            # Sort
            rows.sort(reverse=options["reversesort"], key=options["sort_key"])
            # Undecorate
            rows = [row[1:] for row in rows]

        # Slice if necessary
        if not options["oldsortslice"]:
            rows = rows[options["start"] : options["end"]]

        return rows

    def _get_dividers(self, options: OptionsType) -> list[bool]:
        """Return only those dividers that should be printed, based on slicing.

        Arguments:

        options - dictionary of option settings."""
        if options["oldsortslice"]:
            dividers = self._dividers[options["start"] : options["end"]]
        else:
            dividers = self._dividers

        if options["sortby"]:
            dividers = [False for divider in dividers]

        return dividers

    def _format_row(self, row: RowType) -> list[str]:
        return [
            self._format_value(field, value)
            for (field, value) in zip(self._field_names, row)
        ]

    def _format_rows(self, rows: list[RowType]) -> list[list[str]]:
        return [self._format_row(row) for row in rows]

    ##############################
    # PLAIN TEXT STRING METHODS  #
    ##############################

    def get_string(self, **kwargs) -> str:
        """Return string representation of table in current state.

        Arguments:

        title - optional table title
        start - index of first data row to include in output
        end - index of last data row to include in output PLUS ONE (list slice style)
        fields - names of fields (columns) to include
        header - print a header showing field names (True or False)
        use_header_width - reflect width of header (True or False)
        border - print a border around the table (True or False)
        preserve_internal_border - print a border inside the table even if
            border is disabled (True or False)
        hrules - controls printing of horizontal rules after rows.
            Allowed values: HRuleStyle
        vrules - controls printing of vertical rules between columns.
            Allowed values: VRuleStyle
        int_format - controls formatting of integer data
        float_format - controls formatting of floating point data
        custom_format - controls formatting of any column using callable
        padding_width - number of spaces on either side of column data (only used if
            left and right paddings are None)
        left_padding_width - number of spaces on left hand side of column data
        right_padding_width - number of spaces on right hand side of column data
        vertical_char - single character string used to draw vertical lines
        horizontal_char - single character string used to draw horizontal lines
        horizontal_align_char - single character string used to indicate alignment
        junction_char - single character string used to draw line junctions
        junction_char - single character string used to draw line junctions
        top_junction_char - single character string used to draw top line junctions
        bottom_junction_char -
            single character string used to draw bottom line junctions
        right_junction_char - single character string used to draw right line junctions
        left_junction_char - single character string used to draw left line junctions
        top_right_junction_char -
            single character string used to draw top-right line junctions
        top_left_junction_char -
            single character string used to draw top-left line junctions
        bottom_right_junction_char -
            single character string used to draw bottom-right line junctions
        bottom_left_junction_char -
            single character string used to draw bottom-left line junctions
        sortby - name of field to sort rows by
        sort_key - sorting key function, applied to data points before sorting
        reversesort - True or False to sort in descending or ascending order
        row_filter - filter function applied on rows
        print empty - if True, stringify just the header for an empty table,
            if False return an empty string"""

        options = self._get_options(kwargs)

        lines: list[str] = []

        # Don't think too hard about an empty table
        # Is this the desired behaviour?  Maybe we should still print the header?
        if self.rowcount == 0 and (not options["print_empty"] or not options["border"]):
            return ""

        # Get the rows we need to print, taking into account slicing, sorting, etc.
        rows = self._get_rows(options)
        dividers = self._get_dividers(options)

        # Turn all data in all rows into Unicode, formatted as desired
        formatted_rows = self._format_rows(rows)

        # Compute column widths
        self._compute_widths(formatted_rows, options)
        self._hrule = self._stringify_hrule(options)

        # Add title
        title = options["title"] or self._title
        if title:
            lines.append(self._stringify_title(title, options))

        # Add header or top of border
        if options["header"]:
            lines.append(self._stringify_header(options))
        elif options["border"] and options["hrules"] in (
            HRuleStyle.ALL,
            HRuleStyle.FRAME,
        ):
            lines.append(self._stringify_hrule(options, where="top_"))
            if title and options["vrules"] in (VRuleStyle.ALL, VRuleStyle.FRAME):
                left_j_len = len(self.left_junction_char)
                right_j_len = len(self.right_junction_char)
                lines[-1] = (
                    self.left_junction_char
                    + lines[-1][left_j_len:-right_j_len]
                    + self.right_junction_char
                )

        # Add rows
        for row, divider in zip(formatted_rows[:-1], dividers[:-1]):
            lines.append(self._stringify_row(row, options, self._hrule))
            if divider:
                lines.append(self._stringify_hrule(options))
        if formatted_rows:
            lines.append(
                self._stringify_row(
                    formatted_rows[-1],
                    options,
                    self._stringify_hrule(options, where="bottom_"),
                )
            )

        # Add bottom of border
        if options["border"] and options["hrules"] == HRuleStyle.FRAME:
            lines.append(self._stringify_hrule(options, where="bottom_"))

        if "orgmode" in self.__dict__ and self.orgmode:
            left_j_len = len(self.left_junction_char)
            right_j_len = len(self.right_junction_char)
            lines = [
                "|" + new_line[left_j_len:-right_j_len] + "|"
                for old_line in lines
                for new_line in old_line.split("\n")
            ]

        return "\n".join(lines)

    def _stringify_hrule(
        self, options: OptionsType, where: Literal["top_", "bottom_", ""] = ""
    ) -> str:
        if not options["border"] and not options["preserve_internal_border"]:
            return ""
        lpad, rpad = self._get_padding_widths(options)
        if options["vrules"] in (VRuleStyle.ALL, VRuleStyle.FRAME):
            bits = [options[where + "left_junction_char"]]  # type: ignore[literal-required]
        else:
            bits = [options["horizontal_char"]]
        # For tables with no data or fieldnames
        if not self._field_names:
            bits.append(options[where + "right_junction_char"])  # type: ignore[literal-required]
            return "".join(bits)
        for field, width in zip(self._field_names, self._widths):
            if options["fields"] and field not in options["fields"]:
                continue

            line = (width + lpad + rpad) * options["horizontal_char"]

            # If necessary, add column alignment characters (e.g. ":" for Markdown)
            if self._horizontal_align_char:
                if self._align[field] in ("l", "c"):
                    line = " " + self._horizontal_align_char + line[2:]
                if self._align[field] in ("c", "r"):
                    line = line[:-2] + self._horizontal_align_char + " "

            bits.append(line)
            if options["vrules"] == VRuleStyle.ALL:
                bits.append(options[where + "junction_char"])  # type: ignore[literal-required]
            else:
                bits.append(options["horizontal_char"])
        if options["vrules"] in (VRuleStyle.ALL, VRuleStyle.FRAME):
            bits.pop()
            bits.append(options[where + "right_junction_char"])  # type: ignore[literal-required]

        if options["preserve_internal_border"] and not options["border"]:
            bits = bits[1:-1]

        return "".join(bits)

    def _stringify_title(self, title: str, options: OptionsType) -> str:
        lines: list[str] = []
        lpad, rpad = self._get_padding_widths(options)
        if options["border"]:
            if options["vrules"] == VRuleStyle.ALL:
                options["vrules"] = VRuleStyle.FRAME
                lines.append(self._stringify_hrule(options, "top_"))
                options["vrules"] = VRuleStyle.ALL
            elif options["vrules"] == VRuleStyle.FRAME:
                lines.append(self._stringify_hrule(options, "top_"))
        bits: list[str] = []
        endpoint = (
            options["vertical_char"]
            if options["vrules"] in (VRuleStyle.ALL, VRuleStyle.FRAME)
            and options["border"]
            else " "
        )
        bits.append(endpoint)
        title = " " * lpad + title + " " * rpad
        lpad, rpad = self._get_padding_widths(options)
        sum_widths = sum([n + lpad + rpad + 1 for n in self._widths])

        bits.append(self._justify(title, sum_widths - 1, "c"))
        bits.append(endpoint)
        lines.append("".join(bits))
        return "\n".join(lines)

    def _stringify_header(self, options: OptionsType) -> str:
        bits: list[str] = []
        lpad, rpad = self._get_padding_widths(options)
        if options["border"]:
            if options["hrules"] in (HRuleStyle.ALL, HRuleStyle.FRAME):
                bits.append(self._stringify_hrule(options, "top_"))
                if options["title"] and options["vrules"] in (
                    VRuleStyle.ALL,
                    VRuleStyle.FRAME,
                ):
                    left_j_len = len(self.left_junction_char)
                    right_j_len = len(self.right_junction_char)
                    bits[-1] = (
                        self.left_junction_char
                        + bits[-1][left_j_len:-right_j_len]
                        + self.right_junction_char
                    )
                bits.append("\n")
            if options["vrules"] in (VRuleStyle.ALL, VRuleStyle.FRAME):
                bits.append(options["vertical_char"])
            else:
                bits.append(" ")
        # For tables with no data or field names
        if not self._field_names:
            if options["vrules"] in (VRuleStyle.ALL, VRuleStyle.FRAME):
                bits.append(options["vertical_char"])
            else:
                bits.append(" ")
        for field, width in zip(self._field_names, self._widths):
            if options["fields"] and field not in options["fields"]:
                continue
            if self._header_style == "cap":
                fieldname = field.capitalize()
            elif self._header_style == "title":
                fieldname = field.title()
            elif self._header_style == "upper":
                fieldname = field.upper()
            elif self._header_style == "lower":
                fieldname = field.lower()
            else:
                fieldname = field
            if _str_block_width(fieldname) > width:
                fieldname = fieldname[:width]
            bits.append(
                " " * lpad
                + self._justify(fieldname, width, self._align[field])
                + " " * rpad
            )
            if options["border"] or options["preserve_internal_border"]:
                if options["vrules"] == VRuleStyle.ALL:
                    bits.append(options["vertical_char"])
                else:
                    bits.append(" ")

        # If only preserve_internal_border is true, then we just appended
        # a vertical character at the end when we wanted a space
        if not options["border"] and options["preserve_internal_border"]:
            bits.pop()
            bits.append(" ")
        # If vrules is FRAME, then we just appended a space at the end
        # of the last field, when we really want a vertical character
        if options["border"] and options["vrules"] == VRuleStyle.FRAME:
            bits.pop()
            bits.append(options["vertical_char"])
        if (options["border"] or options["preserve_internal_border"]) and options[
            "hrules"
        ] != HRuleStyle.NONE:
            bits.append("\n")
            bits.append(self._hrule)
        return "".join(bits)

    def _stringify_row(self, row: list[str], options: OptionsType, hrule: str) -> str:
        import textwrap

        for index, field, value, width in zip(
            range(len(row)), self._field_names, row, self._widths
        ):
            # Enforce max widths
            lines = value.split("\n")
            new_lines: list[str] = []
            for line in lines:
                if (
                    line == "None"
                    and (none_val := self.none_format.get(field)) is not None
                ):
                    line = none_val
                if _str_block_width(line) > width:
                    line = textwrap.fill(
                        line, width, break_on_hyphens=options["break_on_hyphens"]
                    )
                new_lines.append(line)
            lines = new_lines
            value = "\n".join(lines)
            row[index] = value

        row_height = 0
        for c in row:
            h = _get_size(c)[1]
            if h > row_height:
                row_height = h

        bits: list[list[str]] = []
        lpad, rpad = self._get_padding_widths(options)
        for y in range(row_height):
            bits.append([])
            if options["border"]:
                if options["vrules"] in (VRuleStyle.ALL, VRuleStyle.FRAME):
                    bits[y].append(self.vertical_char)
                else:
                    bits[y].append(" ")

        for field, value, width in zip(self._field_names, row, self._widths):
            valign = self._valign[field]
            lines = value.split("\n")
            d_height = row_height - len(lines)
            if d_height:
                if valign == "m":
                    lines = (
                        [""] * int(d_height / 2)
                        + lines
                        + [""] * (d_height - int(d_height / 2))
                    )
                elif valign == "b":
                    lines = [""] * d_height + lines
                else:
                    lines = lines + [""] * d_height

            for y, line in enumerate(lines):
                if options["fields"] and field not in options["fields"]:
                    continue

                bits[y].append(
                    " " * lpad
                    + self._justify(line, width, self._align[field])
                    + " " * rpad
                )
                if options["border"] or options["preserve_internal_border"]:
                    if options["vrules"] == VRuleStyle.ALL:
                        bits[y].append(self.vertical_char)
                    else:
                        bits[y].append(" ")

        # If only preserve_internal_border is true, then we just appended
        # a vertical character at the end when we wanted a space
        if not options["border"] and options["preserve_internal_border"]:
            bits[-1].pop()
            bits[-1].append(" ")

        # If vrules is FRAME, then we just appended a space at the end
        # of the last field, when we really want a vertical character
        for y in range(row_height):
            if options["border"] and options["vrules"] == VRuleStyle.FRAME:
                bits[y].pop()
                bits[y].append(options["vertical_char"])

        if options["border"] and options["hrules"] == HRuleStyle.ALL:
            bits[row_height - 1].append("\n")
            bits[row_height - 1].append(hrule)

        bits_str = ["".join(bits_y) for bits_y in bits]
        return "\n".join(bits_str)

    def paginate(self, page_length: int = 58, line_break: str = "\f", **kwargs) -> str:
        pages: list[str] = []
        kwargs["start"] = kwargs.get("start", 0)
        true_end = kwargs.get("end", self.rowcount)
        while True:
            kwargs["end"] = min(kwargs["start"] + page_length, true_end)
            pages.append(self.get_string(**kwargs))
            if kwargs["end"] == true_end:
                break
            kwargs["start"] += page_length
        return line_break.join(pages)

    ##############################
    # CSV STRING METHODS         #
    ##############################
    def get_csv_string(self, **kwargs) -> str:
        """Return string representation of CSV formatted table in the current state

        Keyword arguments are first interpreted as table formatting options, and
        then any unused keyword arguments are passed to csv.writer(). For
        example, get_csv_string(header=False, delimiter='\t') would use
        header as a PrettyTable formatting option (skip the header row) and
        delimiter as a csv.writer keyword argument.
        """
        import csv

        options = self._get_options(kwargs)
        csv_options = {
            key: value for key, value in kwargs.items() if key not in options
        }
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer, **csv_options)

        if options.get("header"):
            if options["fields"]:
                csv_writer.writerow(
                    [f for f in self._field_names if f in options["fields"]]
                )
            else:
                csv_writer.writerow(self._field_names)

        rows = self._get_rows(options)
        if options["fields"]:
            rows = [
                [d for f, d in zip(self._field_names, row) if f in options["fields"]]
                for row in rows
            ]
        for row in rows:
            csv_writer.writerow(row)

        return csv_buffer.getvalue()

    ##############################
    # JSON STRING METHODS        #
    ##############################
    def get_json_string(self, **kwargs) -> str:
        """Return string representation of JSON formatted table in the current state

        Keyword arguments are first interpreted as table formatting options, and
        then any unused keyword arguments are passed to json.dumps(). For
        example, get_json_string(header=False, indent=2) would use header as
        a PrettyTable formatting option (skip the header row) and indent as a
        json.dumps keyword argument.
        """
        import json

        options = self._get_options(kwargs)
        json_options: dict[str, Any] = {
            "indent": 4,
            "separators": (",", ": "),
            "sort_keys": True,
        }
        json_options.update(
            {key: value for key, value in kwargs.items() if key not in options}
        )
        objects: list[list[str] | dict[str, Any]] = []

        if options.get("header"):
            if options["fields"]:
                objects.append([f for f in self._field_names if f in options["fields"]])
            else:
                objects.append(self.field_names)
        rows = self._get_rows(options)
        if options["fields"]:
            for row in rows:
                objects.append(
                    {
                        f: d
                        for f, d in zip(self._field_names, row)
                        if f in options["fields"]
                    }
                )
        else:
            for row in rows:
                objects.append(dict(zip(self._field_names, row)))

        return json.dumps(objects, **json_options)

    ##############################
    # HTML STRING METHODS        #
    ##############################

    def get_html_string(self, **kwargs) -> str:
        """Return string representation of HTML formatted version of table in current
        state.

        Arguments:

        title - optional table title
        start - index of first data row to include in output
        end - index of last data row to include in output PLUS ONE (list slice style)
        fields - names of fields (columns) to include
        header - print a header showing field names (True or False)
        escape_header - escapes the text within a header (True or False)
        border - print a border around the table (True or False)
        preserve_internal_border - print a border inside the table even if
            border is disabled (True or False)
        hrules - controls printing of horizontal rules after rows.
            Allowed values: HRuleStyle
        vrules - controls printing of vertical rules between columns.
            Allowed values: VRuleStyle
        int_format - controls formatting of integer data
        float_format - controls formatting of floating point data
        custom_format - controls formatting of any column using callable
        padding_width - number of spaces on either side of column data (only used if
            left and right paddings are None)
        left_padding_width - number of spaces on left hand side of column data
        right_padding_width - number of spaces on right hand side of column data
        sortby - name of field to sort rows by
        sort_key - sorting key function, applied to data points before sorting
        row_filter - filter function applied on rows
        attributes - dictionary of name/value pairs to include as HTML attributes in the
            <table> tag
        format - Controls whether or not HTML tables are formatted to match
            styling options (True or False)
        escape_data - escapes the text within a data field (True or False)
        xhtml - print <br/> tags if True, <br> tags if False
        break_on_hyphens - Whether long lines are broken on hypens or not, default: True
        """

        options = self._get_options(kwargs)

        if options["format"]:
            string = self._get_formatted_html_string(options)
        else:
            string = self._get_simple_html_string(options)

        return string

    def _get_simple_html_string(self, options: OptionsType) -> str:
        from html import escape

        lines: list[str] = []
        if options["xhtml"]:
            linebreak = "<br/>"
        else:
            linebreak = "<br>"

        open_tag = ["<table"]
        if options["attributes"]:
            for attr_name, attr_value in options["attributes"].items():
                open_tag.append(f' {escape(attr_name)}="{escape(attr_value)}"')
        open_tag.append(">")
        lines.append("".join(open_tag))

        # Title
        title = options["title"] or self._title
        if title:
            lines.append(f"    <caption>{escape(title)}</caption>")

        # Headers
        if options["header"]:
            lines.append("    <thead>")
            lines.append("        <tr>")
            for field in self._field_names:
                if options["fields"] and field not in options["fields"]:
                    continue
                if options["escape_header"]:
                    field = escape(field)

                lines.append(
                    "            <th>{}</th>".format(field.replace("\n", linebreak))
                )

            lines.append("        </tr>")
            lines.append("    </thead>")

        # Data
        lines.append("    <tbody>")
        rows = self._get_rows(options)
        formatted_rows = self._format_rows(rows)
        for row in formatted_rows:
            lines.append("        <tr>")
            for field, datum in zip(self._field_names, row):
                if options["fields"] and field not in options["fields"]:
                    continue
                if options["escape_data"]:
                    datum = escape(datum)

                lines.append(
                    "            <td>{}</td>".format(datum.replace("\n", linebreak))
                )
            lines.append("        </tr>")
        lines.append("    </tbody>")
        lines.append("</table>")

        return "\n".join(lines)

    def _get_formatted_html_string(self, options: OptionsType) -> str:
        from html import escape

        lines: list[str] = []
        lpad, rpad = self._get_padding_widths(options)
        if options["xhtml"]:
            linebreak = "<br/>"
        else:
            linebreak = "<br>"

        open_tag = ["<table"]
        if options["border"]:
            if (
                options["hrules"] == HRuleStyle.ALL
                and options["vrules"] == VRuleStyle.ALL
            ):
                open_tag.append(' frame="box" rules="all"')
            elif (
                options["hrules"] == HRuleStyle.FRAME
                and options["vrules"] == VRuleStyle.FRAME
            ):
                open_tag.append(' frame="box"')
            elif (
                options["hrules"] == HRuleStyle.FRAME
                and options["vrules"] == VRuleStyle.ALL
            ):
                open_tag.append(' frame="box" rules="cols"')
            elif options["hrules"] == HRuleStyle.FRAME:
                open_tag.append(' frame="hsides"')
            elif options["hrules"] == HRuleStyle.ALL:
                open_tag.append(' frame="hsides" rules="rows"')
            elif options["vrules"] == VRuleStyle.FRAME:
                open_tag.append(' frame="vsides"')
            elif options["vrules"] == VRuleStyle.ALL:
                open_tag.append(' frame="vsides" rules="cols"')
        if not options["border"] and options["preserve_internal_border"]:
            open_tag.append(' rules="cols"')
        if options["attributes"]:
            for attr_name, attr_value in options["attributes"].items():
                open_tag.append(f' {escape(attr_name)}="{escape(attr_value)}"')
        open_tag.append(">")
        lines.append("".join(open_tag))

        # Title
        title = options["title"] or self._title
        if title:
            lines.append(f"    <caption>{escape(title)}</caption>")

        # Headers
        if options["header"]:
            lines.append("    <thead>")
            lines.append("        <tr>")
            for field in self._field_names:
                if options["fields"] and field not in options["fields"]:
                    continue
                if options["escape_header"]:
                    field = escape(field)

                content = field.replace("\n", linebreak)
                lines.append(
                    f'            <th style="'
                    f"padding-left: {lpad}em; "
                    f"padding-right: {rpad}em; "
                    f'text-align: center">{content}</th>'
                )
            lines.append("        </tr>")
            lines.append("    </thead>")

        # Data
        lines.append("    <tbody>")
        rows = self._get_rows(options)
        formatted_rows = self._format_rows(rows)
        aligns: list[str] = []
        valigns: list[str] = []
        for field in self._field_names:
            aligns.append(
                {"l": "left", "r": "right", "c": "center"}[self._align[field]]
            )
            valigns.append(
                {"t": "top", "m": "middle", "b": "bottom"}[self._valign[field]]
            )
        for row in formatted_rows:
            lines.append("        <tr>")
            for field, datum, align, valign in zip(
                self._field_names, row, aligns, valigns
            ):
                if options["fields"] and field not in options["fields"]:
                    continue
                if options["escape_data"]:
                    datum = escape(datum)

                content = datum.replace("\n", linebreak)
                lines.append(
                    f'            <td style="'
                    f"padding-left: {lpad}em; "
                    f"padding-right: {rpad}em; "
                    f"text-align: {align}; "
                    f'vertical-align: {valign}">{content}</td>'
                )
            lines.append("        </tr>")
        lines.append("    </tbody>")
        lines.append("</table>")

        return "\n".join(lines)

    ##############################
    # LATEX STRING METHODS       #
    ##############################

    def get_latex_string(self, **kwargs) -> str:
        """Return string representation of LaTex formatted version of table in current
        state.

        Arguments:

        start - index of first data row to include in output
        end - index of last data row to include in output PLUS ONE (list slice style)
        fields - names of fields (columns) to include
        header - print a header showing field names (True or False)
        border - print a border around the table (True or False)
        preserve_internal_border - print a border inside the table even if
            border is disabled (True or False)
        hrules - controls printing of horizontal rules after rows.
            Allowed values: HRuleStyle
        vrules - controls printing of vertical rules between columns.
            Allowed values: VRuleStyle
        int_format - controls formatting of integer data
        float_format - controls formatting of floating point data
        sortby - name of field to sort rows by
        sort_key - sorting key function, applied to data points before sorting
        row_filter - filter function applied on rows
        format - Controls whether or not HTML tables are formatted to match
            styling options (True or False)
        """
        options = self._get_options(kwargs)

        if options["format"]:
            string = self._get_formatted_latex_string(options)
        else:
            string = self._get_simple_latex_string(options)
        return string

    def _get_simple_latex_string(self, options: OptionsType) -> str:
        lines: list[str] = []

        wanted_fields = []
        if options["fields"]:
            wanted_fields = [
                field for field in self._field_names if field in options["fields"]
            ]
        else:
            wanted_fields = self._field_names

        alignments = "".join([self._align[field] for field in wanted_fields])

        begin_cmd = f"\\begin{{tabular}}{{{alignments}}}"
        lines.append(begin_cmd)

        # Headers
        if options["header"]:
            lines.append(" & ".join(wanted_fields) + " \\\\")

        # Data
        rows = self._get_rows(options)
        formatted_rows = self._format_rows(rows)
        for row in formatted_rows:
            wanted_data = [
                d for f, d in zip(self._field_names, row) if f in wanted_fields
            ]
            lines.append(" & ".join(wanted_data) + " \\\\")

        lines.append("\\end{tabular}")

        return "\r\n".join(lines)

    def _get_formatted_latex_string(self, options: OptionsType) -> str:
        lines: list[str] = []

        if options["fields"]:
            wanted_fields = [
                field for field in self._field_names if field in options["fields"]
            ]
        else:
            wanted_fields = self._field_names

        wanted_alignments = [self._align[field] for field in wanted_fields]
        if options["border"] and options["vrules"] == VRuleStyle.ALL:
            alignment_str = "|".join(wanted_alignments)
        elif not options["border"] and options["preserve_internal_border"]:
            alignment_str = "|".join(wanted_alignments)
        else:
            alignment_str = "".join(wanted_alignments)

        if options["border"] and options["vrules"] in [
            VRuleStyle.ALL,
            VRuleStyle.FRAME,
        ]:
            alignment_str = "|" + alignment_str + "|"

        begin_cmd = f"\\begin{{tabular}}{{{alignment_str}}}"
        lines.append(begin_cmd)
        if options["border"] and options["hrules"] in [
            HRuleStyle.ALL,
            HRuleStyle.FRAME,
        ]:
            lines.append("\\hline")

        # Headers
        if options["header"]:
            lines.append(" & ".join(wanted_fields) + " \\\\")
        if (options["border"] or options["preserve_internal_border"]) and options[
            "hrules"
        ] in [HRuleStyle.ALL, HRuleStyle.HEADER]:
            lines.append("\\hline")

        # Data
        rows = self._get_rows(options)
        formatted_rows = self._format_rows(rows)
        rows = self._get_rows(options)
        for row in formatted_rows:
            wanted_data = [
                d for f, d in zip(self._field_names, row) if f in wanted_fields
            ]
            lines.append(" & ".join(wanted_data) + " \\\\")
            if options["border"] and options["hrules"] == HRuleStyle.ALL:
                lines.append("\\hline")

        if options["border"] and options["hrules"] == HRuleStyle.FRAME:
            lines.append("\\hline")

        lines.append("\\end{tabular}")

        return "\r\n".join(lines)

    ##############################
    # MEDIAWIKI STRING METHODS   #
    ##############################

    def get_mediawiki_string(self, **kwargs) -> str:
        """
        Return string representation of the table in MediaWiki table markup.
        The generated markup follows simple MediaWiki syntax. For example:
            {| class="wikitable"
            |+ Optional caption
            |-
            ! Header1 !! Header2 !! Header3
            |-
            | Data1 || Data2 || Data3
            |-
            | Data4 || Data5 || Data6
            |}
        """

        options = self._get_options(kwargs)
        lines: list[str] = []

        if (
            options.get("attributes")
            and isinstance(options["attributes"], dict)
            and options["attributes"]
        ):
            attr_str = " ".join(f'{k}="{v}"' for k, v in options["attributes"].items())
            lines.append("{| " + attr_str)
        else:
            lines.append('{| class="wikitable"')

        caption = options.get("title", self._title)
        if caption:
            lines.append("|+ " + caption)

        if options.get("header"):
            lines.append("|-")
            headers = []
            fields_option = options.get("fields")
            for field in self._field_names:
                if fields_option is not None and field not in fields_option:
                    continue
                headers.append(field)
            if headers:
                header_line = " !! ".join(headers)
                lines.append("! " + header_line)

        rows = self._get_rows(options)
        formatted_rows = self._format_rows(rows)
        for row in formatted_rows:
            lines.append("|-")
            cells = []
            fields_option = options.get("fields")
            for field, cell in zip(self._field_names, row):
                if fields_option is not None and field not in fields_option:
                    continue
                cells.append(cell)
            if cells:
                lines.append("| " + " || ".join(cells))

        lines.append("|}")
        return "\n".join(lines)


##############################
# UNICODE WIDTH FUNCTION     #
##############################


@lru_cache
def _str_block_width(val: str) -> int:
    import wcwidth

    val = _osc8_re.sub(r"\1", val)
    return wcwidth.wcswidth(_re.sub("", val))


##############################
# TABLE FACTORIES            #
##############################


def from_csv(fp, field_names: Sequence[str] | None = None, **kwargs) -> PrettyTable:
    import csv

    fmtparams = {}
    for param in [
        "delimiter",
        "doublequote",
        "escapechar",
        "lineterminator",
        "quotechar",
        "quoting",
        "skipinitialspace",
        "strict",
    ]:
        if param in kwargs:
            fmtparams[param] = kwargs.pop(param)
    if fmtparams:
        reader = csv.reader(fp, **fmtparams)
    else:
        dialect = csv.Sniffer().sniff(fp.read(1024))
        fp.seek(0)
        reader = csv.reader(fp, dialect)

    table = PrettyTable(**kwargs)
    if field_names:
        table.field_names = field_names
    else:
        table.field_names = [x.strip() for x in next(reader)]

    for row in reader:
        table.add_row([x.strip() for x in row])

    return table


def from_db_cursor(cursor: Cursor, **kwargs) -> PrettyTable | None:
    if cursor.description:
        table = PrettyTable(**kwargs)
        table.field_names = [col[0] for col in cursor.description]
        for row in cursor.fetchall():
            table.add_row(row)
        return table
    return None


def from_json(json_string: str | bytes, **kwargs) -> PrettyTable:
    import json

    table = PrettyTable(**kwargs)
    objects = json.loads(json_string)
    table.field_names = objects[0]
    for obj in objects[1:]:
        row = [obj[key] for key in table.field_names]
        table.add_row(row)
    return table


class TableHandler(HTMLParser):
    def __init__(self, **kwargs) -> None:
        HTMLParser.__init__(self)
        self.kwargs = kwargs
        self.tables: list[PrettyTable] = []
        self.last_row: list[str] = []
        self.rows: list[tuple[list[str], bool]] = []
        self.max_row_width = 0
        self.active: str | None = None
        self.last_content = ""
        self.is_last_row_header = False
        self.colspan = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.active = tag
        if tag == "th":
            self.is_last_row_header = True
        for key, value in attrs:
            if key == "colspan":
                self.colspan = int(value)  # type: ignore[arg-type]

    def handle_endtag(self, tag: str) -> None:
        if tag in ["th", "td"]:
            stripped_content = self.last_content.strip()
            self.last_row.append(stripped_content)
            if self.colspan:
                for _ in range(1, self.colspan):
                    self.last_row.append("")
                self.colspan = 0

        if tag == "tr":
            self.rows.append((self.last_row, self.is_last_row_header))
            self.max_row_width = max(self.max_row_width, len(self.last_row))
            self.last_row = []
            self.is_last_row_header = False
        if tag == "table":
            table = self.generate_table(self.rows)
            self.tables.append(table)
            self.rows = []
        self.last_content = " "
        self.active = None

    def handle_data(self, data: str) -> None:
        self.last_content += data

    def generate_table(self, rows: list[tuple[list[str], bool]]) -> PrettyTable:
        """
        Generates from a list of rows a PrettyTable object.
        """
        table = PrettyTable(**self.kwargs)
        for row in self.rows:
            if len(row[0]) < self.max_row_width:
                appends = self.max_row_width - len(row[0])
                for i in range(1, appends):
                    row[0].append("-")

            if row[1]:
                self.make_fields_unique(row[0])
                table.field_names = row[0]
            else:
                table.add_row(row[0])
        return table

    def make_fields_unique(self, fields: list[str]) -> None:
        """
        iterates over the row and make each field unique
        """
        for i in range(len(fields)):
            for j in range(i + 1, len(fields)):
                if fields[i] == fields[j]:
                    fields[j] += "'"


def from_html(html_code: str, **kwargs) -> list[PrettyTable]:
    """
    Generates a list of PrettyTables from a string of HTML code. Each <table> in
    the HTML becomes one PrettyTable object.
    """

    parser = TableHandler(**kwargs)
    parser.feed(html_code)
    return parser.tables


def from_html_one(html_code: str, **kwargs) -> PrettyTable:
    """
    Generates a PrettyTable from a string of HTML code which contains only a
    single <table>
    """

    tables = from_html(html_code, **kwargs)
    try:
        assert len(tables) == 1
    except AssertionError:
        msg = "More than one <table> in provided HTML code. Use from_html instead."
        raise ValueError(msg)
    return tables[0]


def from_mediawiki(wiki_text: str, **kwargs) -> PrettyTable:
    """
    Returns a PrettyTable instance from simple MediaWiki table markup.
    Note that the table should have a header row.
    Arguments:
    wiki_text -- Multiline string containing MediaWiki table markup
    (Enter within '''   ''')
    """
    lines = wiki_text.strip().split("\n")
    table = PrettyTable(**kwargs)
    header = None
    rows = []
    inside_table = False
    for line in lines:
        line = line.strip()
        if line.startswith("{|"):
            inside_table = True
            continue
        if line.startswith("|}"):
            break
        if not inside_table:
            continue
        if line.startswith("|-"):
            continue
        if line.startswith("|+"):
            continue
        if line.startswith("!"):
            header = [cell.strip() for cell in re.split(r"\s*!!\s*", line[1:])]
            table.field_names = header
            continue
        if line.startswith("|"):
            row_data = [cell.strip() for cell in re.split(r"\s*\|\|\s*", line[1:])]
            rows.append(row_data)
            continue

    if header:
        for row in rows:
            if len(row) != len(header):
                error_message = "Row length mismatch between header and body."
                raise ValueError(error_message)
            table.add_row(row)
    else:
        msg = "No valid header found in the MediaWiki table."
        raise ValueError(msg)
    return table


def _warn_deprecation(name: str, module_globals: dict[str, Any]) -> Any:
    if (val := module_globals.get(f"_DEPRECATED_{name}")) is None:
        msg = f"module '{__name__}' has no attribute '{name}'"
        raise AttributeError(msg)
    module_globals[name] = val
    if name in {"FRAME", "ALL", "NONE", "HEADER"}:
        msg = (
            f"the '{name}' constant is deprecated, "
            "use the 'HRuleStyle' and 'VRuleStyle' enums instead"
        )
    else:
        msg = f"the '{name}' constant is deprecated, use the 'TableStyle' enum instead"

    import warnings

    warnings.warn(msg, DeprecationWarning, stacklevel=3)
    return val


def __getattr__(name: str) -> Any:
    return _warn_deprecation(name, module_globals=globals())
