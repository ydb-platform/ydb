# -*- coding: utf-8 -*-
"""These preprocessor functions are used to process data prior to output."""

import string
from datetime import datetime

from cli_helpers import utils
from cli_helpers.compat import text_type, int_types, float_types, HAS_PYGMENTS, Token


def truncate_string(
    data, headers, max_field_width=None, skip_multiline_string=True, **_
):
    """Truncate very long strings. Only needed for tabular
    representation, because trying to tabulate very long data
    is problematic in terms of performance, and does not make any
    sense visually.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param int max_field_width: Width to truncate field for display
    :return: The processed data and headers.
    :rtype: tuple
    """
    return (
        (
            [
                utils.truncate_string(v, max_field_width, skip_multiline_string)
                for v in row
            ]
            for row in data
        ),
        [
            utils.truncate_string(h, max_field_width, skip_multiline_string)
            for h in headers
        ],
    )


def convert_to_string(data, headers, **_):
    """Convert all *data* and *headers* to strings.

    Binary data that cannot be decoded is converted to a hexadecimal
    representation via :func:`binascii.hexlify`.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :return: The processed data and headers.
    :rtype: tuple

    """
    return (
        ([utils.to_string(v) for v in row] for row in data),
        [utils.to_string(h) for h in headers],
    )


def convert_to_undecoded_string(data, headers, **_):
    """Convert all *data* and *headers* to hex, if needed.

    Binary data is converted to a hexadecimal representation via
    :func:`binascii.hexlify`.

    Unlike convert_to_string(), None values are left as Nones.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :return: The processed data and headers.
    :rtype: tuple

    """
    return (
        ([utils.to_undecoded_string(v) for v in row] for row in data),
        [utils.to_undecoded_string(h) for h in headers],
    )


def override_missing_value(
    data,
    headers,
    style=None,
    missing_value_token=Token.Output.Null,
    missing_value="",
    **_,
):
    """Override missing values in the *data* with *missing_value*.

    A missing value is any value that is :data:`None`.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param style: Style for missing_value.
    :param missing_value_token: The Pygments token used for missing data.
    :param missing_value: The default value to use for missing data.
    :return: The processed data and headers.
    :rtype: tuple

    """

    def fields():
        for row in data:
            processed = []
            for field in row:
                if field is None and style and HAS_PYGMENTS:
                    styled = utils.style_field(
                        missing_value_token, missing_value, style
                    )
                    processed.append(styled)
                elif field is None:
                    processed.append(missing_value)
                else:
                    processed.append(field)
            yield processed

    return (fields(), headers)


def override_tab_value(data, headers, new_value="    ", **_):
    """Override tab values in the *data* with *new_value*.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param new_value: The new value to use for tab.
    :return: The processed data and headers.
    :rtype: tuple

    """
    return (
        (
            [v.replace("\t", new_value) if isinstance(v, text_type) else v for v in row]
            for row in data
        ),
        headers,
    )


def escape_newlines(data, headers, **_):
    """Escape newline characters (\n -> \\n, \r -> \\r)

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :return: The processed data and headers.
    :rtype: tuple

    """
    return (
        (
            [
                (
                    v.replace("\r", r"\r").replace("\n", r"\n")
                    if isinstance(v, text_type)
                    else v
                )
                for v in row
            ]
            for row in data
        ),
        headers,
    )


def bytes_to_string(data, headers, **_):
    """Convert all *data* and *headers* bytes to strings.

    Binary data that cannot be decoded is converted to a hexadecimal
    representation via :func:`binascii.hexlify`.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :return: The processed data and headers.
    :rtype: tuple

    """
    return (
        ([utils.bytes_to_string(v) for v in row] for row in data),
        [utils.bytes_to_string(h) for h in headers],
    )


def align_decimals(data, headers, column_types=(), **_):
    """Align numbers in *data* on their decimal points.

    Whitespace padding is added before a number so that all numbers in a
    column are aligned.

    Outputting data before aligning the decimals::

        1
        2.1
        10.59

    Outputting data after aligning the decimals::

         1
         2.1
        10.59

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param iterable column_types: The columns' type objects (e.g. int or float).
    :return: The processed data and headers.
    :rtype: tuple

    """
    pointpos = len(headers) * [0]
    data = list(data)
    for row in data:
        for i, v in enumerate(row):
            if column_types[i] is float and type(v) in float_types:
                v = text_type(v)
                pointpos[i] = max(utils.intlen(v), pointpos[i])

    def results(data):
        for row in data:
            result = []
            for i, v in enumerate(row):
                if column_types[i] is float and type(v) in float_types:
                    v = text_type(v)
                    result.append((pointpos[i] - utils.intlen(v)) * " " + v)
                else:
                    result.append(v)
            yield result

    return results(data), headers


def quote_whitespaces(data, headers, quotestyle="'", **_):
    """Quote leading/trailing whitespace in *data*.

    When outputing data with leading or trailing whitespace, it can be useful
    to put quotation marks around the value so the whitespace is more
    apparent. If one value in a column needs quoted, then all values in that
    column are quoted to keep things consistent.

    .. NOTE::
       :data:`string.whitespace` is used to determine which characters are
       whitespace.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param str quotestyle: The quotation mark to use (defaults to ``'``).
    :return: The processed data and headers.
    :rtype: tuple

    """
    whitespace = tuple(string.whitespace)
    quote = len(headers) * [False]
    data = list(data)
    for row in data:
        for i, v in enumerate(row):
            v = text_type(v)
            if v.startswith(whitespace) or v.endswith(whitespace):
                quote[i] = True

    def results(data):
        for row in data:
            result = []
            for i, v in enumerate(row):
                quotation = quotestyle if quote[i] else ""
                result.append(
                    "{quotestyle}{value}{quotestyle}".format(
                        quotestyle=quotation, value=v
                    )
                )
            yield result

    return results(data), headers


def style_output(
    data,
    headers,
    style=None,
    header_token=Token.Output.Header,
    odd_row_token=Token.Output.OddRow,
    even_row_token=Token.Output.EvenRow,
    **_,
):
    """Style the *data* and *headers* (e.g. bold, italic, and colors)

    .. NOTE::
        This requires the `Pygments <http://pygments.org/>`_ library to
        be installed. You can install it with CLI Helpers as an extra::
            $ pip install cli_helpers[styles]

    Example usage::

        from cli_helpers.tabular_output.preprocessors import style_output
        from pygments.style import Style
        from pygments.token import Token

        class YourStyle(Style):
            default_style = ""
            styles = {
                Token.Output.Header: 'bold ansibrightred',
                Token.Output.OddRow: 'bg:#eee #111',
                Token.Output.EvenRow: '#0f0'
            }

        headers = ('First Name', 'Last Name')
        data = [['Fred', 'Roberts'], ['George', 'Smith']]

        data, headers = style_output(data, headers, style=YourStyle)

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param str/pygments.style.Style style: A Pygments style. You can `create
        your own styles <https://pygments.org/docs/styles#creating-own-styles>`_.
    :param str header_token: The token type to be used for the headers.
    :param str odd_row_token: The token type to be used for odd rows.
    :param str even_row_token: The token type to be used for even rows.
    :return: The styled data and headers.
    :rtype: tuple

    """
    from cli_helpers.utils import filter_style_table

    relevant_styles = filter_style_table(
        style, header_token, odd_row_token, even_row_token
    )
    if style and HAS_PYGMENTS:
        if relevant_styles.get(header_token):
            headers = [
                utils.style_field(header_token, header, style) for header in headers
            ]
        if relevant_styles.get(odd_row_token) or relevant_styles.get(even_row_token):
            data = (
                [
                    utils.style_field(
                        odd_row_token if i % 2 else even_row_token, f, style
                    )
                    for f in r
                ]
                for i, r in enumerate(data, 1)
            )

    return iter(data), headers


def format_numbers(
    data, headers, column_types=(), integer_format=None, float_format=None, **_
):
    """Format numbers according to a format specification.

    This uses Python's format specification to format numbers of the following
    types: :class:`int`, :class:`py2:long` (Python 2), :class:`float`, and
    :class:`~decimal.Decimal`. See the :ref:`python:formatspec` for more
    information about the format strings.

    .. NOTE::
       A column is only formatted if all of its values are the same type
       (except for :data:`None`).

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param iterable column_types: The columns' type objects (e.g. int or float).
    :param str integer_format: The format string to use for integer columns.
    :param str float_format: The format string to use for float columns.
    :return: The processed data and headers.
    :rtype: tuple

    """
    if (integer_format is None and float_format is None) or not column_types:
        return iter(data), headers

    def _format_number(field, column_type):
        if integer_format and column_type is int and type(field) in int_types:
            return format(field, integer_format)
        elif float_format and column_type is float and type(field) in float_types:
            return format(field, float_format)
        return field

    data = (
        [_format_number(v, column_types[i]) for i, v in enumerate(row)] for row in data
    )
    return data, headers


def format_timestamps(data, headers, column_date_formats=None, **_):
    """Format timestamps according to user preference.

    This allows for per-column formatting for date, time, or datetime like data.

    Add a `column_date_formats` section to your config file with separate lines for each column
    that you'd like to specify a format using `name=format`. Use standard Python strftime
    formatting strings

    Example: `signup_date = "%Y-%m-%d"`

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param str column_date_format: The format strings to use for specific columns.
    :return: The processed data and headers.
    :rtype: tuple

    """
    if column_date_formats is None:
        return iter(data), headers

    def _format_timestamp(value, name, column_date_formats):
        if name not in column_date_formats:
            return value
        try:
            dt = datetime.fromisoformat(value)
            return dt.strftime(column_date_formats[name])
        except (ValueError, TypeError):
            # not a date
            return value

    data = (
        [
            _format_timestamp(v, headers[i], column_date_formats)
            for i, v in enumerate(row)
        ]
        for row in data
    )
    return data, headers
