# -*- coding: utf-8 -*-
"""Format data into a vertical table layout."""

from __future__ import unicode_literals

from cli_helpers.utils import filter_dict_by_key
from .preprocessors import convert_to_string, override_missing_value, style_output

supported_formats = ("vertical",)
preprocessors = (override_missing_value, convert_to_string, style_output)


def _get_separator(num, sep_title, sep_character, sep_length):
    """Get a row separator for row *num*."""
    left_divider_length = right_divider_length = sep_length
    if isinstance(sep_length, tuple):
        left_divider_length, right_divider_length = sep_length
    left_divider = sep_character * left_divider_length
    right_divider = sep_character * right_divider_length
    title = sep_title.format(n=num + 1)

    return "{left_divider}[ {title} ]{right_divider}\n".format(
        left_divider=left_divider, right_divider=right_divider, title=title
    )


def _format_row(headers, row):
    """Format a row."""
    formatted_row = [" | ".join(field) for field in zip(headers, row)]
    return "\n".join(formatted_row)


def vertical_table(
    data, headers, sep_title="{n}. row", sep_character="*", sep_length=27
):
    """Format *data* and *headers* as an vertical table.

    The values in *data* and *headers* must be strings.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param str sep_title: The title given to each row separator. Defaults to
                          ``'{n}. row'``. Any instance of ``'{n}'`` is
                          replaced by the record number.
    :param str sep_character: The character used to separate rows. Defaults to
                              ``'*'``.
    :param int/tuple sep_length: The number of separator characters that should
                                 appear on each side of the *sep_title*. Use
                                 a tuple to specify the left and right values
                                 separately.
    :return: The formatted data.
    :rtype: str

    """
    header_len = max([len(x) for x in headers])
    padded_headers = [x.ljust(header_len) for x in headers]
    formatted_rows = [_format_row(padded_headers, row) for row in data]

    output = []
    for i, result in enumerate(formatted_rows):
        yield _get_separator(i, sep_title, sep_character, sep_length) + result


def adapter(data, headers, **kwargs):
    """Wrap vertical table in a function for TabularOutputFormatter."""
    keys = ("sep_title", "sep_character", "sep_length")
    return vertical_table(data, headers, **filter_dict_by_key(kwargs, keys))
