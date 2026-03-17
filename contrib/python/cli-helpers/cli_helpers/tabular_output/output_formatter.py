# -*- coding: utf-8 -*-
"""A generic tabular data output formatter interface."""

from __future__ import unicode_literals
from collections import namedtuple

from cli_helpers.compat import (
    text_type,
    binary_type,
    int_types,
    float_types,
    zip_longest,
)
from cli_helpers.utils import unique_items
from . import (
    delimited_output_adapter,
    vertical_table_adapter,
    tabulate_adapter,
    tsv_output_adapter,
    json_output_adapter,
)
from decimal import Decimal

import itertools

MISSING_VALUE = "<null>"
MAX_FIELD_WIDTH = 500

TYPES = {
    type(None): 0,
    bool: 1,
    int: 2,
    float: 3,
    Decimal: 3,
    binary_type: 4,
    text_type: 5,
}

OutputFormatHandler = namedtuple(
    "OutputFormatHandler", "format_name preprocessors formatter formatter_args"
)


class TabularOutputFormatter(object):
    """An interface to various tabular data formatting libraries.

    The formatting libraries supported include:
      - `tabulate <https://bitbucket.org/astanin/python-tabulate>`_
      - `terminaltables <https://robpol86.github.io/terminaltables/>`_
      - a CLI Helper vertical table layout
      - delimited formats (CSV and TSV)

    :param str format_name: An optional, default format name.

    Usage::

      >>> from cli_helpers.tabular_output import TabularOutputFormatter
      >>> formatter = TabularOutputFormatter(format_name='simple')
      >>> data = ((1, 87), (2, 80), (3, 79))
      >>> headers = ('day', 'temperature')
      >>> print(formatter.format_output(data, headers))
        day    temperature
      -----  -------------
          1             87
          2             80
          3             79

    You can use any :term:`iterable` for the data or headers::

      >>> data = enumerate(('87', '80', '79'), 1)
      >>> print(formatter.format_output(data, headers))
        day    temperature
      -----  -------------
          1             87
          2             80
          3             79

    """

    _output_formats = {}

    def __init__(self, format_name=None):
        """Set the default *format_name*."""
        self._format_name = None

        if format_name:
            self.format_name = format_name

    @property
    def format_name(self):
        """The current format name.

        This value must be in :data:`supported_formats`.

        """
        return self._format_name

    @format_name.setter
    def format_name(self, format_name):
        """Set the default format name.

        :param str format_name: The display format name.
        :raises ValueError: if the format is not recognized.

        """
        if format_name in self.supported_formats:
            self._format_name = format_name
        else:
            raise ValueError('unrecognized format_name "{}"'.format(format_name))

    @property
    def supported_formats(self):
        """The names of the supported output formats in a :class:`tuple`."""
        return tuple(self._output_formats.keys())

    @classmethod
    def register_new_formatter(
        cls, format_name, handler, preprocessors=(), kwargs=None
    ):
        """Register a new output formatter.

        :param str format_name: The name of the format.
        :param callable handler: The function that formats the data.
        :param tuple preprocessors: The preprocessors to call before
            formatting.
        :param dict kwargs: Keys/values for keyword argument defaults.

        """
        cls._output_formats[format_name] = OutputFormatHandler(
            format_name, preprocessors, handler, kwargs or {}
        )

    def format_output(
        self,
        data,
        headers,
        format_name=None,
        preprocessors=(),
        column_types=None,
        **kwargs
    ):
        r"""Format the headers and data using a specific formatter.

        *format_name* must be a supported formatter (see
        :attr:`supported_formats`).

        :param iterable data: An :term:`iterable` (e.g. list) of rows.
        :param iterable headers: The column headers.
        :param str format_name: The display format to use (optional, if the
            :class:`TabularOutputFormatter` object has a default format set).
        :param tuple preprocessors: Additional preprocessors to call before
                                    any formatter preprocessors.
        :param \*\*kwargs: Optional arguments for the formatter.
        :return: The formatted data.
        :rtype: str
        :raises ValueError: If the *format_name* is not recognized.

        """
        format_name = format_name or self._format_name
        if format_name not in self.supported_formats:
            raise ValueError('unrecognized format "{}"'.format(format_name))

        (_, _preprocessors, formatter, fkwargs) = self._output_formats[format_name]
        fkwargs.update(kwargs)
        if column_types is None:
            data = list(data)
            column_types = self._get_column_types(data)
        for f in unique_items(preprocessors + _preprocessors):
            data, headers = f(data, headers, column_types=column_types, **fkwargs)
        return formatter(list(data), headers, column_types=column_types, **fkwargs)

    def _get_column_types(self, data):
        """Get a list of the data types for each column in *data*."""
        columns = list(zip_longest(*data))
        return [self._get_column_type(column) for column in columns]

    def _get_column_type(self, column):
        """Get the most generic data type for iterable *column*."""
        type_values = [TYPES[self._get_type(v)] for v in column]
        inverse_types = {v: k for k, v in TYPES.items()}
        return inverse_types[max(type_values)]

    def _get_type(self, value):
        """Get the data type for *value*."""
        if value is None:
            return type(None)
        elif type(value) in int_types:
            return int
        elif type(value) in float_types:
            return float
        elif isinstance(value, binary_type):
            return binary_type
        else:
            return text_type


def format_output(data, headers, format_name, **kwargs):
    r"""Format output using *format_name*.

    This is a wrapper around the :class:`TabularOutputFormatter` class.

    :param iterable data: An :term:`iterable` (e.g. list) of rows.
    :param iterable headers: The column headers.
    :param str format_name: The display format to use.
    :param \*\*kwargs: Optional arguments for the formatter.
    :return: The formatted data.
    :rtype: str

    """
    formatter = TabularOutputFormatter(format_name=format_name)
    return formatter.format_output(data, headers, **kwargs)


for vertical_format in vertical_table_adapter.supported_formats:
    TabularOutputFormatter.register_new_formatter(
        vertical_format,
        vertical_table_adapter.adapter,
        vertical_table_adapter.preprocessors,
        {
            "table_format": vertical_format,
            "missing_value": MISSING_VALUE,
            "max_field_width": None,
        },
    )

for delimited_format in delimited_output_adapter.supported_formats:
    TabularOutputFormatter.register_new_formatter(
        delimited_format,
        delimited_output_adapter.adapter,
        delimited_output_adapter.preprocessors,
        {
            "table_format": delimited_format,
            "missing_value": "",
            "max_field_width": None,
        },
    )

for tabulate_format in tabulate_adapter.supported_formats:
    TabularOutputFormatter.register_new_formatter(
        tabulate_format,
        tabulate_adapter.adapter,
        tabulate_adapter.get_preprocessors(tabulate_format),
        {
            "table_format": tabulate_format,
            "missing_value": MISSING_VALUE,
            "max_field_width": MAX_FIELD_WIDTH,
        },
    ),

for tsv_format in tsv_output_adapter.supported_formats:
    TabularOutputFormatter.register_new_formatter(
        tsv_format,
        tsv_output_adapter.adapter,
        tsv_output_adapter.preprocessors,
        {"table_format": tsv_format, "missing_value": "", "max_field_width": None},
    )

for json_format in json_output_adapter.supported_formats:
    TabularOutputFormatter.register_new_formatter(
        json_format,
        json_output_adapter.adapter,
        json_output_adapter.preprocessors,
        {
            "table_format": json_format,
            "max_field_width": None,
        },
    )
