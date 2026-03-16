# -*- coding: utf-8 -*-
"""Format adapter for the tabulate module."""

from __future__ import unicode_literals

import os

from cli_helpers.utils import filter_dict_by_key
from cli_helpers.compat import (
    Terminal256Formatter,
    TerminalTrueColorFormatter,
    Token,
    StringIO,
)
from .preprocessors import (
    convert_to_string,
    truncate_string,
    override_missing_value,
    style_output,
    HAS_PYGMENTS,
    escape_newlines,
)

import tabulate


tabulate.MIN_PADDING = 0

tabulate._table_formats["psql_unicode"] = tabulate.TableFormat(
    lineabove=tabulate.Line("┌", "─", "┬", "┐"),
    linebelowheader=tabulate.Line("├", "─", "┼", "┤"),
    linebetweenrows=None,
    linebelow=tabulate.Line("└", "─", "┴", "┘"),
    headerrow=tabulate.DataRow("│", "│", "│"),
    datarow=tabulate.DataRow("│", "│", "│"),
    padding=1,
    with_header_hide=None,
)

tabulate._table_formats["double"] = tabulate.TableFormat(
    lineabove=tabulate.Line("╔", "═", "╦", "╗"),
    linebelowheader=tabulate.Line("╠", "═", "╬", "╣"),
    linebetweenrows=None,
    linebelow=tabulate.Line("╚", "═", "╩", "╝"),
    headerrow=tabulate.DataRow("║", "║", "║"),
    datarow=tabulate.DataRow("║", "║", "║"),
    padding=1,
    with_header_hide=None,
)

tabulate._table_formats["ascii"] = tabulate.TableFormat(
    lineabove=tabulate.Line("+", "-", "+", "+"),
    linebelowheader=tabulate.Line("+", "-", "+", "+"),
    linebetweenrows=None,
    linebelow=tabulate.Line("+", "-", "+", "+"),
    headerrow=tabulate.DataRow("|", "|", "|"),
    datarow=tabulate.DataRow("|", "|", "|"),
    padding=1,
    with_header_hide=None,
)

tabulate._table_formats["ascii_escaped"] = tabulate.TableFormat(
    lineabove=tabulate.Line("+", "-", "+", "+"),
    linebelowheader=tabulate.Line("+", "-", "+", "+"),
    linebetweenrows=None,
    linebelow=tabulate.Line("+", "-", "+", "+"),
    headerrow=tabulate.DataRow("|", "|", "|"),
    datarow=tabulate.DataRow("|", "|", "|"),
    padding=1,
    with_header_hide=None,
)

tabulate._table_formats["mysql"] = tabulate.TableFormat(
    lineabove=tabulate.Line("+", "-", "+", "+"),
    linebelowheader=tabulate.Line("+", "-", "+", "+"),
    linebetweenrows=None,
    linebelow=tabulate.Line("+", "-", "+", "+"),
    headerrow=tabulate.DataRow("|", "|", "|"),
    datarow=tabulate.DataRow("|", "|", "|"),
    padding=1,
    with_header_hide=None,
)

tabulate._table_formats["mysql_unicode"] = tabulate.TableFormat(
    lineabove=tabulate.Line("┌", "─", "┬", "┐"),
    linebelowheader=tabulate.Line("├", "─", "┼", "┤"),
    linebetweenrows=None,
    linebelow=tabulate.Line("└", "─", "┴", "┘"),
    headerrow=tabulate.DataRow("│", "│", "│"),
    datarow=tabulate.DataRow("│", "│", "│"),
    padding=1,
    with_header_hide=None,
)

# "minimal" is the same as "plain", but without headers
tabulate._table_formats["minimal"] = tabulate._table_formats["plain"]

tabulate.multiline_formats["psql_unicode"] = "psql_unicode"
tabulate.multiline_formats["double"] = "double"
tabulate.multiline_formats["ascii"] = "ascii"
tabulate.multiline_formats["minimal"] = "minimal"
tabulate.multiline_formats["mysql"] = "mysql"
tabulate.multiline_formats["mysql_unicode"] = "mysql_unicode"

supported_markup_formats = (
    "mediawiki",
    "html",
    "latex",
    "latex_booktabs",
    "textile",
    "moinmoin",
    "jira",
)
supported_table_formats = (
    "ascii",
    "ascii_escaped",
    "plain",
    "simple",
    "minimal",
    "grid",
    "fancy_grid",
    "pipe",
    "orgtbl",
    "psql",
    "psql_unicode",
    "rst",
    "github",
    "double",
    "mysql",
    "mysql_unicode",
)

supported_formats = supported_markup_formats + supported_table_formats

default_kwargs = {
    "ascii": {"numalign": "left"},
    "ascii_escaped": {"numalign": "left"},
    "mysql": {"numalign": "right"},
    "mysql_unicode": {"numalign": "right"},
}
headless_formats = ("minimal",)


def get_preprocessors(format_name):
    common_formatters = (
        override_missing_value,
        convert_to_string,
        truncate_string,
        style_output,
    )

    if tabulate.multiline_formats.get(format_name):
        return common_formatters + (style_output_table(format_name),)
    else:
        return common_formatters + (escape_newlines, style_output_table(format_name))


def style_output_table(format_name=""):
    def style_output(
        data,
        headers,
        style=None,
        table_separator_token=Token.Output.TableSeparator,
        **_,
    ):
        """Style the *table* a(e.g. bold, italic, and colors)

        .. NOTE::
            This requires the `Pygments <http://pygments.org/>`_ library to
            be installed. You can install it with CLI Helpers as an extra::
                $ pip install cli_helpers[styles]

        Example usage::

            from cli_helpers.tabular_output import tabulate_adapter
            from pygments.style import Style
            from pygments.token import Token

            class YourStyle(Style):
                default_style = ""
                styles = {
                    Token.Output.TableSeparator: '#ansigray'
                }

            headers = ('First Name', 'Last Name')
            data = [['Fred', 'Roberts'], ['George', 'Smith']]
            style_output_table = tabulate_adapter.style_output_table('psql')
            style_output_table(data, headers, style=CliStyle)

            data, headers = style_output(data, headers, style=YourStyle)
            output = tabulate_adapter.adapter(data, headers, style=YourStyle)

        :param iterable data: An :term:`iterable` (e.g. list) of rows.
        :param iterable headers: The column headers.
        :param str/pygments.style.Style style: A Pygments style. You can `create
        your own styles <https://pygments.org/docs/styles#creating-own-styles>`_.
        :param str table_separator_token: The token type to be used for the table separator.
        :return: data and headers.
        :rtype: tuple

        """
        if style and HAS_PYGMENTS and format_name in supported_table_formats:
            if "truecolor" in os.getenv("COLORTERM", "").lower():
                formatter = TerminalTrueColorFormatter(style=style)
            else:
                formatter = Terminal256Formatter(style=style)

            def style_field(token, field):
                """Get the styled text for a *field* using *token* type."""
                s = StringIO()
                formatter.format(((token, field),), s)
                return s.getvalue()

            def addColorInElt(elt):
                if not elt:
                    return elt
                if elt.__class__ == tabulate.Line:
                    return tabulate.Line(
                        *(style_field(table_separator_token, val) for val in elt)
                    )
                if elt.__class__ == tabulate.DataRow:
                    return tabulate.DataRow(
                        *(style_field(table_separator_token, val) for val in elt)
                    )
                return elt

            srcfmt = tabulate._table_formats[format_name]
            newfmt = tabulate.TableFormat(*(addColorInElt(val) for val in srcfmt))
            tabulate._table_formats[format_name] = newfmt

        return iter(data), headers

    return style_output


def adapter(data, headers, table_format=None, preserve_whitespace=False, **kwargs):
    """Wrap tabulate inside a function for TabularOutputFormatter."""
    keys = (
        "floatfmt",
        "numalign",
        "stralign",
        "showindex",
        "disable_numparse",
        "colalign",
    )
    tkwargs = {"tablefmt": table_format}
    tkwargs.update(filter_dict_by_key(kwargs, keys))

    if table_format in supported_markup_formats:
        tkwargs.update(numalign=None, stralign=None)

    tabulate.PRESERVE_WHITESPACE = preserve_whitespace

    tkwargs.update(default_kwargs.get(table_format, {}))
    if table_format in headless_formats:
        headers = []
    return iter(tabulate.tabulate(data, headers, **tkwargs).split("\n"))
