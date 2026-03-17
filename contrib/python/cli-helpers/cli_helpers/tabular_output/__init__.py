# -*- coding: utf-8 -*-
"""CLI Helper's tabular output module makes it easy to format your data using
various formatting libraries.

When formatting data, you'll primarily use the
:func:`~cli_helpers.tabular_output.format_output` function and
:class:`~cli_helpers.tabular_output.TabularOutputFormatter` class.

"""

from .output_formatter import format_output, TabularOutputFormatter

__all__ = ["format_output", "TabularOutputFormatter"]
