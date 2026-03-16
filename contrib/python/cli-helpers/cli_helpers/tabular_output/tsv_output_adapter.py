# -*- coding: utf-8 -*-
"""A tsv data output adapter"""

from __future__ import unicode_literals

from .preprocessors import bytes_to_string, override_missing_value, convert_to_string
from itertools import chain
from cli_helpers.utils import replace

supported_formats = ("tsv", "tsv_noheader")
preprocessors = (override_missing_value, bytes_to_string, convert_to_string)


def adapter(data, headers, table_format="tsv", **kwargs):
    """Wrap the formatting inside a function for TabularOutputFormatter."""
    if table_format == "tsv":
        for row in chain((headers,), data):
            yield "\t".join((replace(r, (("\n", r"\n"), ("\t", r"\t"))) for r in row))
    elif table_format == "tsv_noheader":
        for row in data:
            yield "\t".join((replace(r, (("\n", r"\n"), ("\t", r"\t"))) for r in row))
    else:
        raise ValueError(f"Invalid table_format specified: {table_format}.")
