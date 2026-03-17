# -*- coding: utf-8 -*-
"""A delimited data output adapter (e.g. CSV, TSV)."""

from __future__ import unicode_literals
import contextlib

from cli_helpers.compat import csv, StringIO
from cli_helpers.utils import filter_dict_by_key
from .preprocessors import bytes_to_string, override_missing_value

supported_formats = ("csv", "csv-tab", "csv-noheader", "csv-tab-noheader")
preprocessors = (override_missing_value, bytes_to_string)


class linewriter(object):
    def __init__(self):
        self.reset()

    def reset(self):
        self.line = None

    def write(self, d):
        self.line = d


def adapter(data, headers, table_format="csv", **kwargs):
    """Wrap the formatting inside a function for TabularOutputFormatter."""
    keys = (
        "dialect",
        "delimiter",
        "doublequote",
        "escapechar",
        "quotechar",
        "quoting",
        "skipinitialspace",
        "strict",
    )
    if table_format in ("csv", "csv-noheader"):
        delimiter = ","
    elif table_format in ("csv-tab", "csv-tab-noheader"):
        delimiter = "\t"
    else:
        raise ValueError("Invalid table_format specified.")

    ckwargs = {"delimiter": delimiter, "lineterminator": ""}
    ckwargs.update(filter_dict_by_key(kwargs, keys))

    l = linewriter()
    writer = csv.writer(l, **ckwargs)
    if "noheader" not in table_format:
        writer.writerow(headers)
        yield l.line

    for row in data:
        l.reset()
        writer.writerow(row)
        yield l.line
