#
# Copyright (C) 2011 - 2025 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=import-error
"""A backend module using simplejson to load and dump JSON data files.

- Format to support: JSON, http://www.json.org
- Requirements: simplejson
- Development Status :: 5 - Production/Stable
- Limitations: None obvious
- Special options:

  - All options of simplejson.load{s,} and simplejson.dump{s,} except
    object_hook should work.

  - See also: https://simplejson.readthedocs.io

Changelog:

.. versionchanged:: 0.9.8

   - Exported from ..json.py
"""
from __future__ import annotations

import typing

import simplejson as json

from .. import base
from .common import (
    JSON_LOAD_OPTS, JSON_DUMP_OPTS, Parser as BaseParser,
)


JSON_LOAD_OPTS = (*JSON_LOAD_OPTS, "use_decimal")
JSON_DUMP_OPTS = (
    *JSON_DUMP_OPTS,
    "use_decimal", "namedtuple_as_object", "tuple_as_array",
    "bigint_as_string", "item_sort_key", "for_json",
    "ignore_nan", "int_as_string_bitcount",
    "iterable_as_array",
)


class Parser(BaseParser):
    """Parser for JSON files using simplejson."""

    _cid: typing.ClassVar[str] = "json.simplejson"
    _load_opts = JSON_LOAD_OPTS
    _dump_opts = JSON_DUMP_OPTS

    _load_from_string_fn = base.to_method(json.loads)
    _load_from_stream_fn = base.to_method(json.load)
    _dump_to_string_fn = base.to_method(json.dumps)
    _dump_to_stream_fn = base.to_method(json.dump)
