#
# Copyright (C) 2018 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Globals, functions common in some JSON backend modules.

Changelog:

.. versionadded:: 0.9.8
"""
from __future__ import annotations

import typing

from .. import base


JSON_LOAD_OPTS: tuple[str, ...] = (
    "cls", "object_hook", "parse_float", "parse_int",
    "parse_constant", "object_pairs_hook",
)
JSON_DUMP_OPTS: tuple[str, ...] = (
    "skipkeys", "ensure_ascii", "check_circular", "allow_nan",
    "cls", "indent", "separators", "default", "sort_keys",
)
JSON_DICT_OPTS: tuple[str, ...] = (
    "object_pairs_hook", "object_hook",
)


class Parser(base.StringStreamFnParser):
    """Parser for JSON files."""

    _cid: typing.ClassVar[str] = "json.stdlib"
    _type: typing.ClassVar[str] = "json"
    _extensions: tuple[str, ...] = ("json", "jsn", "js")
    _ordered: typing.ClassVar[bool] = True
    _allow_primitives: typing.ClassVar[bool] = True

    # .. note:: These may be overwritten.
    _load_opts = JSON_LOAD_OPTS
    _dump_opts = JSON_DUMP_OPTS
    _dict_opts = JSON_DICT_OPTS
