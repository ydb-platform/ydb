#
# Copyright (C) 2011 - 2025 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# Ref. python -c "import json; help(json)"
#
# pylint: disable=import-error
"""A backend module using the standard json lib to load and dump JSON data.

- Format to support: JSON, http://www.json.org
- Requirements: json in python standard library
- Development Status :: 5 - Production/Stable
- Limitations: None obvious
- Special options:

  - All options of json.load{s,} and json.dump{s,} except object_hook
    should work.

  - See also: https://docs.python.org/3/library/json.html or
    https://docs.python.org/2/library/json.html

Changelog:

.. versionchanged:: 0.9.8

   - Moved from ..json.py
   - Drop simplejson support from this module

.. versionchanged:: 0.9.6

   - Add support of loading primitives other than mapping objects.

.. versionadded:: 0.0.1
"""
from __future__ import annotations

import json
import typing

from .. import base
from .common import Parser as BaseParser


class Parser(BaseParser):
    """Parser for JSON files."""

    _cid: typing.ClassVar[str] = "json.stdlib"
    _priority: typing.ClassVar[int] = 30  # Higher priority than others.

    _load_from_string_fn = base.to_method(json.loads)
    _load_from_stream_fn = base.to_method(json.load)
    _dump_to_string_fn = base.to_method(json.dumps)
    _dump_to_stream_fn = base.to_method(json.dump)
