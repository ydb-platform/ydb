#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
# Ref. python -c "import json; help(json)"
#
# pylint: disable=import-error
r"""JSON backend:

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
from __future__ import absolute_import

import json
import anyconfig.backend.base

from .common import Parser as BaseParser


class Parser(BaseParser):
    """
    Parser for JSON files.
    """
    _cid = "std.json"
    _priority = 30  # Higher priority than others.

    _load_from_string_fn = anyconfig.backend.base.to_method(json.loads)
    _load_from_stream_fn = anyconfig.backend.base.to_method(json.load)
    _dump_to_string_fn = anyconfig.backend.base.to_method(json.dumps)
    _dump_to_stream_fn = anyconfig.backend.base.to_method(json.dump)

# vim:sw=4:ts=4:et:
