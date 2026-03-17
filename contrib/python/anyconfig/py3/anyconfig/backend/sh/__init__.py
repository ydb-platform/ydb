#
# Copyright (C) 2011 - 2024 Satoru SATOH <satoru.satoh @ gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump sh files.

- sh.variables: builtin parser for shell variables

Changelog:

.. versionchanged:: 0.14.0

   - Re-organized
"""
from . import variables
from ..base import ParserClssT


PARSERS: ParserClssT = [variables.Parser]
