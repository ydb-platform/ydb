#
# Copyright (C) 2011 - 2024 Satoru SATOH <satoru.satoh @ gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump .properties files.

- properties.builtin: builtin parser [default]

Changelog:

.. versionchanged:: 0.14.0

   - Re-organized
"""
from . import builtin
from ..base import ParserClssT


PARSERS: ParserClssT = [builtin.Parser]
