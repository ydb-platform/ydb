#
# Copyright (C) 2011 - 2024 Satoru SATOH <satoru.satoh @ gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump XML files.

- xml.etree: parser using ElementTree in standard library [default]

Changelog:

.. versionchanged:: 0.14.0

   - Re-organized
"""
from . import etree
from ..base import ParserClssT


PARSERS: ParserClssT = [etree.Parser]
