#
# Copyright (C) 2011 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump INI data.

- ini.configparser: python standard support library [default]

Changelog:

.. versionchanged:: 0.14.0

   - Re-organized
"""
from . import configparser
from ..base import ParserClssT


PARSERS: ParserClssT = [configparser.Parser]
