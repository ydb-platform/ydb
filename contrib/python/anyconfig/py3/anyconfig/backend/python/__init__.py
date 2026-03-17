#
# Copyright (C) 2023, 2024 Satoru SATOH <satoru.satoh @ gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump python code holding data.

- python.builtin: builtin parser [default]

Changelog:

.. versionadded:: 0.14.0
"""
from . import builtin
from ..base import ParserClssT


PARSERS: ParserClssT = [builtin.Parser]
