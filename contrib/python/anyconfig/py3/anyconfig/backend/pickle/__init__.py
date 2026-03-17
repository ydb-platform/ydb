#
# Copyright (C) 2011 - 2024 Satoru SATOH <satoru.satoh@gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump pickle data.

- pickle.stdlib: python standard library to load and dump pickle files
  [default]

Changelog:

.. versionchanged:: 0.14.0

   - Re-organized
"""
from . import stdlib
from ..base import ParserClssT


PARSERS: ParserClssT = [stdlib.Parser]
