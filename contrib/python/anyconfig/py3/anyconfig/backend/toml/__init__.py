#
# Copyright (C) 2015 - 2023 Satoru SATOH <satoru.satoh @ gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump TOML data files.

- toml: https://github.com/uiri/toml
- tomli: https://github.com/hukkin/tomli
- tomllib and tomli-w:
  - tomlib: https://docs.python.org/3/library/tomllib.html
  - toml-w: https://github.com/hukkin/tomli-w
- tomlkit: https://tomlkit.readthedocs.io/

Changelog:

.. versionchanged:: 0.9.8

   - Added tomli, tomllib and tomli-w, and tomlkit support
"""
from ..base import ParserClssT

PARSERS: ParserClssT = []

try:
    from . import tomllib
    PARSERS.append(tomllib.Parser)
except ImportError:
    pass

try:
    from . import toml
    PARSERS.append(toml.Parser)
except ImportError:
    pass

try:
    from . import tomlkit
    PARSERS.append(tomlkit.Parser)
except ImportError:
    pass
