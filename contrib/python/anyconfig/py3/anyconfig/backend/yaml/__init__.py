#
# Copyright (C) 2011 - 2024 Satoru SATOH <satoru.satoh @ gmail.com>
# SPDX-License-Identifier: MIT
#
"""Backend modules to load and dump YAML data files.

- pyyaml: PyYAML, http://pyyaml.org [default]
- ruamel.yaml: ruamel.yaml, https://bitbucket.org/ruamel/yaml

Changelog:

.. versionchanged:: 0.9.8

   - Split PyYaml-based and ruamel.yaml based backend modules
   - Add support of some of ruamel.yaml specific features.
"""
from ..base import ParserClssT


try:
    from . import pyyaml
    PARSERS: ParserClssT = [pyyaml.Parser]
except ImportError:
    PARSERS: ParserClssT = []  # type: ignore[no-redef]

try:
    from . import ruamel
    PARSERS.append(ruamel.Parser)
except ImportError:
    pass

# vim:sw=4:ts=4:et:
