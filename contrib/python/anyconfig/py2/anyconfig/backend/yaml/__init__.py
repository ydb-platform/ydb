#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
r"""YAML backends:

- pyyaml: PyYAML, http://pyyaml.org [default]
- ruamel.yaml: ruamel.yaml, https://bitbucket.org/ruamel/yaml

Changelog:

.. versionchanged:: 0.9.8

   - Split PyYaml-based and ruamel.yaml based backend modules
   - Add support of some of ruamel.yaml specific features.
"""
from __future__ import absolute_import

try:
    from . import pyyaml
    PARSERS = [pyyaml.Parser]
except ImportError:
    PARSERS = []

try:
    from . import ruamel_yaml as ryaml
    PARSERS.append(ryaml.Parser)
except ImportError:
    ryaml = False

# vim:sw=4:ts=4:et:
