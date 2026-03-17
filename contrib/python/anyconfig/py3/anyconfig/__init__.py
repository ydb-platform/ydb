#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# For 'anyconfig.open':
# pylint: disable=redefined-builtin
r"""anyconfig provides generic interface for config files in various formats.

.. module:: anyconfig
   :platform: Unix, Windows
   :synopsis: Generic interface to load config file in various formats.

python-anyconfig is a `MIT licensed <http://opensource.org/licenses/MIT>`_
python library provides common APIs to access to configuration files in various
formats with some useful features such as contents merge, templates and schema
validation/generation support.

- Home: https://github.com/ssato/python-anyconfig
- (Latest) Doc: http://python-anyconfig.readthedocs.org/en/latest/
- PyPI: https://pypi.python.org/pypi/anyconfig
- Copr RPM repos: https://copr.fedoraproject.org/coprs/ssato/python-anyconfig/

"""
from .api import (
    dump, dumps, single_load, multi_load, load, loads,
    open, version,
    UnknownFileTypeError, UnknownParserTypeError,
    UnknownProcessorTypeError, ValidationError,
    MS_REPLACE, MS_NO_REPLACE, MS_DICTS, MS_DICTS_AND_LISTS, MERGE_STRATEGIES,
    merge, get, set_,
    load_plugins, list_types, list_by_cid, list_by_type, list_by_extension,
    findall, find,
    try_query,
    validate, is_valid, gen_schema,
)


__all__ = [
    "dump", "dumps",  # dump APIs.
    "single_load", "multi_load", "load", "loads",  # load APIs.
    "get", "set_",  # accessor APIs.
    "merge", "open", "version",  # other APIs.

    # anyconfig.common
    "UnknownFileTypeError",
    "UnknownParserTypeError",
    "UnknownProcessorTypeError",
    "ValidationError",

    # anyconfig.dicsts
    "MERGE_STRATEGIES",
    "MS_DICTS",
    "MS_DICTS_AND_LISTS",
    "MS_NO_REPLACE",
    "MS_REPLACE",

    # anyconfig.parsers
    "find",
    "findall",
    "list_by_cid",
    "list_by_extension",
    "list_by_type",
    "list_types",
    "load_plugins",

    # anyconfig.query
    "try_query",

    # anyconfig.validate
    "validate", "is_valid", "gen_schema",  # validation APIs.
]

# vim:sw=4:ts=4:et:
