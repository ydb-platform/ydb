#
# Copyright (C) 2012 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
# For 'anyconfig.open':
# pylint: disable=redefined-builtin
r"""
.. module:: anyconfig
   :platform: Unix, Windows
   :synopsis: Generic interface to loaders for various config file formats.

python-anyconfig is a `MIT licensed <http://opensource.org/licenses/MIT>`_
python library provides common APIs to access to configuration files in various
formats with some useful features such as contents merge, templates and schema
validation/generation support.

- Home: https://github.com/ssato/python-anyconfig
- (Latest) Doc: http://python-anyconfig.readthedocs.org/en/latest/
- PyPI: https://pypi.python.org/pypi/anyconfig
- Copr RPM repos: https://copr.fedoraproject.org/coprs/ssato/python-anyconfig/

"""
from .globals import AUTHOR, VERSION
from .api import (
    single_load, multi_load, load, loads, dump, dumps, validate, gen_schema,
    list_types, find, findall, merge, get, set_, open,
    MS_REPLACE, MS_NO_REPLACE, MS_DICTS, MS_DICTS_AND_LISTS,
    UnknownProcessorTypeError, UnknownFileTypeError
)

__author__ = AUTHOR
__version__ = VERSION

__all__ = [
    "single_load", "multi_load", "load", "loads", "dump", "dumps", "validate",
    "gen_schema", "list_types", "find", "findall", "merge",
    "get", "set_", "open",
    "MS_REPLACE", "MS_NO_REPLACE", "MS_DICTS", "MS_DICTS_AND_LISTS",
    "UnknownProcessorTypeError", "UnknownFileTypeError"
]

# vim:sw=4:ts=4:et:
