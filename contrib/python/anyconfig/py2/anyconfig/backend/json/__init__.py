#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
r"""JSON backends:

- std.json: python standard JSON support library [default]
- simplejson: https://github.com/simplejson/simplejson

Changelog:

.. versionchanged:: 0.9.8

   - Started to split JSON support modules
"""
from __future__ import absolute_import
from . import default

Parser = default.Parser  # To keep backward compatibility.
PARSERS = [Parser]

try:
    from ._simplejson import Parser as SimpleJsonParser
    PARSERS.append(SimpleJsonParser)
except ImportError:
    pass

# vim:sw=4:ts=4:et:
