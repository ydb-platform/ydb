#
# Copyright (C) 2016 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
r"""Simple Shell vars' definitions backend:

- Format to support: Simple shell variables' definitions w/o any shell variable
  expansions nor complex shell statements such as conditionals, etc.
- Requirements: None (built-in)
- Development Status :: 3 - Alpha
- Limitations: Currently, it only supports a varialbe defined in a line.
- Special options: None

Changelog:

.. versionadded:: 0.7.0

   - Added an experimental parser for simple shelll vars' definitions w/o shell
     variable expansions nor complex shell statements like conditionals.
"""
from __future__ import absolute_import

import logging
import itertools
import os
import re

import anyconfig.backend.base


LOGGER = logging.getLogger(__name__)


def _parseline(line):
    """
    Parse a line contains shell variable definition.

    :param line: A string to parse, must not start with '#' (comment)
    :return: A tuple of (key, value), both key and value may be None

    >>> _parseline("aaa=")
    ('aaa', '')
    >>> _parseline("aaa=bbb")
    ('aaa', 'bbb')
    >>> _parseline("aaa='bb b'")
    ('aaa', 'bb b')
    >>> _parseline('aaa="bb#b"')
    ('aaa', 'bb#b')
    >>> _parseline('aaa="bb\\"b"')
    ('aaa', 'bb"b')
    >>> _parseline("aaa=bbb   # ccc")
    ('aaa', 'bbb')
    """
    match = re.match(r"^\s*(export)?\s*(\S+)=(?:(?:"
                     r"(?:\"(.*[^\\])\")|(?:'(.*[^\\])')|"
                     r"(?:([^\"'#\s]+)))?)\s*#*", line)
    if not match:
        LOGGER.warning("Invalid line found: %s", line)
        return (None, None)

    tpl = match.groups()
    vals = list(itertools.dropwhile(lambda x: x is None, tpl[2:]))
    return (tpl[1], vals[0] if vals else '')


def load(stream, container=dict):
    """
    Load and parse a file or file-like object 'stream' provides simple shell
    variables' definitions.

    :param stream: A file or file like object
    :param container:
        Factory function to create a dict-like object to store properties
    :return: Dict-like object holding shell variables' definitions

    >>> from anyconfig.compat import StringIO as to_strm
    >>> load(to_strm(''))
    {}
    >>> load(to_strm("# "))
    {}
    >>> load(to_strm("aaa="))
    {'aaa': ''}
    >>> load(to_strm("aaa=bbb"))
    {'aaa': 'bbb'}
    >>> load(to_strm("aaa=bbb # ..."))
    {'aaa': 'bbb'}
    """
    ret = container()

    for line in stream:
        line = line.rstrip()
        if line is None or not line:
            continue

        (key, val) = _parseline(line)
        if key is None:
            LOGGER.warning("Empty val in the line: %s", line)
            continue

        ret[key] = val

    return ret


class Parser(anyconfig.backend.base.StreamParser):
    """
    Parser for Shell variable definition files.
    """
    _cid = "shellvars"
    _type = "shellvars"
    _ordered = True
    _dict_opts = ["ac_dict"]

    def load_from_stream(self, stream, container, **kwargs):
        """
        Load config from given file like object 'stream'.

        :param stream:
            A file or file like object of shell scripts define shell variables
        :param container: callble to make a container object
        :param kwargs: optional keyword parameters (ignored)

        :return: Dict-like object holding config parameters
        """
        return load(stream, container=container)

    def dump_to_stream(self, cnf, stream, **kwargs):
        """
        Dump config 'cnf' to a file or file-like object 'stream'.

        :param cnf: Shell variables data to dump
        :param stream: Shell script file or file like object
        :param kwargs: backend-specific optional keyword parameters :: dict
        """
        for key, val in anyconfig.compat.iteritems(cnf):
            stream.write("%s='%s'%s" % (key, val, os.linesep))

# vim:sw=4:ts=4:et:
