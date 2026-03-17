#
# Copyright (C) 2016 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""A simple backend module to load and dump files contain shell variables.

- Format to support: Simple shell variables' definitions w/o any shell variable
  expansions nor complex shell statements such as conditionals, etc.
- Requirements: None (built-in)
- Development Status :: 4 - Beta
- Limitations: Currently, it only supports a varialbe defined in a line.
- Special options: None

Changelog:

.. versionadded:: 0.7.0

   - Added an experimental parser for simple shelll vars' definitions w/o shell
     variable expansions nor complex shell statements like conditionals.
"""
from __future__ import annotations

import itertools
import re
import typing
import warnings

from .. import base
from ... import utils


def _parseline(
    line: str,
) -> tuple[str | None, str | None]:
    """Parse a line contains shell variable definition.

    :param line: A string to parse, must not start with '#' (comment)
    :return: A tuple of (key, value), both key and value may be None
    """
    match = re.match(
        r"^\s*(export)?\s*(\S+)=(?:(?:"
        r"(?:\"(.*[^\\])\")|(?:'(.*[^\\])')|"
        r"(?:([^\"'#\s]+)))?)\s*#*",
        line,
    )
    if not match:
        warnings.warn(
            f"Invalid line found: {line}", category=SyntaxWarning,
            stacklevel=2,
        )
        return (None, None)

    tpl = match.groups()
    vals = list(itertools.dropwhile(lambda x: x is None, tpl[2:]))
    return (tpl[1], vals[0] if vals else "")


def load(
    stream: typing.IO, container: base.GenContainerT = dict,
    **_kwargs: typing.Any,
) -> base.InDataT:
    """Load shell variable definitions data from ``stream``.

    :param stream: A file or file like object
    :param container:
        Factory function to create a dict-like object to store properties
    :return: Dict-like object holding shell variables' definitions
    """
    ret = container()

    for line_ in stream:
        line = line_.rstrip()
        if line is None or not line:
            continue

        (key, val) = _parseline(line)
        if key is None:
            warnings.warn(
                f"Empty val in the line: {line}",
                category=SyntaxWarning, stacklevel=2,
            )
            continue

        ret[key] = val

    return ret


class Parser(base.StreamParser):
    """Parser for Shell variable definition files."""

    _cid: typing.ClassVar[str] = "sh.variables"
    _type: typing.ClassVar[str] = "shellvars"
    _extensions: tuple[str, ...] = ("sh", )
    _ordered: typing.ClassVar[bool] = True
    _dict_opts: tuple[str, ...] = ("ac_dict", )

    def load_from_stream(
        self, stream: typing.IO, container: base.GenContainerT,
        **kwargs: typing.Any,
    ) -> base.InDataT:
        """Load config from given file like object ``stream``.

        :param stream:
            A file or file like object of shell scripts define shell variables
        :param container: callble to make a container object
        :param kwargs: optional keyword parameters (ignored)

        :return: Dict-like object holding config parameters
        """
        return load(stream, container=container, **kwargs)

    def dump_to_stream(
        self, cnf: base.InDataExT, stream: typing.IO,
        **_kwargs: typing.Any,
    ) -> None:
        """Dump config dat ``cnf`` to a file or file-like object ``stream``.

        :param cnf: Shell variables data to dump
        :param stream: Shell script file or file like object
        :param kwargs: backend-specific optional keyword parameters :: dict
        """
        if utils.is_dict_like(cnf):
            stream.writelines(
                f"{key}='{val}'\n"
                for key, val in cnf.items()
            )
