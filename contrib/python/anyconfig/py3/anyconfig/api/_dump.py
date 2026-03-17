#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Provides the API to dump (serialize) objects."""
from __future__ import annotations

import typing

from .. import common, ioinfo, parsers

if typing.TYPE_CHECKING:
    from .datatypes import ParserT


def dump(
    data: common.InDataExT, out: ioinfo.PathOrIOInfoT,
    ac_parser: parsers.MaybeParserT = None,
    **options: typing.Any,
) -> None:
    """Save ``data`` to ``out`` in specified or detected format.

    :param data: A mapping object may have configurations data to dump
    :param out:
        An output file path, a file, a file-like object, :class:`pathlib.Path`
        object represents the file or a namedtuple 'anyconfig.ioinfo.IOInfo'
        object represents output to dump some data to.
    :param ac_parser: Forced parser type or parser object
    :param options:
        Backend specific optional arguments, e.g. {"indent": 2} for JSON
        loader/dumper backend

    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    ioi = ioinfo.make(out)
    psr: ParserT = parsers.find(ioi, forced_type=ac_parser)
    psr.dump(data, ioi, **options)


def dumps(
    data: common.InDataExT, ac_parser: parsers.MaybeParserT = None,
    **options: typing.Any,
) -> str:
    """Return a str representation of ``data`` in specified format.

    :param data: Config data object to dump
    :param ac_parser: Forced parser type or ID or parser object
    :param options: see :func:`dump`

    :return: Backend-specific string representation for the given data
    :raises: ValueError, UnknownProcessorTypeError
    """
    psr: ParserT = parsers.find(None, forced_type=ac_parser)
    return psr.dumps(data, **options)
