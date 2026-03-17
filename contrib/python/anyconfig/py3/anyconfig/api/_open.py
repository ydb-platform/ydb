#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""A API to open files by detecting those type automatically."""
from __future__ import annotations

import typing
import warnings

from .. import ioinfo, parsers

if typing.TYPE_CHECKING:
    from .datatypes import ParserT


# pylint: disable=redefined-builtin
def open(
    path: ioinfo.PathOrIOInfoT,
    mode: str | None = None,
    ac_parser: parsers.MaybeParserT = None,
    **options: dict[str, typing.Any],
) -> typing.IO:
    """Open given file ``path`` with appropriate open flag.

    :param path: Configuration file path
    :param mode:
        Can be 'r' and 'rb' for reading (default) or 'w', 'wb' for writing.
        Please note that even if you specify 'r' or 'w', it will be changed to
        'rb' or 'wb' if selected backend, xml and configobj for example, for
        given config file prefer that.
    :param options:
        Optional keyword arguments passed to the internal file opening APIs of
        each backends such like 'buffering' optional parameter passed to
        builtin 'open' function.

    :return: A file object or None on any errors
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    if not path:
        msg = f"Invalid argument, path: {path!r}"
        raise ValueError(msg)

    ioi = ioinfo.make(path)
    if ioinfo.is_stream(ioi):
        warnings.warn(f"Looks already opened stream: {ioi!r}", stacklevel=2)
        return typing.cast("typing.IO", ioi.src)

    psr: ParserT = parsers.find(ioi, forced_type=ac_parser)

    if mode is not None and mode.startswith("w"):
        return psr.wopen(ioi.path, **options)

    return psr.ropen(ioi.path, **options)
