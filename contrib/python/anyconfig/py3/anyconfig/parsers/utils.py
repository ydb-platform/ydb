#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Internal APIs to load, list and find parser class objects."""
from __future__ import annotations

import typing

from ..backend import ParserT, ParsersT
from .parsers import Parsers

if typing.TYPE_CHECKING:
    from .. import ioinfo


MaybeParserT = typing.Optional[
    typing.Union[str, ParserT, type[ParserT]]
]


def load_plugins() -> None:
    """[Re-]Load pluggable processors."""
    Parsers().load_plugins()


def list_types() -> list[str]:
    """List supported processor types."""
    return sorted(Parsers().list_x("type"))


def list_by_cid() -> list[tuple[str, ParsersT]]:
    """List processors by each cid."""
    return Parsers().list_by_x("cid")


def list_by_type() -> list[tuple[str, ParsersT]]:
    """List processor by eacch type."""
    return Parsers().list_by_x("type")


def list_by_extension() -> list[tuple[str, ParsersT]]:
    """List processor by file extension supported."""
    return Parsers().list_by_x("extensions")


def findall(
    obj: ioinfo.PathOrIOInfoT | None = None,
    forced_type: str | None = None,
) -> list[ParserT]:
    """Find out processor objects can process data from given ``obj``.

    ``obj`` may be a file path, file or file-like object, pathlib.Path object
    or an 'anyconfig.ioinfo.IOInfo' (namedtuple) object.

    :param obj:
        a file path, file or file-like object, pathlib.Path object, an
        'anyconfig.ioinfo.IOInfo' (namedtuple) object, or None
    :param forced_type: Forced type or id of the processor

    :return: A list of instances of processor classes to process 'obj'
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    return Parsers().findall(obj, forced_type=forced_type)


def find(
    obj: ioinfo.PathOrIOInfoT | None = None,
    forced_type: str | ParserT | type[ParserT] | None = None,
) -> ParserT:
    """Very similar to the above :func:`findall`.

    However it returns *a processor object* instead of a list of processor
    objects.

    :param obj:
        a file path, file or file-like object, pathlib.Path object, an
        'anyconfig.ioinfo.IOInfo' (namedtuple) object, or None
    :param forced_type: Forced type or id of the processor

    :return:
        An instance of processor class of highest priority to process 'obj'
    :raises: ValueError, UnknownProcessorTypeError, UnknownFileTypeError
    """
    return Parsers().find(obj, forced_type=forced_type)
