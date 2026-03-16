#
# Copyright (C) 2018 - 2026 Satoru SATOH <satoru.satoh gmmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=invalid-name
"""ioinfo.main to provide internal APIs used from other modules."""
from __future__ import annotations

import pathlib
import typing

from . import constants, datatypes, detectors, utils

if typing.TYPE_CHECKING:
    import collections.abc


def from_path_object(path: pathlib.Path) -> datatypes.IOInfo:
    """Get an IOInfo object made from :class:`pathlib.Path` object ``path``."""
    (abs_path, file_ext) = utils.get_path_and_ext(path)

    return datatypes.IOInfo(
        abs_path, datatypes.IOI_PATH_OBJ, str(abs_path), file_ext,
    )


def from_path_str(path: str) -> datatypes.IOInfo:
    """Get an IOInfo object made from a str ``path``."""
    return from_path_object(pathlib.Path(path).resolve())


def from_io_stream(strm: typing.IO) -> datatypes.IOInfo:
    """Get an IOInfo object made from IO stream object ``strm``."""
    path: str = getattr(strm, "name", "")
    if path:
        (_path, file_ext) = utils.get_path_and_ext(pathlib.Path(path))
        abs_path: str = str(_path)
    else:
        (abs_path, file_ext) = (path, "")

    return datatypes.IOInfo(
        strm, datatypes.IOI_STREAM, abs_path, file_ext,
    )


def make(obj: typing.Any) -> datatypes.IOInfo:
    """Make and return a :class:`datatypes.IOInfo` object from ``obj``."""
    if isinstance(obj, datatypes.IOInfo):
        return obj

    if isinstance(obj, str):
        return from_path_str(obj)

    if isinstance(obj, pathlib.Path):
        return from_path_object(obj)

    # Which is better? isinstance(obj, io.IOBase):
    if getattr(obj, "read", False):
        return from_io_stream(obj)

    raise ValueError(repr(obj))


def make_itr(
    obj: typing.Any, marker: str = constants.GLOB_MARKER,
) -> collections.abc.Iterator[datatypes.IOInfo]:
    """Make and yield a series of :class:`datatypes.IOInfo` objects."""
    if isinstance(obj, datatypes.IOInfo):
        yield obj

    elif detectors.is_path_str(obj):
        for path in utils.expand_from_path(pathlib.Path(obj)):
            yield from_path_object(path)

    elif detectors.is_path_obj(obj):
        for path in utils.expand_from_path(obj):
            yield from_path_object(path)

    elif detectors.is_io_stream(obj):
        yield from_io_stream(obj)

    else:
        for item in obj:
            yield from make_itr(item, marker=marker)


def makes(
    obj: typing.Any, marker: str = constants.GLOB_MARKER,
) -> list[datatypes.IOInfo]:
    """Make and return a list of :class:`datatypes.IOInfo` objects."""
    return list(make_itr(obj, marker=marker))
