#
# Copyright (C) 2012 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Provide functions to detect str, pathlib.Path, stream and IOInfo objects."""
from __future__ import annotations

import pathlib
import typing

from . import datatypes


IOI_KEYS_LIST: list[str] = sorted(datatypes.IOI_KEYS)


def is_path_str(obj: typing.Any) -> bool:
    """Test if given object ``obj`` is a str represents a file path."""
    return isinstance(obj, str)


def is_path_obj(obj: typing.Any) -> bool:
    """Test if given object ``obj`` is a pathlib.Path object."""
    return isinstance(obj, pathlib.Path)


def is_io_stream(obj: typing.Any) -> bool:
    """Test if given object ``obj`` is a file stream, file/file-like object."""
    return callable(getattr(obj, "read", False))


def is_ioinfo(obj: typing.Any) -> bool:
    """Test if given object ``obj`` is an IOInfo namedtuple objejct."""
    if isinstance(obj, tuple):
        to_dict = getattr(obj, "_asdict", False)
        if to_dict and callable(to_dict):
            keys = sorted(to_dict().keys())
            return keys == IOI_KEYS_LIST

    return False


def is_stream(obj: typing.Any) -> bool:
    """Test if given object ``obj`` is an IOInfo object with stream type."""
    return getattr(obj, "type", None) == datatypes.IOI_STREAM
