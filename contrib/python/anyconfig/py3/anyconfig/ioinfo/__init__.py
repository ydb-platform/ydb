#
# Copyright (C) 2021 - 2023 Satoru SATOH <satoru.satoh@gmail.com>
# SPDX-License-Identifier: MIT
#
r"""ioinfo module to provide functions to create IOInfo objects.

IOInfo objects wrap pathlib.Path and io objects.

.. versionchanged:: 0.12.0

- Restructure and migrate some utility functions in .utils into this module.

.. versionchanged:: 0.10.1

- simplify inspect_io_obj and make; detect type in make, remove the member
  opener from ioinfo object, etc.

.. versionadded:: 0.9.5

- Add functions to make and process input and output object holding some
  attributes like input and output type (path, stream or pathlib.Path object),
  path, opener, etc.
"""
from .datatypes import IOInfo, PathOrIOInfoT
from .detectors import is_stream
from .factory import make, makes
from .utils import get_encoding

__all__ = [
    "IOInfo", "PathOrIOInfoT",
    "is_stream",
    "make", "makes",
    "get_encoding",
]

# vim:sw=4:ts=4:et:
