#
# Copyright (C) 2012 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=consider-using-with
"""Module to provide backward compatibility for plugins."""
from __future__ import annotations

import pathlib
import typing

if typing.TYPE_CHECKING:
    from .datatypes import PathOrStrT


class BinaryFilesMixin:
    """Mixin class to open configuration files as a binary data."""

    _open_flags: tuple[str, str] = ("rb", "wb")

    @classmethod
    def ropen(
        cls, filepath: PathOrStrT, **options: typing.Any,
    ) -> typing.IO:
        """Open ``filepath`` with read only mode.

        :param filepath: Path to file to open to read data
        """
        return pathlib.Path(filepath).open(
            cls._open_flags[0], **options,
        )

    @classmethod
    def wopen(
        cls, filepath: PathOrStrT, **options: typing.Any,
    ) -> typing.IO:
        """Open ``filepath`` with write mode.

        :param filepath: Path to file to open to write data to
        """
        return pathlib.Path(filepath).open(
            cls._open_flags[1], **options,
        )
