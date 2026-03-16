#
# Copyright (C) 2018 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Abstract processor module.

.. versionadded:: 0.9.5

   - Add to abstract processors such like Parsers (loaders and dumpers).
"""
from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    import builtins


class Processor:
    """Abstract processor class to provide basic implementation.

    - _type: type indicates data types it can process
    - _priority:
        Priority to select it if there are others of same type from 0 (lowest)
        to 99 (highest)
    - _extensions: File extensions of data type it can process

    .. note::
       This class ifself is not a singleton but its children classes should so
       in most cases, I think.
    """

    _cid: typing.ClassVar[str] = ""
    _type: typing.ClassVar[str] = ""
    _priority: typing.ClassVar[int] = 0
    _extensions: tuple[str, ...] = ()

    @classmethod
    def cid(cls) -> str:
        """Processor class ID."""
        return cls._cid

    @classmethod
    def type(cls) -> str:
        """Processors' type."""
        return str(cls._type)

    @classmethod
    def priority(cls) -> int:
        """Processors's priority."""
        return cls._priority

    @classmethod
    def extensions(cls) -> tuple[str, ...]:
        """Get the list of file extensions of files it can process."""
        return cls._extensions

    @classmethod
    def __eq__(
        cls, other: builtins.type[Processor],  # type: ignore[override]
    ) -> bool:
        """Test equality."""
        return cls.cid() == other.cid()

    @classmethod
    def __hash__(cls) -> int:
        """Test equality."""
        return hash(cls.cid())

    def __str__(self) -> str:
        """Provide a string representation."""
        return (
            f"<Processor cid={self.cid()}, type={self.type()}, "
            f"prio={self.priority()}, extensions={self.extensions()!r}"
        )
