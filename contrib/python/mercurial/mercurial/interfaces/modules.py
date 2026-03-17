# modules.py - protocol classes for dynamically loaded modules
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import typing

from typing import (
    Callable,
    Protocol,
)

if typing.TYPE_CHECKING:
    BDiffBlock = tuple[int, int, int, int]
    """An entry in the list returned by bdiff.{xdiff,}blocks()."""

    BDiffBlocksFnc = Callable[[bytes, bytes], list[BDiffBlock]]
    """The signature of `bdiff.blocks()` and `bdiff.xdiffblocks()`."""


class Base85(Protocol):
    """A Protocol class for the various base85 module implementations."""

    def b85encode(self, text: bytes, pad: bool = False) -> bytes:
        """encode text in base85 format"""

    def b85decode(self, text: bytes) -> bytes:
        """decode base85-encoded text"""


class BDiff(Protocol):
    """A Protocol class for the various bdiff module implementations."""

    def splitnewlines(self, text: bytes) -> list[bytes]:
        """like str.splitlines, but only split on newlines."""

    def bdiff(self, a: bytes, b: bytes) -> bytes:
        ...

    def blocks(self, a: bytes, b: bytes) -> list[BDiffBlock]:
        ...

    def fixws(self, text: bytes, allws: bool) -> bytes:
        ...

    xdiffblocks: BDiffBlocksFnc | None
    """This method is currently only available in the ``cext`` module."""


class CharEncoding(Protocol):
    """A Protocol class for the various charencoding module implementations."""

    def isasciistr(self, s: bytes) -> bool:
        """Can the byte string be decoded with the ``ascii`` codec?"""

    def asciilower(self, s: bytes) -> bytes:
        """convert a string to lowercase if ASCII

        Raises UnicodeDecodeError if non-ASCII characters are found."""

    def asciiupper(self, s: bytes) -> bytes:
        """convert a string to uppercase if ASCII

        Raises UnicodeDecodeError if non-ASCII characters are found."""

    def jsonescapeu8fast(self, u8chars: bytes, paranoid: bool) -> bytes:
        """Convert a UTF-8 byte string to JSON-escaped form (fast path)

        Raises ValueError if non-ASCII characters have to be escaped.
        """


class MPatch(Protocol):
    """A protocol class for the various mpatch module implementations."""

    def patches(self, a: bytes, bins: list[bytes]) -> bytes:
        ...

    def patchedsize(self, orig: int, delta: bytes) -> int:
        ...
