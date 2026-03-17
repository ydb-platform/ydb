from __future__ import annotations

import array
import struct
from typing import TYPE_CHECKING

from . import units

if TYPE_CHECKING:
    from io import BytesIO
    from pathlib import Path

    from typing_extensions import Self


class Dictionary:
    """
    Dictionary class for retrieval and binary I/O.
    """

    def __init__(self) -> None:
        self._units = array.array("I")

    ROOT = 0
    "Root index"

    def has_value(self, index: int) -> bool:
        """Checks if a given index is related to the end of a key."""
        return units.has_leaf(self._units[index])

    def value(self, index: int) -> int:
        """Gets a value from a given index."""
        offset = units.offset(self._units[index])
        value_index = (index ^ offset) & units.PRECISION_MASK
        return units.value(self._units[value_index])

    def read(self, fp: BytesIO) -> None:
        """Reads a dictionary from an input stream."""
        base_size = struct.unpack("=I", fp.read(4))[0]
        self._units.fromfile(fp, base_size)

    def contains(self, key: bytes) -> bool:
        """Exact matching."""
        index = self.follow_bytes(key, self.ROOT)
        if index is None:
            return False
        return self.has_value(index)

    def find(self, key: bytes) -> int:
        """Exact matching (returns value)"""
        index = self.follow_bytes(key, self.ROOT)
        if index is None:
            return -1
        if not self.has_value(index):
            return -1
        return self.value(index)

    def follow_char(self, label: int, index: int) -> int | None:
        """Follows a transition"""
        offset = units.offset(self._units[index])
        next_index = (index ^ offset ^ label) & units.PRECISION_MASK

        if units.label(self._units[next_index]) != label:
            return None

        return next_index

    def follow_bytes(self, s: bytes, index: int) -> int | None:
        """Follows transitions."""
        for ch in s:
            index = self.follow_char(ch, index)
            if index is None:
                return None

        return index

    @classmethod
    def load(cls, path: str | Path) -> Self:
        dawg = cls()
        with open(path, "rb") as f:
            dawg.read(f)
        return dawg


class Guide:
    ROOT = 0

    def __init__(self) -> None:
        self._units = array.array("B")

    def child(self, index: int) -> int:
        return self._units[index * 2]

    def sibling(self, index: int) -> int:
        return self._units[index * 2 + 1]

    def read(self, fp: BytesIO) -> None:
        base_size = struct.unpack("=I", fp.read(4))[0]
        self._units.fromfile(fp, base_size * 2)

    def size(self) -> int:
        return len(self._units)


class Completer:
    _dic: Dictionary | None
    _guide: Guide | None

    def __init__(self, dic: Dictionary | None = None, guide: Guide | None = None) -> None:
        self._dic = dic
        self._guide = guide

    def value(self) -> int:
        return self._dic.value(self._last_index)

    def start(self, index: int, prefix: bytes = b"") -> None:
        self.key = bytearray(prefix)

        if self._guide.size():
            self._index_stack = [index]
            self._last_index = self._dic.ROOT
        else:
            self._index_stack = []

    def next(self) -> bool:
        "Gets the next key"

        if not self._index_stack:
            return False

        index = self._index_stack[-1]

        if self._last_index != self._dic.ROOT:
            child_label = self._guide.child(index)  # UCharType

            if child_label:
                # Follows a transition to the first child.
                index = self._follow(child_label, index)
                if index is None:
                    return False
            else:
                while True:
                    sibling_label = self._guide.sibling(index)
                    # Moves to the previous node.
                    if len(self.key) > 0:
                        self.key.pop()

                    self._index_stack.pop()
                    if not self._index_stack:
                        return False

                    index = self._index_stack[-1]
                    if sibling_label:
                        # Follows a transition to the next sibling.
                        index = self._follow(sibling_label, index)
                        if index is None:
                            return False
                        break

        return self._find_terminal(index)

    def _follow(self, label: int, index: int) -> int | None:
        next_index = self._dic.follow_char(label, index)
        if next_index is None:
            return None

        self.key.append(label)
        self._index_stack.append(next_index)
        return next_index

    def _find_terminal(self, index: int) -> bool:
        while not self._dic.has_value(index):
            label = self._guide.child(index)

            index = self._dic.follow_char(label, index)
            if index is None:
                return False

            self.key.append(label)
            self._index_stack.append(index)

        self._last_index = index
        return True
