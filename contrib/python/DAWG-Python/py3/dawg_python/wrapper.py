# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import io
import struct
import array
import sys

from library.python.resource import resfs_read

from . import units
from .compat import int_from_byte


class Dictionary(object):
    """
    Dictionary class for retrieval and binary I/O.
    """
    def __init__(self):
        self._units = array.array(str("I"))

    ROOT = 0
    "Root index"

    def has_value(self, index):
        "Checks if a given index is related to the end of a key."
        return units.has_leaf(self._units[index])

    def value(self, index):
        "Gets a value from a given index."
        offset = units.offset(self._units[index])
        value_index = (index ^ offset) & units.PRECISION_MASK
        return units.value(self._units[value_index])

    def read(self, fp):
        "Reads a dictionary from an input stream."
        base_size = struct.unpack(str("=I"), fp.read(4))[0]
        if sys.version_info > (3, 8):
            self._units.frombytes(fp.read(base_size * 4))
        else:
            self._units.fromstring(fp.read(base_size * 4))

    def contains(self, key):
        "Exact matching."
        index = self.follow_bytes(key, self.ROOT)
        if index is None:
            return False
        return self.has_value(index)

    def find(self, key):
        "Exact matching (returns value)"
        index = self.follow_bytes(key, self.ROOT)
        if index is None:
            return -1
        if not self.has_value(index):
            return -1
        return self.value(index)

    def follow_char(self, label, index):
        "Follows a transition"
        offset = units.offset(self._units[index])
        next_index = (index ^ offset ^ label) & units.PRECISION_MASK

        if units.label(self._units[next_index]) != label:
            return None

        return next_index

    def follow_bytes(self, s, index):
        "Follows transitions."
        for ch in s:
            index = self.follow_char(int_from_byte(ch), index)
            if index is None:
                return None

        return index

    @classmethod
    def load(cls, path):
        dawg = cls()
        data = resfs_read(path)
        if data:
            f = io.BytesIO(data)
        else:
            f = open(path, 'rb')
        dawg.read(f)
        f.close()
        return dawg


class Guide(object):

    ROOT = 0

    def __init__(self):
        self._units = array.array(str("B"))

    def child(self, index):
        return self._units[index*2]

    def sibling(self, index):
        return self._units[index*2 + 1]

    def read(self, fp):
        base_size = struct.unpack(str("=I"), fp.read(4))[0]
        if sys.version_info > (3, 8):
            self._units.frombytes(fp.read(base_size * 4 * 2))
        else:
            self._units.fromstring(fp.read(base_size * 4 * 2))

    def size(self):
        return len(self._units)


class Completer(object):

    def __init__(self, dic=None, guide=None):
        self._dic = dic
        self._guide = guide

    def value(self):
        return self._dic.value(self._last_index)

    def start(self, index, prefix=b""):
        self.key = bytearray(prefix)

        if self._guide.size():
            self._index_stack = [index]
            self._last_index = self._dic.ROOT
        else:
            self._index_stack = []

    def next(self):
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
                        #self.key[-1] = 0

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

    def _follow(self, label, index):
        next_index = self._dic.follow_char(label, index)
        if next_index is None:
            return None

        self.key.append(label)
        self._index_stack.append(next_index)
        return next_index

    def _find_terminal(self, index):
        while not self._dic.has_value(index):
            label = self._guide.child(index)

            index = self._dic.follow_char(label, index)
            if index is None:
                return False

            self.key.append(label)
            self._index_stack.append(index)

        self._last_index = index
        return True
