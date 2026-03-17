import array
import os
import unittest
import random
import struct
import sys
from typing import Union, List, TYPE_CHECKING
import xxhash

if TYPE_CHECKING:
    from typing_extensions import Buffer
    Buffer.register(array.ArrayType)
    InputType = Union[str, Buffer]

def getrefcount(obj):
    if hasattr(sys, "getrefcount"):
        return sys.getrefcount(obj)
    else:
        # Non-CPython implementation
        return 0


class TestXXH(unittest.TestCase):
    def test_version(self):
        self.assertTrue(xxhash.VERSION)
        self.assertTrue(xxhash.XXHASH_VERSION)

    def test_buffer_types(self):
        # Various buffer-like objects are accepted, and they give similar values
        args: List[InputType] = [
            b'ab\x00c',
            bytearray(b'ab\x00c'),
            array.array('b', b'ab\x00c'),
        ]
        # An array object with non-1 itemsize
        a = array.array('i', struct.unpack('i', b'ab\x00c'))
        assert a.itemsize == 4
        args.append(a)
        # A memoryview, where supported
        if sys.version_info >= (2, 7):
            args.append(memoryview(b'ab\x00c'))

        for func in [xxhash.xxh32, xxhash.xxh64, xxhash.xxh3_64, xxhash.xxh3_128]:
            old_refcounts = list(map(getrefcount, args))
            # With constructor
            values = set(func(arg).hexdigest() for arg in args)
            self.assertEqual(len(values), 1, values)
            # With update()
            values = set()
            for arg in args:
                x = func()
                x.update(arg)
                values.add(x.hexdigest())
            self.assertEqual(len(values), 1, values)
            # No reference leak in CPython extension
            del arg
            self.assertEqual(list(map(getrefcount, args)), old_refcounts)


if __name__ == '__main__':
    unittest.main()
