#!/usr/bin/env python

"""Unit tests for M2Crypto.BIO.IOBuffer.

Copyright (c) 2000 Ng Pheng Siong. All rights reserved."""

from io import BytesIO

from M2Crypto.BIO import IOBuffer, MemoryBuffer
from tests import unittest


class IOBufferTestCase(unittest.TestCase):

    def setUp(self):
        self._data = b'abcdef\n'
        self.data = self._data * 1024

    def tearDown(self):
        pass

    def test_init_empty(self):
        mb = MemoryBuffer()
        io = IOBuffer(mb)
        out = io.read()
        self.assertEqual(out, b'')

    def test_init_something(self):
        mb = MemoryBuffer(self.data)
        io = IOBuffer(mb)
        out = io.read(len(self.data))
        self.assertEqual(out, self.data)

    def test_read_less_than(self):
        chunk = len(self.data) - 7
        mb = MemoryBuffer(self.data)
        io = IOBuffer(mb)
        out = io.read(chunk)
        self.assertEqual(out, self.data[:chunk])

    def test_read_more_than(self):
        chunk = len(self.data) + 8
        mb = MemoryBuffer(self.data)
        io = IOBuffer(mb)
        out = io.read(chunk)
        self.assertEqual(out, self.data)

    def test_readline(self):
        buf = BytesIO()
        mb = MemoryBuffer(self.data)
        io = IOBuffer(mb)
        while 1:
            out = io.readline()
            if not out:
                break
            buf.write(out)
            self.assertEqual(out, self._data)
        self.assertEqual(buf.getvalue(), self.data)

    def test_readlines(self):
        buf = BytesIO()
        mb = MemoryBuffer(self.data)
        io = IOBuffer(mb)
        lines = io.readlines()
        for line in lines:
            self.assertEqual(line, self._data)
            buf.write(line)
        self.assertEqual(buf.getvalue(), self.data)

    def test_closed(self):
        mb = MemoryBuffer(self.data)
        io = IOBuffer(mb)
        io.close()
        with self.assertRaises(IOError):
            io.write(self.data)
        assert not io.readable() and not io.writeable()

    def test_read_only(self):
        mb = MemoryBuffer(self.data)
        io = IOBuffer(mb, mode='r')
        with self.assertRaises(IOError):
            io.write(self.data)
        assert not io.writeable()


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(IOBufferTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
