#!/usr/bin/env python

"""Unit tests for M2Crypto.BIO.MemoryBuffer.

Copyright (c) 2000 Ng Pheng Siong. All rights reserved."""

import os
import multiprocessing

from platform import system
from sys import version_info
from M2Crypto import m2, six
from M2Crypto.BIO import MemoryBuffer
from tests import unittest


class TimeLimitExpired(Exception):
    pass


def time_limit(timeout, func, exc_msg, *args, **kwargs):

    # multiprocessing.get_context() available in Python >= 3.4
    if six.PY3:
        # Python >=3.8 on MacOS changed start_method to 'spawn' as default.
        #   This creates a new context with the previous 'fork'
        #   start_method. Fixes issue #286.
        if system() == 'Darwin' and version_info.major >= 3 and version_info.minor >= 8:
            start_method = 'fork'
        else:
            # use default context
            start_method = None

        mp = multiprocessing.get_context(start_method)
    else:
        mp = multiprocessing

    p = mp.Process(target=func)
    p.start()
    p.join(timeout)
    if p.is_alive():
        p.terminate()
        raise TimeLimitExpired(exc_msg)


class MemoryBufferTestCase(unittest.TestCase):

    def setUp(self):
        self.data = b'abcdef' * 64

    def tearDown(self):
        pass

    def test_init_empty(self):
        mb = MemoryBuffer()
        self.assertEqual(len(mb), 0)
        out = mb.read()
        assert out is None

    def test_init_empty_cm(self):
        with MemoryBuffer() as mb:
            self.assertEqual(len(mb), 0)
            out = mb.read()
            assert out is None

    def test_init_something(self):
        mb = MemoryBuffer(self.data)
        self.assertEqual(len(mb), len(self.data))
        out = mb.read()
        self.assertEqual(out, self.data)

    def test_init_something_result_bytes(self):
        mb = MemoryBuffer(self.data)
        self.assertEqual(len(mb), len(self.data))
        out = mb.read()
        self.assertIsInstance(out, bytes)

    def test_init_something_cm(self):
        with MemoryBuffer(self.data) as mb:
            self.assertEqual(len(mb), len(self.data))
            out = mb.read()
            self.assertEqual(out, self.data)

    def test_read_less_than(self):
        chunk = len(self.data) - 7
        mb = MemoryBuffer(self.data)
        out = mb.read(chunk)
        self.assertEqual(out, self.data[:chunk])
        self.assertEqual(len(mb), (len(self.data)) - chunk)

    def test_read_more_than(self):
        chunk = len(self.data) + 8
        mb = MemoryBuffer(self.data)
        out = mb.read(chunk)
        self.assertEqual(out, self.data)
        self.assertEqual(len(mb), 0)

    def test_write_close(self):
        mb = MemoryBuffer(self.data)
        assert mb.writeable()
        mb.write_close()
        assert mb.readable()
        with self.assertRaises(IOError):
            mb.write(self.data)
        assert not mb.writeable()

    def test_closed(self):
        mb = MemoryBuffer(self.data)
        mb.close()
        with self.assertRaises(IOError):
            mb.write(self.data)
        m2.err_clear_error()
        assert mb.readable() and not mb.writeable()

    def test_readline(self):
        # test against possible endless loop
        # http://stackoverflow.com/questions/9280550/
        timeout_secs = 10
        time_limit(timeout_secs, run_test,
                   'The readline() should not timeout!')


def run_test(*args, **kwargs):
    sep = os.linesep.encode()
    with MemoryBuffer(b'hello' + sep + b'world' + sep) as mb:
        assert mb.readable()
        assert mb.readline() == b'hello' + sep
        assert mb.readline() == b'world' + sep
    with MemoryBuffer(b'hello' + sep + b'world' + sep) as mb:
        assert mb.readlines() == [b'hello' + sep, b'world' + sep]

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(MemoryBufferTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
