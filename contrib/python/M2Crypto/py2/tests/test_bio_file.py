#!/usr/bin/env python

"""Unit tests for M2Crypto.BIO.File.

Copyright (c) 1999-2002 Ng Pheng Siong. All rights reserved."""

import logging
import os
import platform
import tempfile
import ctypes
if platform.system() == 'Windows':
    import ctypes.wintypes

from M2Crypto.BIO import File, openfile
from tests import unittest

log = logging.getLogger(__name__)


def getCountProcHandles():
    PROCESS_QUERY_INFORMATION = 0x400
    handle = ctypes.windll.kernel32.OpenProcess(
        PROCESS_QUERY_INFORMATION, 0, os.getpid())
    hndcnt = ctypes.wintypes.DWORD()
    ctypes.windll.kernel32.GetProcessHandleCount(
        handle, ctypes.byref(hndcnt))
    sys_value = hndcnt.value
    ctypes.windll.kernel32.CloseHandle(handle)
    return sys_value + 1


class FileTestCase(unittest.TestCase):

    def setUp(self):
        self.data = b'abcdef' * 64
        self.fd, self.fname = tempfile.mkstemp()

        if platform.system() in ['Linux', 'Darwin', 'FreeBSD']:
            self.__dev_fd = "/dev/fd/"

        self.fd_count = self.__mfd()

    def __mfd(self):
        if hasattr(self, '__dev_fd'):
            return len(os.listdir(self.__dev_fd))
        elif platform.system() == 'Windows':
            return getCountProcHandles()
        else:
            return None

    def tearDown(self):

        self.assertEqual(self.fd_count, self.__mfd(),
                         "last test did not close all file descriptors properly")

        try:
            os.close(self.fd)
        except OSError:
            pass

    def test_openfile_rb(self):
        # First create the file using Python's open().
        with open(self.fname, 'wb') as f:
            f.write(self.data)

        # Now open the file using M2Crypto.BIO.openfile().
        with openfile(self.fname, 'rb') as f:
            data = f.read(len(self.data))

        self.assertEqual(data, self.data)

    def test_openfile_wb(self):
        # First create the file using M2Crypto.BIO.openfile().
        with openfile(self.fname, 'wb') as f:
            f.write(self.data)

        # Now open the file using Python's open().
        with open(self.fname, 'rb') as f:
            data = f.read(len(self.data))

        self.assertEqual(data, self.data)

    def test_closed(self):
        f = openfile(self.fname, 'wb')
        f.write(self.data)
        f.close()
        with self.assertRaises(IOError):
            f.write(self.data)

    def test_use_pyfile(self):
        # First create the file.
        with open(self.fname, 'wb') as f:
            f2 = File(f)
            f2.write(self.data)
            f2.close()

        # Now read the file.
        with open(self.fname, 'rb') as f:
            in_data = f.read(len(self.data))

        self.assertEqual(len(in_data), len(self.data))
        self.assertEqual(in_data, self.data)

    def test_readline_bin(self):
        with open(self.fname, 'wb') as f:
            f.write(b'hello\nworld\n')
        with openfile(self.fname, 'rb') as f:
            self.assertTrue(f.readable())
            self.assertEqual(f.readline(), b'hello\n')
            self.assertEqual(f.readline(), b'world\n')
        with openfile(self.fname, 'rb') as f:
            self.assertEqual(
                f.readlines(),
                [b'hello\n', b'world\n'])

    def test_readline(self):
        sep = os.linesep.encode()
        with open(self.fname, 'w') as f:
            f.write('hello\nworld\n')
        with openfile(self.fname, 'rb') as f:
            self.assertTrue(f.readable())
            self.assertEqual(f.readline(), b'hello' + sep)
            self.assertEqual(f.readline(), b'world' + sep)
        with openfile(self.fname, 'rb') as f:
            self.assertEqual(
                f.readlines(),
                [b'hello' + sep, b'world' + sep])

    def test_tell_seek(self):
        with open(self.fname, 'w') as f:
            f.write('hello world')
        with openfile(self.fname, 'r') as f:
            # Seek absolute
            f.seek(6)
            self.assertEqual(f.tell(), 6)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(FileTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner().run(suite())
