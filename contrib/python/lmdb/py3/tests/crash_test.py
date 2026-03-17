#
# Copyright 2013-2021 The py-lmdb authors, all rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted only as authorized by the OpenLDAP
# Public License.
#
# A copy of this license is available in the file LICENSE in the
# top-level directory of the distribution or, alternatively, at
# <http://www.OpenLDAP.org/license.html>.
#
# OpenLDAP is a registered trademark of the OpenLDAP Foundation.
#
# Individual files and/or contributed packages may be copyright by
# other parties and/or subject to additional restrictions.
#
# This work also contains materials derived from public sources.
#
# Additional information about OpenLDAP can be obtained at
# <http://www.openldap.org/>.
#

# This is not a test suite! More like a collection of triggers for previously
# observed crashes. Want to contribute to py-lmdb? Please write a test suite!
#
# what happens when empty keys/ values passed to various funcs
# incorrect types
# try to break cpython arg parsing - too many/few/incorrect args
# Various efforts to cause Python-level leaks.
#

from __future__ import absolute_import
from __future__ import with_statement

import sys
import itertools
import random
import unittest
import multiprocessing

import lmdb
import testlib

from testlib import B
from testlib import O


class CrashTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    # Various efforts to cause segfaults.

    def setUp(self):
        self.path, self.env = testlib.temp_env()
        with self.env.begin(write=True) as txn:
            txn.put(B('dave'), B(''))
            txn.put(B('dave2'), B(''))

    def testOldCrash(self):
        txn = self.env.begin()
        dir(iter(txn.cursor()))

    def testCloseWithTxn(self):
        txn = self.env.begin(write=True)
        self.env.close()
        self.assertRaises(Exception, (lambda: list(txn.cursor())))

    def testDoubleClose(self):
        self.env.close()
        self.env.close()

    def testTxnCloseActiveIter(self):
        with self.env.begin() as txn:
            it = txn.cursor().iternext()
        self.assertRaises(Exception, (lambda: list(it)))

    def testDbCloseActiveIter(self):
        db = self.env.open_db(key=B('dave3'))
        with self.env.begin(write=True) as txn:
            txn.put(B('a'), B('b'), db=db)
            it = txn.cursor(db=db).iternext()
        self.assertRaises(Exception, (lambda: list(it)))


class IteratorTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.path, self.env = testlib.temp_env()
        self.txn = self.env.begin(write=True)
        self.c = self.txn.cursor()

    def testEmpty(self):
        self.assertEqual([], list(self.c))
        self.assertEqual([], list(self.c.iternext()))
        self.assertEqual([], list(self.c.iterprev()))

    def testFilled(self):
        testlib.putData(self.txn)
        self.assertEqual(testlib.ITEMS, list(self.c))
        self.assertEqual(testlib.ITEMS, list(self.c))
        self.assertEqual(testlib.ITEMS, list(self.c.iternext()))
        self.assertEqual(testlib.ITEMS[::-1], list(self.txn.cursor().iterprev()))
        self.assertEqual(testlib.ITEMS[::-1], list(self.c.iterprev()))
        self.assertEqual(testlib.ITEMS, list(self.c))

    def testFilledSkipForward(self):
        testlib.putData(self.txn)
        self.c.set_range(B('b'))
        self.assertEqual(testlib.ITEMS[1:], list(self.c))

    def testFilledSkipReverse(self):
        testlib.putData(self.txn)
        self.c.set_range(B('b'))
        self.assertEqual(testlib.REV_ITEMS[-2:], list(self.c.iterprev()))

    def testFilledSkipEof(self):
        testlib.putData(self.txn)
        self.assertEqual(False, self.c.set_range(B('z')))
        self.assertEqual(testlib.REV_ITEMS, list(self.c.iterprev()))


class BigReverseTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    # Test for issue with MDB_LAST+MDB_PREV skipping chunks of database.
    def test_big_reverse(self):
        path, env = testlib.temp_env()
        txn = env.begin(write=True)
        keys = [B('%05d' % i) for i in range(0xffff)]
        for k in keys:
            txn.put(k, k, append=True)
        assert list(txn.cursor().iterprev(values=False)) == list(reversed(keys))


class MultiCursorDeleteTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.path, self.env = testlib.temp_env()

    def test1(self):
        """Ensure MDB_NEXT is ignored on `c1' when it was previously positioned
        on the key that `c2' just deleted."""
        txn = self.env.begin(write=True)
        cur = txn.cursor()
        while cur.first():
            cur.delete()

        for i in range(1, 10):
            cur.put(O(ord('a') + i) * i, B(''))

        c1 = txn.cursor()
        c1f = c1.iternext(values=False)
        while next(c1f) != B('ddd'):
            pass
        c2 = txn.cursor()
        assert c2.set_key(B('ddd'))
        c2.delete()
        assert next(c1f) == B('eeee')

    def test_monster(self):
        # Generate predictable sequence of sizes.
        rand = random.Random()
        rand.seed(0)

        txn = self.env.begin(write=True)
        keys = []
        for i in range(20000):
            key = B('%06x' % i)
            val = B('x' * rand.randint(76, 350))
            assert txn.put(key, val)
            keys.append(key)

        deleted = 0
        for key in txn.cursor().iternext(values=False):
            assert txn.delete(key), key
            deleted += 1

        assert deleted == len(keys), deleted


class TxnFullTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_17bf75b12eb94d9903cd62329048b146d5313bad(self):
        """
        me_txn0 previously cached MDB_TXN_ERROR permanently. Fixed by
        17bf75b12eb94d9903cd62329048b146d5313bad.
        """
        path, env = testlib.temp_env(map_size=4096 * 9, sync=False, max_spare_txns=0)
        for i in itertools.count():
            try:
                with env.begin(write=True) as txn:
                    txn.put(B(str(i)), B(str(i)))
            except lmdb.MapFullError:
                break

        # Should not crash with MDB_BAD_TXN:
        with env.begin(write=True) as txn:
            txn.delete(B('1'))


class EmptyIterTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_python3_iternext_segfault(self):
        # https://github.com/dw/py-lmdb/issues/105
        _, env = testlib.temp_env()
        txn = env.begin()
        cur = txn.cursor()
        ite = cur.iternext()
        nex = getattr(ite, 'next', getattr(ite, '__next__', None))
        assert nex is not None
        self.assertRaises(StopIteration, nex)


class MultiputTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_multiput_segfault(self):
        # http://github.com/jnwatson/py-lmdb/issues/173
        _, env = testlib.temp_env()
        db = env.open_db(b'foo', dupsort=True)
        txn = env.begin(db=db, write=True)
        txn.put(b'a', b'\x00\x00\x00\x00\x00\x00\x00\x00')
        txn.put(b'a', b'\x05')
        txn.put(b'a', b'\t')
        txn.put(b'a', b'\r')
        txn.put(b'a', b'\x11')
        txn.put(b'a', b'\x15')
        txn.put(b'a', b'\x19')
        txn.put(b'a', b'\x1d')
        txn.put(b'a', b'!')
        txn.put(b'a', b'%')
        txn.put(b'a', b')')
        txn.put(b'a', b'-')
        txn.put(b'a', b'1')
        txn.put(b'a', b'5')
        txn.commit()

class InvalidArgTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_duplicate_arg(self):
        # https://github.com/jnwatson/py-lmdb/issues/203
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        c = txn.cursor()
        self.assertRaises(TypeError, c.get, b'a', key=True)

class BadCursorTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_cursor_open_failure(self):
        '''
        Test the error path for when mdb_cursor_open fails

        Note:
            this only would crash if cpython is built with Py_TRACE_REFS
        '''
        # https://github.com/jnwatson/py-lmdb/issues/216
        path, env = testlib.temp_env()
        db = env.open_db(b'db', dupsort=True)
        env.close()
        del env

        env = lmdb.open(path, readonly=True, max_dbs=4)
        txn1 = env.begin(write=False)
        db = env.open_db(b'db', dupsort=True, txn=txn1)
        txn2 = env.begin(write=False)
        self.assertRaises(lmdb.InvalidParameterError, txn2.cursor, db=db)

MINDBSIZE = 64 * 1024 * 2  # certain ppcle Linux distros have a 64K page size

if sys.version_info[:2] >= (3, 4):
    class MapResizeTest(unittest.TestCase):

        def tearDown(self):
            testlib.cleanup()

        @staticmethod
        def do_resize(path):
            '''
            Increase map size and fill up database, making sure that the root page is no longer
            accessible in the main process.
            '''
            with lmdb.open(path, max_dbs=10, create=False, map_size=MINDBSIZE) as env:
                env.open_db(b'foo')
                env.set_mapsize(MINDBSIZE * 2)
                count = 0
                try:
                    # Figure out how many keyvals we can enter before we run out of space
                    with env.begin(write=True) as txn:
                        while True:
                            datum = count.to_bytes(4, 'little')
                            txn.put(datum, b'0')
                            count += 1

                except lmdb.MapFullError:
                    # Now put (and commit) just short of that
                    with env.begin(write=True) as txn:
                        for i in range(count - 100):
                            datum = i.to_bytes(4, 'little')
                            txn.put(datum, b'0')
                else:
                    assert 0

        def test_opendb_resize(self):
            '''
            Test that we correctly handle a MDB_MAP_RESIZED in env.open_db.

            Would seg fault in cffi implementation
            '''
            mpctx = multiprocessing.get_context('spawn')
            path, env = testlib.temp_env(max_dbs=10, map_size=MINDBSIZE)
            env.close()
            env = lmdb.open(path, max_dbs=10, map_size=MINDBSIZE, readonly=True)
            proc = mpctx.Process(target=self.do_resize, args=(path,))
            proc.start()
            proc.join(5)
            assert proc.exitcode is not None
            self.assertRaises(lmdb.MapResizedError, env.open_db, b'foo')

if __name__ == '__main__':
    unittest.main()
