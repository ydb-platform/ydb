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

# test delete(dupdata)

from __future__ import absolute_import
from __future__ import with_statement
import sys
import unittest

import lmdb

import testlib
from testlib import B
from testlib import BT

class ContextManagerTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_ok(self):
        path, env = testlib.temp_env()
        txn = env.begin(write=True)
        with txn.cursor() as curs:
            curs.put(B('foo'), B('123'))
        self.assertRaises(Exception, lambda: curs.get(B('foo')))

    def test_crash(self):
        path, env = testlib.temp_env()
        txn = env.begin(write=True)

        try:
            with txn.cursor() as curs:
                curs.put(123, 123)
        except Exception:
            pass
        self.assertRaises(Exception, lambda: curs.get(B('foo')))


class CursorTestBase(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.path, self.env = testlib.temp_env()
        self.txn = self.env.begin(write=True)
        self.c = self.txn.cursor()


class CursorTest(CursorTestBase):
    def testKeyValueItemEmpty(self):
        self.assertEqual(B(''), self.c.key())
        self.assertEqual(B(''), self.c.value())
        self.assertEqual(BT('', ''), self.c.item())

    def testFirstLastEmpty(self):
        self.assertEqual(False, self.c.first())
        self.assertEqual(False, self.c.last())

    def testFirstFilled(self):
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.first())
        self.assertEqual(testlib.ITEMS[0], self.c.item())

    def testLastFilled(self):
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(testlib.ITEMS[-1], self.c.item())

    def testSetKey(self):
        self.assertRaises(Exception, (lambda: self.c.set_key(B(''))))
        self.assertEqual(False, self.c.set_key(B('missing')))
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.set_key(B('b')))
        self.assertEqual(False, self.c.set_key(B('ba')))

    def testSetRange(self):
        self.assertEqual(False, self.c.set_range(B('x')))
        testlib.putData(self.txn)
        self.assertEqual(False, self.c.set_range(B('x')))
        self.assertEqual(True, self.c.set_range(B('a')))
        self.assertEqual(B('a'), self.c.key())
        self.assertEqual(True, self.c.set_range(B('ba')))
        self.assertEqual(B('baa'), self.c.key())
        self.c.set_range(B(''))
        self.assertEqual(B('a'), self.c.key())

    def testDeleteEmpty(self):
        self.assertEqual(False, self.c.delete())

    def testDeleteFirst(self):
        testlib.putData(self.txn)
        self.assertEqual(False, self.c.delete())
        self.c.first()
        self.assertEqual(BT('a', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('b', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('baa', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('d', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())
        self.assertEqual(False, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())

    def testDeleteLast(self):
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(BT('d', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())
        self.assertEqual(False, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())

    def testCount(self):
        self.assertRaises(Exception, (lambda: self.c.count()))
        testlib.putData(self.txn)
        self.c.first()
        # TODO: complete dup key support.
        #self.assertEqual(1, self.c.count())

    def testPut(self):
        pass

class CursorTest2(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.path, self.env = testlib.temp_env()
        self.db = self.env.open_db(b'foo', dupsort=True)
        self.txn = self.env.begin(write=True, db=self.db)
        self.c = self.txn.cursor()

    def testIterWithDeletes(self):
        ''' A problem identified in LMDB 0.9.27 '''
        self.c.put(b'\x00\x01', b'hehe', dupdata=True)
        self.c.put(b'\x00\x02', b'haha', dupdata=True)
        self.c.set_key(b'\x00\x02')
        it = self.c.iternext()
        self.assertEqual((b'\x00\x02', b'haha'), next(it))
        self.txn.delete(b'\x00\x01', b'hehe', db=self.db)
        self.assertRaises(StopIteration, next, it)

class PutmultiTest(CursorTestBase):
    def test_empty_seq(self):
        consumed, added = self.c.putmulti(())
        assert consumed == added == 0

    def test_2list(self):
        l = [BT('a', ''), BT('a', '')]
        consumed, added = self.c.putmulti(l)
        assert consumed == added == 2

        li = iter(l)
        consumed, added = self.c.putmulti(li)
        assert consumed == added == 2

    def test_2list_preserve(self):
        l = [BT('a', ''), BT('a', '')]
        consumed, added = self.c.putmulti(l, overwrite=False)
        assert consumed == 2
        assert added == 1

        assert self.c.set_key(B('a'))
        assert self.c.delete()

        li = iter(l)
        consumed, added = self.c.putmulti(li, overwrite=False)
        assert consumed == 2
        assert added == 1

    def test_bad_seq1(self):
        self.assertRaises(Exception,
                          lambda: self.c.putmulti(range(2)))

    def test_dupsort(self):
        _, env = testlib.temp_env()
        db1 = env.open_db(B('db1'), dupsort=True)
        txn = env.begin(write=True, db=db1)
        with txn.cursor() as c:
            tups = [BT('a', 'value1'), BT('b', 'value1'), BT('b', 'value2')]
            assert (3, 3) == c.putmulti(tups)

    def test_dupsort_putmulti_append(self):
        _, env = testlib.temp_env()
        db1 = env.open_db(B('db1'), dupsort=True)
        txn = env.begin(write=True, db=db1)
        with txn.cursor() as c:
            tups = [BT('a', 'value1'), BT('b', 'value1'), BT('b', 'value2')]
            assert (3, 3) == c.putmulti(tups, append=True)

    def test_dupsort_put_append(self):
        _, env = testlib.temp_env()
        db1 = env.open_db(B('db1'), dupsort=True)
        txn = env.begin(write=True, db=db1)
        with txn.cursor() as c:
            assert c.put(B('a'), B('value1'), append=True)
            assert c.put(B('b'), B('value1'), append=True)
            assert c.put(B('b'), B('value2'), append=True)

class ReplaceTest(CursorTestBase):
    def test_replace(self):
        assert None is self.c.replace(B('a'), B(''))
        assert B('') == self.c.replace(B('a'), B('x'))
        assert B('x') == self.c.replace(B('a'), B('y'))


class ContextManagerTest2(CursorTestBase):
    def test_enter(self):
        with self.c as c:
            assert c is self.c
            c.put(B('a'), B('a'))
            assert c.get(B('a')) == B('a')
        self.assertRaises(Exception,
            lambda: c.get(B('a')))

    def test_exit_success(self):
        with self.txn.cursor() as c:
            c.put(B('a'), B('a'))
        self.assertRaises(Exception,
            lambda: c.get(B('a')))

    def test_exit_failure(self):
        try:
            with self.txn.cursor() as c:
                c.put(B('a'), B('a'))
            raise ValueError
        except ValueError:
            pass
        self.assertRaises(Exception,
            lambda: c.get(B('a')))

    def test_close(self):
        self.c.close()
        self.assertRaises(Exception,
            lambda: c.get(B('a')))

    def test_double_close(self):
        self.c.close()
        self.c.close()
        self.assertRaises(Exception,
            lambda: self.c.put(B('a'), B('a')))

GiB = 1024 * 1024 * 1024

class PreloadTest(CursorTestBase):

    def setUp(self, redo=False):
        env_args = {'writemap': True, 'map_size': GiB}
        if not redo:
            self.path, self.env = testlib.temp_env(**env_args)
        else:
            self.path, self.env = testlib.temp_env(path=self.path, **env_args)
        self.txn = self.env.begin(write=True)
        self.c = self.txn.cursor()

    @unittest.skipIf(not sys.platform.startswith('linux'), "test only works on Linux")
    def test_preload(self):
        """
        Test that reading just the key doesn't prefault the value contents, but
        reading the data does.
        """

        import resource
        self.c.put(B('a'), B('a') * (256 * 1024 * 1024))
        self.txn.commit()
        self.env.close()
        # Just reading the data is obviously going to fault the value in.  The
        # point is to fault it in while the GIL is unlocked.  We use the buffers
        # API so that we're not actually copying the data in.  This doesn't
        # actually show that we're prefaulting with the GIL unlocked, but it
        # does prove we prefault at all, and in 2 correct places.
        self.path, self.env = testlib.temp_env(path=self.path, writemap=True)
        self.txn = self.env.begin(write=True, buffers=True)
        self.c = self.txn.cursor()
        minflts_before = resource.getrusage(resource.RUSAGE_SELF)[6]
        self.c.set_key(B('a'))
        assert bytes(self.c.key()) == B('a')
        minflts_after_key = resource.getrusage(resource.RUSAGE_SELF)[6]

        self.c.value()
        minflts_after_value = resource.getrusage(resource.RUSAGE_SELF)[6]

        epsilon = 60

        # Setting the position doesn't prefault the data
        assert minflts_after_key - minflts_before < epsilon

        # Getting the value does prefault the data, even if we only get it by pointer
        assert minflts_after_value - minflts_after_key > 1000

class CursorReadOnlyTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_cursor_readonly(self):
        '''
        Tests whether you can open a cursor on a sub-db at all in a read-only environment.
        '''
        path, env = testlib.temp_env(max_dbs=10)
        env.open_db(b'foo')
        env.close()
        with lmdb.open(path, max_dbs=10, readonly=True) as env:
            db2 = env.open_db(b'foo')
            with env.begin(db=db2) as txn:
                with txn.cursor(db=db2):
                    pass

if __name__ == '__main__':
    unittest.main()
