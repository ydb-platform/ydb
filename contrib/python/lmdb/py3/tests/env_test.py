#
# Copyright 2013 The py-lmdb authors, all rights reserved.
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

from __future__ import absolute_import
from __future__ import with_statement
import os
import sys
import unittest
import weakref

import testlib
from testlib import B
from testlib import OCT
from testlib import INT_TYPES
from testlib import UnicodeType

import lmdb

# Whether we have the patch that allows env.copy* to take a txn
have_txn_patch = lmdb.version(subpatch=True)[3]

NO_READERS = UnicodeType('(no active readers)\n')

try:
    PAGE_SIZE = os.sysconf(os.sysconf_names['SC_PAGE_SIZE'])
except (AttributeError, KeyError, OSError):
    PAGE_SIZE = 4096


class VersionTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_version(self):
        ver = lmdb.version()
        assert len(ver) == 3
        assert all(isinstance(i, INT_TYPES) for i in ver)
        assert all(i >= 0 for i in ver)

    def test_version_subpatch(self):
        ver = lmdb.version(subpatch=True)
        assert len(ver) == 4
        assert all(isinstance(i, INT_TYPES) for i in ver)
        assert all(i >= 0 for i in ver)

class OpenTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_bad_paths(self):
        self.assertRaises(Exception,
            lambda: lmdb.open('/doesnt/exist/at/all'))
        self.assertRaises(Exception,
            lambda: lmdb.open(testlib.temp_file()))

    def test_ok_path(self):
        path, env = testlib.temp_env()
        assert os.path.exists(path)
        assert os.path.exists(os.path.join(path, 'data.mdb'))
        assert os.path.exists(os.path.join(path, 'lock.mdb'))
        assert env.path() == path

    def test_bad_size(self):
        self.assertRaises(OverflowError,
            lambda: testlib.temp_env(map_size=-123))

    def test_tiny_size(self):
        _, env = testlib.temp_env(map_size=10)
        def txn():
            with env.begin(write=True) as txn:
                txn.put(B('a'), B('a'))
        self.assertRaises(lmdb.MapFullError, txn)

    def test_subdir_false_junk(self):
        path = testlib.temp_file()
        fp = open(path, 'wb')
        fp.write(B('A' * 8192))
        fp.close()
        self.assertRaises(lmdb.InvalidError,
            lambda: lmdb.open(path, subdir=False))

    def test_subdir_false_ok(self):
        path = testlib.temp_file(create=False)
        _, env = testlib.temp_env(path, subdir=False)
        assert os.path.exists(path)
        assert os.path.isfile(path)
        assert os.path.isfile(path + '-lock')
        assert not env.flags()['subdir']

    def test_subdir_true_noexist_nocreate(self):
        path = testlib.temp_dir(create=False)
        self.assertRaises(lmdb.Error,
            lambda: testlib.temp_env(path, subdir=True, create=False))
        assert not os.path.exists(path)

    def test_subdir_true_noexist_create(self):
        path = testlib.temp_dir(create=False)
        path_, env = testlib.temp_env(path, subdir=True, create=True)
        assert path_ == path
        assert env.path() == path

    def test_subdir_true_exist_nocreate(self):
        path, env = testlib.temp_env()
        assert lmdb.open(path, subdir=True, create=False).path() == path

    def test_subdir_true_exist_create(self):
        path, env = testlib.temp_env()
        assert lmdb.open(path, subdir=True, create=True).path() == path

    def test_readonly_false(self):
        path, env = testlib.temp_env(readonly=False)
        with env.begin(write=True) as txn:
            txn.put(B('a'), B(''))
        with env.begin() as txn:
            assert txn.get(B('a')) == B('')
        assert not env.flags()['readonly']

    def test_readonly_true_noexist(self):
        path = testlib.temp_dir(create=False)
        # Open readonly missing store should fail.
        self.assertRaises(lmdb.Error,
            lambda: lmdb.open(path, readonly=True, create=True))
        # And create=True should not have mkdir'd it.
        assert not os.path.exists(path)

    def test_readonly_true_exist(self):
        path, env = testlib.temp_env()
        env2 = lmdb.open(path, readonly=True)
        assert env2.path() == path
        # Attempting a write txn should fail.
        self.assertRaises(lmdb.ReadonlyError,
            lambda: env2.begin(write=True))
        # Flag should be set.
        assert env2.flags()['readonly']

    def test_metasync(self):
        for flag in True, False:
            path, env = testlib.temp_env(metasync=flag)
            assert env.flags()['metasync'] == flag

    def test_lock(self):
        for flag in True, False:
            path, env = testlib.temp_env(lock=flag)
            lock_path = os.path.join(path, 'lock.mdb')
            assert env.flags()['lock'] == flag
            assert flag == os.path.exists(lock_path)

    def test_sync(self):
        for flag in True, False:
            path, env = testlib.temp_env(sync=flag)
            assert env.flags()['sync'] == flag

    def test_map_async(self):
        for flag in True, False:
            path, env = testlib.temp_env(map_async=flag)
            assert env.flags()['map_async'] == flag

    def test_mode_subdir_create(self):
        if sys.platform == 'win32':
            # Mode argument is ignored on Windows; see lmdb.h
            return

        oldmask = os.umask(0)
        try:
            for mode in OCT('777'), OCT('755'), OCT('700'):
                path = testlib.temp_dir(create=False)
                env = lmdb.open(path, subdir=True, create=True, mode=mode)
                fmode = mode & ~OCT('111')
                assert testlib.path_mode(path) == mode
                assert testlib.path_mode(path+'/data.mdb') == fmode
                assert testlib.path_mode(path+'/lock.mdb') == fmode
        finally:
            os.umask(oldmask)

    def test_mode_subdir_nocreate(self):
        if sys.platform == 'win32':
            # Mode argument is ignored on Windows; see lmdb.h
            return

        oldmask = os.umask(0)
        try:
            for mode in OCT('777'), OCT('755'), OCT('700'):
                path = testlib.temp_dir()
                env = lmdb.open(path, subdir=True, create=False, mode=mode)
                fmode = mode & ~OCT('111')
                assert testlib.path_mode(path+'/data.mdb') == fmode
                assert testlib.path_mode(path+'/lock.mdb') == fmode
        finally:
            os.umask(oldmask)

    def test_readahead(self):
        for flag in True, False:
            path, env = testlib.temp_env(readahead=flag)
            assert env.flags()['readahead'] == flag

    def test_writemap(self):
        for flag in True, False:
            path, env = testlib.temp_env(writemap=flag)
            assert env.flags()['writemap'] == flag

    def test_meminit(self):
        for flag in True, False:
            path, env = testlib.temp_env(meminit=flag)
            assert env.flags()['meminit'] == flag

    def test_max_readers(self):
        self.assertRaises(lmdb.InvalidParameterError,
            lambda: testlib.temp_env(max_readers=0))
        for val in 123, 234:
            _, env = testlib.temp_env(max_readers=val)
            assert env.info()['max_readers'] == val

    def test_max_dbs(self):
        self.assertRaises(OverflowError,
            lambda: testlib.temp_env(max_dbs=-1))
        for val in 0, 10, 20:
            _, env = testlib.temp_env(max_dbs=val)
            dbs = [env.open_db(B('db%d' % i)) for i in range(val)]
            self.assertRaises(lmdb.DbsFullError,
                lambda: env.open_db(B('toomany')))


class SetMapSizeTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_invalid(self):
        _, env = testlib.temp_env()
        env.close()
        self.assertRaises(Exception,
            lambda: env.set_mapsize(999999))

    def test_negative(self):
        _, env = testlib.temp_env()
        self.assertRaises(OverflowError,
            lambda: env.set_mapsize(-2015))

    def test_applied(self):
        _, env = testlib.temp_env(map_size=PAGE_SIZE * 8)
        assert env.info()['map_size'] == PAGE_SIZE * 8

        env.set_mapsize(PAGE_SIZE * 16)
        assert env.info()['map_size'] == PAGE_SIZE * 16


class CloseTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_close(self):
        _, env = testlib.temp_env()
        # Attempting things should be ok.
        txn = env.begin(write=True)
        txn.put(B('a'), B(''))
        cursor = txn.cursor()
        list(cursor)
        cursor.first()
        it = iter(cursor)

        env.close()
        # Repeated calls are ignored:
        env.close()
        # Attempting to use invalid objects should crash.
        self.assertRaises(Exception, lambda: txn.cursor())
        self.assertRaises(Exception, lambda: txn.commit())
        self.assertRaises(Exception, lambda: cursor.first())
        self.assertRaises(Exception, lambda: list(it))
        # Abort should be OK though.
        txn.abort()
        # Attempting to start new txn should crash.
        self.assertRaises(Exception,
            lambda: env.begin())


class ContextManagerTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_ok(self):
        path, env = testlib.temp_env()
        with env as env_:
            assert env_ is env
            with env.begin() as txn:
                txn.get(B('foo'))
        self.assertRaises(Exception, lambda: env.begin())

    def test_crash(self):
        path, env = testlib.temp_env()
        try:
            with env as env_:
                assert env_ is env
                with env.begin() as txn:
                    txn.get(123)
        except:
            pass
        self.assertRaises(Exception, lambda: env.begin())


class InfoMethodsTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_path(self):
        path, env = testlib.temp_env()
        assert path == env.path()
        assert isinstance(env.path(), UnicodeType)

        env.close()
        self.assertRaises(Exception,
            lambda: env.path())

    def test_stat(self):
        _, env = testlib.temp_env()
        stat = env.stat()
        for k in 'psize', 'depth', 'branch_pages', 'overflow_pages',\
                 'entries':
            assert isinstance(stat[k], INT_TYPES), k
            assert stat[k] >= 0

        assert stat['entries'] == 0
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()
        stat = env.stat()
        assert stat['entries'] == 1

        env.close()
        self.assertRaises(Exception,
            lambda: env.stat())

    def test_info(self):
        _, env = testlib.temp_env()
        info = env.info()
        for k in 'map_addr', 'map_size', 'last_pgno', 'last_txnid', \
                 'max_readers', 'num_readers':
            assert isinstance(info[k], INT_TYPES), k
            assert info[k] >= 0

        assert info['last_txnid'] == 0
        txn = env.begin(write=True)
        txn.put(B('a'), B(''))
        txn.commit()
        info = env.info()
        assert info['last_txnid'] == 1

        env.close()
        self.assertRaises(Exception,
            lambda: env.info())

    def test_flags(self):
        _, env = testlib.temp_env()
        info = env.flags()
        for k in 'subdir', 'readonly', 'metasync', 'sync', 'map_async',\
                 'readahead', 'writemap':
            assert isinstance(info[k], bool)

        env.close()
        self.assertRaises(Exception,
            lambda: env.flags())

    def test_max_key_size(self):
        _, env = testlib.temp_env()
        mks = env.max_key_size()
        assert isinstance(mks, INT_TYPES)
        assert mks > 0

        env.close()
        self.assertRaises(Exception,
            lambda: env.max_key_size())

    def test_max_readers(self):
        _, env = testlib.temp_env()
        mr = env.max_readers()
        assert isinstance(mr, INT_TYPES)
        assert mr > 0 and mr == env.info()['max_readers']

        env.close()
        self.assertRaises(Exception,
            lambda: env.max_readers())

    def test_readers(self):
        _, env = testlib.temp_env(max_spare_txns=0)
        r = env.readers()
        assert isinstance(r, UnicodeType)
        assert r == NO_READERS

        rtxn = env.begin()
        r2 = env.readers()
        assert isinstance(env.readers(), UnicodeType)
        assert env.readers() != r

        env.close()
        self.assertRaises(Exception,
            lambda: env.readers())


class OtherMethodsTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_copy(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()

        dest_dir = testlib.temp_dir()

        if have_txn_patch:
            with env.begin() as txn:
                self.assertRaises(Exception,
                                  lambda: env.copy(dest_dir, txn=txn))

        env.copy(dest_dir, compact=True)
        assert os.path.exists(dest_dir + '/data.mdb')

        cenv = lmdb.open(dest_dir)
        ctxn = cenv.begin()
        assert ctxn.get(B('a')) == B('b')

        env.close()
        self.assertRaises(Exception,
            lambda: env.copy(testlib.temp_dir()))

    def test_copy_compact(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()

        dest_dir = testlib.temp_dir()
        env.copy(dest_dir, compact=True)
        assert os.path.exists(dest_dir + '/data.mdb')

        cenv = lmdb.open(dest_dir)
        ctxn = cenv.begin()
        assert ctxn.get(B('a')) == B('b')

        # Test copy with transaction provided
        dest_dir = testlib.temp_dir()
        with env.begin(write=True) as txn:
            copy_txn = env.begin()
            txn.put(B('b'), B('b'))
        assert ctxn.get(B('b')) is None

        if have_txn_patch:

            env.copy(dest_dir, compact=True, txn=copy_txn)
            assert os.path.exists(dest_dir + '/data.mdb')

            cenv = lmdb.open(dest_dir)
            ctxn = cenv.begin()
            assert ctxn.get(B('a')) == B('b')
            # Verify that the write that occurred outside the txn isn't seen in the
            # copy
            assert ctxn.get(B('b')) is None

        else:
            self.assertRaises(Exception,
                              lambda: env.copy(dest_dir, compact=True, txn=copy_txn))

        env.close()
        self.assertRaises(Exception,
            lambda: env.copy(testlib.temp_dir()))

    def test_copyfd_compact(self):
        path, env = testlib.temp_env()
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()

        dst_path = testlib.temp_file(create=False)
        fp = open(dst_path, 'wb')
        env.copyfd(fp.fileno())

        dstenv = lmdb.open(dst_path, subdir=False)
        dtxn = dstenv.begin()
        assert dtxn.get(B('a')) == B('b')
        fp.close()

        # Test copy with transaction provided
        dst_path = testlib.temp_file(create=False)
        fp = open(dst_path, 'wb')
        with env.begin(write=True) as txn:
            copy_txn = env.begin()
            txn.put(B('b'), B('b'))
        assert dtxn.get(B('b')) is None

        if have_txn_patch:

            env.copyfd(fp.fileno(), compact=True, txn=copy_txn)

            dstenv = lmdb.open(dst_path, subdir=False)
            dtxn = dstenv.begin()
            assert dtxn.get(B('a')) == B('b')
            # Verify that the write that occurred outside the txn isn't seen in the
            # copy
            assert dtxn.get(B('b')) is None
            dstenv.close()

        else:
            self.assertRaises(Exception,
                lambda: env.copyfd(fp.fileno(), compact=True, txn=copy_txn))

        env.close()
        self.assertRaises(Exception,
            lambda: env.copyfd(fp.fileno()))
        fp.close()

    def test_copyfd(self):
        path, env = testlib.temp_env()
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()

        dst_path = testlib.temp_file(create=False)
        fp = open(dst_path, 'wb')

        if have_txn_patch:
            with env.begin() as txn:
                self.assertRaises(Exception,
                                  lambda: env.copyfd(fp.fileno(), txn=txn))
        env.copyfd(fp.fileno())

        dstenv = lmdb.open(dst_path, subdir=False)
        dtxn = dstenv.begin()
        assert dtxn.get(B('a')) == B('b')

        env.close()
        self.assertRaises(Exception,
            lambda: env.copyfd(fp.fileno()))
        fp.close()

    def test_sync(self):
        _, env = testlib.temp_env()
        env.sync(False)
        env.sync(True)
        env.close()
        self.assertRaises(Exception,
            lambda: env.sync(False))

    @staticmethod
    def _test_reader_check_child(path):
        """Function to run in child process since we can't use fork() on
        win32."""
        env = lmdb.open(path, max_spare_txns=0)
        txn = env.begin()
        os._exit(0)

    def test_reader_check(self):
        if sys.platform == 'win32':
            # Stale writers are cleared automatically on Windows, see lmdb.h
            return

        path, env = testlib.temp_env(max_spare_txns=0)
        rc = env.reader_check()
        assert rc == 0

        # We need to open a separate env since Transaction.abort() always calls
        # reset for a read-only txn, the actual abort doesn't happen until
        # __del__, when Transaction discovers there is no room for it on the
        # freelist.
        env1 = lmdb.open(path)
        txn1 = env1.begin()
        assert env.readers() != NO_READERS
        assert env.reader_check() == 0

        # Start a child, open a txn, then crash the child.
        rc = os.spawnle(os.P_WAIT, sys.executable, sys.executable,
                        'test_reader_check_child', path,
                        {'Y_PYTHON_ENTRY_POINT': __name__})

        assert rc == 0
        assert env.reader_check() == 1
        assert env.reader_check() == 0
        assert env.readers() != NO_READERS

        txn1.abort()
        env1.close()
        assert env.readers() == NO_READERS

        env.close()
        self.assertRaises(Exception,
            lambda: env.reader_check())


class BeginTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_begin_closed(self):
        _, env = testlib.temp_env()
        env.close()
        self.assertRaises(Exception,
            lambda: env.begin())

    def test_begin_readonly(self):
        _, env = testlib.temp_env()
        txn = env.begin()
        # Read txn can't write.
        self.assertRaises(lmdb.ReadonlyError,
            lambda: txn.put(B('a'), B('')))
        txn.abort()

    def test_begin_write(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        # Write txn can write.
        assert txn.put(B('a'), B(''))
        txn.commit()

    def test_bind_db(self):
        _, env = testlib.temp_env()
        main = env.open_db(None)
        self.assertRaises(ValueError, lambda: env.open_db(None, dupsort=True))
        sub = env.open_db(B('db1'))

        txn = env.begin(write=True, db=sub)
        assert txn.put(B('b'), B(''))           # -> sub
        assert txn.put(B('a'), B(''), db=main)  # -> main
        txn.commit()

        txn = env.begin()
        assert txn.get(B('a')) == B('')
        assert txn.get(B('b')) is None
        assert txn.get(B('a'), db=sub) is None
        assert txn.get(B('b'), db=sub) == B('')
        txn.abort()

    def test_parent_readonly(self):
        _, env = testlib.temp_env()
        parent = env.begin()
        # Nonsensical.
        self.assertRaises(lmdb.InvalidParameterError,
            lambda: env.begin(parent=parent))

    def test_parent(self):
        _, env = testlib.temp_env()
        parent = env.begin(write=True)
        parent.put(B('a'), B('a'))

        child = env.begin(write=True, parent=parent)
        assert child.get(B('a')) == B('a')
        assert child.put(B('a'), B('b'))
        child.abort()

        # put() should have rolled back
        assert parent.get(B('a')) == B('a')

        child = env.begin(write=True, parent=parent)
        assert child.put(B('a'), B('b'))
        child.commit()

        # put() should be visible
        assert parent.get(B('a')) == B('b')

    def test_buffers(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True, buffers=True)
        assert txn.put(B('a'), B('a'))
        b = txn.get(B('a'))
        assert b is not None
        assert len(b) == 1
        assert not isinstance(b, type(B('')))
        txn.commit()

        txn = env.begin(buffers=False)
        b = txn.get(B('a'))
        assert b is not None
        assert len(b) == 1
        assert isinstance(b, type(B('')))
        txn.abort()


class OpenDbTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_main(self):
        _, env = testlib.temp_env()
        # Start write txn, so we cause deadlock if open_db attempts txn.
        txn = env.begin(write=True)
        # Now get main DBI, we should already be open.
        db = env.open_db(None)
        # w00t, no deadlock.

        flags = db.flags(txn)
        assert not flags['reverse_key']
        assert not flags['dupsort']
        txn.abort()

    def test_unicode(self):
        _, env = testlib.temp_env()
        assert env.open_db(B('myindex')) is not None
        self.assertRaises(TypeError,
            lambda: env.open_db(UnicodeType('myindex')))

    def test_sub_notxn(self):
        _, env = testlib.temp_env()
        assert env.info()['last_txnid'] == 0
        db1 = env.open_db(B('subdb1'))
        assert env.info()['last_txnid'] == 1
        db2 = env.open_db(B('subdb2'))
        assert env.info()['last_txnid'] == 2

        env.close()
        self.assertRaises(Exception,
            lambda: env.open_db('subdb3'))

    def test_sub_rotxn(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=False)
        self.assertRaises(lmdb.ReadonlyError,
            lambda: env.open_db(B('subdb'), txn=txn))

    def test_sub_txn(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        db1 = env.open_db(B('subdb1'), txn=txn)
        db2 = env.open_db(B('subdb2'), txn=txn)
        for db in db1, db2:
            assert db.flags(txn) == {
                'dupfixed': False,
                'dupsort': False,
                'integerdup': False,
                'integerkey': False,
                'reverse_key': False,
            }
        txn.commit()

    def test_reopen(self):
        path, env = testlib.temp_env()
        db1 = env.open_db(B('subdb1'))
        env.close()
        env = lmdb.open(path, max_dbs=10)
        db1 = env.open_db(B('subdb1'))

    FLAG_SETS = [
        (flag, val)
        for flag in (
            'reverse_key', 'dupsort', 'integerkey', 'integerdup', 'dupfixed'
        )
        for val in (True, False)
    ]

    def test_flags(self):
        path, env = testlib.temp_env()
        txn = env.begin(write=True)

        for flag, val in self.FLAG_SETS:
            key = B('%s-%s' % (flag, val))
            db = env.open_db(key, txn=txn, **{flag: val})
            assert db.flags(txn)[flag] == val
            assert db.flags(None)[flag] == val
            assert db.flags()[flag] == val
            self.assertRaises(TypeError, lambda: db.flags(1, 2, 3))

        txn.commit()
        # Test flag persistence.
        env.close()
        env = lmdb.open(path, max_dbs=10)
        txn = env.begin(write=True)

        for flag, val in self.FLAG_SETS:
            key = B('%s-%s' % (flag, val))
            db = env.open_db(key, txn=txn)
            assert db.flags(txn)[flag] == val

    def test_readonly_env_main(self):
        path, env = testlib.temp_env()
        env.close()

        env = lmdb.open(path, readonly=True)
        db = env.open_db(None)

    def test_readonly_env_sub_noexist(self):
        # https://github.com/dw/py-lmdb/issues/109
        path, env = testlib.temp_env()
        env.close()

        env = lmdb.open(path, max_dbs=10, readonly=True)
        self.assertRaises(lmdb.NotFoundError,
            lambda: env.open_db(B('node_schedules'), create=False))

    def test_readonly_env_sub_eperm(self):
        # https://github.com/dw/py-lmdb/issues/109
        path, env = testlib.temp_env()
        env.close()

        env = lmdb.open(path, max_dbs=10, readonly=True)
        self.assertRaises(lmdb.ReadonlyError,
            lambda: env.open_db(B('node_schedules'), create=True))

    def test_readonly_env_sub(self):
        # https://github.com/dw/py-lmdb/issues/109
        path, env = testlib.temp_env()
        assert env.open_db(B('node_schedules')) is not None
        env.close()

        env = lmdb.open(path, max_dbs=10, readonly=True)
        db = env.open_db(B('node_schedules'), create=False)
        assert db is not None


reader_count = lambda env: env.readers().count('\n') - 1

class SpareTxnTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_none(self):
        _, env = testlib.temp_env(max_spare_txns=0)
        assert 0 == reader_count(env)

        t1 = env.begin()
        assert 1 == reader_count(env)

        t2 = env.begin()
        assert 2 == reader_count(env)

        t1.abort()
        del t1
        assert 1 == reader_count(env)

        t2.abort()
        del t2
        assert 0 == reader_count(env)

    def test_one(self):
        _, env = testlib.temp_env(max_spare_txns=1)
        # 1 here, since CFFI uses a temporary reader during init.
        assert 1 >= reader_count(env)

        t1 = env.begin()
        assert 1 == reader_count(env)

        t2 = env.begin()
        assert 2 == reader_count(env)

        t1.abort()
        del t1
        assert 2 == reader_count(env)  # 1 live, 1 cached

        t2.abort()
        del t2
        assert 1 == reader_count(env)  # 1 cached

        t3 = env.begin()
        assert 1 == reader_count(env)  # 1 live

        t3.abort()
        del t3
        assert 1 == reader_count(env)  # 1 cached


class LeakTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_open_unref_does_not_leak(self):
        temp_dir = testlib.temp_dir()
        env = lmdb.open(temp_dir)
        ref = weakref.ref(env)
        env = None
        testlib.debug_collect()
        assert ref() is None

    def test_open_close_does_not_leak(self):
        temp_dir = testlib.temp_dir()
        env = lmdb.open(temp_dir)
        env.close()
        ref = weakref.ref(env)
        env = None
        testlib.debug_collect()
        assert ref() is None

    def test_weakref_callback_invoked_once(self):
        temp_dir = testlib.temp_dir()
        env = lmdb.open(temp_dir)
        env.close()
        count = [0]
        def callback(ref):
            count[0] += 1
        ref = weakref.ref(env, callback)
        env = None
        testlib.debug_collect()
        assert ref() is None
        assert count[0] == 1


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'test_reader_check_child':
        OtherMethodsTest._test_reader_check_child(sys.argv[2])
    else:
        unittest.main()
