from __future__ import absolute_import
from __future__ import with_statement
import unittest

import testlib, struct
from testlib import KEYSFIXED, ITEMS_MULTI_FIXEDKEY
from testlib import putBigDataMultiFixed

class GetMultiTestBase(unittest.TestCase):

    def tearDown(self):
        testlib.cleanup()

    def setUp(self, dupsort=None, dupfixed=None):
        self.db_key = "testdb".encode('utf-8')
        self.path, self.env = testlib.temp_env(max_dbs=1)
        self.txn = self.env.begin(write=True)
        self.db = self.env.open_db(
            key=self.db_key, txn=self.txn,
            dupsort=dupsort,
            dupfixed=dupfixed
            )
        putBigDataMultiFixed(self.txn, db=self.db)
        self.c = self.txn.cursor(db=self.db)

    def matchList(self, ls_a, ls_b):
        return ((not (ls_a or ls_b)) or
            (ls_a and ls_b and all(map(lambda x, y: x == y, ls_a, ls_b))))


class GetMultiTestNoDupsortNoDupfixed(GetMultiTestBase):

    ITEMS2_MULTI_NODUP = ITEMS_MULTI_FIXEDKEY[1::2]

    def setUp(self, dupsort=False, dupfixed=False):
        super(GetMultiTestNoDupsortNoDupfixed, self).setUp(dupsort=dupsort, dupfixed=dupfixed)

    def testGetMulti(self):
        test_list = self.c.getmulti(KEYSFIXED)
        self.assertEqual(self.matchList(test_list, self.ITEMS2_MULTI_NODUP), True)


class GetMultiTestDupsortNoDupfixed(GetMultiTestBase):

    def setUp(self, dupsort=True, dupfixed=False):
        super(GetMultiTestDupsortNoDupfixed, self).setUp(dupsort=dupsort, dupfixed=dupfixed)

    def testGetMulti(self):
        test_list = self.c.getmulti(KEYSFIXED, dupdata=True)
        self.assertEqual(self.matchList(test_list, ITEMS_MULTI_FIXEDKEY), True)


class GetMultiTestDupsortDupfixed(GetMultiTestBase):

    def setUp(self, dupsort=True, dupfixed=True):
        super(GetMultiTestDupsortDupfixed, self).setUp(dupsort=dupsort, dupfixed=dupfixed)

    def testGetMulti(self):
        test_list = self.c.getmulti(KEYSFIXED, dupdata=True, dupfixed_bytes=1)
        self.assertEqual(self.matchList(test_list, ITEMS_MULTI_FIXEDKEY), True)

class GetMultiTestDupsortDupfixedKeyfixed(GetMultiTestBase):

    def setUp(self, dupsort=True, dupfixed=True):
        super(GetMultiTestDupsortDupfixedKeyfixed, self).setUp(dupsort=dupsort, dupfixed=dupfixed)

    def testGetMulti(self):
        val_bytes = 1
        arr = bytearray(self.c.getmulti(
            KEYSFIXED, dupdata=True,
            dupfixed_bytes=val_bytes, keyfixed=True
        ))
        asserts = []
        for i, kv in enumerate(ITEMS_MULTI_FIXEDKEY):
            key, val = kv
            asserts.extend((
                struct.pack('b', arr[i*2]) == key,
                struct.pack('b', arr[i*2+1]) == val
            ))
        self.assertEqual(all(asserts), True)


if __name__ == '__main__':
    unittest.main()