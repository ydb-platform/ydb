#! /usr/bin/env python
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

# test delete(dupdata)

from __future__ import absolute_import
from __future__ import with_statement
import unittest

import testlib
from testlib import B
from testlib import BT
from testlib import KEYS, ITEMS, KEYS2, ITEMS2
from testlib import putData, putBigData


class IterationTestBase(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.path, self.env = testlib.temp_env()  # creates 10 databases
        self.txn = self.env.begin(write=True)
        putData(self.txn)
        self.c = self.txn.cursor()
        self.empty_entry = (B(''), B(''))

    def matchList(self, ls_a, ls_b):
        return all(map(lambda x, y: x == y, ls_a, ls_b))


class IterationTestBase2(unittest.TestCase):
    """ This puts more data than its predecessor"""

    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.path, self.env = testlib.temp_env()  # creates 10 databases
        self.txn = self.env.begin(write=True)
        putBigData(self.txn)  # HERE!
        self.c = self.txn.cursor()
        self.empty_entry = ('', '')

    def matchList(self, ls_a, ls_b):
        return all(map(lambda x, y: x == y, ls_a, ls_b))


class IterationTest(IterationTestBase):
    def testFromStart(self):
        # From start
        self.c.first()
        self.assertEqual(self.c.key(), KEYS[0])  # start of db
        test_list = [i for i in iter(self.c)]
        self.assertEqual(self.matchList(test_list, ITEMS), True)
        self.assertEqual(self.c.item(), self.empty_entry)  # end of db

    def testFromStartWithIternext(self):
        # From start with iternext
        self.c.first()
        self.assertEqual(self.c.key(), KEYS[0])  # start of db
        test_list = [i for i in self.c.iternext()]
        # remaining elements in db
        self.assertEqual(self.matchList(test_list, ITEMS), True)
        self.assertEqual(self.c.item(), self.empty_entry)  # end of db

    def testFromStartWithNext(self):
        # From start with next
        self.c.first()
        self.assertEqual(self.c.key(), KEYS[0])  # start of db
        test_list = []
        while 1:
            test_list.append(self.c.item())
            if not self.c.next():
                break
        self.assertEqual(self.matchList(test_list, ITEMS), True)

    def testFromExistentKeySetKey(self):
        self.c.first()
        self.c.set_key(KEYS[1])
        self.assertEqual(self.c.key(), KEYS[1])
        test_list = [i for i in self.c.iternext()]
        self.assertEqual(self.matchList(test_list, ITEMS[1:]), True)

    def testFromExistentKeySetRange(self):
        self.c.first()
        self.c.set_range(KEYS[1])
        self.assertEqual(self.c.key(), KEYS[1])
        test_list = [i for i in self.c.iternext()]
        self.assertEqual(self.matchList(test_list, ITEMS[1:]), True)

    def testFromNonExistentKeySetRange(self):
        self.c.first()
        self.c.set_range(B('c'))
        self.assertEqual(self.c.key(), B('d'))
        test_list = [i for i in self.c.iternext()]
        test_items = [i for i in ITEMS if i[0] > B('c')]
        self.assertEqual(self.matchList(test_list, test_items), True)

    def testFromLastKey(self):
        self.c.last()
        self.assertEqual(self.c.key(), KEYS[-1])
        test_list = [i for i in self.c.iternext()]
        self.assertEqual(self.matchList(test_list, ITEMS[-1:]), True)

    def testFromNonExistentKeyPastEnd(self):
        self.c.last()
        self.assertEqual(self.c.key(), KEYS[-1])
        # next() fails, leaving iterator in an unpositioned state.
        self.c.next()
        self.assertEqual(self.c.item(), self.empty_entry)
        # iternext() from an unpositioned state proceeds from start of DB.
        test_list = list(self.c.iternext())
        self.assertEqual(test_list, ITEMS)


class ReverseIterationTest(IterationTestBase):
    def testFromStartRev(self):
        # From start
        self.c.first()
        self.assertEqual(self.c.key(), KEYS[0])  # start of db
        test_list = [i for i in self.c.iterprev()]
        self.assertEqual(self.matchList(test_list, ITEMS[:1][::-1]), True)
        self.assertEqual(self.c.item(), self.empty_entry)  # very start of db

    def testFromExistentKeySetKeyRev(self):
        self.c.first()
        self.c.set_key(KEYS[2])
        self.assertEqual(self.c.key(), KEYS[2])
        test_list = [i for i in self.c.iterprev()]
        self.assertEqual(self.matchList(test_list, ITEMS[:3][::-1]), True)

    def testFromExistentKeySetRangeRev(self):
        self.c.first()
        self.c.set_range(KEYS[2])
        self.assertEqual(self.c.key(), KEYS[2])
        test_list = [i for i in self.c.iterprev()]
        self.assertEqual(self.matchList(test_list, ITEMS[:3][::-1]), True)

    def testFromNonExistentKeySetRangeRev(self):
        self.c.first()
        self.c.set_range(B('c'))
        self.assertEqual(self.c.key(), B('d'))
        test_list = [i for i in self.c.iterprev()]
        test_items = [i for i in ITEMS if i[0] <= B('d')]
        test_items = test_items[::-1]
        self.assertEqual(self.matchList(test_list, test_items), True)

    def testFromLastKeyRev(self):
        self.c.last()
        self.assertEqual(self.c.key(), KEYS[-1])
        test_list = [i for i in self.c.iterprev()]
        self.assertEqual(self.matchList(test_list, ITEMS[::-1]), True)

    def testFromLastKeyWithPrevRev(self):
        self.c.last()
        self.assertEqual(self.c.key(), KEYS[-1])  # end of db
        test_list = []
        while 1:
            test_list.append(self.c.item())
            if not self.c.prev():
                break
        self.assertEqual(self.matchList(test_list, ITEMS[::-1]), True)

    def testFromNonExistentKeyPastEndRev(self):
        self.c.first()
        self.assertEqual(self.c.key(), KEYS[0])
        # prev() fails, leaving iterator in an unpositioned state.
        self.c.prev()
        self.assertEqual(self.c.item(), self.empty_entry)
        # iterprev() from an unpositioned state proceeds from end of DB.
        test_list = list(self.c.iterprev())
        self.assertEqual(test_list, ITEMS[::-1])

class IterationTestWithDupsBase(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.path, self.env = testlib.temp_env()
        db = self.env.open_db(B('db1'), dupsort=True)
        self.txn = self.env.begin(db, write=True)
        for _ in range(2):
            putData(self.txn)
        self.c = self.txn.cursor()
        self.empty_entry = ('', '')

    def matchList(self, ls_a, ls_b):
        return all(map(lambda x, y: x == y, ls_a, ls_b))


class IterationTestWithDups(IterationTestWithDupsBase):
    pass


class SeekIterationTest(IterationTestBase2):
    def testForwardIterationSeek(self):
        self.c.first()
        test_list = []
        for i in self.c.iternext():
            test_list.append(i)
            # skips d and e
            if self.c.key() == B('baa'):
                self.c.set_key(B('e'))
        test_item = [i for i in ITEMS2 if i[0] not in (B('d'), B('e'))]
        self.assertEqual(test_list, test_item)

    def testPutDuringIteration(self):
        self.c.first()
        test_list = []
        c = self.txn.cursor()
        for i in c.iternext():
            test_list.append(i)
            # adds 'i' upon seeing 'e'
            if c.key() == B('e'):
                self.c.put(B('i'), B(''))
        test_item = ITEMS2 + [(B('i'), B(''))]
        self.assertEqual(test_list, test_item)

    def testDeleteDuringIteration(self):
        self.c.first()
        test_list = []
        for i in self.c.iternext():
            # deletes 'e' upon seeing it
            if self.c.key() == B('e'):
                # Causes 'e' to be deleted, and advances cursor to next
                # element.
                self.c.delete()
                i = self.c.item()
            test_list.append(i)

        test_item = [i for i in ITEMS2 if i[0] != B('e')]
        self.assertEqual(test_list, test_item)


if __name__ == '__main__':
    unittest.main()
