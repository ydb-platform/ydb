# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2003-2007, 2009-2011 Nominum, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND NOMINUM DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import copy
import unittest

import dns.set

# for convenience
S = dns.set.Set

class SetTestCase(unittest.TestCase):

    def testLen1(self):
        s1 = S()
        self.assertEqual(len(s1), 0)

    def testLen2(self):
        s1 = S([1, 2, 3])
        self.assertEqual(len(s1), 3)

    def testLen3(self):
        s1 = S([1, 2, 3, 3, 3])
        self.assertEqual(len(s1), 3)

    def testUnion1(self):
        s1 = S([1, 2, 3])
        s2 = S([1, 2, 3])
        e = S([1, 2, 3])
        self.assertEqual(s1 | s2, e)

    def testUnion2(self):
        s1 = S([1, 2, 3])
        s2 = S([])
        e = S([1, 2, 3])
        self.assertEqual(s1 | s2, e)

    def testUnion3(self):
        s1 = S([1, 2, 3])
        s2 = S([3, 4])
        e = S([1, 2, 3, 4])
        self.assertEqual(s1 | s2, e)

    def testUnionPlusSyntax(self):
        s1 = S([1, 2, 3])
        s2 = S([3, 4])
        e = S([1, 2, 3, 4])
        self.assertEqual(s1 + s2, e)

    def testIntersection1(self):
        s1 = S([1, 2, 3])
        s2 = S([1, 2, 3])
        e = S([1, 2, 3])
        self.assertEqual(s1 & s2, e)

    def testIntersection2(self):
        s1 = S([0, 1, 2, 3])
        s2 = S([1, 2, 3, 4])
        e = S([1, 2, 3])
        self.assertEqual(s1 & s2, e)

    def testIntersection3(self):
        s1 = S([1, 2, 3])
        s2 = S([])
        e = S([])
        self.assertEqual(s1 & s2, e)

    def testIntersection4(self):
        s1 = S([1, 2, 3])
        s2 = S([5, 4])
        e = S([])
        self.assertEqual(s1 & s2, e)

    def testDifference1(self):
        s1 = S([1, 2, 3])
        s2 = S([5, 4])
        e = S([1, 2, 3])
        self.assertEqual(s1 - s2, e)

    def testDifference2(self):
        s1 = S([1, 2, 3])
        s2 = S([])
        e = S([1, 2, 3])
        self.assertEqual(s1 - s2, e)

    def testDifference3(self):
        s1 = S([1, 2, 3])
        s2 = S([3, 2])
        e = S([1])
        self.assertEqual(s1 - s2, e)

    def testDifference4(self):
        s1 = S([1, 2, 3])
        s2 = S([3, 2, 1])
        e = S([])
        self.assertEqual(s1 - s2, e)

    def testSubset1(self):
        s1 = S([1, 2, 3])
        s2 = S([3, 2, 1])
        self.assertTrue(s1.issubset(s2))

    def testSubset2(self):
        s1 = S([1, 2, 3])
        self.assertTrue(s1.issubset(s1))

    def testSubset3(self):
        s1 = S([])
        s2 = S([1, 2, 3])
        self.assertTrue(s1.issubset(s2))

    def testSubset4(self):
        s1 = S([1])
        s2 = S([1, 2, 3])
        self.assertTrue(s1.issubset(s2))

    def testSubset5(self):
        s1 = S([])
        s2 = S([])
        self.assertTrue(s1.issubset(s2))

    def testSubset6(self):
        s1 = S([1, 4])
        s2 = S([1, 2, 3])
        self.assertTrue(not s1.issubset(s2))

    def testSuperset1(self):
        s1 = S([1, 2, 3])
        s2 = S([3, 2, 1])
        self.assertTrue(s1.issuperset(s2))

    def testSuperset2(self):
        s1 = S([1, 2, 3])
        self.assertTrue(s1.issuperset(s1))

    def testSuperset3(self):
        s1 = S([1, 2, 3])
        s2 = S([])
        self.assertTrue(s1.issuperset(s2))

    def testSuperset4(self):
        s1 = S([1, 2, 3])
        s2 = S([1])
        self.assertTrue(s1.issuperset(s2))

    def testSuperset5(self):
        s1 = S([])
        s2 = S([])
        self.assertTrue(s1.issuperset(s2))

    def testSuperset6(self):
        s1 = S([1, 2, 3])
        s2 = S([1, 4])
        self.assertTrue(not s1.issuperset(s2))

    def testUpdate1(self):
        s1 = S([1, 2, 3])
        u = (4, 5, 6)
        e = S([1, 2, 3, 4, 5, 6])
        s1.update(u)
        self.assertEqual(s1, e)

    def testUpdate2(self):
        s1 = S([1, 2, 3])
        u = []
        e = S([1, 2, 3])
        s1.update(u)
        self.assertEqual(s1, e)

    def testGetitem(self):
        s1 = S([1, 2, 3])
        i0 = s1[0]
        i1 = s1[1]
        i2 = s1[2]
        s2 = S([i0, i1, i2])
        self.assertEqual(s1, s2)

    def testGetslice(self):
        s1 = S([1, 2, 3])
        slice = s1[0:2]
        self.assertEqual(len(slice), 2)
        item = s1[2]
        slice.append(item)
        s2 = S(slice)
        self.assertEqual(s1, s2)

    def testDelitem(self):
        s1 = S([1, 2, 3])
        del s1[0]
        self.assertEqual(list(s1), [2, 3])

    def testDelslice(self):
        s1 = S([1, 2, 3])
        del s1[0:2]
        self.assertEqual(list(s1), [3])

    def testRemoveNonexistent(self):
        s1 = S([1, 2, 3])
        s2 = S([1, 2, 3])
        with self.assertRaises(ValueError):
            s1.remove(4)
        self.assertEqual(s1, s2)

    def testDiscardNonexistent(self):
        s1 = S([1, 2, 3])
        s2 = S([1, 2, 3])
        s1.discard(4)
        self.assertEqual(s1, s2)

    def testCopy(self):
        s1 = S([1, 2, 3])
        s2 = s1.copy()
        s1.remove(1)
        self.assertNotEqual(s1, s2)
        s1.add(1)
        self.assertEqual(s1, s2)
        s2 = copy.copy(s1)
        self.assertEqual(s1, s2)

    def testBadUpdates(self):
        s = S([1, 2, 3])
        self.assertRaises(ValueError, lambda: s.union_update(1))
        self.assertRaises(ValueError, lambda: s.intersection_update(1))

    def testSelfUpdates(self):
        expected = S([1, 2, 3])
        s = S([1, 2, 3])
        s.union_update(s)
        self.assertEqual(s, expected)
        s.intersection_update(s)
        self.assertEqual(s, expected)
        s.difference_update(s)
        self.assertTrue(len(s) == 0)

    def testBadSubsetSuperset(self):
        s = S([1, 2, 3])
        self.assertRaises(ValueError, lambda: s.issubset(123))
        self.assertRaises(ValueError, lambda: s.issuperset(123))

    def testIncrementalOperators(self):
        s = S([1, 2, 3])
        s += S([5, 4])
        self.assertEqual(s, S([1, 2, 3, 4, 5]))
        s -= S([1, 2])
        self.assertEqual(s, S([3, 4, 5]))
        s |= S([1, 2])
        self.assertEqual(s, S([1, 2, 3, 4, 5]))
        s &= S([1, 2])
        self.assertEqual(s, S([1, 2]))

if __name__ == '__main__':
    unittest.main()
