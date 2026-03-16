# -*- coding: utf-8 -*-

#
# furl - URL manipulation made simple.
#
# Ansgar Grunseid
# grunseid.com
# grunseid@gmail.com
#
# License: Build Amazing Things (Unlicense)
#

import unittest
from itertools import chain, product, permutations

import six
from furl.omdict1D import omdict1D
from orderedmultidict import omdict

_unique = object()


class TestOmdict1D(unittest.TestCase):

    def setUp(self):
        self.key = 'sup'
        self.keys = [1, 2, -1, 'a', None, 0.9]
        self.values = [1, 2, None]
        self.valuelists = [[], [1], [1, 2, 3], [None, None, 1]]

    def test_update_updateall(self):
        data, omd1, omd2 = omdict(), omdict1D(), omdict1D()

        # All permutations of (self.keys, self.values) and (self.keys,
        # self.valuelists).
        allitems = chain(product(self.keys, self.values),
                         product(self.keys, self.valuelists))

        # All updates of length one item, two items, and three items.
        iterators = [permutations(allitems, 1),
                     permutations(allitems, 2),
                     permutations(allitems, 3),
                     permutations(allitems, 4),
                     ]

        for iterator in iterators:
            for update in iterator:
                data.update(update)
                omd1.update(update)
                omd2.updateall(update)
                for key in six.iterkeys(omd1):
                    if isinstance(data[key], list):
                        assert omd1[key] == data[key][-1]
                    else:
                        assert omd1[key] == data[key]
                for key in six.iterkeys(omd2):
                    data_values_unpacked = []
                    for value in data.getlist(key):
                        if isinstance(value, list):
                            data_values_unpacked.extend(value)
                        else:
                            data_values_unpacked.append(value)

                    assert omd2.getlist(key) == data_values_unpacked

        # Test different empty list value locations.
        update_tests = [([(1, None), (2, None)],
                         [(1, [1, 11]), (2, [2, 22])],
                         [(1, 11), (2, 22)]),
                        ([(1, None), (2, None)],
                         [(1, []), (1, 1), (1, 11)],
                         [(1, 11), (2, None)]),
                        ([(1, None), (2, None)],
                         [(1, 1), (1, []), (1, 11)],
                         [(1, 11), (2, None)]),
                        ([(1, None), (2, None)],
                         [(1, 1), (1, 11), (1, [])],
                         [(2, None)]),
                        ]
        for init, update, result in update_tests:
            omd = omdict1D(init)
            omd.update(update)
            assert omd.allitems() == result

        updateall_tests = [([(1, None), (2, None)],
                            [(1, [1, 11]), (2, [2, 22])],
                            [(1, 1), (2, 2), (1, 11), (2, 22)]),
                           ([(1, None), (2, None)],
                            [(1, []), (1, 1), (1, 11)],
                            [(1, 1), (2, None), (1, 11)]),
                           ([(1, None), (2, None)],
                            [(1, 1), (1, []), (1, 11)],
                            [(1, 11), (2, None)]),
                           ([(1, None), (2, None)],
                            [(1, 1), (1, 11), (1, [])],
                            [(2, None)]),
                           ]
        for init, update, result in updateall_tests:
            omd = omdict1D(init)
            omd.updateall(update)
            assert omd.allitems() == result

    def test_add(self):
        runningsum = []
        omd = omdict1D()
        for valuelist in self.valuelists:
            runningsum += valuelist
            if valuelist:
                assert omd.add(self.key, valuelist) == omd
                assert omd[self.key] == omd.get(self.key) == runningsum[0]
                assert omd.getlist(self.key) == runningsum
            else:
                assert self.key not in omd

        runningsum = []
        omd = omdict1D()
        for value in self.values:
            runningsum += [value]
            assert omd.add(self.key, value) == omd
            assert omd[self.key] == omd.get(self.key) == runningsum[0]
            assert omd.getlist(self.key) == runningsum

        # Empty list of values adds nothing.
        assert _unique not in omd
        assert omd.add(_unique, []) == omd
        assert _unique not in omd

    def test_set(self):
        omd1, omd2, omd3 = omdict1D(), omdict1D(), omdict1D()

        for valuelist in self.valuelists:
            omd1[self.key] = valuelist
            assert omd2.set(self.key, valuelist) == omd2
            assert omd3.setlist(self.key, valuelist) == omd3
            assert omd1 == omd2 == omd3 and omd1.getlist(self.key) == valuelist

        # Empty list of values deletes that key and all its values,
        # equivalent to del omd[somekey].
        omd = omdict1D()
        assert _unique not in omd
        omd.set(_unique, [])
        assert omd == omd
        assert _unique not in omd

        omd.set(_unique, [1, 2, 3])
        assert omd.getlist(_unique) == [1, 2, 3]
        omd.set(_unique, [])
        assert _unique not in omd

    def test_setitem(self):
        omd = omdict1D()
        for value, valuelist in six.moves.zip(self.values, self.valuelists):
            if valuelist:
                omd[self.key] = valuelist
                assert omd[self.key] == omd.get(self.key) == valuelist[0]
                assert omd.getlist(self.key) == valuelist
            else:
                assert self.key not in omd

            omd[self.key] = value
            assert omd[self.key] == omd.get(self.key) == value
            assert omd.getlist(self.key) == [value]

        # Empty list of values deletes that key and all its values,
        # equivalent to del omd[somekey].
        omd = omdict1D()
        assert _unique not in omd
        omd[_unique] = []
        assert omd == omd
        assert _unique not in omd

        omd[_unique] = [1, 2, 3]
        assert omd.getlist(_unique) == [1, 2, 3]
        omd[_unique] = []
        assert _unique not in omd
