# coding: utf-8

from __future__ import print_function

import sys
import os
import string
import random
import pytest

assert sys.version_info < (3, )

from ruamel.ordereddict import ordereddict, sorteddict

from test_ordereddict import TestBase, all_lowercase

class TestPy2(TestBase):
    def test_sorted_from_ordered(self):
        i = self.z.items(reverse=True)
        random.shuffle(i)
        i = ordereddict(i)
        sd = sorteddict(i)
        assert sd == self.z

    def test_items_rev(self):
        print([x for x in reversed(self.z.items())])
        for index, y in enumerate([x for x in reversed(self.z.items())]):
            i = 25 - index
            assert all_lowercase[i] == y[0]
            assert i == y[1]

    def test_keys_rev(self):
        "unlikely to function in a non-ordered dictionary"
        index = 25
        for y in self.z.keys(reverse=True):
            assert all_lowercase[index] == y
            index -= 1

    def test_iterkeys(self):
        index = 0
        for y in self.z.iterkeys():
            assert all_lowercase[index] == y
            index += 1
        assert index == 26

    #@pytest.mark.skipif(sys.version_info[:2] != (2,7),
    #                    reason="only in 2.7")
    def test_iterkeys_rev(self):
        index = 0
        for y in self.z.iterkeys(reverse=True):
            assert all_lowercase[25 - index] == y
            index += 1
        assert index == 26

    def test_iterkeys_iterator(self):
        tmp = self.z.iterkeys()
        assert tmp.__length_hint__() == 26

    def test_iter(self):
        res = ""
        for y in self.z:
            res += y
        assert all_lowercase == res

    def test_itervalues(self):
        index = 0
        for index, y in enumerate(self.z.itervalues()):
            assert index == y

    def test_itervalues_rev(self):
        index = 0
        for y in self.z.itervalues(reverse=True):
            assert 25 - index == y
            index += 1
        assert index == 26

    def test_iteritems(self):
        index = 0
        for index, y in enumerate(self.z.iteritems()):
            assert all_lowercase[index] == y[0]
            assert index == y[1]

    def test_iteritems_rev(self):
        index = 0
        for y in self.z.iteritems(reverse=True):
            assert all_lowercase[25-index] == y[0]
            assert 25 - index == y[1]
            index += 1
        assert index == 26
