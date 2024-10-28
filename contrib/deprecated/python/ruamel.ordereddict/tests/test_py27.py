# coding: utf-8

import sys
import pytest

from ruamel.ordereddict import ordereddict, sorteddict

assert sys.version_info[:2] == (2, 7)

class TestOrderedDictView:
    def test_key(self):
        d = ordereddict()
        d['a'] = 1
        d['b'] = 2
        print(d.viewkeys())
        assert "dict_keys(['a', 'b'])" == str(d.viewkeys())

    def test_values(self):
        d = ordereddict()
        d['a'] = 1
        d['b'] = 2
        print(d.viewvalues())
        assert "dict_values([1, 2])" == str(d.viewvalues())

    def test_items(self):
        d = ordereddict()
        d['a'] = 1
        d['b'] = 2
        print(d.viewitems())
        assert "dict_items([('a', 1), ('b', 2)])" == str(d.viewitems())

