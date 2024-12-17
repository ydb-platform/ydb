##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Base Mapping tests
"""
from operator import __getitem__


def _testIReadMapping(self, inst, state, absent):
    for key in state:
        self.assertEqual(inst[key], state[key])
        self.assertEqual(inst.get(key, None), state[key])
        self.assertIn(key, inst)

    for key in absent:
        self.assertEqual(inst.get(key, None), None)
        self.assertEqual(inst.get(key), None)
        self.assertEqual(inst.get(key, self), self)
        self.assertRaises(KeyError, __getitem__, inst, key)


def _test_keys(self, inst, state):
    # Return the keys of the mapping object
    inst_keys = sorted(inst.keys())
    state_keys = sorted(state.keys())
    self.assertEqual(inst_keys, state_keys)


def _test_iter(self, inst, state):
    # Return the keys of the mapping object
    inst_keys = sorted(inst)
    state_keys = sorted(state.keys())
    self.assertEqual(inst_keys, state_keys)


def _test_values(self, inst, state):
    # Return the values of the mapping object
    inst_values = sorted(inst.values())
    state_values = sorted(state.values())
    self.assertEqual(inst_values, state_values)


def _test_items(self, inst, state):
    # Return the items of the mapping object
    inst_items = sorted(inst.items())
    state_items = sorted(state.items())
    self.assertEqual(inst_items, state_items)


def _test___len__(self, inst, state):
    # Return the number of items
    self.assertEqual(len(inst), len(state))


def _testIEnumerableMapping(self, inst, state):
    _test_keys(self, inst, state)
    _test_items(self, inst, state)
    _test_values(self, inst, state)
    _test___len__(self, inst, state)


class BaseTestIReadMapping:

    def testIReadMapping(self):
        inst = self._IReadMapping__sample()
        state = self._IReadMapping__stateDict()
        absent = self._IReadMapping__absentKeys()
        _testIReadMapping(self, inst, state, absent)


class BaseTestIEnumerableMapping(BaseTestIReadMapping):
    # Mapping objects whose items can be enumerated

    def test_keys(self):
        # Return the keys of the mapping object
        inst = self._IEnumerableMapping__sample()
        state = self._IEnumerableMapping__stateDict()
        _test_keys(self, inst, state)

    def test_values(self):
        # Return the values of the mapping object
        inst = self._IEnumerableMapping__sample()
        state = self._IEnumerableMapping__stateDict()
        _test_values(self, inst, state)

    def test_items(self):
        # Return the items of the mapping object
        inst = self._IEnumerableMapping__sample()
        state = self._IEnumerableMapping__stateDict()
        _test_items(self, inst, state)

    def test___len__(self):
        # Return the number of items
        inst = self._IEnumerableMapping__sample()
        state = self._IEnumerableMapping__stateDict()
        _test___len__(self, inst, state)

    def _IReadMapping__stateDict(self):
        return self._IEnumerableMapping__stateDict()

    def _IReadMapping__sample(self):
        return self._IEnumerableMapping__sample()

    def _IReadMapping__absentKeys(self):
        return self._IEnumerableMapping__absentKeys()
