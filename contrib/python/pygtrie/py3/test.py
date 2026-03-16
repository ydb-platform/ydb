#!/usr/bin/python

"""trie module unit tests."""

__author__ = 'Michal Nazarewicz <mina86@mina86.com>'
__copyright__ = ('Copyright 2014-2017 Google LLC',
                 'Copyright 2018-2019 Michal Nazarewicz <mina86@mina86.com>')

import array
import base64
import collections
import copy
import pickle
import sys
import unittest

import pygtrie

# pylint: disable=missing-docstring


class _TrieFactoryParameteriser(object):
    # pylint: disable=no-self-argument, invalid-name

    def __make_update_trie_factory(update):  # pylint: disable=unused-private-member
        def factory(trie_ctor, d):
            t = trie_ctor()
            update(t, d)
            return t
        return factory

    def __setter_trie_factory(trie_ctor, d):  # pylint: disable=unused-private-member
        t = trie_ctor()
        for k, v in d.items():
            t[k] = v
        return t

    def __sorted_trie_factory(trie_ctor, d):  # pylint: disable=unused-private-member
        t = trie_ctor(d)
        t.enable_sorting(True)
        return t

    _FACTORIES = ((
        'TrieFromNamedArgs',
        lambda trie_ctor, d: trie_ctor(**d)
    ), (
        'TrieFromTuples',
        lambda trie_ctor, d: trie_ctor(d.items())
    ), (
        'TrieFromDict',
        lambda trie_ctor, d: trie_ctor(d)
    ), (
        'TrieFromTrie',
        lambda trie_ctor, d: trie_ctor(trie_ctor(d))
    ), (
        'UpdateWithNamedArgs',
        __make_update_trie_factory(lambda t, d: t.update(**d))  # pylint: disable=too-many-function-args
    ), (
        'UpdateWithTuples',
        __make_update_trie_factory(lambda t, d: t.update(d.items()))  # pylint: disable=too-many-function-args
    ), (
        'UpdateWithDict',
        __make_update_trie_factory(lambda t, d: t.update(d))  # pylint: disable=too-many-function-args
    ), (
        'Setters',
        __setter_trie_factory
    ), (
        'Sorted',
        __sorted_trie_factory
    ))

    def __call__(self, cls):
        for name in list(cls.__dict__):
            if name.startswith('_do_test_'):
                self.__parameterise_method(cls, name)
        return cls

    def __parameterise_method(self, cls, name):
        orig = getattr(cls, name)
        for factory_name, factory in self._FACTORIES:
            method = self.__make_test_method(orig, factory)
            if orig.__doc__:
                method.__doc__ = '%s using %s trie factory.' % (
                    orig.__doc__[:-2], factory_name)
            setattr(cls, '%s_%s' % (name[4:], factory_name), method)

    @staticmethod
    def __make_test_method(method, factory):
        return lambda self: method(self, trie_factory=factory)


@_TrieFactoryParameteriser()
class TrieTestCase(unittest.TestCase):
    # The below need to be overwritten by subclasses

    # A Trie class being tested
    _TRIE_CTOR = pygtrie.Trie

    # A key to set
    _SHORT_KEY = 'foo'
    # Another key which has all but the last component equal to the ones in
    # _SHORT_KEY.
    _SHORT_KEY2 = 'foO'
    # Another key to set such that _SHORT_KEY is it's prefix
    _LONG_KEY = _SHORT_KEY + 'bar'
    # A key that is not set but _LONG_KEY is it's prefix
    _VERY_LONG_KEY = _LONG_KEY + 'baz'
    # Two more keys which are not set and have no relation to other keys.
    _OTHER_KEY = 'qux'
    _OTHER_KEY2 = 'zzz'
    # A list of prefixes of _SHORT_KEY
    _SHORT_PREFIXES = ('', 'f', 'fo')
    # A list of prefixes of _LONG_KEY which are not prefixes of _SHORT_KEY nor
    # _SHORT_KEY itself
    _LONG_PREFIXES = ('foob', 'fooba')

    @classmethod
    def path_from_key(cls, key):
        """Turns key into a path as used by Trie class being tested."""
        return key

    @classmethod
    def key_from_path(cls, path):
        """Turns path as used by Trie class being tested into a key."""
        return tuple(path)

    # End of stuff that needs to be overwritten by subclasses

    def __init__(self, *args, **kw):
        super(TrieTestCase, self).__init__(*args, **kw)
        # Python 2 compatibility.  Noisy code to confuse pylint so it does not
        # issue deprecated-method warning. pylint: disable=invalid-name
        for new, old in (('assertRegex', 'assertRegexpMatches'),
                         ('assertRaisesRegex', 'assertRaisesRegexp')):
            if not hasattr(self, new):
                setattr(self, new, getattr(self, old))

    def key_from_key(self, key):
        """Turns a key into a form that the Trie will return e.g. in keys()."""
        return self.key_from_path(self.path_from_key(key))

    # pylint: disable=invalid-name

    def assertNodeState(self, t, key, prefix=False, value=None):
        """Asserts a state of given node in a trie.

        Args:
            t: Trie to check the node in.
            key: A key for the node.
            prefix: Whether the node is a prefix of a longer key that is in the
              trie.
            value: If given, value associated with the key.  If missing, node
                has no value associated with it.
        Raises:
            AssertionError: If any assumption is not met.
        """
        if prefix:
            self.assertTrue(t.has_subtrie(key))
            self.assertTrue(bool(t.has_node(key) & pygtrie.Trie.HAS_SUBTRIE))
        else:
            self.assertFalse(t.has_subtrie(key))
            self.assertFalse(bool(t.has_node(key) & pygtrie.Trie.HAS_SUBTRIE))
        if value is None:
            o = object()
            self.assertNotIn(key, t)
            key_error_exception = pygtrie.ShortKeyError if prefix else KeyError
            self.assertRaises(key_error_exception, lambda: t[key])
            self.assertRaises(key_error_exception, t.pop, key)
            self.assertIsNone(t.get(key))
            self.assertIs(o, t.get(key, o))
            self.assertIs(o, t.pop(key, o))
            self.assertFalse(t.has_key(key))
            self.assertNotIn(self.key_from_key(key), list(t.iterkeys()))
            self.assertNotIn(self.key_from_key(key), t.keys())
            self.assertEqual(pygtrie.Trie.HAS_SUBTRIE if prefix else 0,
                             t.has_node(key))
        else:
            self.assertIn(key, t)
            self.assertEqual(value, t[key])
            self.assertEqual(value, t.get(key))
            self.assertEqual(value, t.get(key, object()))
            self.assertTrue(t.has_key(key))
            self.assertTrue(bool(t.has_node(key) & pygtrie.Trie.HAS_VALUE))
            self.assertIn(self.key_from_key(key), list(t.iterkeys()))
            self.assertIn(self.key_from_key(key), t.keys())

    def assertFullTrie(self, t, value=42):
        """Asserts a trie has _SHORT_KEY and _LONG_KEY set to value."""
        self.assertEqual(2, len(t))
        for prefix in self._SHORT_PREFIXES + self._LONG_PREFIXES:
            self.assertNodeState(t, prefix, prefix=True)
        self.assertNodeState(t, self._SHORT_KEY, prefix=True, value=value)
        self.assertNodeState(t, self._LONG_KEY, value=value)
        self.assertNodeState(t, self._VERY_LONG_KEY)
        self.assertNodeState(t, self._OTHER_KEY)

    def assertShortTrie(self, t, value=42):
        """Asserts a trie has only _SHORT_KEY set to value."""
        self.assertEqual(1, len(t))
        for prefix in self._SHORT_PREFIXES:
            self.assertNodeState(t, prefix, prefix=True)
        for key in self._LONG_PREFIXES + (
                self._LONG_KEY, self._VERY_LONG_KEY, self._OTHER_KEY):
            self.assertNodeState(t, key)
        self.assertNodeState(t, self._SHORT_KEY, value=value)

        # self.assertRegex(str(t), r'Trie\([^:]*: [^:]*\)')
        # self.assertRegex(repr(t), r'Trie\(\(\(.*, .*\),\)\)')

    def assertEmptyTrie(self, t):
        """Asserts a trie is empty."""
        self.assertEqual(0, len(t), '%r should be empty: %d' % (t, len(t)))

        for key in self._SHORT_PREFIXES + self._LONG_PREFIXES + (
                self._SHORT_KEY, self._LONG_KEY, self._VERY_LONG_KEY,
                self._OTHER_KEY):
            self.assertNodeState(t, key)

        self.assertRaises(KeyError, t.popitem)

        # self.assertEqual('Trie()', str(t))
        # self.assertEqual('Trie()', repr(t))

    def _assertBasics(self, t):
        self.assertFullTrie(t)

        self.assertEqual(42, t.pop(self._LONG_KEY))
        self.assertShortTrie(t)

        self.assertEqual(42, t.setdefault(self._SHORT_KEY, 24))
        self.assertShortTrie(t)

        t[self._SHORT_KEY] = 24
        self.assertShortTrie(t, 24)

        self.assertEqual(24, t.setdefault(self._LONG_KEY, 24))
        self.assertFullTrie(t, 24)

        del t[self._LONG_KEY]
        self.assertShortTrie(t, 24)

        self.assertEqual((self.key_from_key(self._SHORT_KEY), 24), t.popitem())
        self.assertEmptyTrie(t)

        t[self._LONG_KEY] = '42'
        self.assertRaisesRegex(pygtrie.ShortKeyError, str(self._SHORT_KEY),
                               t.__delitem__, self._SHORT_KEY)

    def _do_test_basics(self, trie_factory):
        """Basic trie tests"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        self._assertBasics(trie_factory(self._TRIE_CTOR, d))

    def _do_test_basics_sorted(self, trie_factory):
        """Basic trie tests with sorting enabled"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)
        t.enable_sorting(True)
        self._assertBasics(t)

    def _do_test_popitem(self, trie_factory):
        """Test popitem method"""
        data = ((self._SHORT_KEY, 'a'),
                (self._LONG_KEY, 'b'),
                (self._OTHER_KEY, 'c'),
                (self._OTHER_KEY2, 'd'))

        t = trie_factory(self._TRIE_CTOR, dict(data))
        got = {}
        while t:
            k, v = t.popitem()
            got[k] = v
        self.assertEqual(dict((self.key_from_key(k), v) for k, v in data), got)

    def _do_test_invalid_arguments(self, trie_factory):
        """Test various methods check for invalid arguments"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        self.assertRaisesRegex(
            ValueError, 'update.. takes at most one positional argument,',
            t.update, (self._LONG_KEY, 42), (self._VERY_LONG_KEY, 42))

        self.assertRaisesRegex(TypeError, r'slice\(.*, None\)',
                               lambda: t[self._SHORT_KEY:self._LONG_KEY])
        self.assertRaisesRegex(TypeError, r"slice\(.*, 'foo'\)",
                               lambda: t[self._SHORT_KEY:self._LONG_KEY:'foo'])
    def _do_test_clear(self, trie_factory):
        """Test clear method"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)
        self.assertFullTrie(t)
        t.clear()
        self.assertEmptyTrie(t)

    def _do_test_iterator(self, trie_factory):
        """Trie iterator tests"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        self.assertEqual([42, 42], t.values())
        self.assertEqual([42, 42], list(t.itervalues()))

        short_key = self.key_from_key(self._SHORT_KEY)
        long_key = self.key_from_key(self._LONG_KEY)

        expected_items = [(short_key, 42), (long_key, 42)]
        self.assertEqual(expected_items, t.items())
        self.assertEqual(expected_items, list(t.iteritems()))

        self.assertEqual([short_key, long_key], list(t))
        self.assertEqual([short_key, long_key], t.keys())
        self.assertEqual([short_key, long_key], list(t.iterkeys()))

    def _do_test_subtrie_iterator(self, trie_factory):
        """Subtrie iterator tests"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        long_key = self.key_from_key(self._LONG_KEY)
        prefix = self._LONG_PREFIXES[0]

        self.assertEqual([42, 42], t.values(prefix=self._SHORT_KEY))
        self.assertEqual([42, 42], list(t.itervalues(prefix=self._SHORT_KEY)))
        self.assertEqual([42], t.values(prefix=prefix))
        self.assertEqual([42], list(t.itervalues(prefix=prefix)))

        expected_items = [(long_key, 42)]
        self.assertEqual(expected_items, t.items(prefix=prefix))
        self.assertEqual(expected_items, list(t.iteritems(prefix=prefix)))

        self.assertEqual([long_key], t.keys(prefix=prefix))
        self.assertEqual([long_key], list(t.iterkeys(prefix=prefix)))

    def _do_test_shallow_iterator(self, trie_factory):
        """Shallow iterator test"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        self.assertEqual([42], t.values(shallow=True))
        self.assertEqual([42], list(t.itervalues(shallow=True)))

        short_key = self.key_from_key(self._SHORT_KEY)

        expected_items = [(short_key, 42)]
        self.assertEqual(expected_items, t.items(shallow=True))
        self.assertEqual(expected_items, list(t.iteritems(shallow=True)))

        self.assertEqual([short_key], t.keys(shallow=True))
        self.assertEqual([short_key], list(t.iterkeys(shallow=True)))

    def _do_test_splice_operations(self, trie_factory):
        """Splice trie operations tests"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        self.assertEqual([42, 42], list(t[self._SHORT_KEY:]))
        self.assertEqual([42], list(t[self._LONG_PREFIXES[0]:]))

        t[self._SHORT_KEY:] = 24
        self.assertShortTrie(t, 24)

        self.assertEqual([24], list(t[self._SHORT_KEY:]))
        self.assertRaises(KeyError, lambda: list(t[self._LONG_PREFIXES[0]:]))

        t[self._LONG_KEY:] = 24
        self.assertFullTrie(t, 24)

        del t[self._SHORT_KEY:]
        self.assertEmptyTrie(t)

    def _do_test_find_one_prefix(self, trie_factory):
        """Shortest and longest prefix finding methods test"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        short_pair = (self.key_from_key(self._SHORT_KEY), 42)
        long_pair = (self.key_from_key(self._LONG_KEY), 42)
        none_pair = (None, None)

        def assert_pair(expected, got):
            self.assertEqual(expected, tuple(got))
            self.assertEqual(expected[0], got.key)
            self.assertEqual(expected[1], got.value)
            if expected[0]:
                self.assertTrue(got)
            else:
                self.assertFalse(got)
                self.assertFalse(got.is_set)
                self.assertFalse(got.has_subtrie)
                self.assertIsNone(got.get())

        assert_pair(short_pair, t.shortest_prefix(self._VERY_LONG_KEY))
        assert_pair(short_pair, t.shortest_prefix(self._LONG_KEY))
        assert_pair(short_pair, t.shortest_prefix(self._VERY_LONG_KEY))
        assert_pair(short_pair, t.shortest_prefix(self._LONG_PREFIXES[-1]))
        assert_pair(short_pair, t.shortest_prefix(self._SHORT_KEY))
        assert_pair(none_pair, t.shortest_prefix(self._SHORT_PREFIXES[-1]))
        assert_pair(none_pair, t.shortest_prefix(self._OTHER_KEY))

        assert_pair(long_pair, t.longest_prefix(self._VERY_LONG_KEY))
        assert_pair(long_pair, t.longest_prefix(self._LONG_KEY))
        assert_pair(long_pair, t.longest_prefix(self._VERY_LONG_KEY))
        assert_pair(short_pair, t.longest_prefix(self._LONG_PREFIXES[-1]))
        assert_pair(short_pair, t.longest_prefix(self._SHORT_KEY))
        assert_pair(none_pair, t.longest_prefix(self._SHORT_PREFIXES[-1]))
        assert_pair(none_pair, t.longest_prefix(self._OTHER_KEY))

    def _do_test_list_prefixes(self, trie_factory):
        """Key prefixes listing method test"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        short_pair = (self.key_from_key(self._SHORT_KEY), 42)
        long_pair = (self.key_from_key(self._LONG_KEY), 42)

        def assert_prefixes(expected, *args):
            got = list(t.prefixes(*args))
            self.assertEqual(expected, [tuple(step) for step in got])
            for e, g in zip(expected, got):
                self.assertTrue(g)
                self.assertEqual(e[0], g.key)
                self.assertEqual(e[1], g.value)

        assert_prefixes([], self._SHORT_PREFIXES[-1])
        assert_prefixes([], self._OTHER_KEY)
        assert_prefixes([short_pair], self._SHORT_KEY)
        assert_prefixes([short_pair], self._LONG_PREFIXES[-1])
        assert_prefixes([short_pair, long_pair], self._LONG_KEY)
        assert_prefixes([short_pair, long_pair], self._VERY_LONG_KEY)

    def _do_test_walk_towards(self, trie_factory):
        """walk_towards method test"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        def assert_step(step):
            self.assertTrue(step)
            self.assertEqual(step[0], step.key)

            is_set = step.key in (self.key_from_key(self._SHORT_KEY),
                                  self.key_from_key(self._LONG_KEY))
            self.assertEqual(is_set, step.is_set)
            self.assertEqual(step.key != self.key_from_key(self._LONG_KEY),
                             step.has_subtrie)

            if not is_set:
                self.assertRaises(KeyError, lambda: step.value)
                self.assertRaises(KeyError, lambda: step[1])
                self.assertEqual('42', step.get('42'))
                self.assertEqual(42, step.setdefault(42))
                self.assertEqual(42, step.value)

            self.assertEqual(42, step.value)
            self.assertEqual(42, step[1])
            self.assertEqual(42, step.get('42'))
            self.assertEqual(42, step.setdefault('42'))
            self.assertEqual(42, step.value)
            step.value = 24
            self.assertEqual(24, step.value)
            step.set('42')
            self.assertEqual('42', step.value)

            # pylint: disable=protected-access
            step.value = 42 if is_set else pygtrie._EMPTY

        def assert_steps(key, raises=False):
            try:
                n = 0
                for step in t.walk_towards(key):
                    assert_step(step)
                    n += 1
                self.assertFalse(raises)
                self.assertEqual(len(self.path_from_key(key)) + 1, n)
            except KeyError:
                self.assertTrue(raises)

        assert_steps(self._SHORT_PREFIXES[-1])
        assert_steps(self._OTHER_KEY[-1], raises=True)
        assert_steps(self._SHORT_KEY)
        assert_steps(self._LONG_PREFIXES[-1])
        assert_steps(self._LONG_KEY)
        assert_steps(self._VERY_LONG_KEY, raises=True)

    def _do_test_pickle(self, trie_factory):
        """https://github.com/google/pygtrie/issues/7"""
        d = dict.fromkeys((self._SHORT_KEY, self._LONG_KEY, self._VERY_LONG_KEY,
                           self._OTHER_KEY), 42)
        t = trie_factory(self._TRIE_CTOR, d)

        for protocol in range(0, pickle.HIGHEST_PROTOCOL + 1):
            pickled = pickle.dumps(t, protocol=protocol)
            u = pickle.loads(pickled)
            self.assertEqual(t, u, 'protocol=' + str(protocol))

    def _test_copy_impl(self, make_copy, is_shallow):
        key = self._LONG_KEY

        trie = self._TRIE_CTOR({key: [42]})
        cpy = make_copy(trie)
        self.assertEqual(trie, cpy)

        cpy[key] = [24]
        self.assertEqual([42], trie[key])
        self.assertEqual([24], cpy[key])
        self.assertNotEqual(trie, cpy)

        del cpy[key]
        self.assertNotEqual(trie, cpy)
        self.assertIn(key, trie)
        self.assertNotIn(key, cpy)

        empty_cpy = make_copy(cpy)
        self.assertEqual(cpy, empty_cpy)

        cpy[key] = [42]
        self.assertEqual(trie, cpy)
        self.assertNotEqual(cpy, empty_cpy)

        cpy = make_copy(trie)
        cpy[key][0] = 24
        self.assertEqual([24 if is_shallow else 42], trie[key])
        self.assertEqual([24], cpy[key])
        if is_shallow:
            self.assertEqual(trie, cpy)
        else:
            self.assertNotEqual(trie, cpy)

    def test_shallow_copy(self):
        """Test creating a shallow copy."""
        self._test_copy_impl(copy.copy, True)

    def test_copy_method(self):
        """Test copy method."""
        self._test_copy_impl(lambda t: t.copy(), True)

    def test_deep_copy(self):
        """Test creating a deep copy."""
        self._test_copy_impl(copy.deepcopy, False)

    def test_prefix_set(self):
        """PrefixSet test."""
        short_key = self.key_from_key(self._SHORT_KEY)
        long_key = self.key_from_key(self._LONG_KEY)
        very_long_key = self.key_from_key(self._VERY_LONG_KEY)
        other_key = self.key_from_key(self._OTHER_KEY)

        ps = pygtrie.PrefixSet(factory=self._TRIE_CTOR)
        self.assertFalse(ps)
        self.assertEqual(0, len(ps))
        self.assertEqual([], list(ps))

        for key in (self._LONG_KEY, self._VERY_LONG_KEY):
            ps.add(key)
            self.assertEqual(1, len(ps))
            self.assertEqual([long_key], list(ps.iter()))
            self.assertEqual([long_key], list(iter(ps)))
            self.assertEqual([long_key], list(ps.iter(self._SHORT_KEY)))
            self.assertEqual([long_key], list(ps.iter(self._LONG_KEY)))
            self.assertEqual([very_long_key],
                             list(ps.iter(self._VERY_LONG_KEY)))
            self.assertEqual([], list(ps.iter(self._OTHER_KEY)))

        ps.add(self._SHORT_KEY)
        self.assertTrue(ps)
        self.assertEqual(1, len(ps))
        self.assertEqual([short_key], list(ps.iter()))
        self.assertEqual([short_key], list(iter(ps)))
        self.assertEqual([short_key], list(ps.iter(self._SHORT_KEY)))
        self.assertEqual([long_key], list(ps.iter(self._LONG_KEY)))
        self.assertEqual([], list(ps.iter(self._OTHER_KEY)))

        ps.add(self._OTHER_KEY)
        self.assertEqual(2, len(ps))
        self.assertEqual(sorted((short_key, other_key)), sorted(ps.iter()))
        self.assertEqual([short_key], list(ps.iter(self._SHORT_KEY)))
        self.assertEqual([long_key], list(ps.iter(self._LONG_KEY)))
        self.assertEqual([other_key], list(ps.iter(self._OTHER_KEY)))

        ps.clear()
        self.assertFalse(ps)
        self.assertEqual(0, len(ps))
        self.assertEqual([], list(ps))

    def _test_prefix_set_copy_impl(self, make_copy):
        ps = pygtrie.PrefixSet(factory=self._TRIE_CTOR)
        ps.add(self._SHORT_KEY)
        self.assertEqual(1, len(ps))

        cpy = make_copy(ps)
        self.assertEqual(ps, cpy)

        ps.add(self._OTHER_KEY)
        self.assertNotEqual(ps, cpy)

    def test_prefix_set_shallow_copy(self):
        self._test_prefix_set_copy_impl(copy.copy)

    def test_prefix_set_copy_method(self):
        self._test_prefix_set_copy_impl(lambda ps: ps.copy())

    def test_prefix_set_deep_copy(self):
        self._test_prefix_set_copy_impl(copy.deepcopy)

    def test_prefix_set_init_prunes_branch(self):
        """Tests long keys are removed during PrefixSet construction."""
        # https://github.com/google/pygtrie/issues/29

        def check(iterable):
            ps = pygtrie.PrefixSet(iterable, factory=self._TRIE_CTOR)
            self.assertEqual(1, len(ps))
            self.assertEqual([self.key_from_key(self._SHORT_KEY)], list(ps))

        check([self._SHORT_KEY])
        check([self._SHORT_KEY, self._LONG_KEY])
        check([self._LONG_KEY, self._SHORT_KEY])

    def test_equality(self):
        """Tests equality comparison."""
        a = self._TRIE_CTOR()
        b = self._TRIE_CTOR({self._SHORT_KEY: 42})
        c = self._TRIE_CTOR({self._SHORT_KEY: '42'})
        d = self._TRIE_CTOR({self._OTHER_KEY: 42})
        e = self._TRIE_CTOR({self._SHORT_KEY: 42, self._OTHER_KEY: 42})
        f = self._TRIE_CTOR({self._SHORT_KEY: 42, self._OTHER_KEY.upper(): 42})

        tries = (a, b, c, d, e, f)
        for x in tries:
            for y in tries:
                if x is y:
                    self.assertEqual(x, y)
                    self.assertEqual(x, copy.copy(y))
                    self.assertEqual(x, copy.deepcopy(y))
                else:
                    self.assertNotEqual(x, y)

    def test_fromkeys(self):
        x = self._TRIE_CTOR({self._SHORT_KEY: 42, self._LONG_KEY: 42})
        # pylint: disable=no-member,protected-access,unexpected-keyword-arg
        if hasattr(self._TRIE_CTOR, 'fromkeys'):
            y = self._TRIE_CTOR.fromkeys((self._SHORT_KEY, self._LONG_KEY), 42)
        else:
            y = type(x).fromkeys((self._SHORT_KEY, self._LONG_KEY), 42,
                                 separator=x._separator)
        self.assertEqual(x, y)

    def _assertToString(self, t, s, r):
        tp = type(t).__name__
        t.enable_sorting(True)
        self.assertEqual(tp + s, str(t))
        self.assertEqual(tp + r, repr(t))

    _PICKLED_PROTO_0 = (
        'Y2NvcHlfcmVnCl9yZWNvbnN0cnVjdG9yCnAwCihjcHlndHJpZQpUcmllCnAxCmNfX2J1aW'
        'x0aW5fXwpvYmplY3QKcDIKTnRwMwpScDQKKGRwNQpWX3Jvb3QKcDYKZzAKKGNweWd0cmll'
        'Cl9Ob2RlCnA3CmcyCk50cDgKUnA5CihscDEwCkwzTAphVmYKcDExCmFWbwpwMTIKYWcxMg'
        'phTDQyTAphTC0zTAphTDNMCmFWYgpwMTMKYVZhCnAxNAphVnIKcDE1CmFMNDJMCmFMLTFM'
        'CmFMMUwKYVZ6CnAxNgphTDQyTAphYnNWX3NvcnRlZApwMTcKSTAwCnNiLg==')

    _PICKLED_PROTO_3 = (
        'gANjcHlndHJpZQpUcmllCnEAKYFxAX1xAihYBQAAAF9yb290cQNjcHlndHJpZQpfTm9kZQ'
        'pxBCmBcQVdcQYoSwNYAQAAAGZxB1gBAAAAb3EIaAhLKkr9////SwNYAQAAAGJxCVgBAAAA'
        'YXEKWAEAAABycQtLKkr/////SwFYAQAAAHpxDEsqZWJYBwAAAF9zb3J0ZWRxDYl1Yi4=')

    def assertUnpickling(self, want, pickled):
        got = pickle.loads(base64.b64decode(pickled))
        self.assertEqual(want, got)

    def test_pickling_proto0(self):
        want = self._TRIE_CTOR((key, 42) for key in ('foo', 'bar', 'baz'))
        self.assertUnpickling(want, self._PICKLED_PROTO_0)

    @unittest.skipIf(sys.version_info[0] < 3, "Protocol 3 requires Python 3+")
    def test_pickling_proto3(self):
        want = self._TRIE_CTOR((key, 42) for key in ('foo', 'bar', 'baz'))
        self.assertUnpickling(want, self._PICKLED_PROTO_3)


class CharTrieTestCase(TrieTestCase):
    _TRIE_CTOR = pygtrie.CharTrie

    _PICKLED_PROTO_0 = (
        'Y2NvcHlfcmVnCl9yZWNvbnN0cnVjdG9yCnAwCihjcHlndHJpZQpDaGFyVHJpZQpwMQpjX1'
        '9idWlsdGluX18Kb2JqZWN0CnAyCk50cDMKUnA0CihkcDUKVl9yb290CnA2CmcwCihjcHln'
        'dHJpZQpfTm9kZQpwNwpnMgpOdHA4ClJwOQoobHAxMApMM0wKYVZmCnAxMQphVm8KcDEyCm'
        'FnMTIKYUw0MkwKYUwtM0wKYUwzTAphVmIKcDEzCmFWYQpwMTQKYVZyCnAxNQphTDQyTAph'
        'TC0xTAphTDFMCmFWegpwMTYKYUw0MkwKYWJzVl9zb3J0ZWQKcDE3CkkwMApzYi4=')

    _PICKLED_PROTO_3 = (
        'gANjcHlndHJpZQpDaGFyVHJpZQpxACmBcQF9cQIoWAUAAABfcm9vdHEDY3B5Z3RyaWUKX0'
        '5vZGUKcQQpgXEFXXEGKEsDWAEAAABmcQdYAQAAAG9xCGgISypK/f///0sDWAEAAABicQlY'
        'AQAAAGFxClgBAAAAcnELSypK/////0sBWAEAAAB6cQxLKmViWAcAAABfc29ydGVkcQ2JdW'
        'Iu')

    @classmethod
    def key_from_path(cls, path):
        return ''.join(path)

    def test_to_string(self):
        self._assertToString(self._TRIE_CTOR(), '()', '([])')
        self._assertToString(self._TRIE_CTOR({self._SHORT_KEY: 42}),
                             '(foo: 42)', "([('foo', 42)])")
        self._assertToString(
            self._TRIE_CTOR({self._SHORT_KEY: 42, self._OTHER_KEY: '42'}),
            '(foo: 42, qux: 42)', "([('foo', 42), ('qux', '42')])")

    def test_prefix_set_pickling_proto0(self):
        pickled = (
            'Y2NvcHlfcmVnCl9yZWNvbnN0cnVjdG9yCnAwCihjcHlndHJpZQpQcmVmaXhTZXQKcD'
            'EKY19fYnVpbHRpbl9fCm9iamVjdApwMgpOdHAzClJwNAooZHA1ClZfdHJpZQpwNgpn'
            'MAooY3B5Z3RyaWUKQ2hhclRyaWUKcDcKZzIKTnRwOApScDkKKGRwMTAKVl9yb290Cn'
            'AxMQpnMAooY3B5Z3RyaWUKX05vZGUKcDEyCmcyCk50cDEzClJwMTQKKGxwMTUKTDNM'
            'CmFWZgpwMTYKYVZvCnAxNwphZzE3CmFJMDEKYUwtM0wKYUwzTAphVmIKcDE4CmFWYQ'
            'pwMTkKYVZyCnAyMAphSTAxCmFMLTFMCmFMMUwKYVZ6CnAyMQphSTAxCmFic1Zfc29y'
            'dGVkCnAyMgpJMDAKc2JzYi4=')
        want = pygtrie.PrefixSet(('foo', 'bar', 'baz'),
                                 factory=pygtrie.CharTrie)
        self.assertUnpickling(want, pickled)

    @unittest.skipIf(sys.version_info[0] < 3, "Protocol 3 requires Python 3+")
    def test_prefix_set_pickling_proto3(self):
        pickled = (
            'gANjcHlndHJpZQpQcmVmaXhTZXQKcQApgXEBfXECWAUAAABfdHJpZXEDY3B5Z3RyaW'
            'UKQ2hhclRyaWUKcQQpgXEFfXEGKFgFAAAAX3Jvb3RxB2NweWd0cmllCl9Ob2RlCnEI'
            'KYFxCV1xCihLA1gBAAAAZnELWAEAAABvcQxoDIhK/f///0sDWAEAAABicQ1YAQAAAG'
            'FxDlgBAAAAcnEPiEr/////SwFYAQAAAHpxEIhlYlgHAAAAX3NvcnRlZHERiXVic2Iu'
        )
        want = pygtrie.PrefixSet(('foo', 'bar', 'baz'),
                                 factory=pygtrie.CharTrie)
        self.assertUnpickling(want, pickled)

    def test_step_repr(self):
        t = self._TRIE_CTOR({'foo': 42, 'foobar': 64})
        self.assertEqual("('foo': 42)", repr(t.shortest_prefix('foobarbaz')))
        self.assertEqual("('foobar': 64)", repr(t.longest_prefix('foobarbaz')))
        self.assertEqual("(None Step)", repr(t.longest_prefix('qux')))


class StringTrieTestCase(TrieTestCase):
    _TRIE_CTOR = staticmethod(
        lambda *args, **kw: pygtrie.StringTrie(*args, separator='~', **kw))  # pylint: disable=unnecessary-lambda

    _SHORT_KEY = '~home~foo'
    _SHORT_KEY2 = '~home~FOO'
    _LONG_KEY = _SHORT_KEY + '~bar~baz'
    _VERY_LONG_KEY = _LONG_KEY + '~qux'
    _OTHER_KEY = '~hom'
    _SHORT_PREFIXES = ('', '~home')
    _LONG_PREFIXES = ('~home~foo~bar',)

    _PICKLED_PROTO_0 = (
        'Y2NvcHlfcmVnCl9yZWNvbnN0cnVjdG9yCnAwCihjcHlndHJpZQpTdHJpbmdUcmllCnAxCm'
        'NfX2J1aWx0aW5fXwpvYmplY3QKcDIKTnRwMwpScDQKKGRwNQpWX3NlcGFyYXRvcgpwNgpW'
        'fgpwNwpzVl9yb290CnA4CmcwCihjcHlndHJpZQpfTm9kZQpwOQpnMgpOdHAxMApScDExCi'
        'hscDEyCkkxCmFWZm9vCnAxMwphSTQyCmFJLTEKYUkxCmFWYmFyCnAxNAphSTQyCmFJLTEK'
        'YUkxCmFWYmF6CnAxNQphSTQyCmFic1Zfc29ydGVkCnAxNgpJMDAKc2Iu')

    _PICKLED_PROTO_3 = (
        'gANjcHlndHJpZQpTdHJpbmdUcmllCnEAKYFxAX1xAihYCgAAAF9zZXBhcmF0b3JxA1gBAA'
        'AAfnEEWAUAAABfcm9vdHEFY3B5Z3RyaWUKX05vZGUKcQYpgXEHXXEIKEsBWAMAAABmb29x'
        'CUsqSv////9LAVgDAAAAYmFycQpLKkr/////SwFYAwAAAGJhenELSyplYlgHAAAAX3Nvcn'
        'RlZHEMiXViLg==')

    @classmethod
    def path_from_key(cls, key):
        return key.split('~')

    @classmethod
    def key_from_path(cls, path):
        return '~'.join(path)

    def test_valid_separator(self):
        t = pygtrie.StringTrie()
        t['foo/bar'] = 42
        self.assertTrue(bool(t.has_node('foo') & pygtrie.Trie.HAS_SUBTRIE))

        t = pygtrie.StringTrie(separator='.')
        t['foo.bar'] = 42
        self.assertTrue(bool(t.has_node('foo') & pygtrie.Trie.HAS_SUBTRIE))

    def test_invalid_separator(self):
        self.assertRaises(TypeError, pygtrie.StringTrie, separator=42)
        self.assertRaises(ValueError, pygtrie.StringTrie, separator='')

    def test_to_string(self):
        self._assertToString(pygtrie.StringTrie(),
                             '(separator=/)',
                             "([], separator='/')")
        self._assertToString(self._TRIE_CTOR(),
                             '(separator=~)',
                             "([], separator='~')")
        self._assertToString(
            self._TRIE_CTOR({self._SHORT_KEY: 42}),
            '(~home~foo: 42, separator=~)',
            "([('~home~foo', 42)], separator='~')")
        self._assertToString(
            self._TRIE_CTOR({self._SHORT_KEY: 42, self._OTHER_KEY: '42'}),
            '(~hom: 42, ~home~foo: 42, separator=~)',
            "([('~hom', '42'), ('~home~foo', 42)], separator='~')")


class SortTest(unittest.TestCase):
    def _do_test_enable_sorting(self, cls, keys):
        keys = sorted(keys)
        # In Python 3 keys are returned in insertion order so we reverse the
        # insertion here.
        t = cls.fromkeys(reversed(keys))

        # Unless dict's hash function is weird, trie's keys should not be
        # returned in order.
        self.assertNotEqual(keys, t.keys())
        self.assertEqual(keys, sorted(t.keys()))

        t.enable_sorting()
        self.assertEqual(keys, t.keys())

        t.enable_sorting(False)
        self.assertNotEqual(keys, t.keys())

    def _do_test_copy_preserve_sorting(self, cls, keys):
        t = cls()
        t.enable_sorting()
        t = copy.copy(t)
        for k in reversed(keys):
            t[k] = k
        self.assertEqual(keys, t.keys())

    _CHAR_TRIE_KEYS = sorted(chr(x) for x in range(32, 127))
    _STR_TRIE_KEYS = ['/' + x for x in _CHAR_TRIE_KEYS if x != '/']

    # pylint: disable=invalid-name
    def test_CharTrie_enable_sorting(self):
        self._do_test_enable_sorting(pygtrie.CharTrie, self._CHAR_TRIE_KEYS)

    def test_StringTrie_enable_sorting(self):
        self._do_test_enable_sorting(pygtrie.StringTrie, self._STR_TRIE_KEYS)


    def test_CharTrie_copy_preserves_sorting(self):
        self._do_test_copy_preserve_sorting(pygtrie.CharTrie,
                                            self._CHAR_TRIE_KEYS)

    def test_StringTrie_copy_preserves_sorting(self):
        self._do_test_copy_preserve_sorting(pygtrie.StringTrie,
                                            self._STR_TRIE_KEYS)


@_TrieFactoryParameteriser()
class TraverseTest(unittest.TestCase):
    _SENTINEL = object()
    _TestNode = collections.namedtuple('TestNode',
                                       'key has_children children value')

    @classmethod
    def _make_test_node(cls, path_conv, path, children, value=_SENTINEL):
        has_children = bool(children)
        return cls._TestNode(
            path_conv(path), has_children, sorted(children), value)

    def assertNode(self, node, key, children=0, value=_SENTINEL):  # pylint: disable=invalid-name
        self.assertTrue(node)
        self.assertEqual(key, node.key)
        self.assertEqual(bool(children), node.has_children)
        self.assertEqual(children, len(node.children))
        self.assertEqual(value, node.value)
        return node

    def _do_test_traverse_empty_tree(self, trie_factory):
        self.assertTrue(pygtrie.Trie.traverse.uses_bool_convertible_children)
        self.assertTrue(getattr(pygtrie.Trie.traverse,
                                'uses_bool_convertible_children', None))

        t = trie_factory(pygtrie.CharTrie, {})
        r = t.traverse(self._make_test_node)
        self.assertNode(r, '', 0)

    def _do_test_traverse_singleton_tree(self, trie_factory):
        t = trie_factory(pygtrie.CharTrie, {'a': 10})
        r = t.traverse(self._make_test_node)
        self.assertNode(r, '', 1)
        self.assertNode(r.children[0], 'a', 0, 10)

    def _do_test_traverse(self, trie_factory):
        t = trie_factory(pygtrie.CharTrie,
                         {'aaa': 1, 'aab': 2, 'aac': 3, 'bb': 4})

        r = t.traverse(self._make_test_node)
        # Result:
        #  <>
        #    a
        #      aa
        #        aaa:1
        #        aab:2
        #        aac:3
        #    b
        #      bb:4
        self.assertNode(r, '', 2)

        # For some reason pylint thinks a_node et al. are strings.
        # pylint: disable=no-member

        a_node = self.assertNode(r.children[0], 'a', 1)
        aa_node = self.assertNode(a_node.children[0], 'aa', 3)
        self.assertNode(aa_node.children[0], 'aaa', 0, 1)
        self.assertNode(aa_node.children[2], 'aac', 0, 3)

        b_node = self.assertNode(r.children[1], 'b', 1)
        self.assertNode(b_node.children[0], 'bb', 0, 4)

    def _do_test_traverse_compressing(self, trie_factory):
        t = trie_factory(pygtrie.CharTrie,
                         {'aaa': 1, 'aab': 2, 'aac': 3, 'bb': 4})

        def make(path_conv, path, children, value=self._SENTINEL):
            children = sorted(children)
            if value is self._SENTINEL and len(children) == 1:
                # There is only one prefix.
                return children[0]
            return self._TestNode(
                path_conv(path), bool(children), children, value)

        r = t.traverse(make)
        # Result:
        # <>
        #  aa
        #    aaa:1
        #    aab:2
        #    aac:3
        #  bb:4
        self.assertNode(r, '', 2)

        # For some reason pylint thinks a_node et al. are strings.
        # pylint: disable=no-member

        aa_node = self.assertNode(r.children[0], 'aa', 3)
        self.assertNode(aa_node.children[0], 'aaa', 0, 1)
        self.assertNode(aa_node.children[1], 'aab', 0, 2)
        self.assertNode(aa_node.children[2], 'aac', 0, 3)

        self.assertNode(r.children[1], 'bb', 0, 4)

    def _do_test_traverse_ignore_subtrie(self, trie_factory):
        t = trie_factory(pygtrie.CharTrie,
                         {'aaa': 1, 'aab': 2, 'aac': 3, 'b': 4})

        cnt = [0]

        def make(path_conv, path, children, value=self._SENTINEL):
            cnt[0] += 1
            if path and path[0] == 'a':
                return None
            children = [ch for ch in children if ch is not None]
            return self._TestNode(
                path_conv(path), bool(children), children, value)

        r = t.traverse(make)
        # Result:
        # <>
        #  b:4
        self.assertNode(r, '', 1)
        self.assertNode(r.children[0], 'b', 0, 4)
        self.assertEqual(3, cnt[0])


class RecursionTest(unittest.TestCase):
    """Test for deep recursion.

    https://github.com/google/pygtrie/issues/8
    """

    # This code is taken from traverse docstring
    @classmethod
    def _undirected_graph_from_trie(cls, trie):
        """Converts trie into a graph and returns its nodes."""
        Node = collections.namedtuple('Node', 'label neighbours')  # pylint: disable=invalid-name

        class Builder(object):
            def __init__(self, path_conv, path, children, _=None):
                self.node = Node(path_conv(path), [])
                self.children = children
                self.parent = None

            def build(self, queue):
                for builder in self.children:
                    builder.parent = self.node
                    queue.append(builder)
                if self.parent:
                    self.parent.neighbours.append(self.node)
                    self.node.neighbours.append(self.parent)
                return self.node

        nodes = [trie.traverse(Builder)]
        i = 0
        while i < len(nodes):
            nodes[i] = nodes[i].build(nodes)
            i += 1
        return nodes

    def test_traverse_small(self):
        """Test code that is shown in traverse's docstring; test correctness."""
        t = pygtrie.StringTrie()
        t['foo/bar/baz'] = True
        t['foo/qux/baz'] = True
        t['bar/baz/foo'] = True

        nodes = self._undirected_graph_from_trie(t)
        got = sorted((f.label, t.label) for f in nodes for t in f.neighbours)
        self.assertEqual([('', 'bar'),
                          ('', 'foo'),
                          ('bar', ''),
                          ('bar', 'bar/baz'),
                          ('bar/baz', 'bar'),
                          ('bar/baz', 'bar/baz/foo'),
                          ('bar/baz/foo', 'bar/baz'),
                          ('foo', ''),
                          ('foo', 'foo/bar'),
                          ('foo', 'foo/qux'),
                          ('foo/bar', 'foo'),
                          ('foo/bar', 'foo/bar/baz'),
                          ('foo/bar/baz', 'foo/bar'),
                          ('foo/qux', 'foo'),
                          ('foo/qux', 'foo/qux/baz'),
                          ('foo/qux/baz', 'foo/qux')], got)

    def test_large_trie(self):
        """Test handling of large tries which would overflow stack."""
        tostring = (getattr(array.array, 'tobytes', None) or # Python 3
                    getattr(array.array, 'tostring'))  # Python 3

        trie = pygtrie.Trie()
        for x in range(100):
            y = tostring(array.array('h', range(x, 1000)))
            trie[y] = x

        # Plain iteration
        n = 0
        for _ in trie.iteritems():
            n += 1
        self.assertEqual(100, n)

        # Copy
        self.assertEqual(trie, copy.copy(trie))
        self.assertEqual(trie, copy.deepcopy(trie))

        # This takes more time to run than the rest of the unit tests so disable
        # this.  We're already testing _undirected_graph_from_trie so additional
        # testing should not be necessary.
        #nodes = self._undirected_graph_from_trie(trie)
        #self.assertEqual((100 * 1000 - (100 * 99) / 2) * 2 + 1, len(nodes))


class EqualityTest(unittest.TestCase):
    """Tests for equality comparisons."""

    def test_trie_ne_char_trie(self):
        """Check that Trie and CharTrie are ne even if they have the same nodes.

        Trie returns keys as tuples while CharTrie returns them as strings.
        Because of that, even if they have the same nodes, they will produce
        different (key, value) pairs.
        """
        trie = pygtrie.Trie([('foo', 42)])
        char_trie = pygtrie.CharTrie([('foo', 42)])
        # pylint: disable=protected-access
        self.assertTrue(trie._root.equals(char_trie._root))
        self.assertNotEqual(trie, char_trie)
        self.assertFalse(trie.strictly_equals(char_trie))

    def test_strieng_trie_eq(self):
        """Test that StringTrie takes separator into consideration."""
        trie_slash = pygtrie.StringTrie([('foo/bar', 42)])
        trie_dot0 = pygtrie.StringTrie([('foo.bar', 42)], separator='.')
        trie_dot1 = pygtrie.StringTrie([('foo.bar', 42)], separator='.')
        self.assertEqual(trie_slash, trie_slash)
        self.assertTrue(trie_slash.strictly_equals(trie_slash))
        # pylint: disable=protected-access
        self.assertTrue(trie_slash._root.equals(trie_dot0._root))
        self.assertNotEqual(trie_slash, trie_dot0)
        self.assertFalse(trie_slash.strictly_equals(trie_dot0))
        self.assertEqual(trie_dot0, trie_dot1)
        self.assertTrue(trie_dot0.strictly_equals(trie_dot1))

    def test_mapping_eq(self):
        """Test comparison with non-Trie mapping types."""
        # pylint: disable=import-outside-toplevel
        try:
            from collections import abc
        except ImportError:  # Python 2 compatibility
            abc = collections

        class Mapping(abc.Mapping):
            def __getitem__(self, key):
                if key == 'foo/bar.baz':
                    return 42
                raise KeyError(key)

            def __iter__(self):
                return iter(('foo/bar.baz',))

            def __len__(self):
                return 1

        trie_slash = pygtrie.StringTrie([('foo/bar.baz', 42)], separator='/')
        trie_dot = pygtrie.StringTrie([('foo/bar.baz', 42)], separator='.')
        char_trie = pygtrie.CharTrie([('foo/bar.baz', 42)])
        dictionary = {'foo/bar.baz': 42}
        mapping = Mapping()

        all_tries = (trie_slash, trie_dot, char_trie)
        for a in all_tries:
            for b in all_tries:
                self.assertEqual(a is b, a.strictly_equals(b))

        all_mappings = (trie_slash, trie_dot, char_trie, dictionary, mapping)
        for a in all_mappings:
            for b in all_mappings:
                self.assertEqual(a, b)

        dictionary['other-key'] = 24
        all_tries = (trie_slash, trie_dot, char_trie)
        for trie in all_tries:
            self.assertNotEqual(dictionary, trie)
            self.assertNotEqual(trie, dictionary)


class MergeTest(unittest.TestCase):
    """Tests for merge method."""

    def test_merge_tries(self):
        trie = pygtrie.Trie({'foo': 1, 'bar': 2})

        def test(want, src, overwrite=False):
            trie.merge(src, overwrite=overwrite)
            self.assertEqual(0, len(src))
            self.assertEqual(pygtrie.Trie(want), trie)

        test({'foo': 1, 'bar': 2, 'baz': 3},
             pygtrie.Trie({'bar': 0, 'baz': 3}))
        test({'foo': 1, 'bar': 2, 'baz': 4, 'fo': 5},
             pygtrie.Trie({'baz': 4, 'fo': 5}), overwrite=True)
        test({'foo': 1, 'bar': 2, 'baz': 4, 'fo': 5, 'qux': 6},
             pygtrie.CharTrie({'qux': 6}))

        st = pygtrie.StringTrie({'foo/bar/baz': 42})
        self.assertRaises(TypeError, trie.merge, st)

    def test_merge_string_tries(self):

        def test(want, other, overwrite):
            trie = pygtrie.StringTrie({'foo/bar': 42})
            trie.merge(other, overwrite=overwrite)
            self.assertEqual(want, dict(trie.items()))

        test({'foo/bar': 42, 'bar/baz': 2},
             pygtrie.StringTrie({'foo.bar': 4, 'bar.baz': 2}, separator='.'),
             False)
        test({'foo/bar': 4, 'bar/baz': 2},
             pygtrie.StringTrie({'foo.bar': 4, 'bar.baz': 2}, separator='.'),
             True)
        test({'foo/bar': 42, 'q/u/x': 2}, pygtrie.Trie({'qux': 2}), False)
        test({'foo/bar': 42, 'q/u/x': 2}, pygtrie.CharTrie({'qux': 2}), False)


if __name__ == '__main__':
    unittest.main()
