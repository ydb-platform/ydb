##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
"""Test of the Vocabulary and related support APIs.
"""
import unittest


class SimpleTermTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema.vocabulary import SimpleTerm
        return SimpleTerm

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def test_class_conforms_to_ITokenizedTerm(self):
        from zope.interface.verify import verifyClass

        from zope.schema.interfaces import ITokenizedTerm
        verifyClass(ITokenizedTerm, self._getTargetClass())

    def test_instance_conforms_to_ITokenizedTerm(self):
        from zope.interface.verify import verifyObject

        from zope.schema.interfaces import ITokenizedTerm
        verifyObject(ITokenizedTerm, self._makeOne('VALUE'))

    def test_ctor_defaults(self):
        from zope.schema.interfaces import ITitledTokenizedTerm
        term = self._makeOne('VALUE')
        self.assertEqual(term.value, 'VALUE')
        self.assertEqual(term.token, 'VALUE')
        self.assertEqual(term.title, None)
        self.assertFalse(ITitledTokenizedTerm.providedBy(term))

    def test_ctor_explicit(self):
        from zope.schema.interfaces import ITitledTokenizedTerm
        term = self._makeOne('TERM', 'TOKEN', 'TITLE')
        self.assertEqual(term.value, 'TERM')
        self.assertEqual(term.token, 'TOKEN')
        self.assertEqual(term.title, 'TITLE')
        self.assertTrue(ITitledTokenizedTerm.providedBy(term))

    def test_bytes_value(self):
        from zope.schema.interfaces import ITitledTokenizedTerm
        term = self._makeOne(b'term')
        self.assertEqual(term.value, b'term')
        self.assertEqual(term.token, 'term')
        self.assertFalse(ITitledTokenizedTerm.providedBy(term))

    def test_bytes_non_ascii_value(self):
        from zope.schema.interfaces import ITitledTokenizedTerm
        term = self._makeOne(b'Snowman \xe2\x98\x83')
        self.assertEqual(term.value, b'Snowman \xe2\x98\x83')
        self.assertEqual(term.token, 'Snowman \\xe2\\x98\\x83')
        self.assertFalse(ITitledTokenizedTerm.providedBy(term))

    def test_unicode_non_ascii_value(self):
        from zope.schema.interfaces import ITitledTokenizedTerm
        term = self._makeOne('Snowman \u2603')
        self.assertEqual(term.value, 'Snowman \u2603')
        self.assertEqual(term.token, 'Snowman \\u2603')
        self.assertFalse(ITitledTokenizedTerm.providedBy(term))

    def test__eq__and__hash__(self):
        term = self._makeOne('value')
        # Equal to itself
        self.assertEqual(term, term)
        # Not equal to a different class
        self.assertNotEqual(term, object())
        self.assertNotEqual(object(), term)

        term2 = self._makeOne('value')
        # Equal to another with the same value
        self.assertEqual(term, term2)
        # equal objects hash the same
        self.assertEqual(hash(term), hash(term2))

        # Providing tokens or titles that differ
        # changes equality
        term = self._makeOne('value', 'token')
        self.assertNotEqual(term, term2)
        self.assertNotEqual(hash(term), hash(term2))

        term2 = self._makeOne('value', 'token')
        self.assertEqual(term, term2)
        self.assertEqual(hash(term), hash(term2))

        term = self._makeOne('value', 'token', 'title')
        self.assertNotEqual(term, term2)
        self.assertNotEqual(hash(term), hash(term2))

        term2 = self._makeOne('value', 'token', 'title')
        self.assertEqual(term, term2)
        self.assertEqual(hash(term), hash(term2))


class SimpleVocabularyTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema.vocabulary import SimpleVocabulary
        return SimpleVocabulary

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def test_class_conforms_to_IVocabularyTokenized(self):
        from zope.interface.verify import verifyClass

        from zope.schema.interfaces import IVocabularyTokenized
        verifyClass(IVocabularyTokenized, self._getTargetClass())

    def test_instance_conforms_to_IVocabularyTokenized(self):
        from zope.interface.verify import verifyObject

        from zope.schema.interfaces import IVocabularyTokenized
        verifyObject(IVocabularyTokenized, self._makeOne(()))

    def test_ctor_additional_interfaces(self):
        from zope.interface import Interface

        from zope.schema.vocabulary import SimpleTerm

        class IStupid(Interface):
            pass

        VALUES = [1, 4, 2, 9]
        vocabulary = self._makeOne([SimpleTerm(x) for x in VALUES], IStupid)
        self.assertTrue(IStupid.providedBy(vocabulary))
        self.assertEqual(len(vocabulary), len(VALUES))
        for value, term in zip(VALUES, vocabulary):
            self.assertEqual(term.value, value)
        for value in VALUES:
            self.assertIn(value, vocabulary)
        self.assertNotIn('ABC', vocabulary)
        for term in vocabulary:
            self.assertIs(vocabulary.getTerm(term.value), term)
            self.assertIs(vocabulary.getTermByToken(term.token), term)

    def test_fromValues(self):
        from zope.interface import Interface

        from zope.schema.interfaces import ITokenizedTerm

        class IStupid(Interface):
            pass

        VALUES = [1, 4, 2, 9]
        vocabulary = self._getTargetClass().fromValues(VALUES)
        self.assertEqual(len(vocabulary), len(VALUES))
        for value, term in zip(VALUES, vocabulary):
            self.assertTrue(ITokenizedTerm.providedBy(term))
            self.assertEqual(term.value, value)
        for value in VALUES:
            self.assertIn(value, vocabulary)

    def test_fromItems(self):
        from zope.interface import Interface

        from zope.schema.interfaces import ITokenizedTerm

        class IStupid(Interface):
            pass

        ITEMS = [('one', 1), ('two', 2), ('three', 3), ('fore!', 4)]
        vocabulary = self._getTargetClass().fromItems(ITEMS)
        self.assertEqual(len(vocabulary), len(ITEMS))
        for item, term in zip(ITEMS, vocabulary):
            self.assertTrue(ITokenizedTerm.providedBy(term))
            self.assertEqual(term.token, item[0])
            self.assertEqual(term.value, item[1])
        for item in ITEMS:
            self.assertIn(item[1], vocabulary)

    def test_fromItems_triples(self):
        from zope.interface import Interface

        from zope.schema.interfaces import ITitledTokenizedTerm

        class IStupid(Interface):
            pass

        ITEMS = [
            ('one', 1, 'title 1'),
            ('two', 2, 'title 2'),
            ('three', 3, 'title 3'),
            ('fore!', 4, 'title four')
        ]
        vocabulary = self._getTargetClass().fromItems(ITEMS)
        self.assertEqual(len(vocabulary), len(ITEMS))
        for item, term in zip(ITEMS, vocabulary):
            self.assertTrue(ITitledTokenizedTerm.providedBy(term))
            self.assertEqual(term.token, item[0])
            self.assertEqual(term.value, item[1])
            self.assertEqual(term.title, item[2])
        for item in ITEMS:
            self.assertIn(item[1], vocabulary)

    def test_createTerm(self):
        from zope.schema.vocabulary import SimpleTerm
        VALUES = [1, 4, 2, 9]
        for value in VALUES:
            term = self._getTargetClass().createTerm(value)
            self.assertIsInstance(term, SimpleTerm)
            self.assertEqual(term.value, value)
            self.assertEqual(term.token, str(value))

    def test_getTerm_miss(self):
        vocabulary = self._makeOne(())
        self.assertRaises(LookupError, vocabulary.getTerm, 'nonesuch')

    def test_getTermByToken_miss(self):
        vocabulary = self._makeOne(())
        self.assertRaises(LookupError, vocabulary.getTermByToken, 'nonesuch')

    def test_nonunique_tokens(self):
        klass = self._getTargetClass()
        self.assertRaises(ValueError, klass.fromValues, [2, '2'])
        self.assertRaises(
            ValueError,
            klass.fromItems,
            [(1, 'one'), ('1', 'another one')]
        )
        self.assertRaises(
            ValueError,
            klass.fromItems,
            [(0, 'one'), (1, 'one')]
        )

    def test_nonunique_tokens_swallow(self):
        klass = self._getTargetClass()
        items = [(0, 'one'), (1, 'one')]
        terms = [klass.createTerm(value, token) for (token, value) in items]
        vocab = self._getTargetClass()(terms, swallow_duplicates=True)
        self.assertEqual(vocab.getTerm('one').token, '1')

    def test_nonunique_token_message(self):
        try:
            self._getTargetClass().fromValues([2, '2'])
        except ValueError as e:
            self.assertEqual(str(e), "term tokens must be unique: '2'")

    def test_nonunique_token_messages(self):
        try:
            self._getTargetClass().fromItems([(0, 'one'), (1, 'one')])
        except ValueError as e:
            self.assertEqual(str(e), "term values must be unique: 'one'")

    def test_overriding_createTerm(self):
        class MyTerm:
            def __init__(self, value):
                self.value = value
                self.token = repr(value)
                self.nextvalue = value + 1

        class MyVocabulary(self._getTargetClass()):
            def createTerm(cls, value):
                return MyTerm(value)
            createTerm = classmethod(createTerm)

        vocab = MyVocabulary.fromValues([1, 2, 3])
        for term in vocab:
            self.assertEqual(term.value + 1, term.nextvalue)

    def test__eq__and__hash__(self):
        from zope import interface

        values = [1, 4, 2, 9]
        vocabulary = self._getTargetClass().fromValues(values)

        # Equal to itself
        self.assertEqual(vocabulary, vocabulary)
        # Not to other classes
        self.assertNotEqual(vocabulary, object())
        self.assertNotEqual(object(), vocabulary)

        # Equal to another object with the same values
        vocabulary2 = self._getTargetClass().fromValues(values)
        self.assertEqual(vocabulary, vocabulary2)
        self.assertEqual(hash(vocabulary), hash(vocabulary2))

        # Changing the values or the interfaces changes
        # equality
        class IFoo(interface.Interface):
            "an interface"

        vocabulary = self._getTargetClass().fromValues(values, IFoo)
        self.assertNotEqual(vocabulary, vocabulary2)
        # Interfaces are not taken into account in the hash; that's
        # OK: equal hashes do not imply equal objects
        self.assertEqual(hash(vocabulary), hash(vocabulary2))

        vocabulary2 = self._getTargetClass().fromValues(values, IFoo)
        self.assertEqual(vocabulary, vocabulary2)
        self.assertEqual(hash(vocabulary), hash(vocabulary2))


# Test _createTermTree via TreeVocabulary.fromDict


class TreeVocabularyTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema.vocabulary import TreeVocabulary
        return TreeVocabulary

    def tree_vocab_2(self):
        region_tree = {
            ('regions', 'Regions'): {
                ('aut', 'Austria'): {
                    ('tyr', 'Tyrol'): {
                        ('auss', 'Ausserfern'): {},
                    }
                },
                ('ger', 'Germany'): {
                    ('bav', 'Bavaria'): {}
                },
            }
        }
        return self._getTargetClass().fromDict(region_tree)

    def business_tree(self):
        return {
            ('services', 'services', 'Services'): {
                ('reservations', 'reservations', 'Reservations'): {
                    ('res_host', 'res_host', 'Res Host'): {},
                    ('res_gui', 'res_gui', 'Res GUI'): {},
                },
                ('check_in', 'check_in', 'Check-in'): {
                    ('dcs_host', 'dcs_host', 'DCS Host'): {},
                },
            },
            ('infrastructure', 'infrastructure', 'Infrastructure'): {
                ('communication_network', 'communication_network',
                 'Communication/Network'): {
                    ('messaging', 'messaging', 'Messaging'): {},
                },
                ('data_transaction', 'data_transaction',
                 'Data/Transaction'): {
                    ('database', 'database', 'Database'): {},
                },
                ('security', 'security', 'Security'): {},
            },
        }

    def tree_vocab_3(self):
        return self._getTargetClass().fromDict(self.business_tree())

    def test_only_titled_if_triples(self):
        from zope.schema.interfaces import ITitledTokenizedTerm
        no_titles = self.tree_vocab_2()
        for term in no_titles:
            self.assertIsNone(term.title)
            self.assertFalse(ITitledTokenizedTerm.providedBy(term))

        all_titles = self.tree_vocab_3()
        for term in all_titles:
            self.assertIsNotNone(term.title)
            self.assertTrue(ITitledTokenizedTerm.providedBy(term))

    def test_implementation(self):
        from zope.interface.common.mapping import IEnumerableMapping
        from zope.interface.verify import verifyObject

        from zope.schema.interfaces import ITreeVocabulary
        from zope.schema.interfaces import IVocabulary
        from zope.schema.interfaces import IVocabularyTokenized
        for v in [self.tree_vocab_2(), self.tree_vocab_3()]:
            self.assertTrue(verifyObject(IEnumerableMapping, v))
            self.assertTrue(verifyObject(IVocabulary, v))
            self.assertTrue(verifyObject(IVocabularyTokenized, v))
            self.assertTrue(verifyObject(ITreeVocabulary, v))

    def test_additional_interfaces(self):
        from zope.interface import Interface

        class IStupid(Interface):
            pass

        v = self._getTargetClass().fromDict({('one', '1'): {}}, IStupid)
        self.assertTrue(IStupid.providedBy(v))

    def test_ordering(self):
        # The TreeVocabulary makes use of an OrderedDict to store its
        # internal tree representation.
        #
        # Check that the keys are indeed ordered.
        from collections import OrderedDict

        d = {
            (1, 'new_york', 'New York'): {
                (2, 'ny_albany', 'Albany'): {},
                (3, 'ny_new_york', 'New York'): {},
            },
            (4, 'california', 'California'): {
                (5, 'ca_los_angeles', 'Los Angeles'): {},
                (6, 'ca_san_francisco', 'San Francisco'): {},
            },
            (7, 'texas', 'Texas'): {},
            (8, 'florida', 'Florida'): {},
            (9, 'utah', 'Utah'): {},
        }
        dict_ = OrderedDict(sorted(d.items(), key=lambda t: t[0]))
        vocab = self._getTargetClass().fromDict(dict_)
        # Test keys
        self.assertEqual(
            [k.token for k in vocab.keys()],
            ['1', '4', '7', '8', '9']
        )
        # Test __iter__
        self.assertEqual(
            [k.token for k in vocab],
            ['1', '4', '7', '8', '9']
        )

        self.assertEqual(
            [k.token for k in vocab[[k for k in vocab.keys()][0]].keys()],
            ['2', '3']
        )
        self.assertEqual(
            [k.token for k in vocab[[k for k in vocab.keys()][1]].keys()],
            ['5', '6']
        )

    def test_indexes(self):
        # TreeVocabulary creates three indexes for quick lookups,
        # term_by_value, term_by_value and path_by_value.
        tv2 = self.tree_vocab_2()
        self.assertEqual(
            [k for k in sorted(tv2.term_by_value.keys())],
            ['Ausserfern', 'Austria', 'Bavaria', 'Germany', 'Regions', 'Tyrol']
        )

        self.assertEqual(
            [k for k in sorted(tv2.term_by_token.keys())],
            ['auss', 'aut', 'bav', 'ger', 'regions', 'tyr']
        )

        self.assertEqual(
            [k for k in sorted(tv2.path_by_value.keys())],
            ['Ausserfern', 'Austria', 'Bavaria', 'Germany', 'Regions', 'Tyrol']
        )

        self.assertEqual(
            [k for k in sorted(tv2.path_by_value.values())],
            [
                ['Regions'],
                ['Regions', 'Austria'],
                ['Regions', 'Austria', 'Tyrol'],
                ['Regions', 'Austria', 'Tyrol', 'Ausserfern'],
                ['Regions', 'Germany'],
                ['Regions', 'Germany', 'Bavaria'],
            ]
        )

        self.assertEqual(
            [k for k in sorted(self.tree_vocab_3().term_by_value.keys())],
            [
                'check_in',
                'communication_network',
                'data_transaction',
                'database',
                'dcs_host',
                'infrastructure',
                'messaging',
                'res_gui',
                'res_host',
                'reservations',
                'security',
                'services',
            ]
        )

        self.assertEqual(
            [k for k in sorted(self.tree_vocab_3().term_by_token.keys())],
            [
                'check_in',
                'communication_network',
                'data_transaction',
                'database',
                'dcs_host',
                'infrastructure',
                'messaging',
                'res_gui',
                'res_host',
                'reservations',
                'security',
                'services',
            ]
        )

        self.assertEqual(
            [k for k in sorted(self.tree_vocab_3().path_by_value.values())],
            [
                ['infrastructure'],
                ['infrastructure', 'communication_network'],
                ['infrastructure', 'communication_network', 'messaging'],
                ['infrastructure', 'data_transaction'],
                ['infrastructure', 'data_transaction', 'database'],
                ['infrastructure', 'security'],
                ['services'],
                ['services', 'check_in'],
                ['services', 'check_in', 'dcs_host'],
                ['services', 'reservations'],
                ['services', 'reservations', 'res_gui'],
                ['services', 'reservations', 'res_host'],
            ]
        )

    def test_termpath(self):
        tv2 = self.tree_vocab_2()
        tv3 = self.tree_vocab_3()
        self.assertEqual(
            tv2.getTermPath('Bavaria'),
            ['Regions', 'Germany', 'Bavaria']
        )
        self.assertEqual(
            tv2.getTermPath('Austria'),
            ['Regions', 'Austria']
        )
        self.assertEqual(
            tv2.getTermPath('Ausserfern'),
            ['Regions', 'Austria', 'Tyrol', 'Ausserfern']
        )
        self.assertEqual(
            tv2.getTermPath('Non-existent'),
            []
        )
        self.assertEqual(
            tv3.getTermPath('database'),
            ["infrastructure", "data_transaction", "database"]
        )

    def test_len(self):
        # len returns the number of all nodes in the dict
        self.assertEqual(len(self.tree_vocab_2()), 1)
        self.assertEqual(len(self.tree_vocab_3()), 2)

    def test_contains(self):
        tv2 = self.tree_vocab_2()
        self.assertTrue('Regions' in tv2 and
                        'Austria' in tv2 and
                        'Bavaria' in tv2)

        self.assertNotIn('bav', tv2)
        self.assertNotIn('foo', tv2)
        self.assertNotIn({}, tv2)  # not hashable

        tv3 = self.tree_vocab_3()
        self.assertTrue('database' in tv3 and
                        'security' in tv3 and
                        'services' in tv3)

        self.assertNotIn('Services', tv3)
        self.assertNotIn('Database', tv3)
        self.assertNotIn({}, tv3)  # not hashable

    def test_values_and_items(self):
        for v in (self.tree_vocab_2(), self.tree_vocab_3()):
            for term in v:
                self.assertEqual([i for i in v.values()],
                                 [i for i in v._terms.values()])
                self.assertEqual([i for i in v.items()],
                                 [i for i in v._terms.items()])

    def test_get(self):
        for v in [self.tree_vocab_2(), self.tree_vocab_3()]:
            for key, value in v.items():
                self.assertEqual(v.get(key), value)
                self.assertEqual(v[key], value)

    def test_get_term(self):
        for v in (self.tree_vocab_2(), self.tree_vocab_3()):
            for term in v:
                self.assertIs(v.getTerm(term.value), term)
                self.assertIs(v.getTermByToken(term.token), term)
            self.assertRaises(LookupError, v.getTerm, 'non-present-value')
            self.assertRaises(LookupError,
                              v.getTermByToken, 'non-present-token')

    def test_nonunique_values_and_tokens(self):
        # Since we do term and value lookups, all terms' values and tokens
        # must be unique. This rule applies recursively.
        self.assertRaises(
            ValueError, self._getTargetClass().fromDict,
            {
                ('one', '1'): {},
                ('two', '1'): {},
            })
        self.assertRaises(
            ValueError, self._getTargetClass().fromDict,
            {
                ('one', '1'): {},
                ('one', '2'): {},
            })
        # Even nested tokens must be unique.
        self.assertRaises(
            ValueError, self._getTargetClass().fromDict,
            {
                ('new_york', 'New York'): {
                    ('albany', 'Albany'): {},
                    ('new_york', 'New York'): {},
                },
            })
        # The same applies to nested values.
        self.assertRaises(
            ValueError, self._getTargetClass().fromDict,
            {
                ('1', 'new_york'): {
                    ('2', 'albany'): {},
                    ('3', 'new_york'): {},
                },
            })
        # The title attribute does however not have to be unique.
        self._getTargetClass().fromDict({
            ('1', 'new_york', 'New York'): {
                ('2', 'ny_albany', 'Albany'): {},
                ('3', 'ny_new_york', 'New York'): {},
            },
        })
        self._getTargetClass().fromDict({
            ('one', '1', 'One'): {},
            ('two', '2', 'One'): {},
        })

    def test_nonunique_value_message(self):
        try:
            self._getTargetClass().fromDict({
                ('one', '1'): {},
                ('two', '1'): {},
            })
        except ValueError as e:
            self.assertEqual(str(e), "Term values must be unique: '1'")

    def test_nonunique_token_message(self):
        try:
            self._getTargetClass().fromDict({
                ('one', '1'): {},
                ('one', '2'): {},
            })
        except ValueError as e:
            self.assertEqual(str(e), "Term tokens must be unique: 'one'")

    def test_recursive_methods(self):
        # Test the _createTermTree and _getPathToTreeNode methods
        from zope.schema.vocabulary import _createTermTree
        tree = _createTermTree({}, self.business_tree())
        vocab = self._getTargetClass().fromDict(self.business_tree())

        term_path = vocab._getPathToTreeNode(tree, "infrastructure")
        vocab_path = vocab._getPathToTreeNode(vocab, "infrastructure")
        self.assertEqual(term_path, vocab_path)
        self.assertEqual(term_path, ["infrastructure"])

        term_path = vocab._getPathToTreeNode(tree, "security")
        vocab_path = vocab._getPathToTreeNode(vocab, "security")
        self.assertEqual(term_path, vocab_path)
        self.assertEqual(term_path, ["infrastructure", "security"])

        term_path = vocab._getPathToTreeNode(tree, "database")
        vocab_path = vocab._getPathToTreeNode(vocab, "database")
        self.assertEqual(term_path, vocab_path)
        self.assertEqual(term_path,
                         ["infrastructure", "data_transaction", "database"])

        term_path = vocab._getPathToTreeNode(tree, "dcs_host")
        vocab_path = vocab._getPathToTreeNode(vocab, "dcs_host")
        self.assertEqual(term_path, vocab_path)
        self.assertEqual(term_path, ["services", "check_in", "dcs_host"])

        term_path = vocab._getPathToTreeNode(tree, "dummy")
        vocab_path = vocab._getPathToTreeNode(vocab, "dummy")
        self.assertEqual(term_path, vocab_path)
        self.assertEqual(term_path, [])


class RegistryTests(unittest.TestCase):
    # Tests of the simple vocabulary and presentation registries.

    def setUp(self):
        from zope.schema.vocabulary import _clear
        _clear()

    def tearDown(self):
        from zope.schema.vocabulary import _clear
        _clear()

    def test_setVocabularyRegistry(self):
        from zope.schema.vocabulary import getVocabularyRegistry
        from zope.schema.vocabulary import setVocabularyRegistry
        r = _makeDummyRegistry()
        setVocabularyRegistry(r)
        self.assertIs(getVocabularyRegistry(), r)

    def test_getVocabularyRegistry(self):
        from zope.schema.interfaces import IVocabularyRegistry
        from zope.schema.vocabulary import getVocabularyRegistry
        r = getVocabularyRegistry()
        self.assertTrue(IVocabularyRegistry.providedBy(r))

    # TODO: still need to test the default implementation


def _makeSampleVocabulary():
    from zope.interface import implementer

    from zope.schema.interfaces import IVocabulary

    class SampleTerm:
        pass

    @implementer(IVocabulary)
    class SampleVocabulary:

        def __iter__(self):
            raise AssertionError("Not called")

        def __contains__(self, value):
            return 0 <= value < 10

        def __len__(self):  # pragma: no cover
            return 10

        def getTerm(self, value):
            raise AssertionError("Not called.")

    return SampleVocabulary()


def _makeDummyRegistry():
    from zope.schema.vocabulary import VocabularyRegistry

    class DummyRegistry(VocabularyRegistry):
        def get(self, object, name):
            raise AssertionError("Not called")
    return DummyRegistry()
