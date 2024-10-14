##############################################################################
#
# Copyright (c) 2014 Zope Foundation and Contributors.
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
"""Resolution ordering utility tests"""
import unittest


# pylint:disable=blacklisted-name
# pylint:disable=protected-access
# pylint:disable=attribute-defined-outside-init

class Test__mergeOrderings(unittest.TestCase):

    def _callFUT(self, orderings):
        from zope.interface.ro import _legacy_mergeOrderings
        return _legacy_mergeOrderings(orderings)

    def test_empty(self):
        self.assertEqual(self._callFUT([]), [])

    def test_single(self):
        self.assertEqual(self._callFUT(['a', 'b', 'c']), ['a', 'b', 'c'])

    def test_w_duplicates(self):
        self.assertEqual(self._callFUT([['a'], ['b', 'a']]), ['b', 'a'])

    def test_suffix_across_multiple_duplicates(self):
        O1 = ['x', 'y', 'z']
        O2 = ['q', 'z']
        O3 = [1, 3, 5]
        O4 = ['z']
        self.assertEqual(self._callFUT([O1, O2, O3, O4]),
                         ['x', 'y', 'q', 1, 3, 5, 'z'])


class Test__flatten(unittest.TestCase):

    def _callFUT(self, ob):
        from zope.interface.ro import _legacy_flatten
        return _legacy_flatten(ob)

    def test_w_empty_bases(self):

        class Foo:
            pass

        foo = Foo()
        foo.__bases__ = ()
        self.assertEqual(self._callFUT(foo), [foo])

    def test_w_single_base(self):

        class Foo:
            pass

        self.assertEqual(self._callFUT(Foo), [Foo, object])

    def test_w_bases(self):

        class Foo:
            pass

        class Bar(Foo):
            pass

        self.assertEqual(self._callFUT(Bar), [Bar, Foo, object])

    def test_w_diamond(self):

        class Foo:
            pass

        class Bar(Foo):
            pass

        class Baz(Foo):
            pass

        class Qux(Bar, Baz):
            pass

        self.assertEqual(self._callFUT(Qux),
                         [Qux, Bar, Foo, object, Baz, Foo, object])


class Test_ro(unittest.TestCase):
    maxDiff = None

    def _callFUT(self, ob, **kwargs):
        from zope.interface.ro import _legacy_ro
        return _legacy_ro(ob, **kwargs)

    def test_w_empty_bases(self):

        class Foo:
            pass

        foo = Foo()
        foo.__bases__ = ()
        self.assertEqual(self._callFUT(foo), [foo])

    def test_w_single_base(self):

        class Foo:
            pass

        self.assertEqual(self._callFUT(Foo), [Foo, object])

    def test_w_bases(self):

        class Foo:
            pass

        class Bar(Foo):
            pass

        self.assertEqual(self._callFUT(Bar), [Bar, Foo, object])

    def test_w_diamond(self):

        class Foo:
            pass

        class Bar(Foo):
            pass

        class Baz(Foo):
            pass

        class Qux(Bar, Baz):
            pass

        self.assertEqual(self._callFUT(Qux),
                         [Qux, Bar, Baz, Foo, object])

    def _make_IOErr(self):
        # This can't be done in the standard C3 ordering.

        class Foo:
            def __init__(self, name, *bases):
                self.__name__ = name
                self.__bases__ = bases

            def __repr__(self):  # pragma: no cover
                return self.__name__

        # Mimic what classImplements(IOError, IIOError)
        # does.
        IEx = Foo('IEx')
        IStdErr = Foo('IStdErr', IEx)
        IEnvErr = Foo('IEnvErr', IStdErr)
        IIOErr = Foo('IIOErr', IEnvErr)
        IOSErr = Foo('IOSErr', IEnvErr)

        IOErr = Foo('IOErr', IEnvErr, IIOErr, IOSErr)
        return IOErr, [IOErr, IIOErr, IOSErr, IEnvErr, IStdErr, IEx]

    def test_non_orderable(self):
        IOErr, bases = self._make_IOErr()

        self.assertEqual(self._callFUT(IOErr), bases)

    def test_mixed_inheritance_and_implementation(self):
        # https://github.com/zopefoundation/zope.interface/issues/8
        # This test should fail, but doesn't, as described in that issue.
        # pylint:disable=inherit-non-class
        from zope.interface import Interface
        from zope.interface import implementedBy
        from zope.interface import implementer
        from zope.interface import providedBy

        class IFoo(Interface):
            pass

        @implementer(IFoo)
        class ImplementsFoo:
            pass

        class ExtendsFoo(ImplementsFoo):
            pass

        class ImplementsNothing:
            pass

        class ExtendsFooImplementsNothing(ExtendsFoo, ImplementsNothing):
            pass

        self.assertEqual(
            self._callFUT(providedBy(ExtendsFooImplementsNothing())),
            [implementedBy(ExtendsFooImplementsNothing),
             implementedBy(ExtendsFoo),
             implementedBy(ImplementsFoo),
             IFoo,
             Interface,
             implementedBy(ImplementsNothing),
             implementedBy(object)])


class C3Setting:

    def __init__(self, setting, value):
        self._setting = setting
        self._value = value

    def __enter__(self):
        from zope.interface import ro
        setattr(ro.C3, self._setting.__name__, self._value)

    def __exit__(self, t, v, tb):
        from zope.interface import ro
        setattr(ro.C3, self._setting.__name__, self._setting)


class TestC3(unittest.TestCase):
    def _makeOne(self, C, strict=False, base_mros=None):
        from zope.interface.ro import C3
        return C3.resolver(C, strict, base_mros)

    def test_base_mros_given(self):
        c3 = self._makeOne(
            type(self),
            base_mros={unittest.TestCase: unittest.TestCase.__mro__}
        )
        memo = c3.memo
        self.assertIn(unittest.TestCase, memo)
        # We used the StaticMRO class
        self.assertIsNone(memo[unittest.TestCase].had_inconsistency)

    def test_one_base_optimization(self):
        c3 = self._makeOne(type(self))
        # Even though we didn't call .mro() yet, the MRO has been
        # computed.
        self.assertIsNotNone(c3._C3__mro)  # pylint:disable=no-member
        c3._merge = None
        self.assertEqual(c3.mro(), list(type(self).__mro__))


class Test_ROComparison(unittest.TestCase):

    class MockC3:
        direct_inconsistency = False
        bases_had_inconsistency = False

    def _makeOne(self, c3=None, c3_ro=(), legacy_ro=()):
        from zope.interface.ro import _ROComparison
        return _ROComparison(c3 or self.MockC3(), c3_ro, legacy_ro)

    def test_inconsistent_label(self):
        comp = self._makeOne()
        self.assertEqual('no', comp._inconsistent_label)

        comp.c3.direct_inconsistency = True
        self.assertEqual("direct", comp._inconsistent_label)

        comp.c3.bases_had_inconsistency = True
        self.assertEqual("direct+bases", comp._inconsistent_label)

        comp.c3.direct_inconsistency = False
        self.assertEqual('bases', comp._inconsistent_label)
