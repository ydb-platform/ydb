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
"""Test Interface accessor methods.
"""
import unittest


# pylint:disable=inherit-non-class


class FieldReadAccessorTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema.accessors import FieldReadAccessor
        return FieldReadAccessor

    def _makeOne(self, field=None):
        from zope.schema import Text
        if field is None:
            field = Text(__name__='testing')
        return self._getTargetClass()(field)

    def test_ctor_not_created_inside_interface(self):
        from zope.schema import Text

        field = Text(title='Hmm')
        wrapped = self._makeOne(field)
        self.assertIs(wrapped.field, field)
        self.assertEqual(wrapped.__name__, '')  # __name__ set when in iface
        self.assertEqual(wrapped.__doc__, 'get Hmm')

    def test_ctor_created_inside_interface(self):
        from zope.interface import Interface

        from zope.schema import Text

        field = Text(title='Hmm')

        class IFoo(Interface):
            getter = self._makeOne(field)
        getter = IFoo['getter']
        self.assertEqual(getter.__name__, 'getter')
        self.assertEqual(getter.__doc__, 'get Hmm')

    def test___provides___w_field_no_provides(self):
        from zope.interface import implementedBy
        from zope.interface import providedBy
        wrapped = self._makeOne(object())
        self.assertEqual(list(providedBy(wrapped)),
                         list(implementedBy(self._getTargetClass())))

    def test___provides___w_field_w_provides(self):
        from zope.interface import implementedBy
        from zope.interface import providedBy
        from zope.interface.interfaces import IAttribute
        from zope.interface.interfaces import IMethod

        from zope.schema import Text

        # When wrapping a field that provides stuff,
        # we provide the same stuff, with the addition of
        # IMethod at the correct spot in the IRO (just before
        # IAttribute).
        field = Text()
        field_provides = list(providedBy(field))
        wrapped = self._makeOne(field)
        wrapped_provides = list(providedBy(wrapped))

        index_of_attribute = field_provides.index(IAttribute)
        expected = list(field_provides)
        expected.insert(index_of_attribute, IMethod)
        self.assertEqual(expected, wrapped_provides)
        for iface in list(implementedBy(self._getTargetClass())):
            self.assertIn(iface, wrapped_provides)

    def test___provides___w_field_w_provides_strict(self):
        from zope.interface import ro
        attr = 'STRICT_IRO'
        try:
            getattr(ro.C3, attr)
        except AttributeError:  # pragma: no cover
            # https://github.com/zopefoundation/zope.interface/issues/194
            # zope.interface 5.0.0 used this incorrect spelling.
            attr = 'STRICT_RO'
            getattr(ro.C3, attr)
        setattr(ro.C3, attr, True)
        try:
            self.test___provides___w_field_w_provides()
        finally:
            setattr(ro.C3, attr, getattr(ro.C3, 'ORIG_' + attr))

    def test_getSignatureString(self):
        wrapped = self._makeOne()
        self.assertEqual(wrapped.getSignatureString(), '()')

    def test_getSignatureInfo(self):
        wrapped = self._makeOne()
        info = wrapped.getSignatureInfo()
        self.assertEqual(info['positional'], ())
        self.assertEqual(info['required'], ())
        self.assertEqual(info['optional'], ())
        self.assertEqual(info['varargs'], None)
        self.assertEqual(info['kwargs'], None)

    def test_get_miss(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()
        getter = IFoo['getter']

        class Foo:
            pass
        self.assertRaises(AttributeError, getter.get, Foo())

    def test_get_hit(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()
        getter = IFoo['getter']

        class Foo:
            def getter(self):
                return '123'
        self.assertEqual(getter.get(Foo()), '123')

    def test_query_miss_implicit_default(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()
        getter = IFoo['getter']

        class Foo:
            pass
        self.assertEqual(getter.query(Foo()), None)

    def test_query_miss_explicit_default(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()
        getter = IFoo['getter']

        class Foo:
            pass
        self.assertEqual(getter.query(Foo(), 234), 234)

    def test_query_hit(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()
        getter = IFoo['getter']

        class Foo:
            def getter(self):
                return '123'

        self.assertEqual(getter.query(Foo()), '123')

    def test_set_readonly(self):
        from zope.interface import Interface

        from zope.schema import Text
        field = Text(readonly=True)

        class IFoo(Interface):
            getter = self._makeOne(field)
        getter = IFoo['getter']

        class Foo:
            def getter(self):
                raise AssertionError("Not called")
        self.assertRaises(TypeError, getter.set, Foo(), '456')

    def test_set_no_writer(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()
        getter = IFoo['getter']

        class Foo:
            def getter(self):
                raise AssertionError("Not called")

        self.assertRaises(AttributeError, getter.set, Foo(), '456')

    def test_set_w_writer(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()

        getter = IFoo['getter']
        _called_with = []

        class Writer:
            pass

        writer = Writer()
        # pylint:disable=attribute-defined-outside-init
        writer.__name__ = 'setMe'
        # pylint:enable=attribute-defined-outside-init
        getter.writer = writer

        class Foo:
            def setMe(self, value):
                _called_with.append(value)

        getter.set(Foo(), '456')
        self.assertEqual(_called_with, ['456'])

    def test_bind(self):
        from zope.interface import Interface

        class IFoo(Interface):
            getter = self._makeOne()

        getter = IFoo['getter']
        context = object()
        bound = getter.bind(context)
        self.assertEqual(bound.__name__, 'getter')
        self.assertIsInstance(bound.field, getter.field.__class__)
        self.assertIs(bound.field.context, context)


class FieldWriteAccessorTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema.accessors import FieldWriteAccessor
        return FieldWriteAccessor

    def _makeOne(self, field=None):
        from zope.schema import Text
        if field is None:
            field = Text(__name__='testing')
        return self._getTargetClass()(field)

    def test_ctor_not_created_inside_interface(self):
        from zope.schema import Text

        field = Text(title='Hmm')
        wrapped = self._makeOne(field)
        self.assertIs(wrapped.field, field)
        self.assertEqual(wrapped.__name__, '')  # __name__ set when in iface
        self.assertEqual(wrapped.__doc__, 'set Hmm')

    def test_ctor_created_inside_interface(self):
        from zope.interface import Interface

        from zope.schema import Text

        field = Text(title='Hmm')

        class IFoo(Interface):
            setter = self._makeOne(field)

        setter = IFoo['setter']
        self.assertEqual(setter.__name__, 'setter')
        self.assertEqual(setter.__doc__, 'set Hmm')

    def test_getSignatureString(self):
        wrapped = self._makeOne()
        self.assertEqual(wrapped.getSignatureString(), '(newvalue)')

    def test_getSignatureInfo(self):
        wrapped = self._makeOne()
        info = wrapped.getSignatureInfo()
        self.assertEqual(info['positional'], ('newvalue',))
        self.assertEqual(info['required'], ('newvalue',))
        self.assertEqual(info['optional'], ())
        self.assertEqual(info['varargs'], None)
        self.assertEqual(info['kwargs'], None)


class Test_accessors(unittest.TestCase):

    def _callFUT(self, *args, **kw):
        from zope.schema.accessors import accessors
        return accessors(*args, **kw)

    def test_w_only_read_accessor(self):
        from zope.interface import Interface

        from zope.schema import Text

        field = Text(title='Hmm', readonly=True)

        class IFoo(Interface):
            getter, = self._callFUT(field)

        getter = IFoo['getter']
        self.assertEqual(getter.__name__, 'getter')
        self.assertEqual(getter.__doc__, 'get Hmm')
        self.assertEqual(getter.getSignatureString(), '()')
        info = getter.getSignatureInfo()
        self.assertEqual(info['positional'], ())
        self.assertEqual(info['required'], ())
        self.assertEqual(info['optional'], ())
        self.assertEqual(info['varargs'], None)
        self.assertEqual(info['kwargs'], None)

    def test_w_read_and_write_accessors(self):
        from zope.interface import Interface

        from zope.schema import Text

        field = Text(title='Hmm')

        class IFoo(Interface):
            getter, setter = self._callFUT(field)

        getter = IFoo['getter']
        self.assertEqual(getter.__name__, 'getter')
        self.assertEqual(getter.getSignatureString(), '()')
        info = getter.getSignatureInfo()
        self.assertEqual(info['positional'], ())
        self.assertEqual(info['required'], ())
        self.assertEqual(info['optional'], ())
        self.assertEqual(info['varargs'], None)
        self.assertEqual(info['kwargs'], None)
        setter = IFoo['setter']
        self.assertEqual(setter.__name__, 'setter')
        self.assertEqual(setter.getSignatureString(), '(newvalue)')
        info = setter.getSignatureInfo()
        self.assertEqual(info['positional'], ('newvalue',))
        self.assertEqual(info['required'], ('newvalue',))
        self.assertEqual(info['optional'], ())
        self.assertEqual(info['varargs'], None)
        self.assertEqual(info['kwargs'], None)
