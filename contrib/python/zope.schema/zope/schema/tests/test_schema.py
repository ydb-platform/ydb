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
"""Schema field tests
"""
import unittest


def _makeSchema():

    from zope.interface import Interface

    from zope.schema import Bytes

    class ISchemaTest(Interface):
        title = Bytes(
            title="Title",
            description="Title",
            default=b"",
            required=True)
        description = Bytes(
            title="Description",
            description="Description",
            default=b"",
            required=True)
        spam = Bytes(
            title="Spam",
            description="Spam",
            default=b"",
            required=True)
    return ISchemaTest


def _makeDerivedSchema():

    from zope.schema import Bytes

    base = _makeSchema()

    class ISchemaTestSubclass(base):
        foo = Bytes(
            title='Foo',
            description='Fooness',
            default=b"",
            required=False)
    return ISchemaTestSubclass


class Test_getFields(unittest.TestCase):

    def _callFUT(self, schema):
        from zope.schema import getFields
        return getFields(schema)

    def test_simple(self):
        fields = self._callFUT(_makeSchema())

        self.assertIn('title', fields)
        self.assertIn('description', fields)
        self.assertIn('spam', fields)

        # test whether getName() has the right value
        for key, value in fields.items():
            self.assertEqual(key, value.getName())

    def test_derived(self):
        fields = self._callFUT(_makeDerivedSchema())

        self.assertIn('title', fields)
        self.assertIn('description', fields)
        self.assertIn('spam', fields)
        self.assertIn('foo', fields)

        # test whether getName() has the right value
        for key, value in fields.items():
            self.assertEqual(key, value.getName())


class Test_getFieldsInOrder(unittest.TestCase):

    def _callFUT(self, schema):
        from zope.schema import getFieldsInOrder
        return getFieldsInOrder(schema)

    def test_simple(self):
        fields = self._callFUT(_makeSchema())
        field_names = [name for name, field in fields]
        self.assertEqual(field_names, ['title', 'description', 'spam'])
        for key, value in fields:
            self.assertEqual(key, value.getName())

    def test_derived(self):
        fields = self._callFUT(_makeDerivedSchema())
        field_names = [name for name, field in fields]
        self.assertEqual(field_names, ['title', 'description', 'spam', 'foo'])
        for key, value in fields:
            self.assertEqual(key, value.getName())


class Test_getFieldNames(unittest.TestCase):

    def _callFUT(self, schema):
        from zope.schema import getFieldNames
        return getFieldNames(schema)

    def test_simple(self):
        names = self._callFUT(_makeSchema())
        self.assertEqual(len(names), 3)
        self.assertIn('title', names)
        self.assertIn('description', names)
        self.assertIn('spam', names)

    def test_derived(self):
        names = self._callFUT(_makeDerivedSchema())
        self.assertEqual(len(names), 4)
        self.assertIn('title', names)
        self.assertIn('description', names)
        self.assertIn('spam', names)
        self.assertIn('foo', names)


class Test_getFieldNamesInOrder(unittest.TestCase):

    def _callFUT(self, schema):
        from zope.schema import getFieldNamesInOrder
        return getFieldNamesInOrder(schema)

    def test_simple(self):
        names = self._callFUT(_makeSchema())
        self.assertEqual(names, ['title', 'description', 'spam'])

    def test_derived(self):
        names = self._callFUT(_makeDerivedSchema())
        self.assertEqual(names, ['title', 'description', 'spam', 'foo'])


class Test_getValidationErrors(unittest.TestCase):

    def _callFUT(self, schema, object):
        from zope.schema import getValidationErrors
        return getValidationErrors(schema, object)

    def test_schema(self):
        from zope.interface import Interface

        class IEmpty(Interface):
            pass

        errors = self._callFUT(IEmpty, object())
        self.assertEqual(len(errors), 0)

    def test_schema_with_field_errors(self):
        from zope.interface import Interface

        from zope.schema import Text
        from zope.schema.interfaces import SchemaNotFullyImplemented

        class IWithRequired(Interface):
            must = Text(required=True)

        errors = self._callFUT(IWithRequired, object())
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0], 'must')
        self.assertEqual(errors[0][1].__class__, SchemaNotFullyImplemented)
        self.assertIsNone(errors[0][1].value)
        self.assertEqual(IWithRequired['must'],
                         errors[0][1].field)

    def test_schema_with_invariant_errors(self):
        from zope.interface import Interface
        from zope.interface import invariant
        from zope.interface.exceptions import Invalid

        class IWithFailingInvariant(Interface):
            @invariant
            def _epic_fail(obj):
                raise Invalid('testing')

        errors = self._callFUT(IWithFailingInvariant, object())
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0], None)
        self.assertEqual(errors[0][1].__class__, Invalid)

    def test_schema_with_invariant_ok(self):
        from zope.interface import Interface
        from zope.interface import invariant

        class IWithPassingInvariant(Interface):
            @invariant
            def _hall_pass(obj):
                pass

        errors = self._callFUT(IWithPassingInvariant, object())
        self.assertEqual(len(errors), 0)


class Test_getSchemaValidationErrors(unittest.TestCase):

    def _callFUT(self, schema, object):
        from zope.schema import getSchemaValidationErrors
        return getSchemaValidationErrors(schema, object)

    def test_schema_wo_fields(self):
        from zope.interface import Attribute
        from zope.interface import Interface

        class INoFields(Interface):
            def method():
                "A method."
            attr = Attribute('ignoreme')

        errors = self._callFUT(INoFields, object())
        self.assertEqual(len(errors), 0)

    def test_schema_with_fields_ok(self):
        from zope.interface import Interface

        from zope.schema import Text

        class IWithFields(Interface):
            foo = Text()
            bar = Text()

        class Obj:
            foo = 'Foo'
            bar = 'Bar'

        errors = self._callFUT(IWithFields, Obj())
        self.assertEqual(len(errors), 0)

    def test_schema_with_missing_field(self):
        from zope.interface import Interface

        from zope.schema import Text
        from zope.schema.interfaces import SchemaNotFullyImplemented

        class IWithRequired(Interface):
            must = Text(required=True)

        errors = self._callFUT(IWithRequired, object())
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0], 'must')
        self.assertEqual(errors[0][1].__class__, SchemaNotFullyImplemented)
        self.assertIsNone(errors[0][1].value)
        self.assertEqual(IWithRequired['must'],
                         errors[0][1].field)

    def test_schema_with_invalid_field(self):
        from zope.interface import Interface

        from zope.schema import Int
        from zope.schema.interfaces import TooSmall

        class IWithMinium(Interface):
            value = Int(required=True, min=0)

        class Obj:
            value = -1

        errors = self._callFUT(IWithMinium, Obj())
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0], 'value')
        self.assertEqual(errors[0][1].__class__, TooSmall)
