##############################################################################
#
# Copyright (c) 2012 Zope Foundation and Contributors.
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
import decimal
import doctest
import unicodedata
import unittest


# pylint:disable=protected-access,inherit-non-class,blacklisted-name
# pylint:disable=attribute-defined-outside-init


class InterfaceConformanceTestsMixin:

    def _getTargetClass(self):
        raise NotImplementedError

    def _getTargetInterface(self):
        raise NotImplementedError

    def _getTargetInterfaces(self):
        # Return the primary and any secondary interfaces
        return [self._getTargetInterface()]

    def _makeOne(self, *args, **kwargs):
        return self._makeOneFromClass(self._getTargetClass(),
                                      *args,
                                      **kwargs)

    def _makeOneFromClass(self, cls, *args, **kwargs):
        return cls(*args, **kwargs)

    def test_class_conforms_to_iface(self):
        from zope.interface.verify import verifyClass
        cls = self._getTargetClass()
        __traceback_info__ = cls
        for iface in self._getTargetInterfaces():
            verifyClass(iface, cls)

    def test_instance_conforms_to_iface(self):
        from zope.interface.verify import verifyObject
        instance = self._makeOne()
        __traceback_info__ = instance
        for iface in self._getTargetInterfaces():
            verifyObject(iface, instance)

    def test_iface_is_first_in_sro(self):
        from zope.interface import implementedBy
        implemented = implementedBy(self._getTargetClass())
        __traceback_info__ = implemented.__sro__
        self.assertIs(implemented, implemented.__sro__[0])
        self.assertIs(self._getTargetInterface(), implemented.__sro__[1])

    def test_implements_consistent__sro__(self):
        from zope.interface import implementedBy
        from zope.interface import ro
        __traceback_info__ = implementedBy(self._getTargetClass()).__sro__
        self.assertTrue(
            ro.is_consistent(implementedBy(self._getTargetClass())))

    def test_iface_consistent_ro(self):
        from zope.interface import ro
        __traceback_info__ = self._getTargetInterface().__iro__
        self.assertTrue(ro.is_consistent(self._getTargetInterface()))


class EqualityTestsMixin(InterfaceConformanceTestsMixin):

    def test_is_hashable(self):
        field = self._makeOne()
        hash(field)  # doesn't raise

    def test_equal_instances_have_same_hash(self):
        # Equal objects should have equal hashes
        field1 = self._makeOne()
        field2 = self._makeOne()
        self.assertIsNot(field1, field2)
        self.assertEqual(field1, field2)
        self.assertEqual(hash(field1), hash(field2))

    def test_instances_in_different_interfaces_not_equal(self):
        from zope import interface

        field1 = self._makeOne()
        field2 = self._makeOne()
        self.assertEqual(field1, field2)
        self.assertEqual(hash(field1), hash(field2))

        class IOne(interface.Interface):
            one = field1

        class ITwo(interface.Interface):
            two = field2

        self.assertEqual(field1, field1)
        self.assertEqual(field2, field2)
        self.assertNotEqual(field1, field2)
        self.assertNotEqual(hash(field1), hash(field2))

    def test_hash_across_unequal_instances(self):
        # Hash equality does not imply equal objects.
        # Our implementation only considers property names,
        # not values. That's OK, a dict still does the right thing.
        field1 = self._makeOne(title='foo')
        field2 = self._makeOne(title='bar')
        self.assertIsNot(field1, field2)
        self.assertNotEqual(field1, field2)
        self.assertEqual(hash(field1), hash(field2))

        d = {field1: 42}
        self.assertIn(field1, d)
        self.assertEqual(42, d[field1])
        self.assertNotIn(field2, d)
        with self.assertRaises(KeyError):
            d.__getitem__(field2)

    def test___eq___different_type(self):
        left = self._makeOne()

        class Derived(self._getTargetClass()):
            pass
        right = self._makeOneFromClass(Derived)
        self.assertNotEqual(left, right)
        self.assertNotEqual(left, right)

    def test___eq___same_type_different_attrs(self):
        left = self._makeOne(required=True)
        right = self._makeOne(required=False)
        self.assertNotEqual(left, right)
        self.assertNotEqual(left, right)

    def test___eq___same_type_same_attrs(self):
        left = self._makeOne()
        self.assertEqual(left, left)

        right = self._makeOne()
        self.assertEqual(left, right)
        self.assertEqual(left, right)


class OrderableMissingValueMixin:
    mvm_missing_value = -1
    mvm_default = 0

    def test_missing_value_no_min_or_max(self):
        # We should be able to provide a missing_value without
        # also providing a min or max. But note that we must still
        # provide a default.
        # See https://github.com/zopefoundation/zope.schema/issues/9
        Kind = self._getTargetClass()
        self.assertTrue(Kind.min._allow_none)
        self.assertTrue(Kind.max._allow_none)

        field = self._makeOne(missing_value=self.mvm_missing_value,
                              default=self.mvm_default)
        self.assertIsNone(field.min)
        self.assertIsNone(field.max)
        self.assertEqual(self.mvm_missing_value, field.missing_value)


class OrderableTestsMixin:

    def assertRaisesTooBig(self, field, value):
        from zope.schema.interfaces import TooBig
        with self.assertRaises(TooBig) as exc:
            field.validate(value)

        ex = exc.exception
        self.assertEqual(value, ex.value)
        self.assertEqual(field.max, ex.bound)
        self.assertEqual(TooBig.TOO_LARGE, ex.violation_direction)

    def assertRaisesTooSmall(self, field, value):
        from zope.schema.interfaces import TooSmall
        with self.assertRaises(TooSmall) as exc:
            field.validate(value)

        ex = exc.exception
        self.assertEqual(value, ex.value)
        self.assertEqual(field.min, ex.bound)
        self.assertEqual(TooSmall.TOO_SMALL, ex.violation_direction)

    MIN = 10
    MAX = 20
    VALID = (10, 11, 19, 20)
    TOO_SMALL = (9, -10)
    TOO_BIG = (21, 22)

    def test_validate_min(self):
        field = self._makeOne(min=self.MIN)
        for value in self.VALID + self.TOO_BIG:
            field.validate(value)
        for value in self.TOO_SMALL:
            self.assertRaisesTooSmall(field, value)

    def test_validate_max(self):
        field = self._makeOne(max=self.MAX)
        for value in self.VALID + self.TOO_SMALL:
            field.validate(value)
        for value in self.TOO_BIG:
            self.assertRaisesTooBig(field, value)

    def test_validate_min_and_max(self):
        field = self._makeOne(min=self.MIN, max=self.MAX)
        for value in self.TOO_SMALL:
            self.assertRaisesTooSmall(field, value)
        for value in self.VALID:
            field.validate(value)
        for value in self.TOO_BIG:
            self.assertRaisesTooBig(field, value)


class LenTestsMixin:

    def assertRaisesTooLong(self, field, value):
        from zope.schema.interfaces import TooLong
        with self.assertRaises(TooLong) as exc:
            field.validate(value)

        ex = exc.exception
        self.assertEqual(value, ex.value)
        self.assertEqual(field.max_length, ex.bound)
        self.assertEqual(TooLong.TOO_LARGE, ex.violation_direction)

    def assertRaisesTooShort(self, field, value):
        from zope.schema.interfaces import TooShort
        with self.assertRaises(TooShort) as exc:
            field.validate(value)

        ex = exc.exception
        self.assertEqual(value, ex.value)
        self.assertEqual(field.min_length, ex.bound)
        self.assertEqual(TooShort.TOO_SMALL, ex.violation_direction)


class WrongTypeTestsMixin:

    def assertRaisesWrongType(self, field_or_meth, expected_type,
                              *args, **kwargs):
        from zope.schema.interfaces import WrongType
        field = None
        with self.assertRaises(WrongType) as exc:
            if hasattr(field_or_meth, 'validate'):
                field = field_or_meth
                field.validate(*args, **kwargs)
            else:
                field_or_meth(*args, **kwargs)

        ex = exc.exception
        self.assertIs(ex.expected_type, expected_type)
        if field is not None:
            self.assertIs(ex.field, field)
        if len(args) == 1 and not kwargs:
            # Just a value
            self.assertIs(ex.value, args[0])
        if not args and len(kwargs) == 1:
            # A single keyword argument
            self.assertIs(ex.value, kwargs.popitem()[1])

    def assertAllRaiseWrongType(self, field, expected_type, *values):
        for value in values:
            __traceback_info__ = value
            self.assertRaisesWrongType(field, expected_type, value)


class ValidatedPropertyTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import ValidatedProperty
        return ValidatedProperty

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def test___set___not_missing_w_check(self):
        _checked = []

        def _check(inst, value):
            _checked.append((inst, value))

        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop', _check)
        inst = Test()
        inst.prop = 'PROP'
        self.assertEqual(inst._prop, 'PROP')
        self.assertEqual(_checked, [(inst, 'PROP')])

    def test___set___not_missing_wo_check(self):
        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop')
        inst = Test(ValueError)

        def _provoke(inst):
            inst.prop = 'PROP'
        self.assertRaises(ValueError, _provoke, inst)
        self.assertEqual(inst._prop, None)

    def test___set___w_missing_wo_check(self):
        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop')
        inst = Test(ValueError)
        inst.prop = DummyInst.missing_value
        self.assertEqual(inst._prop, DummyInst.missing_value)

    def test___get__(self):
        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop')
        inst = Test()
        inst._prop = 'PROP'
        self.assertEqual(inst.prop, 'PROP')


class DefaultPropertyTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import DefaultProperty
        return DefaultProperty

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def test___get___wo_defaultFactory_miss(self):
        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop')
        inst = Test()
        inst.defaultFactory = None

        def _provoke(inst):
            return inst.prop
        self.assertRaises(KeyError, _provoke, inst)

    def test___get___wo_defaultFactory_hit(self):
        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop')
        inst = Test()
        inst.defaultFactory = None
        inst._prop = 'PROP'
        self.assertEqual(inst.prop, 'PROP')

    def test__get___wo_defaultFactory_in_dict(self):
        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop')
        inst = Test()
        inst._prop = 'PROP'
        self.assertEqual(inst.prop, 'PROP')

    def test___get___w_defaultFactory_not_ICAF_no_check(self):
        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop')
        inst = Test(ValueError)

        def _factory():
            return 'PROP'
        inst.defaultFactory = _factory

        def _provoke(inst):
            return inst.prop
        self.assertRaises(ValueError, _provoke, inst)

    def test___get___w_defaultFactory_w_ICAF_w_check(self):
        from zope.interface import directlyProvides

        from zope.schema._bootstrapinterfaces import \
            IContextAwareDefaultFactory
        _checked = []

        def _check(inst, value):
            _checked.append((inst, value))

        class Test(DummyInst):
            _prop = None
            prop = self._makeOne('_prop', _check)
        inst = Test(ValueError)
        inst.context = object()
        _called_with = []

        def _factory(context):
            _called_with.append(context)
            return 'PROP'
        directlyProvides(_factory, IContextAwareDefaultFactory)
        inst.defaultFactory = _factory
        self.assertEqual(inst.prop, 'PROP')
        self.assertEqual(_checked, [(inst, 'PROP')])
        self.assertEqual(_called_with, [inst.context])


class FieldTests(EqualityTestsMixin,
                 WrongTypeTestsMixin,
                 unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Field
        return Field

    def _getTargetInterface(self):
        from zope.schema.interfaces import IField
        return IField

    def test_getDoc(self):
        import textwrap
        field = self._makeOne(readonly=True, required=False)
        doc = field.getDoc()
        self.assertIn(':Read Only: True', doc)
        self.assertIn(':Required: False', doc)
        self.assertIn(":Default Value:", doc)
        self.assertNotIn(':Default Factory:', doc)

        field._type = str
        doc = field.getDoc()
        self.assertIn(':Allowed Type: :class:`str`', doc)
        self.assertNotIn(':Default Factory:', doc)

        field.defaultFactory = 'default'
        doc = field.getDoc()
        self.assertNotIn(":Default Value:", doc)
        self.assertIn(':Default Factory:', doc)

        field._type = (str, object)
        doc = field.getDoc()
        self.assertIn(':Allowed Type: :class:`str`, :class:`object`', doc)
        self.assertNotIn('..rubric', doc)

        # value_type and key_type are automatically picked up
        field.value_type = self._makeOne()
        # Make sure the formatting works also with fields that have a title
        field.key_type = self._makeOne(title='Key Type')
        doc = field.getDoc()
        self.assertIn('.. rubric:: Key Type', doc)
        self.assertIn('.. rubric:: Value Type', doc)
        self.assertEqual(
            doc,
            textwrap.dedent("""
            :Implementation: :class:`zope.schema.Field`
            :Read Only: True
            :Required: False
            :Default Factory: 'default'
            :Allowed Type: :class:`str`, :class:`object`

            .. rubric:: Key Type

            Key Type

            :Implementation: :class:`zope.schema.Field`
            :Read Only: False
            :Required: True
            :Default Value: None


            .. rubric:: Value Type


            :Implementation: :class:`zope.schema.Field`
            :Read Only: False
            :Required: True
            :Default Value: None

            """)
        )

        field = self._makeOne(
            title='A title',
            description="""Multiline description.

        Some lines have leading whitespace.

        It gets stripped.
        """)

        doc = field.getDoc()
        self.assertEqual(
            field.getDoc(),
            textwrap.dedent("""\
            A title

            Multiline description.

            Some lines have leading whitespace.

            It gets stripped.

            :Implementation: :class:`zope.schema.Field`
            :Read Only: False
            :Required: True
            :Default Value: None
            """)
        )

    def _test_ctor_description_preserved(self):
        # The exact value of the description is preserved,
        # allowing for MessageID objects.
        import textwrap

        from zope.i18nmessageid import MessageFactory

        msg_factory = MessageFactory('zope')

        description = msg_factory("""Multiline description.

        Some lines have leading whitespace.

        It gets stripped.
        """)

        title = msg_factory('A title')

        field = self._makeOne(title=title, description=description)

        self.assertIs(field.title, title)
        self.assertIs(field.description, description)

        self.assertEqual(
            field.getDoc(),
            textwrap.dedent("""\
            A title

            Multiline description.

            Some lines have leading whitespace.

            It gets stripped.

            :Implementation: :class:`zope.schema.Field`
            :Read Only: False
            :Required: True
            :Default Value: None
            """)
        )

    def test_ctor_description_none(self):
        # None values for description don't break the docs.
        import textwrap

        description = None

        title = 'A title'

        field = self._makeOne(title=title, description=description)

        self.assertIs(field.title, title)
        self.assertIs(field.description, description)

        self.assertEqual(
            field.getDoc(),
            textwrap.dedent("""\
            A title

            :Implementation: :class:`zope.schema.Field`
            :Read Only: False
            :Required: True
            :Default Value: None
            """)
        )

    def test_ctor_defaults(self):

        field = self._makeOne()
        self.assertEqual(field.__name__, '')
        self.assertEqual(field.__doc__, '')
        self.assertEqual(field.title, '')
        self.assertEqual(field.description, '')
        self.assertEqual(field.required, True)
        self.assertEqual(field.readonly, False)
        self.assertEqual(field.constraint(object()), True)
        self.assertEqual(field.default, None)
        self.assertEqual(field.defaultFactory, None)
        self.assertEqual(field.missing_value, None)
        self.assertEqual(field.context, None)

    def test_ctor_w_title_wo_description(self):

        field = self._makeOne('TITLE')
        self.assertEqual(field.__name__, '')
        self.assertEqual(field.__doc__, 'TITLE')
        self.assertEqual(field.title, 'TITLE')
        self.assertEqual(field.description, '')

    def test_ctor_wo_title_w_description(self):

        field = self._makeOne(description='DESC')
        self.assertEqual(field.__name__, '')
        self.assertEqual(field.__doc__, 'DESC')
        self.assertEqual(field.title, '')
        self.assertEqual(field.description, 'DESC')

    def test_ctor_w_both_title_and_description(self):

        field = self._makeOne('TITLE', 'DESC', 'NAME')
        self.assertEqual(field.__name__, 'NAME')
        self.assertEqual(field.__doc__, 'TITLE\n\nDESC')
        self.assertEqual(field.title, 'TITLE')
        self.assertEqual(field.description, 'DESC')

    def test_ctor_order_madness(self):
        klass = self._getTargetClass()
        order_before = klass.order
        field = self._makeOne()
        order_after = klass.order
        self.assertEqual(order_after, order_before + 1)
        self.assertEqual(field.order, order_after)

    def test_explicit_required_readonly_missingValue(self):
        obj = object()
        field = self._makeOne(required=False, readonly=True, missing_value=obj)
        self.assertEqual(field.required, False)
        self.assertEqual(field.readonly, True)
        self.assertEqual(field.missing_value, obj)

    def test_explicit_constraint_default(self):
        _called_with = []
        obj = object()

        def _constraint(value):
            _called_with.append(value)
            return value is obj
        field = self._makeOne(
            required=False, readonly=True, constraint=_constraint, default=obj
        )
        self.assertEqual(field.required, False)
        self.assertEqual(field.readonly, True)
        self.assertEqual(_called_with, [obj])
        self.assertEqual(field.constraint(self), False)
        self.assertEqual(_called_with, [obj, self])
        self.assertEqual(field.default, obj)

    def test_explicit_defaultFactory(self):
        _called_with = []
        obj = object()

        def _constraint(value):
            _called_with.append(value)
            return value is obj

        def _factory():
            return obj
        field = self._makeOne(
            required=False,
            readonly=True,
            constraint=_constraint,
            defaultFactory=_factory,
        )
        self.assertEqual(field.required, False)
        self.assertEqual(field.readonly, True)
        self.assertEqual(field.constraint(self), False)
        self.assertEqual(_called_with, [self])
        self.assertEqual(field.default, obj)
        self.assertEqual(_called_with, [self, obj])
        self.assertEqual(field.defaultFactory, _factory)

    def test_explicit_defaultFactory_returning_missing_value(self):
        def _factory():
            return None
        field = self._makeOne(required=True,
                              defaultFactory=_factory)
        self.assertEqual(field.default, None)

    def test_bind(self):
        obj = object()
        field = self._makeOne()
        bound = field.bind(obj)
        self.assertEqual(bound.context, obj)
        expected = dict(field.__dict__)
        found = dict(bound.__dict__)
        found.pop('context')
        self.assertEqual(found, expected)
        self.assertEqual(bound.__class__, field.__class__)

    def test_validate_missing_not_required(self):
        missing = object()

        field = self._makeOne(  # pragma: no branch
            required=False, missing_value=missing, constraint=lambda x: False,
        )
        self.assertEqual(field.validate(missing), None)  # doesn't raise

    def test_validate_missing_and_required(self):
        from zope.schema._bootstrapinterfaces import RequiredMissing
        missing = object()

        field = self._makeOne(  # pragma: no branch
            required=True, missing_value=missing, constraint=lambda x: False,
        )
        self.assertRaises(RequiredMissing, field.validate, missing)

    def test_validate_wrong_type(self):

        field = self._makeOne(  # pragma: no branch
            required=True, constraint=lambda x: False,
        )
        field._type = str
        self.assertRaisesWrongType(field, str, 1)

    def test_validate_constraint_fails(self):
        from zope.schema._bootstrapinterfaces import ConstraintNotSatisfied

        field = self._makeOne(required=True, constraint=lambda x: False)
        field._type = int
        self.assertRaises(ConstraintNotSatisfied, field.validate, 1)

    def test_validate_constraint_raises_StopValidation(self):
        from zope.schema._bootstrapinterfaces import StopValidation

        def _fail(value):
            raise StopValidation
        field = self._makeOne(required=True, constraint=_fail)
        field._type = int
        field.validate(1)  # doesn't raise

    def test_validate_constraint_raises_custom_exception(self):
        from zope.schema._bootstrapinterfaces import ValidationError

        def _fail(value):
            raise ValidationError
        field = self._makeOne(constraint=_fail)
        with self.assertRaises(ValidationError) as exc:
            field.validate(1)

        self.assertIs(exc.exception.field, field)
        self.assertEqual(exc.exception.value, 1)

    def test_validate_constraint_raises_custom_exception_no_overwrite(self):
        from zope.schema._bootstrapinterfaces import ValidationError

        def _fail(value):
            raise ValidationError(value).with_field_and_value(self, self)
        field = self._makeOne(constraint=_fail)
        with self.assertRaises(ValidationError) as exc:
            field.validate(1)

        self.assertIs(exc.exception.field, self)
        self.assertIs(exc.exception.value, self)

    def test_get_miss(self):
        field = self._makeOne(__name__='nonesuch')
        inst = DummyInst()
        self.assertRaises(AttributeError, field.get, inst)

    def test_get_hit(self):
        field = self._makeOne(__name__='extant')
        inst = DummyInst()
        inst.extant = 'EXTANT'
        self.assertEqual(field.get(inst), 'EXTANT')

    def test_query_miss_no_default(self):
        field = self._makeOne(__name__='nonesuch')
        inst = DummyInst()
        self.assertEqual(field.query(inst), None)

    def test_query_miss_w_default(self):
        field = self._makeOne(__name__='nonesuch')
        inst = DummyInst()
        self.assertEqual(field.query(inst, 'DEFAULT'), 'DEFAULT')

    def test_query_hit(self):
        field = self._makeOne(__name__='extant')
        inst = DummyInst()
        inst.extant = 'EXTANT'
        self.assertEqual(field.query(inst), 'EXTANT')

    def test_set_readonly(self):
        field = self._makeOne(__name__='lirame', readonly=True)
        inst = DummyInst()
        self.assertRaises(TypeError, field.set, inst, 'VALUE')

    def test_set_hit(self):
        field = self._makeOne(__name__='extant')
        inst = DummyInst()
        inst.extant = 'BEFORE'
        field.set(inst, 'AFTER')
        self.assertEqual(inst.extant, 'AFTER')


class ContainerTests(EqualityTestsMixin,
                     unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Container
        return Container

    def _getTargetInterface(self):
        from zope.schema.interfaces import IContainer
        return IContainer

    def test_validate_not_required(self):
        field = self._makeOne(required=False)
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        self.assertRaises(RequiredMissing, field.validate, None)

    def test__validate_not_collection_not_iterable(self):
        from zope.schema._bootstrapinterfaces import NotAContainer
        cont = self._makeOne()
        bad_value = object()
        with self.assertRaises(NotAContainer) as exc:
            cont._validate(bad_value)

        not_cont = exc.exception
        self.assertIs(not_cont.field, cont)
        self.assertIs(not_cont.value, bad_value)

    def test__validate_collection_but_not_iterable(self):
        cont = self._makeOne()

        class Dummy:
            def __contains__(self, item):
                raise AssertionError("Not called")
        cont._validate(Dummy())  # doesn't raise

    def test__validate_not_collection_but_iterable(self):
        cont = self._makeOne()

        class Dummy:
            def __iter__(self):
                return iter(())
        cont._validate(Dummy())  # doesn't raise

    def test__validate_w_collections(self):
        cont = self._makeOne()
        cont._validate(())  # doesn't raise
        cont._validate([])  # doesn't raise
        cont._validate('')  # doesn't raise
        cont._validate({})  # doesn't raise


class IterableTests(ContainerTests):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Iterable
        return Iterable

    def _getTargetInterface(self):
        from zope.schema.interfaces import IIterable
        return IIterable

    def test__validate_collection_but_not_iterable(self):
        from zope.schema._bootstrapinterfaces import NotAnIterator
        itr = self._makeOne()

        class Dummy:
            def __contains__(self, item):
                raise AssertionError("Not called")
        dummy = Dummy()
        with self.assertRaises(NotAnIterator) as exc:
            itr._validate(dummy)

        not_it = exc.exception
        self.assertIs(not_it.field, itr)
        self.assertIs(not_it.value, dummy)


class OrderableTests(unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Orderable
        return Orderable

    def _makeOne(self, *args, **kw):
        # Orderable is a mixin for a type derived from Field
        from zope.schema._bootstrapfields import Field

        class Mixed(self._getTargetClass(), Field):
            pass
        return Mixed(*args, **kw)

    def test_ctor_defaults(self):
        ordb = self._makeOne()
        self.assertEqual(ordb.min, None)
        self.assertEqual(ordb.max, None)
        self.assertEqual(ordb.default, None)

    def test_ctor_default_too_small(self):
        # This test exercises _validate, too
        from zope.schema._bootstrapinterfaces import TooSmall
        self.assertRaises(TooSmall, self._makeOne, min=0, default=-1)

    def test_ctor_default_too_large(self):
        # This test exercises _validate, too
        from zope.schema._bootstrapinterfaces import TooBig
        self.assertRaises(TooBig, self._makeOne, max=10, default=11)


class MinMaxLenTests(LenTestsMixin,
                     unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import MinMaxLen
        return MinMaxLen

    def _makeOne(self, *args, **kw):
        # MinMaxLen is a mixin for a type derived from Field
        from zope.schema._bootstrapfields import Field

        class Mixed(self._getTargetClass(), Field):
            pass
        return Mixed(*args, **kw)

    def test_ctor_defaults(self):
        mml = self._makeOne()
        self.assertEqual(mml.min_length, 0)
        self.assertEqual(mml.max_length, None)

    def test_validate_too_short(self):
        mml = self._makeOne(min_length=1)
        self.assertRaisesTooShort(mml, ())

    def test_validate_too_long(self):
        mml = self._makeOne(max_length=2)
        self.assertRaisesTooLong(mml, (0, 1, 2))


class TextTests(EqualityTestsMixin,
                WrongTypeTestsMixin,
                unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Text
        return Text

    def _getTargetInterface(self):
        from zope.schema.interfaces import IText
        return IText

    def test_ctor_defaults(self):
        txt = self._makeOne()
        self.assertEqual(txt._type, str)

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            b'',
            1,
            1.0,
            (),
            [],
            {},
            set(),
            frozenset(),
            object())

    def test_validate_w_invalid_default(self):

        from zope.schema.interfaces import ValidationError
        self.assertRaises(ValidationError, self._makeOne, default=b'')

    def test_validate_not_required(self):

        field = self._makeOne(required=False)
        field.validate('')
        field.validate('abc')
        field.validate('abc\ndef')
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing

        field = self._makeOne()
        field.validate('')
        field.validate('abc')
        field.validate('abc\ndef')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_fromUnicode_miss(self):
        deadbeef = b'DEADBEEF'
        txt = self._makeOne()
        self.assertRaisesWrongType(txt.fromUnicode, txt._type, deadbeef)

    def test_fromUnicode_hit(self):
        deadbeef = 'DEADBEEF'
        txt = self._makeOne()
        self.assertEqual(txt.fromUnicode(deadbeef), deadbeef)

    def test_normalization(self):
        deadbeef = unicodedata.normalize(
            'NFD', b'\xc3\x84\xc3\x96\xc3\x9c'.decode('utf-8'))
        txt = self._makeOne()
        self.assertEqual(txt.unicode_normalization, 'NFC')
        self.assertEqual(
            [unicodedata.name(c) for c in txt.fromUnicode(deadbeef)],
            [
                'LATIN CAPITAL LETTER A WITH DIAERESIS',
                'LATIN CAPITAL LETTER O WITH DIAERESIS',
                'LATIN CAPITAL LETTER U WITH DIAERESIS',
            ]
        )
        txt = self._makeOne(unicode_normalization=None)
        self.assertEqual(txt.unicode_normalization, None)
        self.assertEqual(
            [unicodedata.name(c) for c in txt.fromUnicode(deadbeef)],
            [
                'LATIN CAPITAL LETTER A',
                'COMBINING DIAERESIS',
                'LATIN CAPITAL LETTER O',
                'COMBINING DIAERESIS',
                'LATIN CAPITAL LETTER U',
                'COMBINING DIAERESIS',
            ]
        )

    def test_normalization_after_pickle(self):
        # test an edge case where `Text` is persisted
        # see https://github.com/zopefoundation/zope.schema/issues/90
        import pickle
        orig_makeOne = self._makeOne

        def makeOne(**kwargs):
            result = orig_makeOne(**kwargs)
            if not kwargs:
                # We should have no state to preserve
                result.__dict__.clear()

            result = pickle.loads(pickle.dumps(result))
            return result

        self._makeOne = makeOne
        self.test_normalization()


class TextLineTests(EqualityTestsMixin,
                    WrongTypeTestsMixin,
                    unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import TextLine
        return TextLine

    def _getTargetInterface(self):
        from zope.schema.interfaces import ITextLine
        return ITextLine

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            b'',
            1,
            1.0,
            (),
            [],
            {},
            set(),
            frozenset(),
            object())

    def test_validate_not_required(self):

        field = self._makeOne(required=False)
        field.validate('')
        field.validate('abc')
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing

        field = self._makeOne()
        field.validate('')
        field.validate('abc')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_constraint(self):

        field = self._makeOne()
        self.assertEqual(field.constraint(''), True)
        self.assertEqual(field.constraint('abc'), True)
        self.assertEqual(field.constraint('abc\ndef'), False)


class PasswordTests(EqualityTestsMixin,
                    WrongTypeTestsMixin,
                    unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Password
        return Password

    def _getTargetInterface(self):
        from zope.schema.interfaces import IPassword
        return IPassword

    def test_set_unchanged(self):
        klass = self._getTargetClass()
        pw = self._makeOne()
        inst = DummyInst()
        before = dict(inst.__dict__)
        pw.set(inst, klass.UNCHANGED_PASSWORD)  # doesn't raise, doesn't write
        after = dict(inst.__dict__)
        self.assertEqual(after, before)

    def test_set_normal(self):
        pw = self._makeOne(__name__='password')
        inst = DummyInst()
        pw.set(inst, 'PASSWORD')
        self.assertEqual(inst.password, 'PASSWORD')

    def test_validate_not_required(self):

        field = self._makeOne(required=False)
        field.validate('')
        field.validate('abc')
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing

        field = self._makeOne()
        field.validate('')
        field.validate('abc')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_validate_unchanged_not_already_set(self):
        klass = self._getTargetClass()
        inst = DummyInst()
        pw = self._makeOne(__name__='password').bind(inst)
        self.assertRaisesWrongType(pw, pw._type, klass.UNCHANGED_PASSWORD)

    def test_validate_unchanged_already_set(self):
        klass = self._getTargetClass()
        inst = DummyInst()
        inst.password = 'foobar'
        pw = self._makeOne(__name__='password').bind(inst)
        pw.validate(klass.UNCHANGED_PASSWORD)  # doesn't raise

    def test_constraint(self):

        field = self._makeOne()
        self.assertEqual(field.constraint(''), True)
        self.assertEqual(field.constraint('abc'), True)
        self.assertEqual(field.constraint('abc\ndef'), False)


class BoolTests(EqualityTestsMixin,
                WrongTypeTestsMixin,
                unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Bool
        return Bool

    def _getTargetInterface(self):
        from zope.schema.interfaces import IBool
        return IBool

    def test_ctor_defaults(self):
        txt = self._makeOne()
        self.assertEqual(txt._type, bool)

    def test_validate_wrong_type(self):
        boo = self._makeOne()
        self.assertRaisesWrongType(boo, boo._type, '')

    def test__validate_w_int(self):
        boo = self._makeOne()
        boo._validate(0)  # doesn't raise
        boo._validate(1)  # doesn't raise

    def test__validate_w_bool(self):
        boo = self._makeOne()
        boo._validate(False)  # doesn't raise
        boo._validate(True)   # doesn't raise

    def test_set_w_int(self):
        boo = self._makeOne(__name__='boo')
        inst = DummyInst()
        boo.set(inst, 0)
        self.assertEqual(inst.boo, False)
        boo.set(inst, 1)
        self.assertEqual(inst.boo, True)

    def test_set_w_bool(self):
        boo = self._makeOne(__name__='boo')
        inst = DummyInst()
        boo.set(inst, False)
        self.assertEqual(inst.boo, False)
        boo.set(inst, True)
        self.assertEqual(inst.boo, True)

    def test_fromUnicode_miss(self):

        txt = self._makeOne()
        self.assertEqual(txt.fromUnicode(''), False)
        self.assertEqual(txt.fromUnicode('0'), False)
        self.assertEqual(txt.fromUnicode('1'), False)
        self.assertEqual(txt.fromUnicode('False'), False)
        self.assertEqual(txt.fromUnicode('false'), False)

    def test_fromUnicode_hit(self):

        txt = self._makeOne()
        self.assertEqual(txt.fromUnicode('True'), True)
        self.assertEqual(txt.fromUnicode('true'), True)


class NumberTests(EqualityTestsMixin,
                  OrderableMissingValueMixin,
                  OrderableTestsMixin,
                  unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Number
        return Number

    def _getTargetInterface(self):
        from zope.schema.interfaces import INumber
        return INumber

    def test_class_conforms_to_iface(self):
        from zope.interface.verify import verifyClass

        from zope.schema._bootstrapinterfaces import IFromBytes
        from zope.schema._bootstrapinterfaces import IFromUnicode
        super().test_class_conforms_to_iface()
        verifyClass(IFromUnicode, self._getTargetClass())
        verifyClass(IFromBytes, self._getTargetClass())

    def test_instance_conforms_to_iface(self):
        from zope.interface.verify import verifyObject

        from zope.schema._bootstrapinterfaces import IFromBytes
        from zope.schema._bootstrapinterfaces import IFromUnicode
        super().test_instance_conforms_to_iface()
        verifyObject(IFromUnicode, self._makeOne())
        verifyObject(IFromBytes, self._makeOne())


class ComplexTests(NumberTests):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Complex
        return Complex

    def _getTargetInterface(self):
        from zope.schema.interfaces import IComplex
        return IComplex


class RealTests(WrongTypeTestsMixin,
                NumberTests):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Real
        return Real

    def _getTargetInterface(self):
        from zope.schema.interfaces import IReal
        return IReal

    def test_ctor_real_min_max(self):
        from fractions import Fraction

        self.assertRaisesWrongType(
            self._makeOne, self._getTargetClass()._type, min='')
        self.assertRaisesWrongType(
            self._makeOne, self._getTargetClass()._type, max='')

        field = self._makeOne(min=Fraction(1, 2), max=2)
        field.validate(1.0)
        field.validate(2.0)
        self.assertRaisesTooSmall(field, 0)
        self.assertRaisesTooSmall(field, 0.4)
        self.assertRaisesTooBig(field, 2.1)


class RationalTests(NumberTests):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Rational
        return Rational

    def _getTargetInterface(self):
        from zope.schema.interfaces import IRational
        return IRational


class IntegralTests(RationalTests):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Integral
        return Integral

    def _getTargetInterface(self):
        from zope.schema.interfaces import IIntegral
        return IIntegral

    def test_validate_not_required(self):
        field = self._makeOne(required=False)
        field.validate(None)
        field.validate(10)
        field.validate(0)
        field.validate(-1)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate(10)
        field.validate(0)
        field.validate(-1)
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_fromUnicode_miss(self):
        txt = self._makeOne()
        self.assertRaises(ValueError, txt.fromUnicode, '')
        self.assertRaises(ValueError, txt.fromUnicode, 'False')
        self.assertRaises(ValueError, txt.fromUnicode, 'True')

    def test_fromUnicode_hit(self):

        txt = self._makeOne()
        self.assertEqual(txt.fromUnicode('0'), 0)
        self.assertEqual(txt.fromUnicode('1'), 1)
        self.assertEqual(txt.fromUnicode('-1'), -1)


class IntTests(IntegralTests):

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Int
        return Int

    def _getTargetInterface(self):
        from zope.schema.interfaces import IInt
        return IInt

    def test_ctor_defaults(self):
        txt = self._makeOne()
        self.assertEqual(txt._type, int)


class DecimalTests(NumberTests):

    mvm_missing_value = decimal.Decimal("-1")
    mvm_default = decimal.Decimal("0")

    MIN = decimal.Decimal(NumberTests.MIN)
    MAX = decimal.Decimal(NumberTests.MAX)
    VALID = tuple(decimal.Decimal(x) for x in NumberTests.VALID)
    TOO_SMALL = tuple(decimal.Decimal(x) for x in NumberTests.TOO_SMALL)
    TOO_BIG = tuple(decimal.Decimal(x) for x in NumberTests.TOO_BIG)

    def _getTargetClass(self):
        from zope.schema._bootstrapfields import Decimal
        return Decimal

    def _getTargetInterface(self):
        from zope.schema.interfaces import IDecimal
        return IDecimal

    def test_validate_not_required(self):
        field = self._makeOne(required=False)
        field.validate(decimal.Decimal("10.0"))
        field.validate(decimal.Decimal("0.93"))
        field.validate(decimal.Decimal("1000.0003"))
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate(decimal.Decimal("10.0"))
        field.validate(decimal.Decimal("0.93"))
        field.validate(decimal.Decimal("1000.0003"))
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_fromUnicode_miss(self):
        from zope.schema.interfaces import ValidationError
        flt = self._makeOne()
        self.assertRaises(ValueError, flt.fromUnicode, '')
        self.assertRaises(ValueError, flt.fromUnicode, 'abc')
        with self.assertRaises(ValueError) as exc:
            flt.fromUnicode('1.4G')

        value_error = exc.exception
        self.assertIs(value_error.field, flt)
        self.assertEqual(value_error.value, '1.4G')
        self.assertIsInstance(value_error, ValidationError)

    def test_fromUnicode_hit(self):
        from decimal import Decimal

        flt = self._makeOne()
        self.assertEqual(flt.fromUnicode('0'), Decimal('0.0'))
        self.assertEqual(flt.fromUnicode('1.23'), Decimal('1.23'))
        self.assertEqual(flt.fromUnicode('12345.6'), Decimal('12345.6'))


class ObjectTests(EqualityTestsMixin,
                  WrongTypeTestsMixin,
                  unittest.TestCase):

    def setUp(self):
        from zope.event import subscribers
        self._before = subscribers[:]

    def tearDown(self):
        from zope.event import subscribers
        subscribers[:] = self._before

    def _getTargetClass(self):
        from zope.schema._field import Object
        return Object

    def _getTargetInterface(self):
        from zope.schema.interfaces import IObject
        return IObject

    def _makeOneFromClass(self, cls, schema=None, *args, **kw):
        if schema is None:
            schema = self._makeSchema()
        return super()._makeOneFromClass(
            cls, schema, *args, **kw)

    def _makeSchema(self, **kw):
        from zope.interface import Interface
        from zope.interface.interface import InterfaceClass
        return InterfaceClass('ISchema', (Interface,), kw)

    def _getErrors(self, f, *args, **kw):
        from zope.schema.interfaces import SchemaNotCorrectlyImplemented
        with self.assertRaises(SchemaNotCorrectlyImplemented) as e:
            f(*args, **kw)
        return e.exception.errors

    def _makeCycles(self):
        from zope.interface import Interface
        from zope.interface import implementer

        from zope.schema import List
        from zope.schema import Object
        from zope.schema._messageid import _

        class IUnit(Interface):
            """A schema that participate to a cycle"""
            boss = Object(
                schema=Interface,
                title=_("Boss"),
                description=_("Boss description"),
                required=False,
            )
            members = List(
                value_type=Object(schema=Interface),
                title=_("Member List"),
                description=_("Member list description"),
                required=False,
            )

        class IPerson(Interface):
            """A schema that participate to a cycle"""
            unit = Object(
                schema=IUnit,
                title=_("Unit"),
                description=_("Unit description"),
                required=False,
            )

        IUnit['boss'].schema = IPerson
        IUnit['members'].value_type.schema = IPerson

        @implementer(IUnit)
        class Unit:
            def __init__(self, person, person_list):
                self.boss = person
                self.members = person_list

        @implementer(IPerson)
        class Person:
            def __init__(self, unit):
                self.unit = unit

        return IUnit, Person, Unit

    def test_class_conforms_to_IObject(self):
        from zope.interface.verify import verifyClass

        from zope.schema.interfaces import IObject
        verifyClass(IObject, self._getTargetClass())

    def test_instance_conforms_to_IObject(self):
        from zope.interface.verify import verifyObject

        from zope.schema.interfaces import IObject
        verifyObject(IObject, self._makeOne())

    def test_ctor_w_bad_schema(self):
        from zope.interface.interfaces import IInterface
        self.assertRaisesWrongType(self._makeOne, IInterface, object())

    def test_validate_not_required(self):
        schema = self._makeSchema()
        objf = self._makeOne(schema, required=False)
        objf.validate(None)  # doesn't raise

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne(required=True)
        self.assertRaises(RequiredMissing, field.validate, None)

    def test__validate_w_empty_schema(self):
        from zope.interface import Interface
        objf = self._makeOne(Interface)
        objf.validate(object())  # doesn't raise

    def test__validate_w_value_not_providing_schema(self):
        from zope.schema._bootstrapfields import Text
        from zope.schema.interfaces import SchemaNotProvided
        schema = self._makeSchema(foo=Text(), bar=Text())
        objf = self._makeOne(schema)
        bad_value = object()
        with self.assertRaises(SchemaNotProvided) as exc:
            objf.validate(bad_value)

        not_provided = exc.exception
        self.assertIs(not_provided.field, objf)
        self.assertIs(not_provided.value, bad_value)
        self.assertEqual(not_provided.args, (schema, bad_value), )

    def test__validate_w_value_providing_schema_but_missing_fields(self):
        from zope.interface import implementer

        from zope.schema._bootstrapfields import Text
        from zope.schema.interfaces import SchemaNotCorrectlyImplemented
        from zope.schema.interfaces import SchemaNotFullyImplemented
        schema = self._makeSchema(foo=Text(), bar=Text())

        @implementer(schema)
        class Broken:
            pass

        objf = self._makeOne(schema)
        broken = Broken()
        with self.assertRaises(SchemaNotCorrectlyImplemented) as exc:
            objf.validate(broken)

        wct = exc.exception
        self.assertIs(wct.field, objf)
        self.assertIs(wct.value, broken)
        self.assertEqual(wct.invariant_errors, [])
        self.assertEqual(
            sorted(wct.schema_errors),
            ['bar', 'foo']
        )
        for name in ('foo', 'bar'):
            error = wct.schema_errors[name]
            self.assertIsInstance(error,
                                  SchemaNotFullyImplemented)
            self.assertEqual(schema[name], error.field)
            self.assertIsNone(error.value)

        # The legacy arg[0] errors list
        errors = self._getErrors(objf.validate, Broken())
        self.assertEqual(len(errors), 2)
        errors = sorted(errors,
                        key=lambda x: (type(x).__name__, str(x.args[0])))
        err = errors[0]
        self.assertIsInstance(err, SchemaNotFullyImplemented)
        nested = err.args[0]
        self.assertIsInstance(nested, AttributeError)
        self.assertIn("'bar'", str(nested))
        err = errors[1]
        self.assertIsInstance(err, SchemaNotFullyImplemented)
        nested = err.args[0]
        self.assertIsInstance(nested, AttributeError)
        self.assertIn("'foo'", str(nested))

    def test__validate_w_value_providing_schema_but_invalid_fields(self):
        from zope.interface import implementer

        from zope.schema._bootstrapfields import Text
        from zope.schema.interfaces import RequiredMissing
        from zope.schema.interfaces import SchemaNotCorrectlyImplemented
        from zope.schema.interfaces import WrongType
        schema = self._makeSchema(foo=Text(), bar=Text())

        @implementer(schema)
        class Broken:
            foo = None
            bar = 1

        objf = self._makeOne(schema)
        broken = Broken()
        with self.assertRaises(SchemaNotCorrectlyImplemented) as exc:
            objf.validate(broken)

        wct = exc.exception
        self.assertIs(wct.field, objf)
        self.assertIs(wct.value, broken)
        self.assertEqual(wct.invariant_errors, [])
        self.assertEqual(
            sorted(wct.schema_errors),
            ['bar', 'foo']
        )
        self.assertIsInstance(wct.schema_errors['foo'], RequiredMissing)
        self.assertIsInstance(wct.schema_errors['bar'], WrongType)

        # The legacy arg[0] errors list
        errors = self._getErrors(objf.validate, Broken())
        self.assertEqual(len(errors), 2)
        errors = sorted(errors, key=lambda x: type(x).__name__)
        err = errors[0]
        self.assertIsInstance(err, RequiredMissing)
        self.assertEqual(err.args, ('foo',))
        err = errors[1]
        self.assertIsInstance(err, WrongType)
        self.assertEqual(err.args, (1, str, 'bar'))

    def test__validate_w_value_providing_schema(self):
        from zope.interface import implementer

        from zope.schema._bootstrapfields import Text
        from zope.schema._field import Choice

        schema = self._makeSchema(
            foo=Text(),
            bar=Text(),
            baz=Choice(values=[1, 2, 3]),
        )

        @implementer(schema)
        class OK:
            foo = 'Foo'
            bar = 'Bar'
            baz = 2
        objf = self._makeOne(schema)
        objf.validate(OK())  # doesn't raise

    def test_validate_w_cycles(self):
        IUnit, Person, Unit = self._makeCycles()
        field = self._makeOne(schema=IUnit)
        person1 = Person(None)
        person2 = Person(None)
        unit = Unit(person1, [person1, person2])
        person1.unit = unit
        person2.unit = unit
        field.validate(unit)  # doesn't raise

    def test_validate_w_cycles_object_not_valid(self):
        from zope.schema.interfaces import SchemaNotCorrectlyImplemented
        from zope.schema.interfaces import SchemaNotProvided
        IUnit, Person, Unit = self._makeCycles()
        field = self._makeOne(schema=IUnit)
        person1 = Person(None)
        person2 = Person(None)
        boss_unit = object()
        boss = Person(boss_unit)
        unit = Unit(boss, [person1, person2])
        person1.unit = unit
        person2.unit = unit
        with self.assertRaises(SchemaNotCorrectlyImplemented) as exc:
            field.validate(unit)

        ex = exc.exception
        self.assertEqual(1, len(ex.schema_errors))
        self.assertEqual(1, len(ex.errors))
        self.assertEqual(0, len(ex.invariant_errors))

        boss_error = ex.schema_errors['boss']
        self.assertIsInstance(boss_error, SchemaNotCorrectlyImplemented)

        self.assertEqual(1, len(boss_error.schema_errors))
        self.assertEqual(1, len(boss_error.errors))
        self.assertEqual(0, len(boss_error.invariant_errors))

        unit_error = boss_error.schema_errors['unit']
        self.assertIsInstance(unit_error, SchemaNotProvided)
        self.assertIs(IUnit, unit_error.schema)
        self.assertIs(boss_unit, unit_error.value)

    def test_validate_w_cycles_collection_not_valid(self):
        from zope.schema.interfaces import SchemaNotCorrectlyImplemented
        IUnit, Person, Unit = self._makeCycles()
        field = self._makeOne(schema=IUnit)
        person1 = Person(None)
        person2 = Person(None)
        person3 = Person(object())
        unit = Unit(person1, [person2, person3])
        person1.unit = unit
        person2.unit = unit
        self.assertRaises(SchemaNotCorrectlyImplemented, field.validate, unit)

    def test_set_emits_IBOAE(self):
        from zope.event import subscribers
        from zope.interface import implementer

        from zope.schema._bootstrapfields import Text
        from zope.schema._field import Choice
        from zope.schema.interfaces import IBeforeObjectAssignedEvent

        schema = self._makeSchema(
            foo=Text(),
            bar=Text(),
            baz=Choice(values=[1, 2, 3]),
        )

        @implementer(schema)
        class OK:
            foo = 'Foo'
            bar = 'Bar'
            baz = 2
        log = []
        subscribers.append(log.append)
        objf = self._makeOne(schema, __name__='field')
        inst = DummyInst()
        value = OK()
        objf.set(inst, value)
        self.assertIs(inst.field, value)
        self.assertEqual(len(log), 5)
        self.assertEqual(IBeforeObjectAssignedEvent.providedBy(log[-1]), True)
        self.assertEqual(log[-1].object, value)
        self.assertEqual(log[-1].name, 'field')
        self.assertEqual(log[-1].context, inst)

    def test_set_allows_IBOAE_subscr_to_replace_value(self):
        from zope.event import subscribers
        from zope.interface import implementer

        from zope.schema._bootstrapfields import Text
        from zope.schema._field import Choice

        schema = self._makeSchema(
            foo=Text(),
            bar=Text(),
            baz=Choice(values=[1, 2, 3]),
        )

        @implementer(schema)
        class OK:
            def __init__(self, foo='Foo', bar='Bar', baz=2):
                self.foo = foo
                self.bar = bar
                self.baz = baz
        ok1 = OK()
        ok2 = OK('Foo2', 'Bar2', 3)
        log = []
        subscribers.append(log.append)

        def _replace(event):
            event.object = ok2
        subscribers.append(_replace)
        objf = self._makeOne(schema, __name__='field')
        inst = DummyInst()
        self.assertEqual(len(log), 4)
        objf.set(inst, ok1)
        self.assertIs(inst.field, ok2)
        self.assertEqual(len(log), 5)
        self.assertEqual(log[-1].object, ok2)
        self.assertEqual(log[-1].name, 'field')
        self.assertEqual(log[-1].context, inst)

    def test_validates_invariants_by_default(self):
        from zope.interface import Interface
        from zope.interface import Invalid
        from zope.interface import implementer
        from zope.interface import invariant

        from zope.schema import Bytes
        from zope.schema import Text

        class ISchema(Interface):

            foo = Text()
            bar = Bytes()

            @invariant
            def check_foo(self):
                if self.foo == 'bar':
                    raise Invalid("Foo is not valid")

            @invariant
            def check_bar(self):
                if self.bar == b'foo':
                    raise Invalid("Bar is not valid")

        @implementer(ISchema)
        class Obj:
            foo = ''
            bar = b''

        field = self._makeOne(ISchema)
        inst = Obj()

        # Fine at first
        field.validate(inst)

        inst.foo = 'bar'
        errors = self._getErrors(field.validate, inst)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].args[0], "Foo is not valid")

        del inst.foo
        inst.bar = b'foo'
        errors = self._getErrors(field.validate, inst)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].args[0], "Bar is not valid")

        # Both invalid
        inst.foo = 'bar'
        errors = self._getErrors(field.validate, inst)
        self.assertEqual(len(errors), 2)
        errors.sort(key=lambda i: i.args)
        self.assertEqual(errors[0].args[0], "Bar is not valid")
        self.assertEqual(errors[1].args[0], "Foo is not valid")

        # We can specifically ask for invariants to be turned off.
        field = self._makeOne(ISchema, validate_invariants=False)
        field.validate(inst)

    def test_schema_defined_by_subclass(self):
        from zope import interface
        from zope.schema.interfaces import SchemaNotProvided

        class IValueType(interface.Interface):
            "The value type schema"

        class Field(self._getTargetClass()):
            schema = IValueType

        field = Field()
        self.assertIs(field.schema, IValueType)

        # Non implementation is bad
        with self.assertRaises(SchemaNotProvided) as exc:
            field.validate(object())

        self.assertIs(IValueType, exc.exception.schema)

        # Actual implementation works
        @interface.implementer(IValueType)
        class ValueType:
            "The value type"

        field.validate(ValueType())

    def test_bound_field_of_collection_with_choice(self):
        # https://github.com/zopefoundation/zope.schema/issues/17
        from zope.interface import Attribute
        from zope.interface import Interface
        from zope.interface import implementer

        from zope.schema import Choice
        from zope.schema import Object
        from zope.schema import Set
        from zope.schema.fieldproperty import FieldProperty
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import IContextSourceBinder
        from zope.schema.interfaces import SchemaNotCorrectlyImplemented
        from zope.schema.interfaces import WrongContainedType
        from zope.schema.vocabulary import SimpleVocabulary

        @implementer(IContextSourceBinder)
        class EnumContext:
            def __call__(self, context):
                return SimpleVocabulary.fromValues(list(context))

        class IMultipleChoice(Interface):
            choices = Set(value_type=Choice(source=EnumContext()))
            # Provide a regular attribute to prove that binding doesn't
            # choke. NOTE: We don't actually verify the existence of this
            # attribute.
            non_field = Attribute("An attribute")

        @implementer(IMultipleChoice)
        class Choices:

            def __init__(self, choices):
                self.choices = choices

            def __iter__(self):
                # EnumContext calls this to make the vocabulary.
                # Fields of the schema of the IObject are bound to the value
                # being validated.
                return iter(range(5))

        class IFavorites(Interface):
            fav = Object(title="Favorites number", schema=IMultipleChoice)

        @implementer(IFavorites)
        class Favorites:
            fav = FieldProperty(IFavorites['fav'])

        # must not raise
        good_choices = Choices({1, 3})
        IFavorites['fav'].validate(good_choices)

        # Ranges outside the context fail
        bad_choices = Choices({1, 8})
        with self.assertRaises(SchemaNotCorrectlyImplemented) as exc:
            IFavorites['fav'].validate(bad_choices)

        e = exc.exception
        self.assertEqual(IFavorites['fav'], e.field)
        self.assertEqual(bad_choices, e.value)
        self.assertEqual(1, len(e.schema_errors))
        self.assertEqual(0, len(e.invariant_errors))
        self.assertEqual(1, len(e.errors))

        fav_error = e.schema_errors['choices']
        self.assertIs(fav_error, e.errors[0])
        self.assertIsInstance(fav_error, WrongContainedType)
        self.assertNotIsInstance(fav_error, SchemaNotCorrectlyImplemented)
        # The field is not actually equal to the one in the interface
        # anymore because its bound.
        self.assertEqual('choices', fav_error.field.__name__)
        self.assertEqual(bad_choices, fav_error.field.context)
        self.assertEqual({1, 8}, fav_error.value)
        self.assertEqual(1, len(fav_error.errors))

        self.assertIsInstance(fav_error.errors[0], ConstraintNotSatisfied)

        # Validation through field property
        favorites = Favorites()
        favorites.fav = good_choices

        # And validation through a field that wants IFavorites
        favorites_field = Object(IFavorites)
        favorites_field.validate(favorites)

        # Check the field property error
        with self.assertRaises(SchemaNotCorrectlyImplemented) as exc:
            favorites.fav = bad_choices

        e = exc.exception
        self.assertEqual(IFavorites['fav'], e.field)
        self.assertEqual(bad_choices, e.value)
        self.assertEqual(['choices'], list(e.schema_errors))

    def test_getDoc(self):
        field = self._makeOne()
        doc = field.getDoc()
        self.assertIn(":Must Provide: :class:", doc)


class DummyInst:
    missing_value = object()

    def __init__(self, exc=None):
        self._exc = exc

    def validate(self, value):
        if self._exc is not None:  # pragma: no branch
            raise self._exc()


def _test_suite():
    import zope.schema._bootstrapfields
    suite = unittest.defaultTestLoader.loadTestsFromName(__name__)
    suite.addTests(doctest.DocTestSuite(
        zope.schema._bootstrapfields,
        optionflags=doctest.ELLIPSIS
    ))
    return suite
