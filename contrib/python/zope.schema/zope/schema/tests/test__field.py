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
import datetime
import doctest
import unittest

from __tests__.tests.test__bootstrapfields import EqualityTestsMixin
from __tests__.tests.test__bootstrapfields import LenTestsMixin
from __tests__.tests.test__bootstrapfields import NumberTests
from __tests__.tests.test__bootstrapfields import OrderableMissingValueMixin
from __tests__.tests.test__bootstrapfields import OrderableTestsMixin
from __tests__.tests.test__bootstrapfields import WrongTypeTestsMixin


# pylint:disable=protected-access
# pylint:disable=too-many-lines
# pylint:disable=inherit-non-class
# pylint:disable=no-member
# pylint:disable=blacklisted-name


class BytesTests(EqualityTestsMixin,
                 WrongTypeTestsMixin,
                 unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import Bytes
        return Bytes

    def _getTargetInterface(self):
        from zope.schema.interfaces import IBytes
        return IBytes

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
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
        self.assertRaises(ValidationError, self._makeOne, default='')

    def test_validate_not_required(self):

        field = self._makeOne(required=False)
        field.validate(b'')
        field.validate(b'abc')
        field.validate(b'abc\ndef')
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing

        field = self._makeOne()
        field.validate(b'')
        field.validate(b'abc')
        field.validate(b'abc\ndef')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_fromUnicode_miss(self):
        byt = self._makeOne()
        self.assertRaises(UnicodeEncodeError, byt.fromUnicode, '\x81')

    def test_fromUnicode_hit(self):
        byt = self._makeOne()
        self.assertEqual(byt.fromUnicode(''), b'')
        self.assertEqual(byt.fromUnicode('DEADBEEF'), b'DEADBEEF')

    def test_fromBytes(self):
        field = self._makeOne()
        self.assertEqual(field.fromBytes(b''), b'')
        self.assertEqual(field.fromBytes(b'DEADBEEF'), b'DEADBEEF')


class ASCIITests(EqualityTestsMixin,
                 WrongTypeTestsMixin,
                 unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import ASCII
        return ASCII

    def _getTargetInterface(self):
        from zope.schema.interfaces import IASCII
        return IASCII

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

    def test__validate_empty(self):
        asc = self._makeOne()
        asc._validate('')  # no error

    def test__validate_non_empty_miss(self):
        from zope.schema.interfaces import InvalidValue
        asc = self._makeOne()
        with self.assertRaises(InvalidValue) as exc:
            asc._validate(chr(129))

        invalid = exc.exception
        self.assertIs(invalid.field, asc)
        self.assertEqual(invalid.value, chr(129))

    def test__validate_non_empty_hit(self):
        asc = self._makeOne()
        for i in range(128):
            asc._validate(chr(i))  # doesn't raise


class BytesLineTests(EqualityTestsMixin,
                     WrongTypeTestsMixin,
                     unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import BytesLine
        return BytesLine

    def _getTargetInterface(self):
        from zope.schema.interfaces import IBytesLine
        return IBytesLine

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
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
        field.validate(None)
        field.validate(b'')
        field.validate(b'abc')
        field.validate(b'\xab\xde')

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing

        field = self._makeOne()
        field.validate(b'')
        field.validate(b'abc')
        field.validate(b'\xab\xde')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_constraint(self):
        field = self._makeOne()
        self.assertEqual(field.constraint(b''), True)
        self.assertEqual(field.constraint(b'abc'), True)
        self.assertEqual(field.constraint(b'abc'), True)
        self.assertEqual(field.constraint(b'\xab\xde'), True)
        self.assertEqual(field.constraint(b'abc\ndef'), False)

    def test_fromBytes(self):
        field = self._makeOne()
        self.assertEqual(field.fromBytes(b''), b'')
        self.assertEqual(field.fromBytes(b'DEADBEEF'), b'DEADBEEF')


class ASCIILineTests(EqualityTestsMixin,
                     WrongTypeTestsMixin,
                     unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import ASCIILine
        return ASCIILine

    def _getTargetInterface(self):
        from zope.schema.interfaces import IASCIILine
        return IASCIILine

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
        from zope.schema.interfaces import InvalidValue
        field = self._makeOne(required=False)
        field.validate(None)
        field.validate('')
        field.validate('abc')
        self.assertRaises(InvalidValue, field.validate, '\xab\xde')

    def test_validate_required(self):
        from zope.schema.interfaces import InvalidValue
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate('')
        field.validate('abc')
        self.assertRaises(InvalidValue, field.validate, '\xab\xde')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_constraint(self):
        field = self._makeOne()
        self.assertEqual(field.constraint(''), True)
        self.assertEqual(field.constraint('abc'), True)
        self.assertEqual(field.constraint('abc'), True)
        # Non-ASCII byltes get checked in '_validate'.
        self.assertEqual(field.constraint('\xab\xde'), True)
        self.assertEqual(field.constraint('abc\ndef'), False)


class FloatTests(NumberTests):

    mvm_missing_value = -1.0
    mvm_default = 0.0

    MIN = float(NumberTests.MIN)
    MAX = float(NumberTests.MAX)
    VALID = tuple(float(x) for x in NumberTests.VALID)
    TOO_SMALL = tuple(float(x) for x in NumberTests.TOO_SMALL)
    TOO_BIG = tuple(float(x) for x in NumberTests.TOO_BIG)

    def _getTargetClass(self):
        from zope.schema._field import Float
        return Float

    def _getTargetInterface(self):
        from zope.schema.interfaces import IFloat
        return IFloat

    def test_validate_not_required(self):
        field = self._makeOne(required=False)
        field.validate(None)
        field.validate(10.0)
        field.validate(0.93)
        field.validate(1000.0003)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate(10.0)
        field.validate(0.93)
        field.validate(1000.0003)
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_fromUnicode_miss(self):

        flt = self._makeOne()
        self.assertRaises(ValueError, flt.fromUnicode, '')
        self.assertRaises(ValueError, flt.fromUnicode, 'abc')
        self.assertRaises(ValueError, flt.fromUnicode, '14.G')

    def test_fromUnicode_hit(self):

        flt = self._makeOne()
        self.assertEqual(flt.fromUnicode('0'), 0.0)
        self.assertEqual(flt.fromUnicode('1.23'), 1.23)
        self.assertEqual(flt.fromUnicode('1.23e6'), 1230000.0)


class DatetimeTests(OrderableMissingValueMixin,
                    OrderableTestsMixin,
                    EqualityTestsMixin,
                    WrongTypeTestsMixin,
                    unittest.TestCase):

    mvm_missing_value = datetime.datetime.now()
    mvm_default = datetime.datetime.now()

    def _getTargetClass(self):
        from zope.schema._field import Datetime
        return Datetime

    def _getTargetInterface(self):
        from zope.schema.interfaces import IDatetime
        return IDatetime

    def test_validate_wrong_types(self):
        from datetime import date
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
            1,
            1.0,
            (),
            [],
            {},
            set(),
            frozenset(),
            object(),
            date.today())

    def test_validate_not_required(self):
        field = self._makeOne(required=False)
        field.validate(None)  # doesn't raise
        field.validate(datetime.datetime.now())  # doesn't raise

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne(required=True)
        self.assertRaises(RequiredMissing, field.validate, None)

    MIN = datetime.datetime(2000, 10, 1)
    MAX = datetime.datetime(2000, 10, 4)
    TOO_BIG = tuple(datetime.datetime(2000, 10, x) for x in (5, 6))
    TOO_SMALL = tuple(datetime.datetime(2000, 9, x) for x in (5, 6))
    VALID = tuple(datetime.datetime(2000, 10, x) for x in (1, 2, 3, 4))


class DateTests(OrderableMissingValueMixin,
                OrderableTestsMixin,
                EqualityTestsMixin,
                WrongTypeTestsMixin,
                unittest.TestCase):

    mvm_missing_value = datetime.date.today()
    mvm_default = datetime.date.today()

    def _getTargetClass(self):
        from zope.schema._field import Date
        return Date

    def _getTargetInterface(self):
        from zope.schema.interfaces import IDate
        return IDate

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
            1,
            1.0,
            (),
            [],
            {},
            set(),
            frozenset(),
            object(),
            datetime.datetime.now())

    def test_validate_not_required(self):
        from datetime import date
        field = self._makeOne(required=False)
        field.validate(None)
        field.validate(date.today())

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate(datetime.datetime.now().date())
        self.assertRaises(RequiredMissing, field.validate, None)

    MIN = datetime.date(2000, 10, 1)
    MAX = datetime.date(2000, 10, 4)
    TOO_BIG = tuple(datetime.date(2000, 10, x) for x in (5, 6))
    TOO_SMALL = tuple(datetime.date(2000, 9, x) for x in (5, 6))
    VALID = tuple(datetime.date(2000, 10, x) for x in (1, 2, 3, 4))


class TimedeltaTests(OrderableMissingValueMixin,
                     OrderableTestsMixin,
                     EqualityTestsMixin,
                     unittest.TestCase):

    mvm_missing_value = datetime.timedelta(minutes=15)
    mvm_default = datetime.timedelta(minutes=12)

    def _getTargetClass(self):
        from zope.schema._field import Timedelta
        return Timedelta

    def _getTargetInterface(self):
        from zope.schema.interfaces import ITimedelta
        return ITimedelta

    def test_validate_not_required(self):
        from datetime import timedelta
        field = self._makeOne(required=False)
        field.validate(None)
        field.validate(timedelta(minutes=15))

    def test_validate_required(self):
        from datetime import timedelta

        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate(timedelta(minutes=15))
        self.assertRaises(RequiredMissing, field.validate, None)

    MIN = datetime.timedelta(minutes=NumberTests.MIN)
    MAX = datetime.timedelta(minutes=NumberTests.MAX)
    VALID = tuple(datetime.timedelta(minutes=x) for x in NumberTests.VALID)
    TOO_SMALL = tuple(
        datetime.timedelta(minutes=x) for x in NumberTests.TOO_SMALL)
    TOO_BIG = tuple(datetime.timedelta(x) for x in NumberTests.TOO_BIG)


class TimeTests(OrderableMissingValueMixin,
                OrderableTestsMixin,
                EqualityTestsMixin,
                unittest.TestCase):

    mvm_missing_value = datetime.time(12, 15, 37)
    mvm_default = datetime.time(12, 25, 42)

    def _getTargetClass(self):
        from zope.schema._field import Time
        return Time

    def _getTargetInterface(self):
        from zope.schema.interfaces import ITime
        return ITime

    def test_validate_not_required(self):
        from datetime import time
        field = self._makeOne(required=False)
        field.validate(None)
        field.validate(time(12, 15, 37))

    def test_validate_required(self):
        from datetime import time

        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate(time(12, 15, 37))
        self.assertRaises(RequiredMissing, field.validate, None)

    MIN = datetime.time(12, 10, 1)
    MAX = datetime.time(12, 10, 4)
    TOO_BIG = tuple(datetime.time(12, 10, x) for x in (5, 6))
    TOO_SMALL = tuple(datetime.time(12, 9, x) for x in (5, 6))
    VALID = tuple(datetime.time(12, 10, x) for x in (1, 2, 3, 4))


class ChoiceTests(EqualityTestsMixin,
                  unittest.TestCase):

    def setUp(self):
        from zope.schema.vocabulary import _clear
        _clear()

    def tearDown(self):
        from zope.schema.vocabulary import _clear
        _clear()

    def _getTargetClass(self):
        from zope.schema._field import Choice
        return Choice

    def _makeOneFromClass(self, cls, *args, **kwargs):
        if (not args
                and 'vocabulary' not in kwargs
                and 'values' not in kwargs
                and 'source' not in kwargs):
            from zope.schema.vocabulary import SimpleVocabulary
            kwargs['vocabulary'] = SimpleVocabulary.fromValues([1, 2, 3])
        return super()._makeOneFromClass(cls, *args, **kwargs)

    def _getTargetInterface(self):
        from zope.schema.interfaces import IChoice
        return IChoice

    def test_ctor_wo_values_vocabulary_or_source(self):
        self.assertRaises(ValueError, self._getTargetClass())

    def test_ctor_invalid_vocabulary(self):
        self.assertRaises(ValueError,
                          self._getTargetClass(), vocabulary=object())

    def test_ctor_invalid_source(self):
        self.assertRaises(ValueError, self._getTargetClass(), source=object())

    def test_ctor_both_vocabulary_and_source(self):
        self.assertRaises(
            ValueError,
            self._makeOne, vocabulary='voc.name', source=object()
        )

    def test_ctor_both_vocabulary_and_values(self):
        self.assertRaises(ValueError,
                          self._makeOne, vocabulary='voc.name', values=[1, 2])

    def test_ctor_w_values(self):
        from zope.schema.vocabulary import SimpleVocabulary
        choose = self._makeOne(values=[1, 2])
        self.assertIsInstance(choose.vocabulary, SimpleVocabulary)
        self.assertEqual(sorted(choose.vocabulary.by_value.keys()), [1, 2])
        self.assertEqual(sorted(choose.source.by_value.keys()), [1, 2])

    def test_ctor_w_unicode_non_ascii_values(self):
        values = ['K\xf6ln', 'D\xfcsseldorf', 'Bonn']
        choose = self._makeOne(values=values)
        self.assertEqual(sorted(choose.vocabulary.by_value.keys()),
                         sorted(values))
        self.assertEqual(sorted(choose.source.by_value.keys()),
                         sorted(values))
        self.assertEqual(
            sorted(choose.vocabulary.by_token.keys()),
            sorted([x.encode('ascii', 'backslashreplace').decode('ascii')
                    for x in values]))

    def test_ctor_w_named_vocabulary(self):
        choose = self._makeOne(vocabulary="vocab")
        self.assertEqual(choose.vocabularyName, 'vocab')

    def test_ctor_w_preconstructed_vocabulary(self):
        v = _makeSampleVocabulary()
        choose = self._makeOne(vocabulary=v)
        self.assertIs(choose.vocabulary, v)
        self.assertIsNone(choose.vocabularyName)

    def test_bind_w_preconstructed_vocabulary(self):
        from zope.schema.interfaces import ValidationError
        from zope.schema.vocabulary import setVocabularyRegistry
        v = _makeSampleVocabulary()
        setVocabularyRegistry(_makeDummyRegistry(v))
        choose = self._makeOne(vocabulary='vocab')
        bound = choose.bind(None)
        self.assertEqual(bound.vocabulary, v)
        self.assertEqual(bound.vocabularyName, 'vocab')
        bound.default = 1
        self.assertEqual(bound.default, 1)

        def _provoke(bound):
            bound.default = 42

        self.assertRaises(ValidationError, _provoke, bound)

    def test_bind_w_voc_not_ICSB(self):
        from zope.interface import implementer

        from zope.schema.interfaces import IBaseVocabulary
        from zope.schema.interfaces import ISource

        @implementer(IBaseVocabulary)
        @implementer(ISource)
        class Vocab:
            def __init__(self):
                pass

        source = self._makeOne(vocabulary=Vocab())
        instance = object()
        target = source.bind(instance)
        self.assertIs(target.vocabulary, source.vocabulary)

    def test_bind_w_voc_is_ICSB(self):
        from zope.interface import implementer

        from zope.schema.interfaces import IContextSourceBinder
        from zope.schema.interfaces import ISource

        @implementer(IContextSourceBinder)
        @implementer(ISource)
        class Vocab:
            def __init__(self, context):
                self.context = context

            def __call__(self, context):
                return self.__class__(context)

        # Chicken-egg
        source = self._makeOne(vocabulary='temp')
        source.vocabulary = Vocab(source)
        source.vocabularyName = None
        instance = object()
        target = source.bind(instance)
        self.assertIs(target.vocabulary.context, instance)

    def test_bind_w_voc_is_ICSB_but_not_ISource(self):
        from zope.interface import implementer

        from zope.schema.interfaces import IContextSourceBinder

        @implementer(IContextSourceBinder)
        class Vocab:
            def __init__(self, context):
                self.context = context

            def __call__(self, context):
                return self.__class__(context)

        # Chicken-egg
        source = self._makeOne(vocabulary='temp')
        source.vocabulary = Vocab(source)
        source.vocabularyName = None
        instance = object()
        self.assertRaises(ValueError, source.bind, instance)

    def test_fromUnicode_miss(self):
        from zope.schema.interfaces import ConstraintNotSatisfied

        flt = self._makeOne(values=('foo', 'bar', 'baz'))
        self.assertRaises(ConstraintNotSatisfied, flt.fromUnicode, '')
        self.assertRaises(ConstraintNotSatisfied, flt.fromUnicode, 'abc')
        with self.assertRaises(ConstraintNotSatisfied) as exc:
            flt.fromUnicode('1.4G')

        cns = exc.exception
        self.assertIs(cns.field, flt)
        self.assertEqual(cns.value, '1.4G')

    def test_fromUnicode_hit(self):

        flt = self._makeOne(values=('foo', 'bar', 'baz'))
        self.assertEqual(flt.fromUnicode('foo'), 'foo')
        self.assertEqual(flt.fromUnicode('bar'), 'bar')
        self.assertEqual(flt.fromUnicode('baz'), 'baz')

    def test__validate_int(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        choice = self._makeOne(values=[1, 3])
        choice._validate(1)  # doesn't raise
        choice._validate(3)  # doesn't raise
        self.assertRaises(ConstraintNotSatisfied, choice._validate, 4)

    def test__validate_string(self):

        from zope.schema.interfaces import ConstraintNotSatisfied
        choice = self._makeOne(values=['a', 'c'])
        choice._validate('a')  # doesn't raise
        choice._validate('c')  # doesn't raise
        choice._validate('c')  # doesn't raise
        self.assertRaises(ConstraintNotSatisfied, choice._validate, 'd')

    def test__validate_tuple(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        choice = self._makeOne(values=[(1, 2), (5, 6)])
        choice._validate((1, 2))  # doesn't raise
        choice._validate((5, 6))  # doesn't raise
        self.assertRaises(ConstraintNotSatisfied, choice._validate, [5, 6])
        self.assertRaises(ConstraintNotSatisfied, choice._validate, ())

    def test__validate_mixed(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        choice = self._makeOne(values=[1, 'b', (0.2,)])
        choice._validate(1)  # doesn't raise
        choice._validate('b')  # doesn't raise
        choice._validate((0.2,))  # doesn't raise
        self.assertRaises(ConstraintNotSatisfied, choice._validate, '1')
        self.assertRaises(ConstraintNotSatisfied, choice._validate, 0.2)

    def test__validate_w_named_vocabulary_invalid(self):
        from zope.schema._field import MissingVocabularyError
        choose = self._makeOne(vocabulary='vocab')
        with self.assertRaises(MissingVocabularyError) as exc:
            choose._validate(42)

        ex = exc.exception
        self.assertIsInstance(ex, ValueError)
        self.assertIsInstance(ex, LookupError)
        self.assertIs(ex.field, choose)
        self.assertEqual(ex.value, 42)

    def test__validate_w_named_vocabulary_raises_LookupError(self):
        # Whether the vocab registry raises VocabularyRegistryError
        # or the generic LookupError documented by IVocabularyLookup,
        # we do the same thing
        from zope.schema.vocabulary import setVocabularyRegistry

        class Reg:
            def get(self, *args):
                raise LookupError

        setVocabularyRegistry(Reg())
        self.test__validate_w_named_vocabulary_invalid()

    def test__validate_w_named_vocabulary_passes_context(self):
        from zope.schema.vocabulary import setVocabularyRegistry
        context = object()
        choice = self._makeOne(vocabulary='vocab')

        class Reg:
            called_with = ()

            def get(self, *args):
                self.called_with += args
                return _makeSampleVocabulary()

        reg = Reg()
        setVocabularyRegistry(reg)

        choice = choice.bind(context)
        choice._validate(1)

        self.assertEqual(reg.called_with, (context, 'vocab'))

    def test__validate_w_named_vocabulary(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.vocabulary import setVocabularyRegistry
        v = _makeSampleVocabulary()
        setVocabularyRegistry(_makeDummyRegistry(v))
        choose = self._makeOne(vocabulary='vocab')
        choose._validate(1)
        choose._validate(3)
        self.assertRaises(ConstraintNotSatisfied, choose._validate, 42)

    def test__validate_source_is_ICSB_unbound(self):
        from zope.interface import implementer

        from zope.schema.interfaces import IContextSourceBinder

        @implementer(IContextSourceBinder)
        class SampleContextSourceBinder:
            def __call__(self, context):
                raise AssertionError("This is not called")

        choice = self._makeOne(source=SampleContextSourceBinder())
        self.assertRaises(TypeError, choice.validate, 1)

    def test__validate_source_is_ICSB_bound(self):
        from zope.interface import implementer

        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import IContextSourceBinder
        from __tests__.tests.test_vocabulary import _makeSampleVocabulary

        @implementer(IContextSourceBinder)
        class SampleContextSourceBinder:
            def __call__(self, context):
                return _makeSampleVocabulary()

        s = SampleContextSourceBinder()
        choice = self._makeOne(source=s)
        # raises not iterable with unbound field
        self.assertRaises(TypeError, choice.validate, 1)
        o = object()
        clone = choice.bind(o)
        clone._validate(1)
        clone._validate(3)
        self.assertRaises(ConstraintNotSatisfied, clone._validate, 42)


class URITests(EqualityTestsMixin,
               WrongTypeTestsMixin,
               unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import URI
        return URI

    def _getTargetInterface(self):
        from zope.schema.interfaces import IURI
        return IURI

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

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
        field.validate('http://example.com/')
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate('http://example.com/')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_validate_not_a_uri(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import InvalidURI
        field = self._makeOne()
        with self.assertRaises(InvalidURI) as exc:
            field.validate('')

        invalid = exc.exception
        self.assertIs(invalid.field, field)
        self.assertEqual(invalid.value, '')

        self.assertRaises(InvalidURI, field.validate, 'abc')
        self.assertRaises(InvalidURI, field.validate, '\xab\xde')
        self.assertRaises(ConstraintNotSatisfied,
                          field.validate, 'http://example.com/\nDAV:')

    def test_fromUnicode_ok(self):

        field = self._makeOne()
        self.assertEqual(field.fromUnicode('http://example.com/'),
                         'http://example.com/')

    def test_fromUnicode_invalid(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import InvalidURI

        field = self._makeOne()
        self.assertRaises(InvalidURI, field.fromUnicode, '')
        self.assertRaises(InvalidURI, field.fromUnicode, 'abc')
        self.assertRaises(ConstraintNotSatisfied,
                          field.fromUnicode, 'http://example.com/\nDAV:')


class PythonIdentifierTests(EqualityTestsMixin,
                            WrongTypeTestsMixin,
                            unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import PythonIdentifier
        return PythonIdentifier

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

    def _getTargetInterface(self):
        from zope.schema.interfaces import IPythonIdentifier
        return IPythonIdentifier

    def test_fromUnicode_empty(self):
        pi = self._makeOne()
        self.assertEqual(pi.fromUnicode(''), '')

    def test_fromUnicode_normal(self):
        pi = self._makeOne()
        self.assertEqual(pi.fromUnicode('normal'), 'normal')

    def test_fromUnicode_strips_ws(self):
        pi = self._makeOne()
        self.assertEqual(pi.fromUnicode('   '), '')
        self.assertEqual(pi.fromUnicode(' normal  '), 'normal')

    def test__validate_miss(self):
        from zope.schema.interfaces import InvalidValue
        pi = self._makeOne()
        with self.assertRaises(InvalidValue) as exc:
            pi._validate('not-an-identifier')

        ex = exc.exception
        self.assertIs(ex.field, pi)
        self.assertEqual(ex.value, 'not-an-identifier')

    def test__validate_hit(self):
        pi = self._makeOne()
        pi._validate('is_an_identifier')


class DottedNameTests(EqualityTestsMixin,
                      WrongTypeTestsMixin,
                      unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import DottedName
        return DottedName

    def _getTargetInterface(self):
        from zope.schema.interfaces import IDottedName
        return IDottedName

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

    def test_ctor_defaults(self):
        dotted = self._makeOne()
        self.assertEqual(dotted.min_dots, 0)
        self.assertEqual(dotted.max_dots, None)

    def test_ctor_min_dots_invalid(self):
        self.assertRaises(ValueError, self._makeOne, min_dots=-1)

    def test_ctor_min_dots_valid(self):
        dotted = self._makeOne(min_dots=1)
        self.assertEqual(dotted.min_dots, 1)

    def test_ctor_max_dots_invalid(self):
        self.assertRaises(ValueError, self._makeOne, min_dots=2, max_dots=1)

    def test_ctor_max_dots_valid(self):
        dotted = self._makeOne(max_dots=2)
        self.assertEqual(dotted.max_dots, 2)

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
        field.validate('name')
        field.validate('dotted.name')
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate('name')
        field.validate('dotted.name')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_validate_w_min_dots(self):
        from zope.schema.interfaces import InvalidDottedName
        field = self._makeOne(min_dots=1)
        with self.assertRaises(InvalidDottedName) as exc:
            field.validate('name')
        invalid = exc.exception
        self.assertIs(invalid.field, field)
        self.assertEqual(invalid.value, 'name')

        field.validate('dotted.name')
        field.validate('moar.dotted.name')

    def test_validate_w_max_dots(self):
        from zope.schema.interfaces import InvalidDottedName
        field = self._makeOne(max_dots=1)
        field.validate('name')
        field.validate('dotted.name')
        with self.assertRaises(InvalidDottedName) as exc:
            field.validate('moar.dotted.name')

        invalid = exc.exception
        self.assertIs(invalid.field, field)
        self.assertEqual(invalid.value, 'moar.dotted.name')

    def test_validate_not_a_dotted_name(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import InvalidDottedName
        field = self._makeOne()
        self.assertRaises(InvalidDottedName, field.validate, '')
        self.assertRaises(InvalidDottedName, field.validate, '\xab\xde')
        self.assertRaises(ConstraintNotSatisfied,
                          field.validate, 'http://example.com/\nDAV:')

    def test_fromUnicode_dotted_name_ok(self):
        field = self._makeOne()
        self.assertEqual(field.fromUnicode('dotted.name'), 'dotted.name')

        # Underscores are allowed in any component
        self.assertEqual(field.fromUnicode('dotted._name'), 'dotted._name')
        self.assertEqual(field.fromUnicode('_leading_underscore'),
                         '_leading_underscore')
        self.assertEqual(field.fromUnicode('_dotted.name'), '_dotted.name')
        self.assertEqual(field.fromUnicode('_dotted._name'), '_dotted._name')

    def test_fromUnicode_invalid(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import InvalidDottedName

        field = self._makeOne()
        self.assertRaises(InvalidDottedName, field.fromUnicode, '')
        with self.assertRaises(InvalidDottedName) as exc:
            field.fromUnicode('\u2603')
        invalid = exc.exception
        self.assertIs(invalid.field, field)
        self.assertEqual(invalid.value, '\u2603')

        self.assertRaises(ConstraintNotSatisfied,
                          field.fromUnicode, 'http://example.com/\nDAV:')


class IdTests(EqualityTestsMixin,
              WrongTypeTestsMixin,
              unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import Id
        return Id

    def _getTargetInterface(self):
        from zope.schema.interfaces import IId
        return IId

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

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
        field.validate('http://example.com/')
        field.validate('dotted.name')
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate('http://example.com/')
        field.validate('dotted.name')
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_validate_not_a_uri(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import InvalidId
        field = self._makeOne()
        with self.assertRaises(InvalidId) as exc:
            field.validate('')

        invalid = exc.exception
        self.assertIs(invalid.field, field)
        self.assertEqual(invalid.value, '')

        self.assertRaises(InvalidId, field.validate, 'abc')
        self.assertRaises(InvalidId, field.validate, '\xab\xde')
        self.assertRaises(ConstraintNotSatisfied,
                          field.validate, 'http://example.com/\nDAV:')

    def test_fromUnicode_url_ok(self):

        field = self._makeOne()
        self.assertEqual(field.fromUnicode('http://example.com/'),
                         'http://example.com/')

    def test_fromUnicode_dotted_name_ok(self):

        field = self._makeOne()
        self.assertEqual(field.fromUnicode('dotted.name'), 'dotted.name')

    def test_fromUnicode_invalid(self):
        from zope.schema.interfaces import ConstraintNotSatisfied
        from zope.schema.interfaces import InvalidId

        field = self._makeOne()
        self.assertRaises(InvalidId, field.fromUnicode, '')
        self.assertRaises(InvalidId, field.fromUnicode, 'abc')
        self.assertRaises(InvalidId, field.fromUnicode, '\u2603')
        self.assertRaises(ConstraintNotSatisfied,
                          field.fromUnicode, 'http://example.com/\nDAV:')


class InterfaceFieldTests(EqualityTestsMixin,
                          WrongTypeTestsMixin,
                          unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import InterfaceField
        return InterfaceField

    def _getTargetInterface(self):
        from zope.schema.interfaces import IInterfaceField
        return IInterfaceField

    def test_validate_wrong_types(self):
        from datetime import date

        from zope.interface.interfaces import IInterface

        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            IInterface,
            '',
            b'',
            1,
            1.0,
            (),
            [],
            {},
            set(),
            frozenset(),
            object(),
            date.today())

    def test_validate_not_required(self):
        from zope.interface import Interface

        class DummyInterface(Interface):
            pass

        field = self._makeOne(required=False)
        field.validate(DummyInterface)
        field.validate(None)

    def test_validate_required(self):
        from zope.interface import Interface

        from zope.schema.interfaces import RequiredMissing

        class DummyInterface(Interface):
            pass

        field = self._makeOne(required=True)
        field.validate(DummyInterface)
        self.assertRaises(RequiredMissing, field.validate, None)


class CollectionTests(EqualityTestsMixin,
                      LenTestsMixin,
                      unittest.TestCase):

    _DEFAULT_UNIQUE = False

    def _getTargetClass(self):
        from zope.schema._field import Collection
        return Collection

    def _getTargetInterface(self):
        from zope.schema.interfaces import ICollection
        return ICollection

    _makeCollection = list

    def test_schema_defined_by_subclass(self):
        from zope import interface
        from zope.schema import Object
        from zope.schema.interfaces import SchemaNotProvided
        from zope.schema.interfaces import WrongContainedType

        class IValueType(interface.Interface):
            "The value type schema"

        the_value_type = Object(IValueType)

        class Field(self._getTargetClass()):
            value_type = the_value_type

        field = Field()
        self.assertIs(field.value_type, the_value_type)

        # Empty collection is fine
        field.validate(self._makeCollection([]))

        # Collection with a non-implemented object is bad
        with self.assertRaises(WrongContainedType) as exc:
            field.validate(self._makeCollection([object()]))

        ex = exc.exception
        self.assertIs(ex.__class__, WrongContainedType)
        self.assertEqual(1, len(ex.errors))
        self.assertIsInstance(ex.errors[0], SchemaNotProvided)
        self.assertIs(ex.errors[0].schema, IValueType)

        # Actual implementation works
        @interface.implementer(IValueType)
        class ValueType:
            "The value type"

        field.validate(self._makeCollection([ValueType()]))

    def test_ctor_defaults(self):
        absc = self._makeOne()
        self.assertEqual(absc.value_type, None)
        self.assertEqual(absc.unique, self._DEFAULT_UNIQUE)

    def test_ctor_explicit(self):
        from zope.schema._bootstrapfields import Text
        text = Text()
        absc = self._makeOne(text, True)
        self.assertEqual(absc.value_type, text)
        self.assertEqual(absc.unique, True)

    def test_ctor_w_non_field_value_type(self):
        class NotAField:
            pass
        self.assertRaises(ValueError, self._makeOne, NotAField)

    def test_bind_wo_value_Type(self):
        absc = self._makeOne()
        context = object()
        bound = absc.bind(context)
        self.assertEqual(bound.context, context)
        self.assertEqual(bound.value_type, None)
        self.assertEqual(bound.unique, self._DEFAULT_UNIQUE)

    def test_bind_w_value_Type(self):
        from zope.schema._bootstrapfields import Text
        text = Text()
        absc = self._makeOne(text, True)
        context = object()
        bound = absc.bind(context)
        self.assertEqual(bound.context, context)
        self.assertEqual(isinstance(bound.value_type, Text), True)
        self.assertEqual(bound.value_type.context, context)
        self.assertEqual(bound.unique, True)

    def test__validate_wrong_contained_type(self):
        from zope.schema._bootstrapfields import Text
        from zope.schema.interfaces import WrongContainedType
        from zope.schema.interfaces import WrongType
        text = Text()
        absc = self._makeOne(text)
        with self.assertRaises(WrongContainedType) as exc:
            absc.validate(self._makeCollection([1]))

        wct = exc.exception
        self.assertIs(wct.field, absc)
        self.assertEqual(wct.value, self._makeCollection([1]))
        self.assertIs(wct.__class__, WrongContainedType)
        self.assertEqual(1, len(wct.errors))
        self.assertIsInstance(wct.errors[0], WrongType)
        self.assertIs(wct.errors[0].expected_type, text._type)

    def test__validate_miss_uniqueness(self):
        from zope.schema._bootstrapfields import Text
        from zope.schema.interfaces import NotUnique
        from zope.schema.interfaces import WrongType

        text = Text()
        absc = self._makeOne(text, True)
        with self.assertRaises((NotUnique, WrongType)) as exc:
            absc.validate(['a', 'a'])

        not_uniq = exc.exception
        self.assertIs(not_uniq.field, absc)
        self.assertEqual(not_uniq.value,
                         ['a', 'a'])

    def test_validate_min_length(self):
        field = self._makeOne(min_length=2)
        field.validate(self._makeCollection((1, 2)))
        field.validate(self._makeCollection((1, 2, 3)))
        self.assertRaisesTooShort(field, self._makeCollection())
        self.assertRaisesTooShort(field, self._makeCollection((1,)))

    def test_validate_max_length(self):
        field = self._makeOne(max_length=2)
        field.validate(self._makeCollection())
        field.validate(self._makeCollection((1,)))
        field.validate(self._makeCollection((1, 2)))
        self.assertRaisesTooLong(field, self._makeCollection((1, 2, 3, 4)))
        self.assertRaisesTooLong(field, self._makeCollection((1, 2, 3)))

    def test_validate_min_length_and_max_length(self):
        field = self._makeOne(min_length=1, max_length=2)
        field.validate(self._makeCollection((1,)))
        field.validate(self._makeCollection((1, 2)))
        self.assertRaisesTooShort(field, self._makeCollection())
        self.assertRaisesTooLong(field, self._makeCollection((1, 2, 3)))

    def test_validate_not_required(self):
        field = self._makeOne(required=False)
        field.validate(self._makeCollection())
        field.validate(self._makeCollection((1, 2)))
        field.validate(self._makeCollection((3,)))
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate(self._makeCollection())
        field.validate(self._makeCollection((1, 2)))
        field.validate(self._makeCollection((3,)))
        field.validate(self._makeCollection())
        field.validate(self._makeCollection((1, 2)))
        field.validate(self._makeCollection((3,)))
        self.assertRaises(RequiredMissing, field.validate, None)


class SequenceTests(WrongTypeTestsMixin,
                    CollectionTests):

    def _getTargetClass(self):
        from zope.schema._field import Sequence
        return Sequence

    def _getTargetInterface(self):
        from zope.schema.interfaces import ISequence
        return ISequence

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            1,
            1.0,
            {},
            set(),
            frozenset(),
            object())

    def test_sequence(self):
        from zope.schema._field import abc

        class Sequence(abc.Sequence):
            def __getitem__(self, i):
                raise AssertionError("Not implemented")

            def __len__(self):
                return 0

        sequence = Sequence()
        field = self._makeOne()
        field.validate(sequence)

    def test_mutable_sequence(self):
        from zope.schema._field import abc

        class MutableSequence(abc.MutableSequence):
            def insert(self, index, value):
                raise AssertionError("not implemented")

            def __getitem__(self, name):
                raise AssertionError("not implemented")

            def __iter__(self):
                return iter(())

            def __setitem__(self, name, value):
                raise AssertionError("Not implemented")

            def __len__(self):
                return 0

            __delitem__ = __getitem__

        sequence = MutableSequence()
        field = self._makeOne()
        field.validate(sequence)


class TupleTests(SequenceTests):

    _makeCollection = tuple

    def _getTargetClass(self):
        from zope.schema._field import Tuple
        return Tuple

    def _getTargetInterface(self):
        from zope.schema.interfaces import ITuple
        return ITuple

    def test_mutable_sequence(self):
        from zope.schema.interfaces import WrongType
        with self.assertRaises(WrongType):
            super().test_mutable_sequence()

    def test_sequence(self):
        from zope.schema.interfaces import WrongType
        with self.assertRaises(WrongType):
            super().test_sequence()

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
            b'',
            [])
        super().test_validate_wrong_types()


class MutableSequenceTests(SequenceTests):

    def _getTargetClass(self):
        from zope.schema._field import MutableSequence
        return MutableSequence

    def _getTargetInterface(self):
        from zope.schema.interfaces import IMutableSequence
        return IMutableSequence

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
            b'',
            ())
        super().test_validate_wrong_types()

    def test_sequence(self):
        from zope.schema.interfaces import WrongType
        with self.assertRaises(WrongType):
            super().test_sequence()


class ListTests(MutableSequenceTests):

    def _getTargetClass(self):
        from zope.schema._field import List
        return List

    def _getTargetInterface(self):
        from zope.schema.interfaces import IList
        return IList

    def test_mutable_sequence(self):
        from zope.schema.interfaces import WrongType
        with self.assertRaises(WrongType):
            super().test_mutable_sequence()


class SetTests(WrongTypeTestsMixin,
               CollectionTests):

    _DEFAULT_UNIQUE = True
    _makeCollection = set
    _makeWrongSet = frozenset

    def _getTargetClass(self):
        from zope.schema._field import Set
        return Set

    def _getTargetInterface(self):
        from zope.schema.interfaces import ISet
        return ISet

    def test_ctor_disallows_unique(self):
        self.assertRaises(TypeError, self._makeOne, unique=False)
        self._makeOne(unique=True)  # restating the obvious is allowed
        self.assertTrue(self._makeOne().unique)

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
            b'',
            1,
            1.0,
            (),
            [],
            {},
            self._makeWrongSet(),
            object())


class FrozenSetTests(SetTests):

    _makeCollection = frozenset
    _makeWrongSet = set

    def _getTargetClass(self):
        from zope.schema._field import FrozenSet
        return FrozenSet

    def _getTargetInterface(self):
        from zope.schema.interfaces import IFrozenSet
        return IFrozenSet


class MappingTests(EqualityTestsMixin,
                   WrongTypeTestsMixin,
                   LenTestsMixin,
                   unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import Mapping
        return Mapping

    def _getTargetInterface(self):
        from zope.schema.interfaces import IMapping
        return IMapping

    def test_ctor_key_type_not_IField(self):
        self.assertRaises(ValueError, self._makeOne, key_type=object())

    def test_ctor_value_type_not_IField(self):
        self.assertRaises(ValueError, self._makeOne, value_type=object())

    def test_validate_wrong_types(self):
        field = self._makeOne()
        self.assertAllRaiseWrongType(
            field,
            field._type,
            '',
            b'',
            1,
            1.0,
            (),
            [],
            set(),
            frozenset(),
            object())

    def test_validate_not_required(self):
        field = self._makeOne(required=False)
        field.validate({})
        field.validate({1: 'b', 2: 'd'})
        field.validate({3: 'a'})
        field.validate(None)

    def test_validate_required(self):
        from zope.schema.interfaces import RequiredMissing
        field = self._makeOne()
        field.validate({})
        field.validate({1: 'b', 2: 'd'})
        field.validate({3: 'a'})
        self.assertRaises(RequiredMissing, field.validate, None)

    def test_validate_invalid_key_type(self):
        from zope.schema._bootstrapfields import Int
        from zope.schema.interfaces import WrongContainedType
        from zope.schema.interfaces import WrongType
        field = self._makeOne(key_type=Int())
        field.validate({})
        field.validate({1: 'b', 2: 'd'})
        field.validate({3: 'a'})
        with self.assertRaises(WrongContainedType) as exc:
            field.validate({'a': 1})

        wct = exc.exception
        self.assertIs(wct.field, field)
        self.assertEqual(wct.value, {'a': 1})
        self.assertIs(wct.__class__, WrongContainedType)
        self.assertEqual(1, len(wct.errors))
        self.assertIsInstance(wct.errors[0], WrongType)
        self.assertIs(field.key_type._type, wct.errors[0].expected_type)

    def test_validate_invalid_value_type(self):
        from zope.schema._bootstrapfields import Int
        from zope.schema.interfaces import WrongContainedType
        from zope.schema.interfaces import WrongType
        field = self._makeOne(value_type=Int())
        field.validate({})
        field.validate({'b': 1, 'd': 2})
        field.validate({'a': 3})
        with self.assertRaises(WrongContainedType) as exc:
            field.validate({1: 'a'})

        wct = exc.exception
        self.assertIs(wct.field, field)
        self.assertEqual(wct.value, {1: 'a'})
        self.assertIs(wct.__class__, WrongContainedType)
        self.assertEqual(1, len(wct.errors))
        self.assertIsInstance(wct.errors[0], WrongType)
        self.assertIs(field.value_type._type, wct.errors[0].expected_type)

    def test_validate_min_length(self):
        field = self._makeOne(min_length=1)
        field.validate({1: 'a'})
        field.validate({1: 'a', 2: 'b'})
        self.assertRaisesTooShort(field, {})

    def test_validate_max_length(self):
        field = self._makeOne(max_length=1)
        field.validate({})
        field.validate({1: 'a'})
        self.assertRaisesTooLong(field, {1: 'a', 2: 'b'})
        self.assertRaisesTooLong(field, {1: 'a', 2: 'b', 3: 'c'})

    def test_validate_min_length_and_max_length(self):
        field = self._makeOne(min_length=1, max_length=2)
        field.validate({1: 'a'})
        field.validate({1: 'a', 2: 'b'})
        self.assertRaisesTooShort(field, {})
        self.assertRaisesTooLong(field, {1: 'a', 2: 'b', 3: 'c'})

    def test_bind_without_key_and_value_types(self):
        field = self._makeOne()
        context = object()
        field2 = field.bind(context)
        self.assertIsNone(field2.key_type)
        self.assertIsNone(field2.value_type)

    def test_bind_binds_key_and_value_types(self):
        from zope.schema import Int
        field = self._makeOne(key_type=Int(), value_type=Int())
        context = object()
        field2 = field.bind(context)
        self.assertEqual(field2.key_type.context, context)
        self.assertEqual(field2.value_type.context, context)

    def test_mapping(self):
        from zope.schema._field import abc

        class Mapping(abc.Mapping):

            def __getitem__(self, name):
                raise AssertionError("not implemented")

            def __iter__(self):
                return iter(())

            def __len__(self):
                return 0

        mm = Mapping()
        field = self._makeOne()
        field.validate(mm)

    def test_mutable_mapping(self):
        from zope.schema._field import abc

        class MutableMapping(abc.MutableMapping):

            def __getitem__(self, name):
                raise AssertionError("not implemented")

            def __iter__(self):
                return iter(())

            def __setitem__(self, name, value):
                raise AssertionError("Not implemented")

            def __len__(self):
                return 0

            __delitem__ = __getitem__

        mm = MutableMapping()
        field = self._makeOne()
        field.validate(mm)


class MutableMappingTests(MappingTests):

    def _getTargetClass(self):
        from zope.schema._field import MutableMapping
        return MutableMapping

    def _getTargetInterface(self):
        from zope.schema.interfaces import IMutableMapping
        return IMutableMapping

    def test_mapping(self):
        from zope.schema.interfaces import WrongType
        with self.assertRaises(WrongType):
            super().test_mapping()


class DictTests(MutableMappingTests):

    def _getTargetClass(self):
        from zope.schema._field import Dict
        return Dict

    def _getTargetInterface(self):
        from zope.schema.interfaces import IDict
        return IDict

    def test_mutable_mapping(self):
        from zope.schema.interfaces import WrongType
        with self.assertRaises(WrongType):
            super().test_mutable_mapping()


class NativeStringTests(EqualityTestsMixin,
                        WrongTypeTestsMixin,
                        unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import NativeString
        return NativeString

    def _getTargetInterface(self):
        from zope.schema.interfaces import INativeString
        return INativeString

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

    def test_fromBytes(self):
        field = self._makeOne()
        self.assertEqual(field.fromBytes(b''), '')
        self.assertEqual(field.fromBytes(b'DEADBEEF'), 'DEADBEEF')

    def test_fromUnicode(self):
        field = self._makeOne()
        self.assertIsInstance(field.fromUnicode(''), str)
        self.assertEqual(field.fromUnicode(''), '')
        self.assertEqual(field.fromUnicode('DEADBEEF'), 'DEADBEEF')


class NativeStringLineTests(EqualityTestsMixin,
                            WrongTypeTestsMixin,
                            unittest.TestCase):

    def _getTargetClass(self):
        from zope.schema._field import NativeStringLine
        return NativeStringLine

    def _getTargetInterface(self):
        from zope.schema.interfaces import INativeStringLine
        return INativeStringLine

    def _getTargetInterfaces(self):
        from zope.schema.interfaces import IFromBytes
        from zope.schema.interfaces import IFromUnicode
        return [self._getTargetInterface(), IFromUnicode, IFromBytes]

    def test_fromBytes(self):
        field = self._makeOne()
        self.assertEqual(field.fromBytes(b''), '')
        self.assertEqual(field.fromBytes(b'DEADBEEF'), 'DEADBEEF')

    def test_fromUnicode(self):
        field = self._makeOne()
        self.assertIsInstance(field.fromUnicode(''), str)
        self.assertEqual(field.fromUnicode(''), '')
        self.assertEqual(field.fromUnicode('DEADBEEF'), 'DEADBEEF')


class StrippedNativeStringLineTests(NativeStringLineTests):

    def _getTargetClass(self):
        from zope.schema._field import _StrippedNativeStringLine
        return _StrippedNativeStringLine

    def test_strips(self):
        field = self._makeOne()
        self.assertEqual(field.fromBytes(b' '), '')
        self.assertEqual(field.fromUnicode(' '), '')

    def test_iface_is_first_in_sro(self):
        self.skipTest("Not applicable; we inherit implementation but have no "
                      "interface")


def _makeSampleVocabulary():
    from zope.interface import implementer

    from zope.schema.interfaces import IVocabulary

    @implementer(IVocabulary)
    class SampleVocabulary:

        def __iter__(self):
            raise AssertionError("Not implemented")

        def __contains__(self, value):
            return 0 <= value < 10

        def __len__(self):  # pragma: no cover
            return 10

        def getTerm(self, value):
            raise AssertionError("Not implemented")

    return SampleVocabulary()


def _makeDummyRegistry(v):
    from zope.schema.vocabulary import VocabularyRegistry

    class DummyRegistry(VocabularyRegistry):
        def __init__(self, vocabulary):
            VocabularyRegistry.__init__(self)
            self._vocabulary = vocabulary

        def get(self, context, name):
            return self._vocabulary
    return DummyRegistry(v)


def _test_suite():
    import zope.schema._field
    suite = unittest.defaultTestLoader.loadTestsFromName(__name__)
    suite.addTests(doctest.DocTestSuite(
        zope.schema._field,
        optionflags=doctest.ELLIPSIS
    ))
    return suite
