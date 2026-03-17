from decimal import Decimal
import datetime
from jsonobject.exceptions import BadValueError
from jsonobject import JsonObject, ObjectProperty, DateTimeProperty
import unittest
from jsonobject.base import get_settings


class StringConversionsTest(unittest.TestCase):

    EXAMPLES = {
        'decimal': '1.2',
        'date': '2014-02-04',
        'datetime': '2014-01-03T01:02:03Z',
        'dict': {
            'decimal': '1.4',
        },
        'list': ['1.0', '2000-01-01'],
    }
    EXAMPLES_CONVERTED = {
        'decimal': Decimal('1.2'),
        'date': datetime.date(2014, 2, 4),
        'dict': {
            'decimal': Decimal('1.4'),
        },
        'list': [Decimal('1.0'), datetime.date(2000, 1, 1)],
        'datetime': datetime.datetime(2014, 1, 3, 1, 2, 3)
    }

    def test_default_conversions(self):
        class Foo(JsonObject):
            pass
        foo = Foo.wrap(self.EXAMPLES)
        for key, value in self.EXAMPLES_CONVERTED.items():
            self.assertEqual(getattr(foo, key), value)

    def test_no_conversions(self):
        class Foo(JsonObject):
            class Meta(object):
                string_conversions = ()

        foo = Foo.wrap(self.EXAMPLES)
        for key, value in self.EXAMPLES.items():
            self.assertEqual(getattr(foo, key), value)

    def test_nested_1(self):

        class Bar(JsonObject):
            # default string conversions
            pass

        class Foo(JsonObject):
            bar = ObjectProperty(Bar)

            class Meta(object):
                string_conversions = ()

        foo = Foo.wrap({
            # don't convert
            'decimal': '1.0',
            # do convert
            'bar': {'decimal': '2.4'}
        })
        self.assertEqual(foo.decimal, '1.0')
        self.assertNotEqual(foo.decimal, Decimal('1.0'))
        self.assertEqual(foo.bar.decimal, Decimal('2.4'))

    def test_nested_2(self):
        class Bar(JsonObject):

            class Meta(object):
                string_conversions = ()

        class Foo(JsonObject):
            # default string conversions
            bar = ObjectProperty(Bar)

        foo = Foo.wrap({
            # do convert
            'decimal': '1.0',
            # don't convert
            'bar': {'decimal': '2.4'}
        })
        self.assertNotEqual(foo.decimal, '1.0')
        self.assertEqual(foo.decimal, Decimal('1.0'))
        self.assertEqual(foo.bar.decimal, '2.4')

    def test_update_properties(self):
        class Foo(JsonObject):

            class Meta(object):
                update_properties = {datetime.datetime: ExactDateTimeProperty}

        self.assertEqual(
            get_settings(Foo).type_config.properties[datetime.datetime],
            ExactDateTimeProperty
        )
        with self.assertRaisesRegex(BadValueError,
                                     'is not a datetime-formatted string'):
            Foo.wrap(self.EXAMPLES)
        examples = self.EXAMPLES.copy()
        examples['datetime'] = '2014-01-03T01:02:03.012345Z'
        examples_converted = self.EXAMPLES_CONVERTED.copy()
        examples_converted['datetime'] = datetime.datetime(
            2014, 1, 3, 1, 2, 3, 12345)
        foo = Foo.wrap(examples)
        for key, value in examples_converted.items():
            self.assertEqual(getattr(foo, key), value)


class ExactDateTimeProperty(DateTimeProperty):
    def __init__(self, **kwargs):
        if 'exact' in kwargs:
            assert kwargs['exact'] is True
        kwargs['exact'] = True
        super(ExactDateTimeProperty, self).__init__(**kwargs)
