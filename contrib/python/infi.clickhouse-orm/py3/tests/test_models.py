import unittest
import datetime
import pytz

from infi.clickhouse_orm.models import Model, NO_VALUE
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.engines import *
from infi.clickhouse_orm.funcs import F


class ModelTestCase(unittest.TestCase):

    def test_defaults(self):
        # Check that all fields have their explicit or implicit defaults
        instance = SimpleModel()
        self.assertEqual(instance.date_field, datetime.date(1970, 1, 1))
        self.assertEqual(instance.datetime_field, datetime.datetime(1970, 1, 1, tzinfo=pytz.utc))
        self.assertEqual(instance.str_field, 'dozo')
        self.assertEqual(instance.int_field, 17)
        self.assertEqual(instance.float_field, 0)
        self.assertEqual(instance.default_func, NO_VALUE)

    def test_assignment(self):
        # Check that all fields are assigned during construction
        kwargs = dict(
            date_field=datetime.date(1973, 12, 6),
            datetime_field=datetime.datetime(2000, 5, 24, 10, 22, tzinfo=pytz.utc),
            str_field='aloha',
            int_field=-50,
            float_field=3.14
        )
        instance = SimpleModel(**kwargs)
        for name, value in kwargs.items():
            self.assertEqual(kwargs[name], getattr(instance, name))

    def test_assignment_error(self):
        # Check non-existing field during construction
        with self.assertRaises(AttributeError):
            instance = SimpleModel(int_field=7450, pineapple='tasty')
        # Check invalid field values during construction
        with self.assertRaises(ValueError):
            instance = SimpleModel(int_field='nope')
        with self.assertRaises(ValueError):
            instance = SimpleModel(date_field='nope')
        # Check invalid field values during assignment
        instance = SimpleModel()
        with self.assertRaises(ValueError):
            instance.datetime_field = datetime.timedelta(days=1)

    def test_string_conversion(self):
        # Check field conversion from string during construction
        instance = SimpleModel(date_field='1973-12-06', int_field='100', float_field='7')
        self.assertEqual(instance.date_field, datetime.date(1973, 12, 6))
        self.assertEqual(instance.int_field, 100)
        self.assertEqual(instance.float_field, 7)
        # Check field conversion from string during assignment
        instance.int_field = '99'
        self.assertEqual(instance.int_field, 99)

    def test_to_dict(self):
        instance = SimpleModel(date_field='1973-12-06', int_field='100', float_field='7')
        self.assertDictEqual(instance.to_dict(), {
            "date_field": datetime.date(1973, 12, 6),
            "int_field": 100,
            "float_field": 7.0,
            "datetime_field": datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
            "alias_field": NO_VALUE,
            "str_field": "dozo",
            "default_func": NO_VALUE
        })
        self.assertDictEqual(instance.to_dict(include_readonly=False), {
            "date_field": datetime.date(1973, 12, 6),
            "int_field": 100,
            "float_field": 7.0,
            "datetime_field": datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
            "str_field": "dozo",
            "default_func": NO_VALUE
        })
        self.assertDictEqual(
            instance.to_dict(include_readonly=False, field_names=('int_field', 'alias_field', 'datetime_field')), {
                "int_field": 100,
                "datetime_field": datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
            })

    def test_field_name_in_error_message_for_invalid_value_in_constructor(self):
        bad_value = 1
        with self.assertRaises(ValueError) as cm:
            SimpleModel(str_field=bad_value)

        self.assertEqual(
            "Invalid value for StringField: {} (field 'str_field')".format(repr(bad_value)),
            str(cm.exception)
        )

    def test_field_name_in_error_message_for_invalid_value_in_assignment(self):
        instance = SimpleModel()
        bad_value = 'foo'
        with self.assertRaises(ValueError) as cm:
            instance.float_field = bad_value

        self.assertEqual(
            "Invalid value for Float32Field - {} (field 'float_field')".format(repr(bad_value)),
            str(cm.exception)
        )


class SimpleModel(Model):

    date_field = DateField()
    datetime_field = DateTimeField()
    str_field = StringField(default='dozo')
    int_field = Int32Field(default=17)
    float_field = Float32Field()
    alias_field = Float32Field(alias='float_field')
    default_func = Float32Field(default=F.sqrt(float_field) + 17)

    engine = MergeTree('date_field', ('int_field', 'date_field'))
