import unittest
import datetime
import pytz

from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model, NO_VALUE
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.engines import *
from infi.clickhouse_orm.utils import parse_tsv

from .common import get_clickhouse_url


class CompressedFieldsTestCase(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        self.database.create_table(CompressedModel)

    def tearDown(self):
        self.database.drop_database()

    def test_defaults(self):
        # Check that all fields have their explicit or implicit defaults
        instance = CompressedModel()
        self.database.insert([instance])
        self.assertEqual(instance.date_field, datetime.date(1970, 1, 1))
        self.assertEqual(instance.datetime_field, datetime.datetime(1970, 1, 1, tzinfo=pytz.utc))
        self.assertEqual(instance.string_field, 'dozo')
        self.assertEqual(instance.int64_field, 42)
        self.assertEqual(instance.float_field, 0)
        self.assertEqual(instance.nullable_field, None)
        self.assertEqual(instance.array_field, [])

    def test_assignment(self):
        # Check that all fields are assigned during construction
        kwargs = dict(
            uint64_field=217,
            date_field=datetime.date(1973, 12, 6),
            datetime_field=datetime.datetime(2000, 5, 24, 10, 22, tzinfo=pytz.utc),
            string_field='aloha',
            int64_field=-50,
            float_field=3.14,
            nullable_field=-2.718281,
            array_field=['123456789123456','','a']
        )
        instance = CompressedModel(**kwargs)
        self.database.insert([instance])
        for name, value in kwargs.items():
            self.assertEqual(kwargs[name], getattr(instance, name))

    def test_string_conversion(self):
        # Check field conversion from string during construction
        instance = CompressedModel(date_field='1973-12-06', int64_field='100', float_field='7', nullable_field=None, array_field='[a,b,c]')
        self.assertEqual(instance.date_field, datetime.date(1973, 12, 6))
        self.assertEqual(instance.int64_field, 100)
        self.assertEqual(instance.float_field, 7)
        self.assertEqual(instance.nullable_field, None)
        self.assertEqual(instance.array_field, ['a', 'b', 'c'])
        # Check field conversion from string during assignment
        instance.int64_field = '99'
        self.assertEqual(instance.int64_field, 99)

    def test_to_dict(self):
        instance = CompressedModel(date_field='1973-12-06', int64_field='100', float_field='7', array_field='[a,b,c]')
        self.assertDictEqual(instance.to_dict(), {
            "date_field": datetime.date(1973, 12, 6),
            "int64_field": 100,
            "float_field": 7.0,
            "datetime_field": datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
            "alias_field": NO_VALUE,
            'string_field': 'dozo',
            'nullable_field': None,
            'uint64_field': 0,
            'array_field': ['a','b','c']
        })
        self.assertDictEqual(instance.to_dict(include_readonly=False), {
            "date_field": datetime.date(1973, 12, 6),
            "int64_field": 100,
            "float_field": 7.0,
            "datetime_field": datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc),
            'string_field': 'dozo',
            'nullable_field': None,
            'uint64_field': 0,
            'array_field': ['a', 'b', 'c']
        })
        self.assertDictEqual(
            instance.to_dict(include_readonly=False, field_names=('int64_field', 'alias_field', 'datetime_field')), {
                "int64_field": 100,
                "datetime_field": datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
            })

    def test_confirm_compression_codec(self):
        if self.database.server_version < (19, 17):
            raise unittest.SkipTest('ClickHouse version too old')
        instance = CompressedModel(date_field='1973-12-06', int64_field='100', float_field='7', array_field='[a,b,c]')
        self.database.insert([instance])
        r = self.database.raw("select name, compression_codec from system.columns where table = '{}' and database='{}' FORMAT TabSeparatedWithNamesAndTypes".format(instance.table_name(), self.database.db_name))
        lines = r.splitlines()
        field_names = parse_tsv(lines[0])
        field_types = parse_tsv(lines[1])
        data = [tuple(parse_tsv(line)) for line in lines[2:]]
        self.assertListEqual(data, [('uint64_field', 'CODEC(ZSTD(10))'),
                                    ('datetime_field', 'CODEC(Delta(4), ZSTD(1))'),
                                    ('date_field', 'CODEC(Delta(4), ZSTD(22))'),
                                    ('int64_field', 'CODEC(LZ4)'),
                                    ('string_field', 'CODEC(LZ4HC(10))'),
                                    ('nullable_field', 'CODEC(ZSTD(1))'),
                                    ('array_field', 'CODEC(Delta(2), LZ4HC(0))'),
                                    ('float_field', 'CODEC(NONE)'),
                                    ('alias_field', '')])


class CompressedModel(Model):
    uint64_field        = UInt64Field(codec='ZSTD(10)')
    datetime_field      = DateTimeField(codec='Delta,ZSTD')
    date_field          = DateField(codec='Delta(4),ZSTD(22)')
    int64_field         = Int64Field(default=42, codec='LZ4')
    string_field        = StringField(default='dozo', codec='LZ4HC(10)')
    nullable_field      = NullableField(Float32Field(), codec='ZSTD')
    array_field         = ArrayField(FixedStringField(length=15), codec='Delta(2),LZ4HC')
    float_field         = Float32Field(codec='NONE')
    alias_field         = Float32Field(alias='float_field', codec='ZSTD(4)')

    engine = MergeTree('datetime_field', ('uint64_field', 'datetime_field'))
