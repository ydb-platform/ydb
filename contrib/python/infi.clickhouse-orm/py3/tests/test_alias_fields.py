import unittest
from datetime import date

from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model, NO_VALUE
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.engines import *
from infi.clickhouse_orm.funcs import F

from .common import get_clickhouse_url


class AliasFieldsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        self.database.create_table(ModelWithAliasFields)

    def tearDown(self):
        self.database.drop_database()

    def test_insert_and_select(self):
        instance = ModelWithAliasFields(
            date_field='2016-08-30',
            int_field=-10,
            str_field='TEST'
        )
        self.database.insert([instance])
        # We can't select * from table, as it doesn't select materialized and alias fields
        query = 'SELECT date_field, int_field, str_field, alias_int, alias_date, alias_str, alias_func' \
                ' FROM $db.%s ORDER BY alias_date' % ModelWithAliasFields.table_name()
        for model_cls in (ModelWithAliasFields, None):
            results = list(self.database.select(query, model_cls))
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].date_field, instance.date_field)
            self.assertEqual(results[0].int_field, instance.int_field)
            self.assertEqual(results[0].str_field, instance.str_field)
            self.assertEqual(results[0].alias_int, instance.int_field)
            self.assertEqual(results[0].alias_str, instance.str_field)
            self.assertEqual(results[0].alias_date, instance.date_field)
            self.assertEqual(results[0].alias_func, 201608)

    def test_assignment_error(self):
        # I can't prevent assigning at all, in case db.select statements with model provided sets model fields.
        instance = ModelWithAliasFields()
        for value in ('x', [date.today()], ['aaa'], [None]):
            with self.assertRaises(ValueError):
                instance.alias_date = value

    def test_wrong_field(self):
        with self.assertRaises(AssertionError):
            StringField(alias=123)

    def test_duplicate_default(self):
        with self.assertRaises(AssertionError):
            StringField(alias='str_field', default='with default')

        with self.assertRaises(AssertionError):
            StringField(alias='str_field', materialized='str_field')

    def test_default_value(self):
        instance = ModelWithAliasFields()
        self.assertEqual(instance.alias_str, NO_VALUE)
        # Check that NO_VALUE can be assigned to a field
        instance.str_field = NO_VALUE
        # Check that NO_VALUE can be assigned when creating a new instance
        instance2 = ModelWithAliasFields(**instance.to_dict())


class ModelWithAliasFields(Model):
    int_field = Int32Field()
    date_field = DateField()
    str_field = StringField()

    alias_str = StringField(alias=u'str_field')
    alias_int = Int32Field(alias='int_field')
    alias_date = DateField(alias='date_field')
    alias_func = Int32Field(alias=F.toYYYYMM(date_field))

    engine = MergeTree('date_field', ('date_field',))
