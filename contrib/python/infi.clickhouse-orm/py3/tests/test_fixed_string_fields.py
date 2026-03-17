# -*- coding: utf-8 -*-
import unittest

from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.engines import *

from .common import get_clickhouse_url


class FixedStringFieldsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        self.database.create_table(FixedStringModel)

    def tearDown(self):
        self.database.drop_database()

    def _insert_sample_data(self):
        self.database.insert([
            FixedStringModel(date_field='2016-08-30', fstr_field=''),
            FixedStringModel(date_field='2016-08-30'),
            FixedStringModel(date_field='2016-08-31', fstr_field='foo'),
            FixedStringModel(date_field='2016-08-31', fstr_field=u'לילה')
        ])

    def _assert_sample_data(self, results):
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0].fstr_field, '')
        self.assertEqual(results[1].fstr_field, 'ABCDEFGHIJK')
        self.assertEqual(results[2].fstr_field, 'foo')
        self.assertEqual(results[3].fstr_field, u'לילה')

    def test_insert_and_select(self):
        self._insert_sample_data()
        query = 'SELECT * from $table ORDER BY date_field'
        results = list(self.database.select(query, FixedStringModel))
        self._assert_sample_data(results)

    def test_ad_hoc_model(self):
        self._insert_sample_data()
        query = 'SELECT * from $db.fixedstringmodel ORDER BY date_field'
        results = list(self.database.select(query))
        self._assert_sample_data(results)

    def test_assignment_error(self):
        for value in (17, 'this is too long', u'זה ארוך', None, 99.9):
            with self.assertRaises(ValueError):
                FixedStringModel(fstr_field=value)


class FixedStringModel(Model):

    date_field = DateField()
    fstr_field = FixedStringField(12, default='ABCDEFGHIJK')

    engine = MergeTree('date_field', ('date_field',))
