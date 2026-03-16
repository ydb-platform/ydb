# -*- coding: utf-8 -*-
import unittest
from decimal import Decimal

from infi.clickhouse_orm.database import Database, ServerError
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.engines import *

from .common import get_clickhouse_url


class DecimalFieldsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        try:
            self.database.create_table(DecimalModel)
        except ServerError as e:
            # This ClickHouse version does not support decimals yet
            raise unittest.SkipTest(e.message)

    def tearDown(self):
        self.database.drop_database()

    def _insert_sample_data(self):
        self.database.insert([
            DecimalModel(date_field='2016-08-20'),
            DecimalModel(date_field='2016-08-21', dec=Decimal('1.234')),
            DecimalModel(date_field='2016-08-22', dec32=Decimal('12342.2345')),
            DecimalModel(date_field='2016-08-23', dec64=Decimal('12342.23456')),
            DecimalModel(date_field='2016-08-24', dec128=Decimal('-4545456612342.234567')),
        ])

    def _assert_sample_data(self, results):
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0].dec, Decimal(0))
        self.assertEqual(results[0].dec32, Decimal(17))
        self.assertEqual(results[1].dec, Decimal('1.234'))
        self.assertEqual(results[2].dec32, Decimal('12342.2345'))
        self.assertEqual(results[3].dec64, Decimal('12342.23456'))
        self.assertEqual(results[4].dec128, Decimal('-4545456612342.234567'))

    def test_insert_and_select(self):
        self._insert_sample_data()
        query = 'SELECT * from $table ORDER BY date_field'
        results = list(self.database.select(query, DecimalModel))
        self._assert_sample_data(results)

    def test_ad_hoc_model(self):
        self._insert_sample_data()
        query = 'SELECT * from decimalmodel ORDER BY date_field'
        results = list(self.database.select(query))
        self._assert_sample_data(results)

    def test_rounding(self):
        d = Decimal('11111.2340000000000000001')
        self.database.insert([DecimalModel(date_field='2016-08-20', dec=d, dec32=d, dec64=d, dec128=d)])
        m = DecimalModel.objects_in(self.database)[0]
        for val in (m.dec, m.dec32, m.dec64, m.dec128):
            self.assertEqual(val, Decimal('11111.234'))

    def test_assignment_ok(self):
        for value in (True, False, 17, 3.14, '20.5', Decimal('20.5')):
            DecimalModel(dec=value)

    def test_assignment_error(self):
        for value in ('abc', u'זה ארוך', None, float('NaN'), Decimal('-Infinity')):
            with self.assertRaises(ValueError):
                DecimalModel(dec=value)

    def test_aggregation(self):
        self._insert_sample_data()
        result = DecimalModel.objects_in(self.database).aggregate(m='min(dec)', n='max(dec)')
        self.assertEqual(result[0].m, Decimal(0))
        self.assertEqual(result[0].n, Decimal('1.234'))

    def test_precision_and_scale(self):
        # Go over all valid combinations
        for precision in range(1, 39):
            for scale in range(0, precision + 1):
                f = DecimalField(precision, scale)
        # Some invalid combinations
        for precision, scale in [(0, 0), (-1, 7), (7, -1), (39, 5), (20, 21)]:
            with self.assertRaises(AssertionError):
                f = DecimalField(precision, scale)

    def test_min_max(self):
        # In range
        f = DecimalField(3, 1)
        f.validate(f.to_python('99.9', None))
        f.validate(f.to_python('-99.9', None))
        # In range after rounding
        f.validate(f.to_python('99.94', None))
        f.validate(f.to_python('-99.94', None))
        # Out of range
        with self.assertRaises(ValueError):
            f.validate(f.to_python('99.99', None))
        with self.assertRaises(ValueError):
            f.validate(f.to_python('-99.99', None))
        # In range
        f = Decimal32Field(4)
        f.validate(f.to_python('99999.9999', None))
        f.validate(f.to_python('-99999.9999', None))
        # In range after rounding
        f.validate(f.to_python('99999.99994', None))
        f.validate(f.to_python('-99999.99994', None))
        # Out of range
        with self.assertRaises(ValueError):
            f.validate(f.to_python('100000', None))
        with self.assertRaises(ValueError):
            f.validate(f.to_python('-100000', None))


class DecimalModel(Model):

    date_field  = DateField()
    dec         = DecimalField(15, 3)
    dec32       = Decimal32Field(4, default=17)
    dec64       = Decimal64Field(5)
    dec128      = Decimal128Field(6)

    engine = Memory()
