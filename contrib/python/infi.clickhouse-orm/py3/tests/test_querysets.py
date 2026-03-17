# -*- coding: utf-8 -*-
import unittest
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.query import Q
from infi.clickhouse_orm.funcs import F
from .base_test_with_data import *
from datetime import date, datetime
from enum import Enum
from decimal import Decimal

from logging import getLogger
logger = getLogger('tests')

from .common import get_clickhouse_url



class QuerySetTestCase(TestCaseWithData):

    def setUp(self):
        super(QuerySetTestCase, self).setUp()
        self.database.insert(self._sample_data())

    def _test_qs(self, qs, expected_count):
        logger.info(qs.as_sql())
        count = 0
        for instance in qs:
            count += 1
            logger.info('\t[%d]\t%s' % (count, instance.to_dict()))
        self.assertEqual(count, expected_count)
        self.assertEqual(qs.count(), expected_count)

    def test_prewhere(self):
        # We can't distinguish prewhere and where results, it affects performance only.
        # So let's control prewhere acts like where does
        qs = Person.objects_in(self.database)
        self.assertTrue(qs.filter(first_name='Connor', prewhere=True))
        self.assertFalse(qs.filter(first_name='Willy', prewhere=True))

    def test_no_filtering(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs, len(data))

    def test_truthiness(self):
        qs = Person.objects_in(self.database)
        self.assertTrue(qs.filter(first_name='Connor'))
        self.assertFalse(qs.filter(first_name='Willy'))

    def test_filter_null_value(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs.filter(passport=None), 98)
        self._test_qs(qs.exclude(passport=None), 2)
        self._test_qs(qs.filter(passport__ne=None), 2)
        self._test_qs(qs.exclude(passport__ne=None), 98)

    def test_filter_string_field(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs.filter(first_name='Ciaran'), 2)
        self._test_qs(qs.filter(first_name='ciaran'), 0) # case sensitive
        self._test_qs(qs.filter(first_name__iexact='ciaran'), 2) # case insensitive
        self._test_qs(qs.filter(first_name__gt='Whilemina'), 4)
        self._test_qs(qs.filter(first_name__gte='Whilemina'), 5)
        self._test_qs(qs.filter(first_name__lt='Adam'), 1)
        self._test_qs(qs.filter(first_name__lte='Adam'), 2)
        self._test_qs(qs.filter(first_name__in=('Connor', 'Courtney')), 3) # in tuple
        self._test_qs(qs.filter(first_name__in=['Connor', 'Courtney']), 3) # in list
        self._test_qs(qs.filter(first_name__in="'Connor', 'Courtney'"), 3) # in string
        self._test_qs(qs.filter(first_name__not_in="'Connor', 'Courtney'"), 97)
        self._test_qs(qs.filter(first_name__contains='sh'), 3) # case sensitive
        self._test_qs(qs.filter(first_name__icontains='sh'), 6) # case insensitive
        self._test_qs(qs.filter(first_name__startswith='le'), 0) # case sensitive
        self._test_qs(qs.filter(first_name__istartswith='Le'), 2) # case insensitive
        self._test_qs(qs.filter(first_name__istartswith=''), 100) # empty prefix
        self._test_qs(qs.filter(first_name__endswith='IA'), 0) # case sensitive
        self._test_qs(qs.filter(first_name__iendswith='ia'), 3) # case insensitive
        self._test_qs(qs.filter(first_name__iendswith=''), 100) # empty suffix

    def test_filter_with_q_objects(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs.filter(Q(first_name='Ciaran')), 2)
        self._test_qs(qs.filter(Q(first_name='Ciaran') | Q(first_name='Chelsea')), 3)
        self._test_qs(qs.filter(Q(first_name__in=['Warren', 'Whilemina', 'Whitney']) & Q(height__gte=1.7)), 3)
        self._test_qs(qs.filter((Q(first_name__in=['Warren', 'Whilemina', 'Whitney']) & Q(height__gte=1.7) |
                                 (Q(first_name__in=['Victoria', 'Victor', 'Venus']) & Q(height__lt=1.7)))), 4)
        self._test_qs(qs.filter(Q(first_name='Elton') & ~Q(last_name='Smith')), 1)
        # Check operator precendence
        self._test_qs(qs.filter(first_name='Cassady').filter(Q(last_name='Knapp') | Q(last_name='Rogers') | Q(last_name='Gregory')), 2)
        self._test_qs(qs.filter(Q(first_name='Cassady') & Q(last_name='Knapp') | Q(first_name='Beatrice') & Q(last_name='Gregory')), 2)
        self._test_qs(qs.filter(Q(first_name='Courtney') | Q(first_name='Cassady') & Q(last_name='Knapp')), 3)

    def test_filter_unicode_string(self):
        self.database.insert([
            Person(first_name=u'דונלד', last_name=u'דאק')
        ])
        qs = Person.objects_in(self.database)
        self._test_qs(qs.filter(first_name=u'דונלד'), 1)

    def test_filter_float_field(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs.filter(height__gt=2), 0)
        self._test_qs(qs.filter(height__lt=1.61), 4)
        self._test_qs(qs.filter(height__lt='1.61'), 4)
        self._test_qs(qs.exclude(height__lt='1.61'), 96)
        self._test_qs(qs.filter(height__gt=0), 100)
        self._test_qs(qs.exclude(height__gt=0), 0)

    def test_filter_date_field(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs.filter(birthday='1970-12-02'), 1)
        self._test_qs(qs.filter(birthday__eq='1970-12-02'), 1)
        self._test_qs(qs.filter(birthday__ne='1970-12-02'), 99)
        self._test_qs(qs.filter(birthday=date(1970, 12, 2)), 1)
        self._test_qs(qs.filter(birthday__lte=date(1970, 12, 2)), 3)

    def test_mutiple_filter(self):
        qs = Person.objects_in(self.database)
        # Single filter call with multiple conditions is ANDed
        self._test_qs(qs.filter(first_name='Ciaran', last_name='Carver'), 1)
        # Separate filter calls are also ANDed
        self._test_qs(qs.filter(first_name='Ciaran').filter(last_name='Carver'), 1)
        self._test_qs(qs.filter(birthday='1970-12-02').filter(birthday='1986-01-07'), 0)

    def test_multiple_exclude(self):
        qs = Person.objects_in(self.database)
        # Single exclude call with multiple conditions is ANDed
        self._test_qs(qs.exclude(first_name='Ciaran', last_name='Carver'), 99)
        # Separate exclude calls are ORed
        self._test_qs(qs.exclude(first_name='Ciaran').exclude(last_name='Carver'), 98)
        self._test_qs(qs.exclude(birthday='1970-12-02').exclude(birthday='1986-01-07'), 98)

    def test_only(self):
        qs = Person.objects_in(self.database).only('first_name', 'last_name')
        for person in qs:
            self.assertTrue(person.first_name)
            self.assertTrue(person.last_name)
            self.assertFalse(person.height)
            self.assertEqual(person.birthday, date(1970, 1, 1))

    def test_order_by(self):
        qs = Person.objects_in(self.database)
        self.assertFalse('ORDER BY' in qs.as_sql())
        self.assertFalse(qs.order_by_as_sql())
        person = list(qs.order_by('first_name', 'last_name'))[0]
        self.assertEqual(person.first_name, 'Abdul')
        person = list(qs.order_by('-first_name', '-last_name'))[0]
        self.assertEqual(person.first_name, 'Yolanda')
        person = list(qs.order_by('height'))[0]
        self.assertAlmostEqual(person.height, 1.59, places=5)
        person = list(qs.order_by('-height'))[0]
        self.assertAlmostEqual(person.height, 1.8, places=5)

    def test_in_subquery(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs.filter(height__in='SELECT max(height) FROM $table'), 2)
        self._test_qs(qs.filter(first_name__in=qs.only('last_name')), 2)
        self._test_qs(qs.filter(first_name__not_in=qs.only('last_name')), 98)

    def _insert_sample_model(self):
        self.database.create_table(SampleModel)
        now = datetime.now()
        self.database.insert([
            SampleModel(timestamp=now, num=1, color=Color.red),
            SampleModel(timestamp=now, num=2, color=Color.red),
            SampleModel(timestamp=now, num=3, color=Color.blue),
            SampleModel(timestamp=now, num=4, color=Color.white),
        ])

    def _insert_sample_collapsing_model(self):
        self.database.create_table(SampleCollapsingModel)
        now = datetime.now()
        self.database.insert([
            SampleCollapsingModel(timestamp=now, num=1, color=Color.red),
            SampleCollapsingModel(timestamp=now, num=2, color=Color.red),
            SampleCollapsingModel(timestamp=now, num=2, color=Color.red, sign=-1),
            SampleCollapsingModel(timestamp=now, num=2, color=Color.green),
            SampleCollapsingModel(timestamp=now, num=3, color=Color.white),
            SampleCollapsingModel(timestamp=now, num=4, color=Color.white, sign=1),
            SampleCollapsingModel(timestamp=now, num=4, color=Color.white, sign=-1),
            SampleCollapsingModel(timestamp=now, num=4, color=Color.blue, sign=1),
        ])

    def test_filter_enum_field(self):
        self._insert_sample_model()
        qs = SampleModel.objects_in(self.database)
        self._test_qs(qs.filter(color=Color.red), 2)
        self._test_qs(qs.exclude(color=Color.white), 3)
        # Different ways to specify blue
        self._test_qs(qs.filter(color__gt=Color.blue), 1)
        self._test_qs(qs.filter(color__gt='blue'), 1)
        self._test_qs(qs.filter(color__gt=2), 1)

    def test_filter_int_field(self):
        self._insert_sample_model()
        qs = SampleModel.objects_in(self.database)
        self._test_qs(qs.filter(num=1), 1)
        self._test_qs(qs.filter(num__eq=1), 1)
        self._test_qs(qs.filter(num__ne=1), 3)
        self._test_qs(qs.filter(num__gt=1), 3)
        self._test_qs(qs.filter(num__gte=1), 4)
        self._test_qs(qs.filter(num__in=(1, 2, 3)), 3)
        self._test_qs(qs.filter(num__in=range(1, 4)), 3)

    def test_slicing(self):
        db = Database('system', db_url=get_clickhouse_url())
        numbers = list(range(100))
        qs = Numbers.objects_in(db)
        self.assertEqual(qs[0].number, numbers[0])
        self.assertEqual(qs[5].number, numbers[5])
        self.assertEqual([row.number for row in qs[:1]], numbers[:1])
        self.assertEqual([row.number for row in qs[:10]], numbers[:10])
        self.assertEqual([row.number for row in qs[3:10]], numbers[3:10])
        self.assertEqual([row.number for row in qs[9:10]], numbers[9:10])
        self.assertEqual([row.number for row in qs[10:10]], numbers[10:10])

    def test_invalid_slicing(self):
        db = Database('system', db_url=get_clickhouse_url())
        qs = Numbers.objects_in(db)
        with self.assertRaises(AssertionError):
            qs[3:10:2]
        with self.assertRaises(AssertionError):
            qs[-5]
        with self.assertRaises(AssertionError):
            qs[:-5]
        with self.assertRaises(AssertionError):
            qs[50:1]

    def test_pagination(self):
        qs = Person.objects_in(self.database).order_by('first_name', 'last_name')
        # Try different page sizes
        for page_size in (1, 2, 7, 10, 30, 100, 150):
            # Iterate over pages and collect all instances
            page_num = 1
            instances = set()
            while True:
                page = qs.paginate(page_num, page_size)
                self.assertEqual(page.number_of_objects, len(data))
                self.assertGreater(page.pages_total, 0)
                [instances.add(obj.to_tsv()) for obj in page.objects]
                if page.pages_total == page_num:
                    break
                page_num += 1
            # Verify that all instances were returned
            self.assertEqual(len(instances), len(data))

    def test_pagination_last_page(self):
        qs = Person.objects_in(self.database).order_by('first_name', 'last_name')
        # Try different page sizes
        for page_size in (1, 2, 7, 10, 30, 100, 150):
            # Ask for the last page in two different ways and verify equality
            page_a = qs.paginate(-1, page_size)
            page_b = qs.paginate(page_a.pages_total, page_size)
            self.assertEqual(page_a[1:], page_b[1:])
            self.assertEqual([obj.to_tsv() for obj in page_a.objects],
                              [obj.to_tsv() for obj in page_b.objects])

    def test_pagination_invalid_page(self):
        qs = Person.objects_in(self.database).order_by('first_name', 'last_name')
        for page_num in (0, -2, -100):
            with self.assertRaises(ValueError):
                qs.paginate(page_num, 100)

    def test_pagination_with_conditions(self):
        qs = Person.objects_in(self.database).order_by('first_name', 'last_name').filter(first_name__lt='Ava')
        page = qs.paginate(1, 100)
        self.assertEqual(page.number_of_objects, 10)

    def test_distinct(self):
        qs = Person.objects_in(self.database).distinct()
        self._test_qs(qs, 100)
        self._test_qs(qs.only('first_name'), 94)

    def test_materialized_field(self):
        self._insert_sample_model()
        qs = SampleModel.objects_in(self.database)
        for obj in qs:
            self.assertTrue(obj.materialized_date != DateField.min_value)

    def test_alias_field(self):
        self._insert_sample_model()
        qs = SampleModel.objects_in(self.database)
        for obj in qs:
            self.assertTrue(obj.num_squared == obj.num ** 2)

    def test_count_of_slice(self):
        qs = Person.objects_in(self.database)
        self._test_qs(qs[:70], 70)
        self._test_qs(qs[70:80], 10)
        self._test_qs(qs[80:], 20)

    def test_final(self):
        # Final can be used with CollapsingMergeTree/ReplacingMergeTree engines only
        with self.assertRaises(TypeError):
            Person.objects_in(self.database).final()

        self._insert_sample_collapsing_model()
        res = list(SampleCollapsingModel.objects_in(self.database).final().order_by('num'))
        self.assertEqual(4, len(res))
        for item, exp_color in zip(res, (Color.red, Color.green, Color.white, Color.blue)):
            self.assertEqual(exp_color, item.color)

    def test_mixed_filter(self):
        qs = Person.objects_in(self.database)
        qs = qs.filter(Q(first_name='a'), F('greater', Person.height, 1.7), last_name='b')
        self.assertEqual(qs.conditions_as_sql(),
                         "(first_name = 'a') AND (greater(`height`, 1.7)) AND (last_name = 'b')")

    def test_precedence_of_negation(self):
        p = ~Q(first_name='a')
        q = Q(last_name='b')
        r = p & q
        self.assertEqual(r.to_sql(Person), "(last_name = 'b') AND (NOT (first_name = 'a'))")
        r = q & p
        self.assertEqual(r.to_sql(Person), "(last_name = 'b') AND (NOT (first_name = 'a'))")
        r = q | p
        self.assertEqual(r.to_sql(Person), "(last_name = 'b') OR (NOT (first_name = 'a'))")
        r = ~q & p
        self.assertEqual(r.to_sql(Person), "(NOT (last_name = 'b')) AND (NOT (first_name = 'a'))")

    def test_invalid_filter(self):
        qs = Person.objects_in(self.database)
        with self.assertRaises(TypeError):
            qs.filter('foo')


class AggregateTestCase(TestCaseWithData):

    def setUp(self):
        super(AggregateTestCase, self).setUp()
        self.database.insert(self._sample_data())

    def test_aggregate_no_grouping(self):
        qs = Person.objects_in(self.database).aggregate(average_height='avg(height)', count='count()')
        print(qs.as_sql())
        self.assertEqual(qs.count(), 1)
        for row in qs:
            self.assertAlmostEqual(row.average_height, 1.6923, places=4)
            self.assertEqual(row.count, 100)
        # With functions
        qs = Person.objects_in(self.database).aggregate(average_height=F.avg(Person.height), count=F.count())
        print(qs.as_sql())
        self.assertEqual(qs.count(), 1)
        for row in qs:
            self.assertAlmostEqual(row.average_height, 1.6923, places=4)
            self.assertEqual(row.count, 100)

    def test_aggregate_with_filter(self):
        # When filter comes before aggregate
        qs = Person.objects_in(self.database).filter(first_name='Warren').aggregate(average_height='avg(height)', count='count()')
        print(qs.as_sql())
        self.assertEqual(qs.count(), 1)
        for row in qs:
            self.assertAlmostEqual(row.average_height, 1.675, places=4)
            self.assertEqual(row.count, 2)
        # When filter comes after aggregate
        qs = Person.objects_in(self.database).aggregate(average_height='avg(height)', count='count()').filter(first_name='Warren')
        print(qs.as_sql())
        self.assertEqual(qs.count(), 1)
        for row in qs:
            self.assertAlmostEqual(row.average_height, 1.675, places=4)
            self.assertEqual(row.count, 2)

    def test_aggregate_with_filter__funcs(self):
        # When filter comes before aggregate
        qs = Person.objects_in(self.database).filter(Person.first_name=='Warren').aggregate(average_height=F.avg(Person.height), count=F.count())
        print(qs.as_sql())
        self.assertEqual(qs.count(), 1)
        for row in qs:
            self.assertAlmostEqual(row.average_height, 1.675, places=4)
            self.assertEqual(row.count, 2)
        # When filter comes after aggregate
        qs = Person.objects_in(self.database).aggregate(average_height=F.avg(Person.height), count=F.count()).filter(Person.first_name=='Warren')
        print(qs.as_sql())
        self.assertEqual(qs.count(), 1)
        for row in qs:
            self.assertAlmostEqual(row.average_height, 1.675, places=4)
            self.assertEqual(row.count, 2)

    def test_aggregate_with_implicit_grouping(self):
        qs = Person.objects_in(self.database).aggregate('first_name', average_height='avg(height)', count='count()')
        print(qs.as_sql())
        self.assertEqual(qs.count(), 94)
        total = 0
        for row in qs:
            self.assertTrue(1.5 < row.average_height < 2)
            self.assertTrue(0 < row.count < 3)
            total += row.count
        self.assertEqual(total, 100)

    def test_aggregate_with_explicit_grouping(self):
        qs = Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').group_by('weekday')
        print(qs.as_sql())
        self.assertEqual(qs.count(), 7)
        total = 0
        for row in qs:
            total += row.count
        self.assertEqual(total, 100)

    def test_aggregate_with_order_by(self):
        qs = Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').group_by('weekday')
        days = [row.weekday for row in qs.order_by('weekday')]
        self.assertEqual(days, list(range(1, 8)))

    def test_aggregate_with_indexing(self):
        qs = Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').group_by('weekday')
        total = 0
        for i in range(7):
            total += qs[i].count
        self.assertEqual(total, 100)

    def test_aggregate_with_slicing(self):
        qs = Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').group_by('weekday')
        total = sum(row.count for row in qs[:3]) + sum(row.count for row in qs[3:])
        self.assertEqual(total, 100)

    def test_aggregate_with_pagination(self):
        qs = Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').group_by('weekday')
        total = 0
        page_num = 1
        while True:
            page = qs.paginate(page_num, page_size=3)
            self.assertEqual(page.number_of_objects, 7)
            total += sum(row.count for row in page.objects)
            if page.pages_total == page_num:
                break
            page_num += 1
        self.assertEqual(total, 100)

    def test_aggregate_with_wrong_grouping(self):
        with self.assertRaises(AssertionError):
            Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').group_by('first_name')

    def test_aggregate_with_no_calculated_fields(self):
        with self.assertRaises(AssertionError):
            Person.objects_in(self.database).aggregate()

    def test_aggregate_with_only(self):
        # Cannot put only() after aggregate()
        with self.assertRaises(NotImplementedError):
            Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').only('weekday')
        # When only() comes before aggregate(), it gets overridden
        qs = Person.objects_in(self.database).only('last_name').aggregate(average_height='avg(height)', count='count()')
        self.assertTrue('last_name' not in qs.as_sql())

    def test_aggregate_on_aggregate(self):
        with self.assertRaises(NotImplementedError):
            Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').aggregate(s='sum(height)')

    def test_filter_on_calculated_field(self):
        # This is currently not supported, so we expect it to fail
        with self.assertRaises(AttributeError):
            qs = Person.objects_in(self.database).aggregate(weekday='toDayOfWeek(birthday)', count='count()').group_by('weekday')
            qs = qs.filter(weekday=1)
            self.assertEqual(qs.count(), 1)

    def test_aggregate_with_distinct(self):
        # In this case distinct has no effect
        qs = Person.objects_in(self.database).aggregate(average_height='avg(height)').distinct()
        print(qs.as_sql())
        self.assertEqual(qs.count(), 1)

    def test_aggregate_with_totals(self):
        qs = Person.objects_in(self.database).aggregate('first_name', count='count()').\
            with_totals().order_by('-count')[:5]
        print(qs.as_sql())
        result = list(qs)
        self.assertEqual(len(result), 6)
        for row in result[:-1]:
            self.assertEqual(2, row.count)

        self.assertEqual(100, result[-1].count)

    def test_double_underscore_field(self):
        class Mdl(Model):
            the__number = Int32Field()
            the__next__number = Int32Field()
            engine = Memory()
        qs = Mdl.objects_in(self.database).filter(the__number=1)
        self.assertEqual(qs.conditions_as_sql(), 'the__number = 1')
        qs = Mdl.objects_in(self.database).filter(the__number__gt=1)
        self.assertEqual(qs.conditions_as_sql(), 'the__number > 1')
        qs = Mdl.objects_in(self.database).filter(the__next__number=1)
        self.assertEqual(qs.conditions_as_sql(), 'the__next__number = 1')
        qs = Mdl.objects_in(self.database).filter(the__next__number__gt=1)
        self.assertEqual(qs.conditions_as_sql(), 'the__next__number > 1')

    def test_limit_by(self):
        if self.database.server_version < (19, 17):
            raise unittest.SkipTest('ClickHouse version too old')
        # Test without offset
        qs = Person.objects_in(self.database).aggregate('first_name', 'last_name', 'height', n='count()').\
            order_by('first_name', '-height').limit_by(1, 'first_name')
        self.assertEqual(qs.count(), 94)
        self.assertEqual(list(qs)[89].last_name, 'Bowen')
        # Test with funcs and fields
        qs = Person.objects_in(self.database).aggregate(Person.first_name, Person.last_name, Person.height, n=F.count()).\
            order_by(Person.first_name, '-height').limit_by(1, F.upper(Person.first_name))
        self.assertEqual(qs.count(), 94)
        self.assertEqual(list(qs)[89].last_name, 'Bowen')
        # Test with limit and offset, also mixing LIMIT with LIMIT BY
        qs = Person.objects_in(self.database).filter(height__gt=1.67 + 0.0001).order_by('height', 'first_name')
        limited_qs = qs.limit_by((0, 3), 'height')
        self.assertEqual([p.first_name for p in limited_qs[:3]], ['Amanda', 'Buffy', 'Dora'])
        limited_qs = qs.limit_by((3, 3), 'height')
        self.assertEqual([p.first_name for p in limited_qs[:3]], ['Elton', 'Josiah', 'Macaulay'])
        limited_qs = qs.limit_by((6, 3), 'height')
        self.assertEqual([p.first_name for p in limited_qs[:3]], ['Norman', 'Octavius', 'Oliver'])


Color = Enum('Color', u'red blue green yellow brown white black')


class SampleModel(Model):

    timestamp = DateTimeField()
    materialized_date = DateField(materialized='toDate(timestamp)')
    num = Int32Field()
    color = Enum8Field(Color)
    num_squared = Int32Field(alias='num*num')

    engine = MergeTree('materialized_date', ('materialized_date',))


class SampleCollapsingModel(SampleModel):

    sign = Int8Field(default=1)

    engine = CollapsingMergeTree('materialized_date', ('num',), 'sign')


class Numbers(Model):

    number = UInt64Field()


