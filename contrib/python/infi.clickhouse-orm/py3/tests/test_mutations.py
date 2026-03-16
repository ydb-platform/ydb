import unittest
from infi.clickhouse_orm import F
from .base_test_with_data import *
from time import sleep


class MutationsTestCase(TestCaseWithData):

    def setUp(self):
        super().setUp()
        if self.database.server_version < (18,):
            raise unittest.SkipTest('ClickHouse version too old')
        self._insert_all()

    def _wait_for_mutations(self):
        sql = 'SELECT * FROM system.mutations WHERE is_done = 0'
        while list(self.database.raw(sql)):
            sleep(0.25)

    def test_delete_all(self):
        Person.objects_in(self.database).delete()
        self._wait_for_mutations()
        self.assertFalse(Person.objects_in(self.database))

    def test_delete_with_where_cond(self):
        cond = Person.first_name == 'Cassady'
        self.assertTrue(Person.objects_in(self.database).filter(cond))
        Person.objects_in(self.database).filter(cond).delete()
        self._wait_for_mutations()
        self.assertFalse(Person.objects_in(self.database).filter(cond))
        self.assertTrue(Person.objects_in(self.database).exclude(cond))

    def test_delete_with_prewhere_cond(self):
        cond = F.toYear(Person.birthday) == 1977
        self.assertTrue(Person.objects_in(self.database).filter(cond))
        Person.objects_in(self.database).filter(cond, prewhere=True).delete()
        self._wait_for_mutations()
        self.assertFalse(Person.objects_in(self.database).filter(cond))
        self.assertTrue(Person.objects_in(self.database).exclude(cond))

    def test_update_all(self):
        Person.objects_in(self.database).update(height=0)
        self._wait_for_mutations()
        for p in Person.objects_in(self.database): print(p.height)
        self.assertFalse(Person.objects_in(self.database).exclude(height=0))

    def test_update_with_where_cond(self):
        cond = Person.first_name == 'Cassady'
        Person.objects_in(self.database).filter(cond).update(height=0)
        self._wait_for_mutations()
        self.assertFalse(Person.objects_in(self.database).filter(cond).exclude(height=0))

    def test_update_with_prewhere_cond(self):
        cond = F.toYear(Person.birthday) == 1977
        Person.objects_in(self.database).filter(cond, prewhere=True).update(height=0)
        self._wait_for_mutations()
        self.assertFalse(Person.objects_in(self.database).filter(cond).exclude(height=0))

    def test_update_multiple_fields(self):
        Person.objects_in(self.database).update(height=0, passport=None)
        self._wait_for_mutations()
        self.assertFalse(Person.objects_in(self.database).exclude(height=0))
        self.assertFalse(Person.objects_in(self.database).exclude(passport=None))

    def test_chained_update(self):
        Person.objects_in(self.database).update(height=F.rand()).update(passport=99999)
        self._wait_for_mutations()
        self.assertFalse(Person.objects_in(self.database).exclude(passport=99999))

    def test_invalid_state_for_mutations(self):
        base_query = Person.objects_in(self.database)
        queries = [
            base_query[0:1],
            base_query.limit_by(5, 'first_name'),
            base_query.distinct(),
            base_query.aggregate('first_name', count=F.count())
        ]
        for query in queries:
            print(query)
            with self.assertRaises(AssertionError):
                query.delete()
            with self.assertRaises(AssertionError):
                query.update(height=1.8)

    def test_missing_fields_for_update(self):
        with self.assertRaises(AssertionError):
            Person.objects_in(self.database).update()
