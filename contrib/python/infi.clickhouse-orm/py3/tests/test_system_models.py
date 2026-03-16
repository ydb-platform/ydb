import unittest
from datetime import date

import os

from infi.clickhouse_orm.database import Database, DatabaseException
from infi.clickhouse_orm.engines import *
from infi.clickhouse_orm.fields import *
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.system_models import SystemPart

from .common import get_clickhouse_url


class SystemTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)

    def tearDown(self):
        self.database.drop_database()

    def test_insert_system(self):
        m = SystemPart()
        with self.assertRaises(DatabaseException):
            self.database.insert([m])

    def test_create_readonly_table(self):
        with self.assertRaises(DatabaseException):
            self.database.create_table(SystemTestModel)

    def test_drop_readonly_table(self):
        with self.assertRaises(DatabaseException):
            self.database.drop_table(SystemTestModel)


class SystemPartTest(unittest.TestCase):

    BACKUP_DIRS = ['/var/lib/clickhouse/shadow', '/opt/clickhouse/shadow/']

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        self.database.create_table(TestTable)
        self.database.create_table(CustomPartitionedTable)
        self.database.insert([TestTable(date_field=date.today())])
        self.database.insert([CustomPartitionedTable(date_field=date.today(), group_field=13)])

    def tearDown(self):
        self.database.drop_database()

    def _get_backups(self):
        for dir in self.BACKUP_DIRS:
            if os.path.exists(dir):
                _, dirnames, _ = next(os.walk(dir))
                return dirnames
        raise unittest.SkipTest('Cannot find backups dir')

    def test_is_read_only(self):
        self.assertTrue(SystemPart.is_read_only())

    def test_is_system_model(self):
        self.assertTrue(SystemPart.is_system_model())

    def test_get_all(self):
        parts = SystemPart.get(self.database)
        self.assertEqual(len(list(parts)), 2)

    def test_get_active(self):
        parts = list(SystemPart.get_active(self.database))
        self.assertEqual(len(parts), 2)
        parts[0].detach()
        parts = list(SystemPart.get_active(self.database))
        self.assertEqual(len(parts), 1)

    def test_get_conditions(self):
        parts = list(SystemPart.get(self.database, conditions="table='testtable'"))
        self.assertEqual(len(parts), 1)
        parts = list(SystemPart.get(self.database, conditions=u"table='custompartitionedtable'"))
        self.assertEqual(len(parts), 1)
        parts = list(SystemPart.get(self.database, conditions=u"table='invalidtable'"))
        self.assertEqual(len(parts), 0)

    def test_attach_detach(self):
        parts = list(SystemPart.get_active(self.database))
        self.assertEqual(len(parts), 2)
        for p in parts:
            p.detach()
        self.assertEqual(len(list(SystemPart.get_active(self.database))), 0)
        for p in parts:
            p.attach()
        self.assertEqual(len(list(SystemPart.get_active(self.database))), 2)

    def test_drop(self):
        parts = list(SystemPart.get_active(self.database))
        for p in parts:
            p.drop()
        self.assertEqual(len(list(SystemPart.get_active(self.database))), 0)

    def test_freeze(self):
        parts = list(SystemPart.get(self.database))
        # There can be other backups in the folder
        prev_backups = set(self._get_backups())
        for p in parts:
            p.freeze()
        backups = set(self._get_backups())
        self.assertEqual(len(backups), len(prev_backups) + 2)

    def test_fetch(self):
        # TODO Not tested, as I have no replication set
        pass

    def test_query(self):
        SystemPart.objects_in(self.database).count()
        list(SystemPart.objects_in(self.database).filter(table='testtable'))


class TestTable(Model):
    date_field = DateField()

    engine = MergeTree('date_field', ('date_field',))


class CustomPartitionedTable(Model):
    date_field = DateField()
    group_field = UInt32Field()

    engine = MergeTree(order_by=('date_field', 'group_field'), partition_key=('toYYYYMM(date_field)', 'group_field'))


class SystemTestModel(Model):
    _system = True
