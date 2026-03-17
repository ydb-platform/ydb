# -*- coding: utf-8 -*-

from infi.clickhouse_orm.database import DatabaseException, ServerError
from .base_test_with_data import *

from .common import get_clickhouse_url


class ReadonlyTestCase(TestCaseWithData):

    def _test_readonly_db(self, username):
        self._insert_and_check(self._sample_data(), len(data))
        orig_database = self.database
        try:
            self.database = Database(orig_database.db_name, db_url=get_clickhouse_url(), username=username, readonly=True)
            with self.assertRaises(ServerError) as cm:
                self._insert_and_check(self._sample_data(), len(data))
            self._check_db_readonly_err(cm.exception)

            self.assertEqual(self.database.count(Person), 100)
            list(self.database.select('SELECT * from $table', Person))
            with self.assertRaises(ServerError) as cm:
                self.database.drop_table(Person)
            self._check_db_readonly_err(cm.exception, drop_table=True)

            with self.assertRaises(ServerError) as cm:
                self.database.drop_database()
            self._check_db_readonly_err(cm.exception, drop_table=True)
        except ServerError as e:
            if e.code == 192 and e.message.startswith('Unknown user'): # ClickHouse version < 20.3
                raise unittest.SkipTest('Database user "%s" is not defined' % username)
            elif e.code == 516 and e.message.startswith('readonly: Authentication failed'): # ClickHouse version >= 20.3
                raise unittest.SkipTest('Database user "%s" is not defined' % username)
            else:
                raise
        finally:
            self.database = orig_database

    def _check_db_readonly_err(self, exc, drop_table=None):
        self.assertEqual(exc.code, 164)
        print(exc.message)
        if self.database.server_version >= (20, 3):
            self.assertTrue('Cannot execute query in readonly mode' in exc.message)
        elif drop_table:
            self.assertTrue(exc.message.startswith('Cannot drop table in readonly mode'))
        else:
            self.assertTrue(exc.message.startswith('Cannot insert into table in readonly mode'))

    def test_readonly_db_with_default_user(self):
        self._test_readonly_db('default')

    def test_readonly_db_with_readonly_user(self):
        self._test_readonly_db('readonly')

    def test_insert_readonly(self):
        m = ReadOnlyModel(name='readonly')
        self.database.create_table(ReadOnlyModel)
        with self.assertRaises(DatabaseException):
            self.database.insert([m])

    def test_create_readonly_table(self):
        self.database.create_table(ReadOnlyModel)

    def test_drop_readonly_table(self):
        self.database.drop_table(ReadOnlyModel)

    def test_nonexisting_readonly_database(self):
        with self.assertRaises(DatabaseException) as cm:
            db = Database('dummy', db_url=get_clickhouse_url(), readonly=True)
        self.assertEqual(str(cm.exception), 'Database does not exist, and cannot be created under readonly connection')


class ReadOnlyModel(Model):
    _readonly = True

    name = StringField()
    date = DateField()
    engine = MergeTree('date', ('name',))
