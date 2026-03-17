
import unittest
import json

from infi.clickhouse_orm import database, engines, fields, models

from .common import get_clickhouse_url


class JoinTest(unittest.TestCase):

    def setUp(self):
        self.database = database.Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        self.database.create_table(Foo)
        self.database.create_table(Bar)
        self.database.insert([Foo(id=i) for i in range(3)])
        self.database.insert([Bar(id=i, b=i * i) for i in range(3)])

    def print_res(self, query):
        print(query)
        print(json.dumps([row.to_dict() for row in self.database.select(query)]))

    def test_without_db_name(self):
        self.print_res("SELECT * FROM {}".format(Foo.table_name()))
        self.print_res("SELECT * FROM {}".format(Bar.table_name()))
        self.print_res("SELECT b FROM {} ALL LEFT JOIN {} USING id".format(Foo.table_name(), Bar.table_name()))

    def test_with_db_name(self):
        self.print_res("SELECT * FROM $db.{}".format(Foo.table_name()))
        self.print_res("SELECT * FROM $db.{}".format(Bar.table_name()))
        self.print_res("SELECT b FROM $db.{} ALL LEFT JOIN $db.{} USING id".format(Foo.table_name(), Bar.table_name()))

    def test_with_subquery(self):
        self.print_res("SELECT b FROM {} ALL LEFT JOIN (SELECT * from {}) subquery USING id".format(Foo.table_name(), Bar.table_name()))
        self.print_res("SELECT b FROM $db.{} ALL LEFT JOIN (SELECT * from $db.{}) subquery USING id".format(Foo.table_name(), Bar.table_name()))


class Foo(models.Model):
    id = fields.UInt8Field()
    engine = engines.Memory()


class Bar(Foo):
    b = fields.UInt8Field()
