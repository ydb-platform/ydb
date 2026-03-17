import unittest

from infi.clickhouse_orm import *
from .base_test_with_data import Person

from .common import get_clickhouse_url


class ConstraintsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        if self.database.server_version < (19, 14, 3, 3):
            raise unittest.SkipTest('ClickHouse version too old')
        self.database.create_table(PersonWithConstraints)

    def tearDown(self):
        self.database.drop_database()

    def test_insert_valid_values(self):
        self.database.insert([
            PersonWithConstraints(first_name="Mike", last_name="Caruzo", birthday="2000-01-01", height=1.66)
        ])

    def test_insert_invalid_values(self):
        with self.assertRaises(ServerError) as e:
            self.database.insert([
                PersonWithConstraints(first_name="Mike", last_name="Caruzo", birthday="2100-01-01", height=1.66)
            ])
            self.assertEqual(e.code, 469)
            self.assertTrue('Constraint `birthday_in_the_past`' in e.message)

        with self.assertRaises(ServerError) as e:
            self.database.insert([
                PersonWithConstraints(first_name="Mike", last_name="Caruzo", birthday="1970-01-01", height=3)
            ])
            self.assertEqual(e.code, 469)
            self.assertTrue('Constraint `max_height`' in e.message)


class PersonWithConstraints(Person):

    birthday_in_the_past = Constraint(Person.birthday <= F.today())
    max_height = Constraint(Person.height <= 2.75)


