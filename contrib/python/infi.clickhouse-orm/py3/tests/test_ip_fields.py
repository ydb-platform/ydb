import unittest
from ipaddress import IPv4Address, IPv6Address
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.fields import Int16Field, IPv4Field, IPv6Field
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.engines import Memory

from .common import get_clickhouse_url


class IPFieldsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)

    def tearDown(self):
        self.database.drop_database()

    def test_ipv4_field(self):
        if self.database.server_version < (19, 17):
            raise unittest.SkipTest('ClickHouse version too old')
        # Create a model
        class TestModel(Model):
            i = Int16Field()
            f = IPv4Field()
            engine = Memory()
        self.database.create_table(TestModel)
        # Check valid values (all values are the same ip)
        values = [
            '1.2.3.4',
            b'\x01\x02\x03\x04',
            16909060,
            IPv4Address('1.2.3.4')
        ]
        for index, value in enumerate(values):
            rec = TestModel(i=index, f=value)
            self.database.insert([rec])
        for rec in TestModel.objects_in(self.database):
            self.assertEqual(rec.f, IPv4Address(values[0]))
        # Check invalid values
        for value in [None, 'zzz', -1, '123']:
            with self.assertRaises(ValueError):
                TestModel(i=1, f=value)

    def test_ipv6_field(self):
        if self.database.server_version < (19, 17):
            raise unittest.SkipTest('ClickHouse version too old')
        # Create a model
        class TestModel(Model):
            i = Int16Field()
            f = IPv6Field()
            engine = Memory()
        self.database.create_table(TestModel)
        # Check valid values (all values are the same ip)
        values = [
            '2a02:e980:1e::1',
            b'*\x02\xe9\x80\x00\x1e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01',
            55842696359362256756849388082849382401,
            IPv6Address('2a02:e980:1e::1')
        ]
        for index, value in enumerate(values):
            rec = TestModel(i=index, f=value)
            self.database.insert([rec])
        for rec in TestModel.objects_in(self.database):
            self.assertEqual(rec.f, IPv6Address(values[0]))
        # Check invalid values
        for value in [None, 'zzz', -1, '123']:
            with self.assertRaises(ValueError):
                TestModel(i=1, f=value)

