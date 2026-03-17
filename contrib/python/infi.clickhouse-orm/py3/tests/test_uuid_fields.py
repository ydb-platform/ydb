import unittest
from uuid import UUID
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.fields import Int16Field, UUIDField
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.engines import Memory

from .common import get_clickhouse_url


class UUIDFieldsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)

    def tearDown(self):
        self.database.drop_database()

    def test_uuid_field(self):
        if self.database.server_version < (18, 1):
            raise unittest.SkipTest('ClickHouse version too old')
        # Create a model
        class TestModel(Model):
            i = Int16Field()
            f = UUIDField()
            engine = Memory()
        self.database.create_table(TestModel)
        # Check valid values (all values are the same UUID)
        values = [
            '12345678-1234-5678-1234-567812345678',
            '{12345678-1234-5678-1234-567812345678}',
            '12345678123456781234567812345678',
            'urn:uuid:12345678-1234-5678-1234-567812345678',
            b'\x12\x34\x56\x78'*4,
            (0x12345678, 0x1234, 0x5678, 0x12, 0x34, 0x567812345678),
            0x12345678123456781234567812345678,
            UUID(int=0x12345678123456781234567812345678),
        ]
        for index, value in enumerate(values):
            rec = TestModel(i=index, f=value)
            self.database.insert([rec])
        for rec in TestModel.objects_in(self.database):
            self.assertEqual(rec.f, UUID(values[0]))
        # Check invalid values
        for value in [None, 'zzz', -1, '123']:
            with self.assertRaises(ValueError):
                TestModel(i=1, f=value)

