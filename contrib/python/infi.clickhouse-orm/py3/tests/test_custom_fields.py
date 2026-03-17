import unittest
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.fields import Field, Int16Field
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.engines import Memory

from .common import get_clickhouse_url


class CustomFieldsTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)

    def tearDown(self):
        self.database.drop_database()

    def test_boolean_field(self):
        # Create a model
        class TestModel(Model):
            i = Int16Field()
            f = BooleanField()
            engine = Memory()
        self.database.create_table(TestModel)
        # Check valid values
        for index, value in enumerate([1, '1', True, 0, '0', False]):
            rec = TestModel(i=index, f=value)
            self.database.insert([rec])
        self.assertEqual([rec.f for rec in TestModel.objects_in(self.database).order_by('i')],
                          [True, True, True, False, False, False])
        # Check invalid values
        for value in [None, 'zzz', -5, 7]:
            with self.assertRaises(ValueError):
                TestModel(i=1, f=value)


class BooleanField(Field):

    # The ClickHouse column type to use
    db_type = 'UInt8'

    # The default value if empty
    class_default = False

    def to_python(self, value, timezone_in_use):
        # Convert valid values to bool
        if value in (1, '1', True):
            return True
        elif value in (0, '0', False):
            return False
        else:
            raise ValueError('Invalid value for BooleanField: %r' % value)

    def to_db_string(self, value, quote=True):
        # The value was already converted by to_python, so it's a bool
        return '1' if value else '0'

