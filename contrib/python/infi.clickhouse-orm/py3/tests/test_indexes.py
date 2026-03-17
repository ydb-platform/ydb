import unittest

from infi.clickhouse_orm import *

from .common import get_clickhouse_url


class IndexesTest(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)
        if self.database.server_version < (20, 1, 2, 4):
            raise unittest.SkipTest('ClickHouse version too old')

    def tearDown(self):
        self.database.drop_database()

    def test_all_index_types(self):
        self.database.create_table(ModelWithIndexes)


class ModelWithIndexes(Model):

    date = DateField()
    f1 = Int32Field()
    f2 = StringField()

    i1 = Index(f1, type=Index.minmax(), granularity=1)
    i2 = Index(f1, type=Index.set(1000), granularity=2)
    i3 = Index(f2, type=Index.ngrambf_v1(3, 256, 2, 0), granularity=1)
    i4 = Index(F.lower(f2), type=Index.tokenbf_v1(256, 2, 0), granularity=2)
    i5 = Index((F.toQuarter(date), f2), type=Index.bloom_filter(), granularity=3)

    engine = MergeTree('date', ('date',))
