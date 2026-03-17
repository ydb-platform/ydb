import unittest
import datetime

from infi.clickhouse_orm import *

import logging
logging.getLogger("requests").setLevel(logging.WARNING)

from .common import get_clickhouse_url


class _EnginesHelperTestCase(unittest.TestCase):

    def setUp(self):
        self.database = Database('test-db', db_url=get_clickhouse_url(), log_statements=True)

    def tearDown(self):
        self.database.drop_database()


class EnginesTestCase(_EnginesHelperTestCase):
    def _create_and_insert(self, model_class, **kwargs):
        self.database.create_table(model_class)
        self.database.insert([
            model_class(date='2017-01-01', event_id=23423, event_group=13, event_count=7, event_version=1, **kwargs)
        ])

    def test_merge_tree(self):
        class TestModel(SampleModel):
            engine = MergeTree('date', ('date', 'event_id', 'event_group'))
        self._create_and_insert(TestModel)

    def test_merge_tree_with_sampling(self):
        class TestModel(SampleModel):
            engine = MergeTree('date', ('date', 'event_id', 'event_group', 'intHash32(event_id)'), sampling_expr='intHash32(event_id)')
        self._create_and_insert(TestModel)

    def test_merge_tree_with_sampling__funcs(self):
        class TestModel(SampleModel):
            engine = MergeTree('date', ('date', 'event_id', 'event_group', F.intHash32(SampleModel.event_id)), sampling_expr=F.intHash32(SampleModel.event_id))
        self._create_and_insert(TestModel)

    def test_merge_tree_with_granularity(self):
        class TestModel(SampleModel):
            engine = MergeTree('date', ('date', 'event_id', 'event_group'), index_granularity=4096)
        self._create_and_insert(TestModel)

    def test_replicated_merge_tree(self):
        engine = MergeTree('date', ('date', 'event_id', 'event_group'), replica_table_path='/clickhouse/tables/{layer}-{shard}/hits', replica_name='{replica}')
        # In ClickHouse 1.1.54310 custom partitioning key was introduced and new syntax is used
        if self.database.server_version >= (1, 1, 54310):
            expected = "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}') PARTITION BY (toYYYYMM(`date`)) ORDER BY (date, event_id, event_group) SETTINGS index_granularity=8192"
        else:
            expected = "ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}', date, (date, event_id, event_group), 8192)"
        self.assertEqual(engine.create_table_sql(self.database), expected)

    def test_replicated_merge_tree_incomplete(self):
        with self.assertRaises(AssertionError):
            MergeTree('date', ('date', 'event_id', 'event_group'), replica_table_path='/clickhouse/tables/{layer}-{shard}/hits')
        with self.assertRaises(AssertionError):
            MergeTree('date', ('date', 'event_id', 'event_group'), replica_name='{replica}')

    def test_collapsing_merge_tree(self):
        class TestModel(SampleModel):
            engine = CollapsingMergeTree('date', ('date', 'event_id', 'event_group'), 'event_version')
        self._create_and_insert(TestModel)

    def test_summing_merge_tree(self):
        class TestModel(SampleModel):
            engine = SummingMergeTree('date', ('date', 'event_group'), ('event_count',))
        self._create_and_insert(TestModel)

    def test_replacing_merge_tree(self):
        class TestModel(SampleModel):
            engine = ReplacingMergeTree('date', ('date', 'event_id', 'event_group'), 'event_uversion')
        self._create_and_insert(TestModel)

    def test_tiny_log(self):
        class TestModel(SampleModel):
            engine = TinyLog()
        self._create_and_insert(TestModel)

    def test_log(self):
        class TestModel(SampleModel):
            engine = Log()
        self._create_and_insert(TestModel)

    def test_memory(self):
        class TestModel(SampleModel):
            engine = Memory()
        self._create_and_insert(TestModel)

    def test_merge(self):
        class TestModel1(SampleModel):
            engine = TinyLog()

        class TestModel2(SampleModel):
            engine = TinyLog()

        class TestMergeModel(MergeModel, SampleModel):
            engine = Merge('^testmodel')

        self.database.create_table(TestModel1)
        self.database.create_table(TestModel2)
        self.database.create_table(TestMergeModel)

        # Insert operations are restricted for this model type
        with self.assertRaises(DatabaseException):
            self.database.insert([
                TestMergeModel(date='2017-01-01', event_id=23423, event_group=13, event_count=7, event_version=1)
            ])

        # Testing select
        self.database.insert([
            TestModel1(date='2017-01-01', event_id=1, event_group=1, event_count=1, event_version=1)
        ])
        self.database.insert([
            TestModel2(date='2017-01-02', event_id=2, event_group=2, event_count=2, event_version=2)
        ])
        # event_uversion is materialized field. So * won't select it and it will be zero
        res = self.database.select('SELECT *, _table, event_uversion FROM $table ORDER BY event_id', model_class=TestMergeModel)
        res = list(res)
        self.assertEqual(2, len(res))
        self.assertDictEqual({
            '_table': 'testmodel1',
            'date': datetime.date(2017, 1, 1),
            'event_id': 1,
            'event_group': 1,
            'event_count': 1,
            'event_version': 1,
            'event_uversion': 1
        }, res[0].to_dict(include_readonly=True))
        self.assertDictEqual({
            '_table': 'testmodel2',
            'date': datetime.date(2017, 1, 2),
            'event_id': 2,
            'event_group': 2,
            'event_count': 2,
            'event_version': 2,
            'event_uversion': 2
        }, res[1].to_dict(include_readonly=True))

    def test_custom_partitioning(self):
        class TestModel(SampleModel):
            engine = MergeTree(
                order_by=('date', 'event_id', 'event_group'),
                partition_key=('toYYYYMM(date)', 'event_group')
            )

        class TestCollapseModel(SampleModel):
            sign = Int8Field()

            engine = CollapsingMergeTree(
                sign_col='sign',
                order_by=('date', 'event_id', 'event_group'),
                partition_key=('toYYYYMM(date)', 'event_group')
            )

        self._create_and_insert(TestModel)
        self._create_and_insert(TestCollapseModel, sign=1)

        # Result order may be different, lets sort manually
        parts = sorted(list(SystemPart.get(self.database)), key=lambda x: x.table)

        self.assertEqual(2, len(parts))
        self.assertEqual('testcollapsemodel', parts[0].table)
        self.assertEqual('(201701, 13)'.replace(' ', ''), parts[0].partition.replace(' ', ''))
        self.assertEqual('testmodel', parts[1].table)
        self.assertEqual('(201701, 13)'.replace(' ', ''), parts[1].partition.replace(' ', ''))

    def test_custom_primary_key(self):
        if self.database.server_version < (18, 1):
            raise unittest.SkipTest('ClickHouse version too old')

        class TestModel(SampleModel):
            engine = MergeTree(
                order_by=('date', 'event_id', 'event_group'),
                partition_key=('toYYYYMM(date)',),
                primary_key=('date', 'event_id')
            )

        class TestCollapseModel(SampleModel):
            sign = Int8Field()

            engine = CollapsingMergeTree(
                sign_col='sign',
                order_by=('date', 'event_id', 'event_group'),
                partition_key=('toYYYYMM(date)',),
                primary_key=('date', 'event_id')
            )

        self._create_and_insert(TestModel)
        self._create_and_insert(TestCollapseModel, sign=1)

        self.assertEqual(2, len(list(SystemPart.get(self.database))))


class SampleModel(Model):

    date            = DateField()
    event_id        = UInt32Field()
    event_group     = UInt32Field()
    event_count     = UInt16Field()
    event_version   = Int8Field()
    event_uversion  = UInt8Field(materialized='abs(event_version)')


class DistributedTestCase(_EnginesHelperTestCase):
    def test_without_table_name(self):
        engine = Distributed('my_cluster')

        with self.assertRaises(ValueError) as cm:
            engine.create_table_sql(self.database)

        exc = cm.exception
        self.assertEqual(str(exc), 'Cannot create Distributed engine: specify an underlying table')

    def test_with_table_name(self):
        engine = Distributed('my_cluster', 'foo')
        sql = engine.create_table_sql(self.database)
        self.assertEqual(sql, 'Distributed(`my_cluster`, `test-db`, `foo`)')

    class TestModel(SampleModel):
        engine = TinyLog()

    def _create_distributed(self, shard_name, underlying=TestModel):
        class TestDistributedModel(DistributedModel, underlying):
            engine = Distributed(shard_name, underlying)

        self.database.create_table(underlying)
        self.database.create_table(TestDistributedModel)
        return TestDistributedModel

    def test_bad_cluster_name(self):
        with self.assertRaises(ServerError) as cm:
            d_model = self._create_distributed('cluster_name')
            self.database.count(d_model)

        exc = cm.exception
        self.assertEqual(exc.code, 701)
        self.assertTrue(exc.message.startswith("Requested cluster 'cluster_name' not found"))

    @unittest.skip("Need to work on the ClickHouse recipe to make this pass")
    def test_verbose_engine_two_superclasses(self):
        class TestModel2(SampleModel):
            engine = Log()

        class TestDistributedModel(DistributedModel, self.TestModel, TestModel2):
            engine = Distributed('test_shard_localhost', self.TestModel)

        self.database.create_table(self.TestModel)
        self.database.create_table(TestDistributedModel)
        self.assertEqual(self.database.count(TestDistributedModel), 0)

    @unittest.skip("Need to work on the ClickHouse recipe to make this pass")
    def test_minimal_engine(self):
        class TestDistributedModel(DistributedModel, self.TestModel):
            engine = Distributed('test_shard_localhost')

        self.database.create_table(self.TestModel)
        self.database.create_table(TestDistributedModel)

        self.assertEqual(self.database.count(TestDistributedModel), 0)

    def test_minimal_engine_two_superclasses(self):
        class TestModel2(SampleModel):
            engine = Log()

        class TestDistributedModel(DistributedModel, self.TestModel, TestModel2):
            engine = Distributed('test_shard_localhost')

        self.database.create_table(self.TestModel)
        with self.assertRaises(TypeError) as cm:
            self.database.create_table(TestDistributedModel)

        exc = cm.exception
        self.assertEqual(str(exc), 'When defining Distributed engine without the table_name ensure '
                                   'that your model has exactly one non-distributed superclass')

    def test_minimal_engine_no_superclasses(self):
        class TestDistributedModel(DistributedModel):
            engine = Distributed('test_shard_localhost')

        self.database.create_table(self.TestModel)
        with self.assertRaises(TypeError) as cm:
            self.database.create_table(TestDistributedModel)

        exc = cm.exception
        self.assertEqual(str(exc), 'When defining Distributed engine without the table_name ensure '
                                   'that your model has a parent model')

    def _test_insert_select(self, local_to_distributed, test_model=TestModel, include_readonly=True):
        d_model = self._create_distributed('test_shard_localhost', underlying=test_model)

        if local_to_distributed:
            to_insert, to_select = test_model, d_model
        else:
            to_insert, to_select = d_model, test_model

        self.database.insert([
            to_insert(date='2017-01-01', event_id=1, event_group=1, event_count=1, event_version=1),
            to_insert(date='2017-01-02', event_id=2, event_group=2, event_count=2, event_version=2)
        ])
        # event_uversion is materialized field. So * won't select it and it will be zero
        res = self.database.select('SELECT *, event_uversion FROM $table ORDER BY event_id',
                                   model_class=to_select)
        res = [row for row in res]
        self.assertEqual(2, len(res))
        self.assertDictEqual({
            'date': datetime.date(2017, 1, 1),
            'event_id': 1,
            'event_group': 1,
            'event_count': 1,
            'event_version': 1,
            'event_uversion': 1
        }, res[0].to_dict(include_readonly=include_readonly))
        self.assertDictEqual({
            'date': datetime.date(2017, 1, 2),
            'event_id': 2,
            'event_group': 2,
            'event_count': 2,
            'event_version': 2,
            'event_uversion': 2
        }, res[1].to_dict(include_readonly=include_readonly))

    @unittest.skip("Bad support of materialized fields in Distributed tables "
                   "https://groups.google.com/forum/#!topic/clickhouse/XEYRRwZrsSc")
    def test_insert_distributed_select_local(self):
        return self._test_insert_select(local_to_distributed=False)

    @unittest.skip("Need to work on the ClickHouse recipe to make this pass")
    def test_insert_local_select_distributed(self):
        return self._test_insert_select(local_to_distributed=True)

    def _test_insert_distributed_select_local_no_materialized_fields(self):
        class TestModel2(self.TestModel):
            event_uversion = UInt8Field(readonly=True)

        return self._test_insert_select(local_to_distributed=False, test_model=TestModel2, include_readonly=False)
