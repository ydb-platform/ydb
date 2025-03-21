from conftest import BaseTestSet, LOGGER
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    DropTable,
    DropTableStore,
)
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads
from helpers.tiering_helper import (
    ObjectStorageParams,
    CreateExternalDataSource,
    DropExternalDataSource,
    UpsertSecret,
    DropSecret,
)
import helpers.data_generators as dg
from helpers.table_helper import AlterTable, ResetSetting, AlterTableStore

from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

from ydb import PrimitiveType, StatusCode
import yatest.common
from moto.server import ThreadedMotoServer

import boto3
import datetime
import random
from typing import Iterable
from string import ascii_lowercase


class TestLoop:

    __test__ = False

    def __init__(self, duration: datetime.timedelta):
        self._deadline = datetime.datetime.now() + duration

    def __iter__(self):
        return self

    def __next__(self):
        if datetime.datetime.now() < self._deadline:
            return None
        raise StopIteration


class S3:
    def __init__(self):
        self._server = None

    def start_server(self) -> str:
        port = yatest.common.network.PortManager().get_port()
        self._server = ThreadedMotoServer(port=port)
        self._server.start()
        return f'http://localhost:{port}'

    def is_server_started(self) -> bool:
        return self._server is not None

    def _make_s3_resource(self, access_key, secret_key, endpoint) -> boto3.Session:
        session = boto3.Session(
            aws_access_key_id=(access_key),
            aws_secret_access_key=(secret_key),
            region_name='ru-central1',
        )
        return session.resource('s3', endpoint_url=endpoint)

    def create_bucket(self, bucket_config: ObjectStorageParams):
        s3 = self._make_s3_resource(
            bucket_config.access_key,
            bucket_config.secret_key,
            bucket_config.endpoint,
        )
        s3.create_bucket(
            Bucket=bucket_config.bucket,
            CreateBucketConfiguration={'LocationConstraint': 'ru-central1'},
        )

    def count_objects(self, bucket_config: ObjectStorageParams):
        s3 = self._make_s3_resource(
            bucket_config.access_key,
            bucket_config.secret_key,
            bucket_config.endpoint,
        )
        bucket = s3.Bucket(bucket_config.bucket)
        return sum(1 for _ in bucket.objects.all())


class TieringTestBase(BaseTestSet):
    @classmethod
    def _get_cluster_config(cls):
        return KikimrConfigGenerator(
            extra_feature_flags=[
                'enable_external_data_sources',
                'enable_tiering_in_column_shard',
                'enable_column_store',
            ],
            column_shard_config={
                'lag_for_compaction_before_tierings_ms': 0,
                'compaction_actualization_lag_ms': 0,
                'optimizer_freshness_check_duration_ms': 0,
                'small_portion_detect_size_limit': 0,
                'alter_object_enabled': True,
            },
            additional_log_configs={
                'TX_TIERING': LogLevels.DEBUG,
                'TX_TIERING_BLOBS_TIER': LogLevels.TRACE,
                'TX_COLUMNSHARD_ACTUALIZATION': LogLevels.TRACE,
            },
        )

    def _setup_tiering_test(self, ctx):
        random.seed(0)

        LOGGER.info('Initializing test parameters')
        self.s3_endpoint = get_external_param('s3-endpoint', '')
        self.s3_buckets = list(get_external_param('s3-buckets', 'ydb-tiering-test-1,ydb-tiering-test-2').split(','))
        self.s3_access_key = get_external_param('s3-access-key', 'access_key')
        self.s3_secret_key = get_external_param('s3-secret-key', 'secret_key')

        assert len(self.s3_buckets) == 2, len(self.s3_buckets)

        self.s3 = S3()
        if not self.s3_endpoint:
            LOGGER.info('Starting S3 server')
            self.s3_endpoint = self.s3.start_server()

        LOGGER.info('Preparing scheme objects')
        sth = ScenarioTestHelper(ctx)

        self.access_key_secret = self._get_test_prefix() + '_access_key'
        self.secret_key_secret = self._get_test_prefix() + '_secret_key'
        sth.execute_scheme_query(UpsertSecret(self.access_key_secret, self.s3_access_key))
        sth.execute_scheme_query(UpsertSecret(self.secret_key_secret, self.s3_secret_key))

        self.s3_configs = [
            ObjectStorageParams(
                endpoint=self.s3_endpoint,
                bucket=bucket,
                access_key_secret=self.access_key_secret,
                secret_key_secret=self.secret_key_secret,
                access_key=self.s3_access_key,
                secret_key=self.s3_secret_key,
            )
            for bucket in self.s3_buckets
        ]

        if self.s3.is_server_started():
            for config in self.s3_configs:
                self.s3.create_bucket(config)

        self.sources: list[str] = []
        for i, s3_config in enumerate(self.s3_configs):
            self.sources.append(f'tier{i}')
            self._override_external_data_source(sth, self.sources[-1], s3_config)

    def _tier_down_tiering_test(self, ctx):
        LOGGER.info('Tiering down scheme objects')
        sth = ScenarioTestHelper(ctx)

        # NOTE: evicted data is not erased when the colummnstore is deleted
        # assert all(s3.count_objects(bucket) == 0 for bucket in s3_configs)

        for source in self.sources:
            sth.execute_scheme_query(DropExternalDataSource(source))

        sth.execute_scheme_query(DropSecret(self.access_key_secret))
        sth.execute_scheme_query(DropSecret(self.secret_key_secret))

    def _override_external_data_source(self, sth, path, config):
        sth.execute_scheme_query(CreateExternalDataSource(path, config, True))

    def _get_test_duration(self, test_class: str) -> datetime.timedelta:
        class_to_duration = {
            'SMALL': datetime.timedelta(minutes=2),
            'MEDIUM': datetime.timedelta(hours=6),
            'LARGE': datetime.timedelta(days=2),
        }
        assert test_class in class_to_duration, test_class
        return class_to_duration[test_class]

    def _get_test_prefix(self):
        return type(self).__name__


class TestAlterTiering(TieringTestBase):
    schema1 = (
        ScenarioTestHelper.Schema()
        .with_column(name='timestamp', type=PrimitiveType.Timestamp, not_null=True)
        .with_column(name='writer', type=PrimitiveType.Uint32, not_null=True)
        .with_column(name='value', type=PrimitiveType.Uint64, not_null=True)
        .with_column(name='data', type=PrimitiveType.String, not_null=True)
        .with_key_columns('timestamp', 'writer', 'value')
    )

    def _loop_bulk_upsert(
        self,
        ctx: TestContext,
        table: str,
        writer_id: int,
        duration: datetime.timedelta,
    ):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        for _ in loop:
            sth.bulk_upsert(
                table,
                dg.DataGeneratorPerColumn(self.schema1, 1000)
                .with_column(
                    'timestamp',
                    dg.ColumnValueGeneratorLambda(lambda: int(datetime.datetime.now().timestamp() * 1000000)),
                )
                .with_column('writer', dg.ColumnValueGeneratorConst(writer_id))
                .with_column('value', dg.ColumnValueGeneratorSequential())
                .with_column(
                    'data',
                    dg.ColumnValueGeneratorConst(random.randbytes(1024)),
                ),
            )

    def _loop_scan(
        self,
        ctx: TestContext,
        table: str,
        duration: datetime.timedelta,
        allow_scan_errors: bool = False,
    ):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        expected_scan_status = {StatusCode.SUCCESS, StatusCode.GENERIC_ERROR} if allow_scan_errors else {StatusCode.SUCCESS}

        for _ in loop:
            LOGGER.info('executing SELECT')
            sth.execute_scan_query(
                f'SELECT MIN(writer) FROM `{sth.get_full_path(table)}`',
                expected_status=expected_scan_status,
            )

    def _loop_set_ttl(
        self,
        ctx: TestContext,
        table: str,
        sources: Iterable[str],
        duration: datetime.timedelta,
    ):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        for _ in loop:
            for source in sources:
                LOGGER.info(f'setting eviction to `{source}`')
                sth.execute_scheme_query(
                    AlterTable(table).set_ttl(
                        [
                            (
                                datetime.timedelta(seconds=1),
                                sth.get_full_path(source),
                            )
                        ],
                        'timestamp',
                    ),
                    retries=2,
                )
            LOGGER.info('resetting eviction')
            sth.execute_scheme_query(AlterTable(table).action(ResetSetting('TTL')), retries=2)

    def _loop_add_drop_column(self, ctx: TestContext, store: str, duration: datetime.timedelta):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        column_name = 'tmp_column_' + ''.join(random.choice(ascii_lowercase) for _ in range(8))
        data_types = [
            PrimitiveType.Int8,
            PrimitiveType.Uint64,
            PrimitiveType.Datetime,
            PrimitiveType.Utf8,
        ]

        for _ in loop:
            LOGGER.info('executing ADD COLUMN')
            sth.execute_scheme_query(
                AlterTableStore(store).add_column(sth.Column(column_name, random.choice(data_types))),
                retries=2,
            )
            LOGGER.info('executing DROP COLUMN')
            sth.execute_scheme_query(AlterTableStore(store).drop_column(column_name), retries=2)

    def scenario_many_tables(self, ctx: TestContext):
        self._setup_tiering_test(ctx)

        self.test_duration = self._get_test_duration(get_external_param('test-class', 'SMALL'))
        self.n_tables = 4
        self.n_writers = 4

        sth = ScenarioTestHelper(ctx)

        sth.execute_scheme_query(CreateTableStore('store').with_schema(self.schema1).existing_ok())
        YdbCluster.get_ydb_driver().table_client.session().create().execute_scheme(
            f'ALTER OBJECT `{sth.get_full_path("store")}` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`'
            f'    {{"levels" : [{{"class_name" : "Zero", "portions_live_duration" : "5s", "expected_blobs_size" : 1000000000000, "portions_count_available" : 2}},'
            f'                  {{"class_name" : "Zero"}}]}}`);'
        )

        sth = ScenarioTestHelper(ctx)
        self.tables: list[str] = []
        for i in range(self.n_tables):
            self.tables.append(f'store/table{i}')
            sth.execute_scheme_query(CreateTable(self.tables[-1]).with_schema(self.schema1).existing_ok())

        LOGGER.info('Starting workload threads')
        threads: TestThreads = TestThreads()

        threads.append(
            TestThread(
                target=self._loop_add_drop_column,
                args=[ctx, 'store', self.test_duration],
            )
        )
        for i, table in enumerate(self.tables):
            for writer in range(self.n_writers):
                threads.append(
                    TestThread(
                        target=self._loop_bulk_upsert,
                        args=[
                            ctx,
                            table,
                            i * self.n_writers + writer,
                            self.test_duration,
                        ],
                    )
                )
            threads.append(
                TestThread(
                    target=self._loop_set_ttl,
                    args=[
                        ctx,
                        table,
                        random.sample(self.sources, len(self.sources)),
                        self.test_duration,
                    ],
                )
            )
            threads.append(TestThread(target=self._loop_scan, args=[ctx, table, self.test_duration]))

        threads.start_and_wait_all()

        assert any(self.s3.count_objects(bucket) != 0 for bucket in self.s3_configs)

        for table in self.tables:
            sth.execute_scheme_query(AlterTable(table).action(ResetSetting('TTL')), retries=2)

        for table in self.tables:
            sth.execute_scheme_query(DropTable(table))
        sth.execute_scheme_query(DropTableStore('store'))

        self._tier_down_tiering_test(ctx)
