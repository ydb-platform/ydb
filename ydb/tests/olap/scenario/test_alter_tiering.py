from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    DropTable,
    DropTableStore,
)
from helpers.thread_helper import TestThread
from helpers.tiering_helper import (
    ObjectStorageParams,
    CreateExternalDataSource,
    DropExternalDataSource,
    UpsertSecret,
    DropSecret
)
import helpers.data_generators as dg
from helpers.table_helper import (
    AlterTable,
    AlterTableStore
)

from ydb.tests.olap.lib.utils import get_external_param, external_param_is_true
from ydb import PrimitiveType, StatusCode
import boto3
import datetime
import random
from typing import Iterable
from string import ascii_lowercase


class TestLoop:
    def __init__(self, duration: datetime.timedelta):
        self._deadline = datetime.datetime.now() + duration

    def __iter__(self):
        return self
    
    def __next__(self):
        if datetime.datetime.now() < self._deadline:
            return None
        return StopIteration


class TestAlterTiering(BaseTestSet):
    schema1 = (
        ScenarioTestHelper.Schema()
        .with_column(name='timestamp', type=PrimitiveType.Timestamp, not_null=True)
        .with_column(name='writer', type=PrimitiveType.Uint32, not_null=True)
        .with_column(name='value', type=PrimitiveType.Uint64, not_null=True)
        .with_column(name='data', type=PrimitiveType.String, not_null=True)
        .with_key_columns('timestamp', 'writer', 'value')
    )

    def _drop_tables(self, prefix: str, count: int, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        for i in range(count):
            sth.execute_scheme_query(DropTable(f'store/{prefix}_{i}'))

    def _loop_bulk_upsert(self, ctx: TestContext, table: str, writer_id: int, duration: datetime.timedelta, allow_scan_errors: bool = False):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        for _ in loop:
            sth.bulk_upsert(
                table,
                dg.DataGeneratorPerColumn(self.schema1, 1000)
                .with_column('timestamp', dg.ColumnValueGeneratorLambda(lambda: int(datetime.datetime.now().timestamp() * 1000000)))
                .with_column('writer', dg.ColumnValueGeneratorConst(writer_id))
                .with_column('value', dg.ColumnValueGeneratorSequential())
                .with_column('data', dg.ColumnValueGeneratorConst(random.randbytes(1024)))
            )

    def _loop_select(self, ctx: TestContext, table: str, writer_id: int, duration: datetime.timedelta, allow_scan_errors: bool = False):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        expected_scan_status = {StatusCode.SUCCESS, StatusCode.GENERIC_ERROR} if allow_scan_errors else {StatusCode.SUCCESS}

        for _ in loop:
            sth.execute_scan_query(f'SELECT COUNT(*) FROM `{sth.get_full_path(table)}`', expected_status=expected_scan_status)

    def _loop_set_ttl(self, ctx: TestContext, table: str, sources: Iterable[str], duration: datetime.timedelta):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        for _ in loop:
            for source in sources:
                sth.execute_scheme_query(AlterTable(table).set_ttl([datetime.timedelta(days=10), source], 'timestamp'), retries=10)
            sth.execute_scheme_query(AlterTable(table).reset_ttl(), retries=10)

    def _loop_add_drop_column(self, ctx: TestContext, store: str, duration: datetime.timedelta):
        loop = TestLoop(duration)
        sth = ScenarioTestHelper(ctx)

        column_name = 'tmp_column_' + ''.join(random.choice(ascii_lowercase) for _ in range(8))
        data_types = [PrimitiveType.Int8, PrimitiveType.Uint64, PrimitiveType.Datetime, PrimitiveType.Utf8]

        for _ in loop:
            sth.execute_scheme_query(AlterTableStore(store).add_column(sth.Column(column_name, random.choice(data_types))), retries=10)
            sth.execute_scheme_query(AlterTableStore(store).drop_column(column_name), retries=10)

    def _override_external_data_source(self, sth, path, config):
        sth.execute_scheme_query(DropExternalDataSource(path, True))
        sth.execute_scheme_query(CreateExternalDataSource(path, config))

    def _make_s3_client(self, access_key, secret_key, endpoint):
        session = boto3.Session(
            aws_access_key_id=(access_key),
            aws_secret_access_key=(secret_key),
            region_name='ru-central1',
        )
        return session.client('s3', endpoint_url=endpoint)
    
    def _get_test_prefix(self):
        return type(self).__name__;

    def _count_objects(self, bucket_config: ObjectStorageParams):
        s3 = self._make_s3_client(bucket_config.access_key_secret, bucket_config.secret_key_secret, bucket_config.endpoint)
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_config.bucket)

        object_count = 0
        for page in page_iterator:
            if 'Contents' in page:
                object_count += len(page['Contents'])

        return object_count

    def scenario_many_tables(self, ctx: TestContext):
        random.seed(42)
        n_tables = 4

        test_duration = datetime.timedelta(seconds=int(get_external_param('test-duration-seconds', '4600')))
        n_tables = int(get_external_param('tables', '4'))
        n_writers = int(get_external_param('writers-per-table', '4'))
        allow_s3_unavailability = external_param_is_true('allow-s3-unavailability')
        is_standalone_tables = external_param_is_true('test-standalone-tables')

        s3_endpoint = get_external_param('s3-endpoint', 'http://storage.yandexcloud.net')
        s3_access_key = get_external_param('s3-access-key', 'YCAJEM3Pg9fMyuX9ZUOJ_fake')
        s3_secret_key = get_external_param('s3-secret-key', 'YCM7Ovup55wDkymyEtO8pw5F10_L5jtVY8w_fake')
        s3_buckets = get_external_param('s3-buckets', 'ydb-tiering-test-1,ydb-tiering-test-2').split(',')

        assert len(s3_buckets) == 2, f'expected 2 bucket configs, got {len(s3_buckets)}'

        sth = ScenarioTestHelper(ctx)

        access_key_secret = self._get_test_prefix() + '_access_key'
        secret_key_secret = self._get_test_prefix() + '_secret_key'
        sth.execute_scheme_query(UpsertSecret(access_key_secret, s3_access_key))
        sth.execute_scheme_query(UpsertSecret(secret_key_secret, s3_secret_key))

        s3_configs = [
            ObjectStorageParams(
                endpoint=s3_endpoint,
                bucket=bucket,
                access_key_secret=s3_access_key,
                secret_key_secret=s3_secret_key
            ) for bucket in s3_buckets
        ]

        sources: list[str] = []
        for i, s3_config in enumerate(s3_configs):
            sources.append(f'{self._get_test_prefix()}EDS{i}')
            self._override_external_data_source(sth, sources[-1], s3_config)

        if not is_standalone_tables:
            sth.execute_scheme_query(CreateTableStore('store').with_schema(self.schema1))

        tables: list[str] = []
        tables_for_tiering_modification: list[str] = []
        for i in range(n_tables):
            if is_standalone_tables:
                tables.append(f'table{i}')
            else:
                tables.append(f'store/table{i}')
            tables_for_tiering_modification.append(tables[-1])
            sth.execute_scheme_query(CreateTable(tables[-1]).with_schema(self.schema1))

        if any(self._count_objects(bucket) != 0 for bucket in s3_configs):
            assert any(sth.get_table_rows_count(table) != 0 for table in tables), \
                'unrelated data in object storage: all tables are empty, but S3 is not'

        threads = []

        threads.append(TestThread(target=self._loop_add_drop_column, args=[ctx, 'store', test_duration]))
        for table in tables_for_tiering_modification:
            threads.append(TestThread(
                target=self._loop_set_ttl,
                args=[ctx, table, random.sample(sources, len(sources)), test_duration]
            ))
        for i, table in enumerate(tables):
            for writer in range(n_writers):
                threads.append(TestThread(target=self._loop_bulk_upsert, args=[ctx, table, i * n_writers + writer, test_duration, allow_s3_unavailability]))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert any(self._count_objects(bucket) != 0 for bucket in s3_configs)

        for table in tables:
            sth.execute_scheme_query(AlterTable(table).reset_ttl())

        for table in tables:
            sth.execute_scheme_query(DropTable(table))
        if not is_standalone_tables:
            sth.execute_scheme_query(DropTableStore('store'))

        sth.execute_scheme_query(DropSecret(access_key_secret, s3_access_key))
        sth.execute_scheme_query(DropSecret(secret_key_secret, s3_secret_key))

        assert all(self._count_objects(bucket) == 0 for bucket in s3_configs)
