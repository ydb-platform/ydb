from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    DropTable,
    DropTableStore,
)
from helpers.tiering_helper import (
    ObjectStorageParams,
    AlterTier,
    CreateTierIfNotExists,
    AlterTieringRule,
    CreateTieringRuleIfNotExists,
    TierConfig,
    TieringPolicy,
    TieringRule,
    DropTier,
    DropTieringRule,
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
import threading
from typing import Iterable, Optional
import itertools
from string import ascii_lowercase


class TestAlterTiering(BaseTestSet):
    schema1 = (
        ScenarioTestHelper.Schema()
        .with_column(name='timestamp', type=PrimitiveType.Timestamp, not_null=True)
        .with_column(name='writer', type=PrimitiveType.Uint32, not_null=True)
        .with_column(name='value', type=PrimitiveType.Uint64, not_null=True)
        .with_column(name='data', type=PrimitiveType.String, not_null=True)
        .with_column(name='timestamp2', type=PrimitiveType.Timestamp, not_null=True)
        .with_key_columns('timestamp', 'writer', 'value')
    )

    class TestThread(threading.Thread):
        def run(self) -> None:
            self.exc = None
            try:
                self.ret = self._target(*self._args, **self._kwargs)
            except BaseException as e:
                self.exc = e

        def join(self, timeout=None):
            super().join(timeout)
            if self.exc:
                raise self.exc
            return self.ret

    def _drop_tables(self, prefix: str, count: int, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        for i in range(count):
            sth.execute_scheme_query(DropTable(f'store/{prefix}_{i}'))

    def _loop_upsert(self, ctx: TestContext, table: str, writer_id: int, duration: datetime.timedelta, allow_scan_errors: bool = False):
        deadline = datetime.datetime.now() + duration
        expected_scan_status = {StatusCode.SUCCESS, StatusCode.GENERIC_ERROR} if allow_scan_errors else {StatusCode.SUCCESS}
        sth = ScenarioTestHelper(ctx)
        rows_written = 0
        i = 0
        while datetime.datetime.now() < deadline:
            sth.bulk_upsert(
                table,
                dg.DataGeneratorPerColumn(self.schema1, 1000)
                .with_column('timestamp', dg.ColumnValueGeneratorLambda(lambda: int(datetime.datetime.now().timestamp() * 1000000)))
                .with_column('writer', dg.ColumnValueGeneratorConst(writer_id))
                .with_column('value', dg.ColumnValueGeneratorSequential(rows_written))
                .with_column('data', dg.ColumnValueGeneratorConst(random.randbytes(1024)))
                .with_column('timestamp2', dg.ColumnValueGeneratorRandom(null_probability=0))
            )
            rows_written += 1000
            i += 1
            scan_result = sth.execute_scan_query(f'SELECT COUNT(*) FROM `{sth.get_full_path(table)}` WHERE writer == {writer_id}', expected_status=expected_scan_status)
            assert scan_result.result_set.rows[0][0] == rows_written

    def _loop_change_tiering_rule(self, ctx: TestContext, table: str, tiering_rules: Iterable[str], duration: datetime.timedelta):
        deadline = datetime.datetime.now() + duration
        sth = ScenarioTestHelper(ctx)
        while datetime.datetime.now() < deadline:
            for tiering_rule in tiering_rules:
                if tiering_rule is not None:
                    sth.execute_scheme_query(AlterTable(table).set_tiering(tiering_rule), retries=10)
                else:
                    sth.execute_scheme_query(AlterTable(table).reset_tiering(), retries=10)
                # Not implemented in SDK
                # assert sth.describe_table(table).tiering == tiering_rule
            sth.execute_scheme_query(AlterTable(table).reset_tiering(), retries=10)
            # assert sth.describe_table(table).tiering == None

    def _loop_alter_tiering_rule(self, ctx: TestContext, tiering_rule: str, default_column_values: Iterable[str], config_values: Iterable[TieringPolicy], duration: datetime.timedelta):
        deadline = datetime.datetime.now() + duration
        sth = ScenarioTestHelper(ctx)
        for default_column, config in zip(itertools.cycle(default_column_values), itertools.cycle(config_values)):
            if datetime.datetime.now() >= deadline:
                break
            sth.execute_scheme_query(AlterTieringRule(tiering_rule, default_column, config), retries=10)

    def _loop_alter_column(self, ctx: TestContext, store: str, duration: datetime.timedelta):
        column_name = 'tmp_column_' + ''.join(random.choice(ascii_lowercase) for _ in range(8))
        data_types = [PrimitiveType.Int8, PrimitiveType.Uint64, PrimitiveType.Datetime, PrimitiveType.Utf8]

        deadline = datetime.datetime.now() + duration
        sth = ScenarioTestHelper(ctx)
        while datetime.datetime.now() < deadline:
            sth.execute_scheme_query(AlterTableStore(store).add_column(sth.Column(column_name, random.choice(data_types))), retries=10)
            sth.execute_scheme_query(AlterTableStore(store).drop_column(column_name), retries=10)

    def _override_tier(self, sth, name, config):
        sth.execute_scheme_query(CreateTierIfNotExists(name, config))
        sth.execute_scheme_query(AlterTier(name, config))

    def _override_tiering_rule(self, sth, name, default_column, config):
        sth.execute_scheme_query(CreateTieringRuleIfNotExists(name, default_column, config))
        sth.execute_scheme_query(AlterTieringRule(name, default_column, config))

    def _make_s3_client(self, access_key, secret_key, endpoint):
        session = boto3.Session(
            aws_access_key_id=(access_key),
            aws_secret_access_key=(secret_key),
            region_name='ru-central1',
        )
        return session.client('s3', endpoint_url=endpoint)

    def _count_objects(self, bucket_config: ObjectStorageParams):
        s3 = self._make_s3_client(bucket_config.access_key, bucket_config.secret_key, bucket_config.endpoint)
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

        s3_configs = [
            ObjectStorageParams(
                scheme='HTTP',
                verify_ssl=False,
                endpoint=s3_endpoint,
                bucket=bucket,
                access_key=s3_access_key,
                secret_key=s3_secret_key
            ) for bucket in s3_buckets
        ]

        sth = ScenarioTestHelper(ctx)

        tiers: list[str] = []
        for i, s3_config in enumerate(s3_configs):
            tiers.append(f'TestAlterTiering:tier{i}')
            self._override_tier(sth, tiers[-1], TierConfig(tiers[-1], s3_config))

        tiering_policy_configs: list[TieringPolicy] = []
        tiering_policy_configs.append(TieringPolicy().with_rule(TieringRule(tiers[0], '1s')))
        tiering_policy_configs.append(TieringPolicy().with_rule(TieringRule(tiers[1], '1s')))
        tiering_policy_configs.append(TieringPolicy().with_rule(TieringRule(tiers[0], '100000d')))
        tiering_policy_configs.append(TieringPolicy().with_rule(TieringRule(tiers[1], '100000d')))
        tiering_policy_configs.append(TieringPolicy().with_rule(TieringRule(tiers[0], '1s')).with_rule(TieringRule(tiers[1], '100000d')))
        tiering_policy_configs.append(TieringPolicy().with_rule(TieringRule(tiers[1], '1s')).with_rule(TieringRule(tiers[0], '100000d')))

        tiering_rules: list[Optional[str]] = []
        for i, config in enumerate(tiering_policy_configs):
            tiering_rules.append(f'TestAlterTiering:tiering_rule{i}')
            self._override_tiering_rule(sth, tiering_rules[-1], 'timestamp', config)
        tiering_rules.append(None)

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
        for i, tiering_rule in enumerate([tiering_rules[0], tiering_rules[1]]):
            if is_standalone_tables:
                tables.append(f'extra_table{i}')
            else:
                tables.append(f'store/extra_table{i}')
            sth.execute_scheme_query(CreateTable(tables[-1]).with_schema(self.schema1))
            sth.execute_scheme_query(AlterTable(tables[-1]).set_tiering(tiering_rule))

        if any(self._count_objects(bucket) != 0 for bucket in s3_configs):
            assert any(sth.get_table_rows_count(table) != 0 for table in tables), \
                'unrelated data in object storage: all tables are empty, but S3 is not'

        threads = []

        # "Alter table drop column" causes scan failures
        threads.append(self.TestThread(target=self._loop_alter_column, args=[ctx, 'store', test_duration]))
        for table in tables_for_tiering_modification:
            threads.append(self.TestThread(
                target=self._loop_change_tiering_rule,
                args=[ctx, table, random.sample(tiering_rules, len(tiering_rules)), test_duration]
            ))
        for i, table in enumerate(tables):
            for writer in range(n_writers):
                threads.append(self.TestThread(target=self._loop_upsert, args=[ctx, table, i * n_writers + writer, test_duration, allow_s3_unavailability]))
        for tiering_rule in tiering_rules:
            threads.append(self.TestThread(
                target=self._loop_alter_tiering_rule,
                args=[ctx, tiering_rule, random.sample(['timestamp', 'timestamp2'], 2), random.sample(tiering_policy_configs, len(tiering_policy_configs)), test_duration]
            ))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert any(self._count_objects(bucket) != 0 for bucket in s3_configs)

        for table in tables:
            sth.execute_scheme_query(AlterTable(table).reset_tiering())

        for tiering in tiering_rules:
            sth.execute_scheme_query(DropTieringRule(tiering))
        for tier in tiers:
            sth.execute_scheme_query(DropTier(tier))

        for table in tables:
            sth.execute_scheme_query(DropTable(table))
        if not is_standalone_tables:
            sth.execute_scheme_query(DropTableStore('store'))

        assert all(self._count_objects(bucket) == 0 for bucket in s3_configs)
