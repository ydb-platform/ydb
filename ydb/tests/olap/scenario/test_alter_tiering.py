from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    DropTable,
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
from helpers.table_helper import AlterTable

from ydb.tests.olap.lib.utils import get_external_param
from ydb import PrimitiveType
from ydb import StatusCode
import datetime
import random
import threading
from typing import Iterable
import time


class TestAlterTiering(BaseTestSet):
    schema1 = (
        ScenarioTestHelper.Schema()
        .with_column(name='timestamp', type=PrimitiveType.Timestamp, not_null=True)
        .with_column(name='writer', type=PrimitiveType.Uint32, not_null=True)
        .with_column(name='value', type=PrimitiveType.Uint64, not_null=True)
        .with_column(name='data', type=PrimitiveType.String, not_null=True)
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

    def _upsert(self, ctx: TestContext, table: str, writer_id: int, duration: datetime.timedelta):
        deadline = datetime.datetime.now() + duration
        sth = ScenarioTestHelper(ctx)
        rows_written = 0
        i = 0
        while datetime.datetime.now() < deadline:
            sth.bulk_upsert(
                table,
                dg.DataGeneratorPerColumn(self.schema1, 1000)
                    .with_column('timestamp', dg.ColumnValueGeneratorRandom(null_probability=0))
                    .with_column('writer', dg.ColumnValueGeneratorConst(writer_id))
                    .with_column('value', dg.ColumnValueGeneratorSequential(rows_written))
                    .with_column('data', dg.ColumnValueGeneratorConst(random.randbytes(1024)))
            )
            rows_written += 1000
            i += 1
            if rows_written > 100000 and i % 10 == 0:
                scan_result = sth.execute_scan_query(f'SELECT COUNT(*) FROM `{sth.get_full_path('store/table')}` WHERE writer == {writer_id}')
                assert scan_result.result_set.rows[0][0] == rows_written

    def _change_tiering_rule(self, ctx: TestContext, table: str, tiering_rules: Iterable[str], duration: datetime.timedelta):
        deadline = datetime.datetime.now() + duration
        sth = ScenarioTestHelper(ctx)
        while datetime.datetime.now() < deadline:
            for tiering_rule in tiering_rules:
                sth.execute_scheme_query(AlterTable(table).set_tiering(tiering_rule))
            sth.execute_scheme_query(AlterTable(table).reset_tiering())

    def scenario_alter_tiering_rule_while_writing(self, ctx: TestContext):
        test_duration = datetime.timedelta(seconds=400)

        s3_endpoint = get_external_param('s3-endpoint', 'storage.yandexcloud.net')
        s3_access_key = get_external_param('s3-access-key', 'YCAJEM3Pg9fMyuX9ZUOJ_fake')
        s3_secret_key = get_external_param('s3-secret-key', 'YCM7Ovup55wDkymyEtO8pw5F10_L5jtVY8w_fake')
        s3_buckets = get_external_param('s3-buckets', 'ydb-tiering-test-1,ydb-tiering-test-2').split(',')

        s3_configs = [
            ObjectStorageParams(
                scheme = 'HTTP',
                verify_ssl = False,
                endpoint = s3_endpoint,
                bucket = bucket,
                access_key = s3_access_key,
                secret_key = s3_secret_key
            ) for bucket in s3_buckets
        ]

        sth = ScenarioTestHelper(ctx)

        tiers: list[str] = []
        tiering_rules: list[str] = []
        for i, s3_config in enumerate(s3_configs):
            tiers.append(f'TestAlterTiering:tier{i}')
            tiering_rules.append(f'TestAlterTiering:tiering_rule{i}')

            tier_config = TierConfig(tiers[-1], s3_config)
            tiering_config = TieringPolicy().with_rule(TieringRule(tiers[-1], '1s'))

            sth.execute_scheme_query(CreateTierIfNotExists(tiers[-1], tier_config))
            sth.execute_scheme_query(CreateTieringRuleIfNotExists(tiering_rules[-1], 'timestamp', tiering_config))

            sth.execute_scheme_query(AlterTier(tiers[-1], tier_config))
            sth.execute_scheme_query(AlterTieringRule(tiering_rules[-1], 'timestamp', tiering_config))

        sth.execute_scheme_query(CreateTableStore('store').with_schema(self.schema1))
        sth.execute_scheme_query(CreateTable('store/table').with_schema(self.schema1))

        threads = []

        threads.append(self.TestThread(
            target=self._change_tiering_rule,
            args=[ctx, 'store/table', tiering_rules, test_duration]
        ))
        writer_id_offset = random.randint(0, 1 << 30)
        for i in range(4):
            threads.append(self.TestThread(target=self._upsert, args=[ctx, 'store/table', writer_id_offset + i, test_duration]))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        for tiering in tiering_rules:
            sth.execute_scheme_query(DropTieringRule(tiering))
        for tier in tiers:
            sth.execute_scheme_query(DropTier(tier))
        
        sth.execute_scheme_query(AlterTable('store/table').set_ttl('P1D', 'timestamp'))

        while sth.execute_scan_query(f'SELECT COUNT(*) FROM `{sth.get_full_path('store/table')}`').result_set.rows[0][0]:
            time.sleep(10)
        
