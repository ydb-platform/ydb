# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestCTASOperations(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.source_table_name = "ctas_src_table"
        self.destination_table_name = "ctas_dst_table"
        self.upsert_batches = 1
        self.rows_count = 32 * 1024 * self.upsert_batches
        self.value_bytes = 1024

        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        yield from self.setup_cluster(table_service_config={
            "enable_oltp_sink": True,
            "enable_olap_sink": True,
            "enable_create_table_as": True,
        }, extra_feature_flags={
            "enable_temp_tables": True,
            "enable_olap_schema_operations": True,
            "enable_move_column_table": True,
        }, column_shard_config={
            "disabled_on_scheme_shard": False,
        })

    def test_ctas_oltp(self):
        self.fill_source_table()

        for _ in self.roll():
            self.assert_ctas(
                self.q_ctas(False, self.destination_table_name),
                self.q_select_count(self.destination_table_name),
                self.q_drop_destination_table(self.destination_table_name),
            )

    def test_ctas_olap(self):
        self.fill_source_table()

        for _ in self.roll():
            self.assert_ctas(
                self.q_ctas(True, self.destination_table_name),
                self.q_select_count(self.destination_table_name),
                self.q_drop_destination_table(self.destination_table_name),
            )

    def fill_source_table(self, create=True):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            if create:
                session_pool.execute_with_retries(self.q_create_source_table())

            for i in range(self.upsert_batches):
                assert self.rows_count % self.upsert_batches == 0
                rows_per_batch = self.rows_count // self.upsert_batches
                rows = []
                for j in range(rows_per_batch):
                    rows.append({'k': i * rows_per_batch + j, 'v': ('0' * self.value_bytes).encode('utf-8') })

                data_struct_type = ydb.StructType()
                data_struct_type.add_member("k", ydb.PrimitiveType.Uint64)
                data_struct_type.add_member("v", ydb.PrimitiveType.String)

                session_pool.execute_with_retries(
                    self.q_upsert_source_table(),
                    {'$data': (rows, ydb.ListType(data_struct_type))})

    def assert_ctas(self, ctas_query, select_query, drop_query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(ctas_query)
            result_sets = session_pool.execute_with_retries(select_query)
            assert result_sets[0].rows[0]["cnt"] == self.rows_count
            session_pool.execute_with_retries(drop_query)

    def q_create_source_table(self):
        return f"""
            CREATE TABLE `{self.source_table_name}` (
                k Uint64 NOT NULL,
                v String,
                PRIMARY KEY (k)
            ) WITH (
                STORE=COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64
            );
        """
    
    def q_upsert_source_table(self):
        return f"""
            DECLARE $data AS List<Struct<k: Uint64, v: String>>;
            UPSERT INTO `{self.source_table_name}`
            SELECT k AS k, v
            FROM AS_TABLE($data);
        """

    def q_ctas(self, is_olap, table_name):
        params = "STORE=ROW" if not is_olap else "STORE=COLUMN"
        return f"""
            CREATE TABLE `{table_name}` (
                PRIMARY KEY (k1, k2)
            ) WITH (
                {params}
            ) AS SELECT Digest::IntHash64(k) AS k1, k AS k2, v FROM `{self.source_table_name}`;
        """

    def q_select_count(self, table_name):
        return f"""
            SELECT COUNT(*) AS cnt FROM `{table_name}`;
        """
    
    def q_drop_destination_table(self, table_name):
        return f"""
            DROP TABLE `{table_name}`;
        """
