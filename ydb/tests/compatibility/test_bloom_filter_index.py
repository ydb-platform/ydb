# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestBloomFilterIndex(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.table_name = "test_bloom_filter"
        yield from self.setup_cluster()

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def test_bloom_filter_survives_version_change(self):
        if min(self.versions) < (26, 2):
            pytest.skip("Bloom filter index syntax requires 26.2+")

        # Create table with multiple bloom filter indexes as scheme objects
        self._execute(f"""
            CREATE TABLE `{self.table_name}` (
                key1 Uint64,
                key2 Uint64,
                value Utf8,
                PRIMARY KEY (key1, key2),
                INDEX idx_bloom_1 LOCAL USING bloom_filter ON (key1) WITH (false_positive_probability=0.05),
                INDEX idx_bloom_2 LOCAL USING bloom_filter ON (key1, key2) WITH (false_positive_probability=0.01)
            );
        """)

        # Verify bloom filters exist as scheme objects via describe_table
        def callee(session):
            return session.describe_table(f"/{self.database_root}/{self.table_name}")

        with ydb.SessionPool(self.driver, size=1) as pool:
            desc = pool.retry_operation_sync(callee)

        # Check that indexes are present with correct columns and status
        by_name = {idx.name: idx for idx in desc.indexes}
        assert "idx_bloom_1" in by_name, "idx_bloom_1 not found in describe_table result"
        assert "idx_bloom_2" in by_name, "idx_bloom_2 not found in describe_table result"

        # Verify idx_bloom_1 has correct columns and status
        idx1 = by_name["idx_bloom_1"]
        assert list(idx1.index_columns) == ["key1"], \
            f"idx_bloom_1 columns {list(idx1.index_columns)!r}, expected ['key1']"
        assert idx1.status == ydb.IndexStatus.READY, \
            f"idx_bloom_1 status {idx1.status!r}, expected READY"

        # Verify idx_bloom_2 has correct columns and status
        idx2 = by_name["idx_bloom_2"]
        assert list(idx2.index_columns) == ["key1", "key2"], \
            f"idx_bloom_2 columns {list(idx2.index_columns)!r}, expected ['key1', 'key2']"
        assert idx2.status == ydb.IndexStatus.READY, \
            f"idx_bloom_2 status {idx2.status!r}, expected READY"

        # Insert data and query before version change
        self._execute(f"UPSERT INTO `{self.table_name}` (key1, key2, value) VALUES (1, 1, 'a'u), (2, 2, 'b'u);")
        result_before = self._execute(f"SELECT * FROM `{self.table_name}` WHERE key1 = 1;")
        assert len(result_before[0].rows) == 1

        # Change cluster version
        self.change_cluster_version()

        # Verify bloom filters still exist as scheme objects via describe_table after version change
        def callee_after(session):
            return session.describe_table(f"/{self.database_root}/{self.table_name}")

        with ydb.SessionPool(self.driver, size=1) as pool:
            desc_after = pool.retry_operation_sync(callee_after)

        # Check that indexes are still present with correct columns and status after version change
        by_name_after = {idx.name: idx for idx in desc_after.indexes}
        assert "idx_bloom_1" in by_name_after, "idx_bloom_1 not found in describe_table after version change"
        assert "idx_bloom_2" in by_name_after, "idx_bloom_2 not found in describe_table after version change"

        # Verify idx_bloom_1 still has correct columns and status after version change
        idx1_after = by_name_after["idx_bloom_1"]
        assert list(idx1_after.index_columns) == ["key1"], \
            f"idx_bloom_1 columns after version change {list(idx1_after.index_columns)!r}, expected ['key1']"
        assert idx1_after.status == ydb.IndexStatus.READY, \
            f"idx_bloom_1 status after version change {idx1_after.status!r}, expected READY"

        # Verify idx_bloom_2 still has correct columns and status after version change
        idx2_after = by_name_after["idx_bloom_2"]
        assert list(idx2_after.index_columns) == ["key1", "key2"], \
            f"idx_bloom_2 columns after version change {list(idx2_after.index_columns)!r}, expected ['key1', 'key2']"
        assert idx2_after.status == ydb.IndexStatus.READY, \
            f"idx_bloom_2 status after version change {idx2_after.status!r}, expected READY"

        # The bloom filter scheme objects should survive version change
        schema_after = self._execute(f"SHOW CREATE TABLE `{self.table_name}`;")

        # Extract the CREATE TABLE query from the result set
        if not (schema_after and schema_after[0].rows and schema_after[0].rows[0]):
            raise AssertionError(f"SHOW CREATE TABLE `{self.table_name}` returned no data.")

        create_query_column_index = -1
        for i, col in enumerate(schema_after[0].columns):
            if col.name == "CreateQuery":
                create_query_column_index = i
                break

        if create_query_column_index == -1:
            raise AssertionError(f"Column 'CreateQuery' not found in SHOW CREATE TABLE result for {self.table_name}.")

        create_query = schema_after[0].rows[0][create_query_column_index]

        # Verify both bloom filter scheme objects survived with complete definitions
        assert "INDEX idx_bloom_1 LOCAL USING bloom_filter ON (key1) WITH (false_positive_probability=0.05)" in create_query, \
            f"Bloom filter scheme object 'idx_bloom_1' not found with complete definition after version change"
        assert "INDEX idx_bloom_2 LOCAL USING bloom_filter ON (key1, key2) WITH (false_positive_probability=0.01)" in create_query, \
            f"Bloom filter scheme object 'idx_bloom_2' not found with complete definition after version change"

        # Data should survive version change, point lookup should work
        result_after = self._execute(f"SELECT * FROM `{self.table_name}` WHERE key1 = 1;")
        assert len(result_after[0].rows) == 1
