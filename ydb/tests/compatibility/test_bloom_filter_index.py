# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestBloomFilterIndex(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.table_name = "test_bloom_filter"
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_local_bloom_filter_index": True,
            }
        )

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def test_bloom_filter_survives_version_change(self):
        if min(self.versions) < (26, 2):
            pytest.skip("Bloom filter index syntax requires 26.2+")

        # Create table with bloom filter index
        self._execute(f"""
            CREATE TABLE `{self.table_name}` (
                key1 Uint64,
                key2 Uint64,
                value Utf8,
                PRIMARY KEY (key1, key2),
                INDEX idx_bloom LOCAL USING bloom_filter ON (key1) WITH (false_positive_probability=0.05)
            );
        """)

        # Insert data and query before version change
        self._execute(f"UPSERT INTO `{self.table_name}` (key1, key2, value) VALUES (1, 1, 'a'u), (2, 2, 'b'u);")
        result_before = self._execute(f"SELECT * FROM `{self.table_name}` WHERE key1 = 1;")
        assert len(result_before[0].rows) == 1

        # Change cluster version
        self.change_cluster_version()

        # The bloom filter index definition should survive version change
        schema_after = self._execute(f"SHOW CREATE TABLE `{self.table_name}`;")
        assert "idx_bloom" in str(schema_after)

        # Data should survive version change, point lookup should work
        result_after = self._execute(f"SELECT * FROM `{self.table_name}` WHERE key1 = 1;")
        assert len(result_after[0].rows) == 1
