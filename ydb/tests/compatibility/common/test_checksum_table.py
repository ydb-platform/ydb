import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestChecksumTable(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.versions[0] >= self.versions[1]:
            pytest.skip("Only check forward compatibility")
        if min(self.versions) < (26, 3, 1):
            pytest.skip("CHECKSUM TABLE requires a source version of 26.3.1 or later")

        yield from self.setup_cluster(use_config_store=True, extra_feature_flags=["enable_checksum_table"])

    def checksums(self, pool):
        checksums = {}
        for algorithm in ("CityHash64", "CityHash128", "Sha256"):
            options = "" if algorithm == "CityHash64" else f' WITH (ALGORITHM = "{algorithm}")'
            result, = pool.execute_with_retries(f"CHECKSUM TABLE checksum_data{options};")
            row, = result.rows
            assert row["algorithm"] == algorithm
            assert row["count"] == 3
            checksums[algorithm] = (row["version"], row["hash"])
        return checksums

    def test_checksum_survives_upgrade(self):
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries("""
                CREATE TABLE checksum_data (id Uint64 NOT NULL, value Utf8, PRIMARY KEY (id))
                WITH (PARTITION_AT_KEYS = (10, 20));
            """)
            pool.execute_with_retries("""
                UPSERT INTO checksum_data (id, value) VALUES (1, "one"u), (11, NULL), (21, ""u);
            """)
            before = self.checksums(pool)

        self.change_cluster_version()

        with ydb.QuerySessionPool(self.driver) as pool:
            assert self.checksums(pool) == before
