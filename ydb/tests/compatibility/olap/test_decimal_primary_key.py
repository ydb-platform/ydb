import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestDecimalPrimaryKey(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        # Do not send the new option to old binaries. Its default must retain the
        # legacy ordering throughout an upgrade/downgrade before opting in.
        yield from self.setup_cluster(column_shard_config={
            "disabled_on_scheme_shard": False,
            "compaction_enabled": False,
        })

    @pytest.mark.parametrize("compound", [False, True])
    def test_legacy_order_survives_restart(self, compound):
        values = ["-257", "-256", "-1", "0", "1", "255", "256", "257", "65536"]
        keys = "prefix, key" if compound else "key"
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries(f"""
                CREATE TABLE decimals (
                    prefix Uint64 NOT NULL,
                    key Decimal(22, 9) NOT NULL,
                    value Int64 NOT NULL,
                    PRIMARY KEY ({keys})
                ) PARTITION BY HASH({keys})
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)
            """)

        def write(offset):
            with ydb.QuerySessionPool(self.driver) as pool:
                # Separate writes exercise persisted portion borders as well as rows.
                for i, value in enumerate(values):
                    pool.execute_with_retries(f"""
                        UPSERT INTO decimals (prefix, key, value)
                        VALUES (0u, Unwrap(CAST("{value}" AS Decimal(22, 9))), {offset + i})
                    """)

        def check(offset):
            with ydb.QuerySessionPool(self.driver) as pool:
                rows = pool.execute_with_retries("SELECT value FROM decimals ORDER BY value")[0].rows
                assert [row["value"] for row in rows] == list(range(offset, offset + len(values)))
                for i, value in enumerate(values):
                    rows = pool.execute_with_retries(f"""
                        SELECT value FROM decimals
                        WHERE prefix = 0u AND key = CAST("{value}" AS Decimal(22, 9))
                    """)[0].rows
                    assert [row["value"] for row in rows] == [offset + i]

        write(0)
        check(0)
        self.change_cluster_version()
        check(0)
        write(100)
        check(100)
        self.change_cluster_version()
        check(100)
