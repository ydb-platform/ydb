import time

import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    current_binary_path,
    init_stable_binary_path,
    init_stable_name,
    inter_stable_binary_path,
    inter_stable_name,
)
from ydb.tests.oss.ydb_sdk_import import ydb


class TestDecimalPrimaryKey(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(nodes=3, erasure=Erasure.NONE, column_shard_config={
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


class TestDecimalPrimaryKeyRepair(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, params=[init_stable_binary_path, inter_stable_binary_path, current_binary_path],
                    ids=[init_stable_name, inter_stable_name, "current"])
    def base_setup(self, request):
        self.current_binary_paths_index = 0
        self.all_binary_paths = [request.param, current_binary_path]

    @pytest.fixture(autouse=True)
    def setup(self, base_setup):
        yield from self.setup_cluster(nodes=3, erasure=Erasure.NONE, column_shard_config={
            "disabled_on_scheme_shard": False,
            "compaction_enabled": False,
        })

    def test_forward_repair_without_critical_schema(self):
        values = [-257, -256, -100, -1, 0, 1, 2, 10, 100, 255, 256, 257, 65536]
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries("""
                CREATE TABLE decimals (
                    key Decimal(22, 9) NOT NULL,
                    value Int64 NOT NULL,
                    PRIMARY KEY (key)
                ) PARTITION BY HASH(key)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)
            """)
            rows = ",".join(f'(Unwrap(CAST("{value}" AS Decimal(22, 9))), {i})' for i, value in enumerate(values))
            pool.execute_with_retries("UPSERT INTO decimals (key, value) VALUES " + rows)
            result = pool.execute_with_retries("SELECT value FROM decimals ORDER BY value")[0].rows
            assert [row["value"] for row in result] == list(range(len(values)))

        self.config.yaml_config["column_shard_config"]["enable_decimal_key_comparison"] = True
        self.change_cluster_version()
        with ydb.QuerySessionPool(self.driver) as pool:
            with pytest.raises(ydb.Error, match="Decimal primary key order changed"):
                pool.execute_with_retries("SELECT value FROM decimals ORDER BY key")
            rows = pool.execute_with_retries(
                "SELECT SUM(Rows) AS rows FROM `decimals/.sys/primary_index_portion_stats` WHERE Activity = 1"
            )[0].rows
            assert rows[0]["rows"] == len(values)

        self.config.yaml_config["column_shard_config"]["repairs"] = [
            {"class_name": "RebuildDecimalPrimaryKey", "description": "decimal-pk-forward-compatibility"},
        ]
        self.stop_driver()
        self.cluster.update_configurator_and_restart(self.config)
        self.driver = self.create_driver()

        def check():
            deadline = time.monotonic() + 120
            with ydb.QuerySessionPool(self.driver) as pool:
                while True:
                    try:
                        result = pool.execute_with_retries("SELECT value FROM decimals ORDER BY key")[0].rows
                        break
                    except ydb.Error as error:
                        if "Decimal primary key order changed" not in str(error) or time.monotonic() >= deadline:
                            raise
                        time.sleep(1)
                assert [row["value"] for row in result] == list(range(len(values)))
                result = pool.execute_with_retries("SELECT value FROM decimals ORDER BY key LIMIT 1")[0].rows
                assert [row["value"] for row in result] == [0]
                for i, value in enumerate(values):
                    result = pool.execute_with_retries(f"""
                        SELECT value FROM decimals WHERE key = CAST("{value}" AS Decimal(22, 9))
                    """)[0].rows
                    assert [row["value"] for row in result] == [i]
                for op, predicate in (("<", lambda value: value < 0), ("<=", lambda value: value <= 0),
                                      (">", lambda value: value > 0), (">=", lambda value: value >= 0)):
                    result = pool.execute_with_retries(f"""
                        SELECT value FROM decimals WHERE key {op} CAST("0" AS Decimal(22, 9)) ORDER BY key
                    """)[0].rows
                    assert [row["value"] for row in result] == [i for i, value in enumerate(values) if predicate(value)]

        check()
        self.stop_driver()
        self.cluster.update_configurator_and_restart(self.config)
        self.driver = self.create_driver()
        check()
