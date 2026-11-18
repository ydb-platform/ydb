import os

import pytest

from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    RollingDowngradeAndUpgradeFixture,
    inter_stable_binary_path,
)
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.oss.ydb_sdk_import import ydb


FLAGS = ("enable_set_column_constraint", "enable_column_shard_set_not_null")
# The compatibility suite's default target is a published prestable. Exercise the new DDL on the checkout.
CURRENT_BINARY = os.environ.get("YDB_CURRENT_BINARY_PATH") or kikimr_driver_path()


class NotNullChecks:
    def execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def create_table(self):
        self.execute("""
            CREATE TABLE `not_null_table` (
                k Uint64 NOT NULL, v Utf8, PRIMARY KEY(k)
            ) PARTITION BY HASH(k) WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        """)
        self.execute("""
            UPSERT INTO `not_null_table` (k, v)
            VALUES (1u, "one"), (2u, "two");
        """)

    def set_not_null(self):
        self.execute("ALTER TABLE `not_null_table` ALTER COLUMN v SET NOT NULL;")

    def check_constraint(self, key):
        rows = self.execute("SELECT k, v FROM `not_null_table` WHERE k <= 2u ORDER BY k;")[0].rows
        assert [(row.k, row.v) for row in rows] == [(1, "one"), (2, "two")]
        with pytest.raises((ydb.BadRequest, ydb.PreconditionFailed, ydb.GenericError)):
            self.execute(f"UPSERT INTO `not_null_table` (k, v) VALUES ({key}u, NULL);")
        self.execute(f'UPSERT INTO `not_null_table` (k, v) VALUES ({key}u, "written");')
        rows = self.execute(f"SELECT v FROM `not_null_table` WHERE k = {key}u;")[0].rows
        assert [row.v for row in rows] == ["written"]
        assert not self.execute("SELECT k FROM `not_null_table` WHERE v IS NULL;")[0].rows

    def configure_feature(self, enabled):
        flags = self.config.yaml_config.setdefault("feature_flags", {})
        for name in FLAGS:
            if enabled:
                flags[name] = True
            else:
                # Old servers must not receive unknown configuration fields.
                flags.pop(name, None)


class TestColumnTableNotNullRestart(NotNullChecks, RestartToAnotherVersionFixture):
    @pytest.fixture(
        autouse=True,
        params=[
            (inter_stable_binary_path, CURRENT_BINARY),
            (CURRENT_BINARY, inter_stable_binary_path),
            (CURRENT_BINARY, CURRENT_BINARY),
        ],
        ids=["stable_to_current", "current_to_stable", "current_to_current"],
    )
    def base_setup(self, request):
        self.all_binary_paths = request.param
        self.current_binary_paths_index = 0
        yield from self.setup_cluster(
            extra_feature_flags=list(FLAGS) if self.all_binary_paths[0] == CURRENT_BINARY else [],
            table_service_config={"enable_olap_sink": True},
            column_shard_config={"disabled_on_scheme_shard": False},
        )

    def upgrade_if_needed(self):
        if self.all_binary_paths[0] != CURRENT_BINARY:
            self.configure_feature(True)
            self.change_cluster_version()

    def test_finalized_constraint(self):
        self.create_table()
        self.upgrade_if_needed()
        self.set_not_null()
        self.check_constraint(3)
        self.configure_feature(False)
        self.change_cluster_version()
        self.check_constraint(4)

    def test_rejected_constraint(self):
        self.create_table()
        self.execute("UPSERT INTO `not_null_table` (k, v) VALUES (3u, NULL);")
        self.upgrade_if_needed()
        with pytest.raises(ydb.PreconditionFailed):
            self.set_not_null()
        self.configure_feature(False)
        self.change_cluster_version()
        self.execute("UPSERT INTO `not_null_table` (k, v) VALUES (4u, NULL);")
        rows = self.execute("SELECT k FROM `not_null_table` WHERE v IS NULL ORDER BY k;")[0].rows
        assert [row.k for row in rows] == [3, 4]


class TestColumnTableNotNullRolling(NotNullChecks, RollingDowngradeAndUpgradeFixture):
    @pytest.fixture(autouse=True)
    def base_setup(self):
        self.all_binary_paths = (CURRENT_BINARY, inter_stable_binary_path)
        yield from self.setup_cluster(
            extra_feature_flags=list(FLAGS),
            table_service_config={"enable_olap_sink": True},
            column_shard_config={"disabled_on_scheme_shard": False},
        )

    def test_finalized_constraint_in_mixed_cluster(self):
        self.create_table()
        self.set_not_null()
        self.configure_feature(False)
        self.cluster.overwrite_configs(self.config.full_config)
        for step, _ in enumerate(self.roll(), start=3):
            self.check_constraint(step)
