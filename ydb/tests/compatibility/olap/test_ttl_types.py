import time

import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


CURRENT = (float("inf"),)
FLAG = "enable_column_shard_extended_ttl_types"
TYPES = ["DyNumber", "Date32", "Datetime64", "Timestamp64"]


class TestColumnTtlTypes(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True)
    def setup(self, type_name):
        if CURRENT not in self.versions:
            pytest.skip("Extended TTL types require the current checkout on one side of the restart")
        flags = [FLAG] if self.versions[0] == CURRENT else []
        if type_name == "DyNumber":
            # Older versions gate the column type itself independently of TTL.
            flags.append("enable_columnshard_dy_number")
        yield from self.setup_cluster(
            extra_feature_flags=flags,
            disabled_feature_flags=["enable_real_system_view_paths"],
            column_shard_config={
                "lag_for_compaction_before_tierings_ms": 0,
                "compaction_actualization_lag_ms": 0,
                "optimizer_freshness_check_duration_ms": 0,
                "small_portion_detect_size_limit": 0,
                "alter_object_enabled": True,
                "periodic_wakeup_activation_period_ms": 1000,
            },
        )

    def query(self, sql):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(sql)

    def enable_ttl(self, type_name):
        unit = ' AS SECONDS' if type_name == "DyNumber" else ''
        self.query(f'ALTER TABLE ttl_types SET TTL Interval("PT1S") ON ts{unit};')

    def wait_for_ids(self, expected):
        deadline = time.monotonic() + 180
        actual = None
        while time.monotonic() < deadline:
            actual = [row.id for row in self.query("SELECT id FROM ttl_types ORDER BY id;")[0].rows]
            if actual == expected:
                return
            time.sleep(1)
        pytest.fail(f"Expected surviving IDs {expected}, got {actual}")

    @pytest.mark.parametrize("type_name", TYPES)
    @pytest.mark.parametrize("expired", [False, True])
    def test_ttl_across_restart(self, type_name, expired):
        self.query(f"""
            CREATE TABLE ttl_types (id Uint64 NOT NULL, ts {type_name}, PRIMARY KEY(id))
            PARTITION BY HASH(id) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=1);
            ALTER OBJECT `{self.database_path}/ttl_types` (TYPE TABLE) SET (
                ACTION=UPSERT_INDEX, NAME=ttl_max, TYPE=MIN_MAX,
                FEATURES=`{{"column_name":"ts", "storage_id":"__LOCAL_METADATA", "inherit_portion_storage":false}}`
            );
        """)
        self.query(f"""
            ALTER OBJECT `{self.database_path}/ttl_types` (TYPE TABLE) SET (
                ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{{"levels":[
                    {{"class_name":"Zero","portions_live_duration":"1s","expected_blobs_size":1572864,"portions_count_available":2}},
                    {{"class_name":"Zero"}}]}}`
            );
        """)
        # Keep expired and future values in separate test tables: TTL deletes whole portions.
        future = {"DyNumber": 'DyNumber("1e125")', "Date32": 'Date32("2100-01-01")',
                  "Datetime64": 'Datetime64("2100-01-01T00:00:00Z")',
                  "Timestamp64": 'Timestamp64("2100-01-01T00:00:00Z")'}[type_name]
        past = 'DyNumber("-1")' if type_name == "DyNumber" else f'CAST(-1 AS {type_name})'
        epoch = 'DyNumber("0")' if type_name == "DyNumber" else f'CAST(0 AS {type_name})'
        values = [(1, past), (2, epoch)] if expired else [(3, future)]
        for id, value in values:
            for _ in range(2):
                self.query(f"UPSERT INTO ttl_types (id, ts) VALUES ({id}ul, {value});")
        self.wait_for_ids([id for id, _ in values])
        before = self.query("SELECT id, CAST(ts AS String) AS value FROM ttl_types ORDER BY id;")[0].rows

        if self.versions[0] == CURRENT:
            self.enable_ttl(type_name)
            if self.versions[1] != CURRENT:
                # Old ColumnShard binaries cannot execute TTL on these types.
                # Remove the setting before downgrade, retaining the column and index.
                self.query("ALTER TABLE ttl_types RESET (TTL);")
            else:
                # Persisted TTL must execute after restart with the flag disabled.
                self.config.yaml_config["feature_flags"][FLAG] = False
        if self.versions[1] != CURRENT:
            self.config.yaml_config["feature_flags"].pop(FLAG, None)
        elif self.versions[0] != CURRENT:
            self.config.yaml_config["feature_flags"][FLAG] = True

        self.change_cluster_version()
        after = self.query("SELECT id, CAST(ts AS String) AS value FROM ttl_types ORDER BY id;")[0].rows
        assert all(row in before for row in after)
        if not expired:
            assert after == before
        if self.versions[1] == CURRENT:
            if self.versions[0] != CURRENT:
                assert after == before
                self.enable_ttl(type_name)
            if expired:
                # Exercise TTL after restart, including current -> current with the flag off.
                for _ in range(2):
                    self.query(f"UPSERT INTO ttl_types (id, ts) VALUES (10ul, {past});")
            self.wait_for_ids([] if expired else [3])
        else:
            # Data and MIN_MAX metadata remain readable by an older binary.
            self.query(f"UPSERT INTO ttl_types (id, ts) VALUES (4ul, {future});")
            assert [row.id for row in self.query("SELECT id FROM ttl_types WHERE id >= 3 ORDER BY id;")[0].rows] == ([4] if expired else [3, 4])
