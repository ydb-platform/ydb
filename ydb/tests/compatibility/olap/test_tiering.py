import logging
import os
import time
import uuid

import boto3
import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb


class TestTieringRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, tree):
        if tree and any(version != (float("inf"),) for version in self.versions):
            pytest.skip("Tree keys require the current checkout on both sides of the restart")
        flags = ["enable_tiering_in_column_shard", "enable_external_data_sources"]
        if tree:
            flags.append("enable_tiering_object_key_tree")
        disabled_flags = ["enable_real_system_view_paths"] if self.versions[0] > self.versions[1] else []
        yield from self.setup_cluster(
            extra_feature_flags=flags,
            disabled_feature_flags=disabled_flags,
            column_shard_config={
                "lag_for_compaction_before_tierings_ms": 0,
                "compaction_actualization_lag_ms": 0,
                "optimizer_freshness_check_duration_ms": 0,
                "small_portion_detect_size_limit": 0,
                "alter_object_enabled": True,
                "max_read_staleness_ms": 2000,
                "periodic_wakeup_activation_period_ms": 1000,
                "gcinterval_ms": 1000,
            },
            additional_log_configs={
                "TX_TIERING": LogLevels.DEBUG,
                "TX_COLUMNSHARD_ACTUALIZATION": LogLevels.DEBUG,
                "TX_COLUMNSHARD_BLOBS_TIER": LogLevels.DEBUG,
            },
            query_service_config={"available_external_data_sources": ["ObjectStorage"]},
        )

    def query(self, sql):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(sql)

    def wait_for(self, condition, description):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(1)
        pytest.fail(description)

    @pytest.mark.parametrize("tree", [False, True])
    def test_tiering_survives_version_change(self, tree):
        endpoint = os.environ["S3_ENDPOINT"]
        bucket_name = "tiering-" + uuid.uuid4().hex
        bucket = boto3.resource(
            "s3", endpoint_url=endpoint,
            aws_access_key_id="minio", aws_secret_access_key="minio123",
            region_name="ru-central1",
        ).Bucket(bucket_name)
        bucket.create(CreateBucketConfiguration={"LocationConstraint": "ru-central1"})
        target = f"`{self.database_path}/tier`" + (".`archive/data`" if tree else "")
        self.query(f"""
            CREATE SECRET `{self.database_path}/tier_access` WITH (value="minio");
            CREATE SECRET `{self.database_path}/tier_secret` WITH (value="minio123");
            CREATE EXTERNAL DATA SOURCE tier WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="{endpoint}/{bucket_name}", AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{self.database_path}/tier_access",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{self.database_path}/tier_secret",
                AWS_REGION="ru-central1"
            );
            CREATE TABLE tiered (ts Timestamp NOT NULL, id Uint64 NOT NULL, payload String, PRIMARY KEY(ts, id))
            WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=2, TTL=Interval("PT1S") TO EXTERNAL DATA SOURCE {target} ON ts);
        """)
        self.query(f"""
            ALTER OBJECT `{self.database_path}/tiered` (TYPE TABLE) SET (
                ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{{"levels": [
                    {{"class_name": "Zero", "portions_live_duration": "5s", "expected_blobs_size": 1572864,
                      "portions_count_available": 2}}, {{"class_name": "Zero"}}]}}`
            );
        """)
        count = 10000
        columns = ydb.BulkUpsertColumns()
        columns.add_column("ts", ydb.PrimitiveType.Timestamp)
        columns.add_column("id", ydb.PrimitiveType.Uint64)
        columns.add_column("payload", ydb.PrimitiveType.String)
        rows = [{"ts": 1000000, "id": i, "payload": (str(i) + "x" * 1024).encode()} for i in range(count)]
        for batch in (rows[::2], rows[1::2]):
            self.driver.table_client.bulk_upsert(f"{self.database_path}/tiered", batch, columns)

        def all_evicted():
            result = self.query(f"""
                SELECT TierName, Optimized, SUM(Rows) AS rows FROM `{self.database_path}/tiered/.sys/primary_index_portion_stats`
                WHERE Activity = 1 GROUP BY TierName, Optimized;
            """)
            logging.info("Tier portions: %s", result[0].rows)
            return sum(row.rows for row in result[0].rows if row.TierName != "__DEFAULT") == count

        self.wait_for(all_evicted, "Data did not reach the external tier")
        keys = [obj.key for obj in bucket.objects.all()]
        assert keys
        if tree:
            assert all(key.startswith("archive/data/") for key in keys), keys
        else:
            assert all("/" not in key for key in keys), keys
        self.change_cluster_version()
        result = self.query("SELECT COUNT(*) AS count, SUM(id) AS checksum FROM tiered;")
        assert result[0].rows[0].count == count
        assert result[0].rows[0].checksum == count * (count - 1) // 2
        result = self.query("SELECT SUM(LENGTH(payload)) AS bytes FROM tiered;")
        assert result[0].rows[0].bytes == sum(len(row["payload"]) for row in rows)
        self.query(f"""
            ALTER TABLE tiered SET TTL
                Interval("PT1S") TO EXTERNAL DATA SOURCE {target}, Interval("PT2S") DELETE ON ts;
        """)
        self.wait_for(lambda: self.query("SELECT COUNT(*) AS count FROM tiered;")[0].rows[0].count == 0,
                      "Expired rows were not deleted after changing version")
        self.wait_for(lambda: not list(bucket.objects.all()), "Tier objects were not collected after changing version")
