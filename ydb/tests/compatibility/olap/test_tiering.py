import logging
import os
import time
import uuid

import boto3
import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb


class TieringTestBase(RestartToAnotherVersionFixture):
    requires_prefix_alter = False

    @pytest.fixture(autouse=True, scope="function")
    def setup(self, tree):
        if (tree or self.requires_prefix_alter) and min(self.versions) < (26, 4):
            pytest.skip("Tiering object key tree is only available since 26-4")

        flags = ["enable_tiering_in_column_shard", "enable_external_data_sources"]
        if tree or self.requires_prefix_alter:
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

    def create_table(self, prefix):
        endpoint = os.environ["S3_ENDPOINT"]
        bucket_name = "tiering-" + uuid.uuid4().hex
        bucket = boto3.resource(
            "s3", endpoint_url=endpoint,
            aws_access_key_id="minio", aws_secret_access_key="minio123",
            region_name="ru-central1",
        ).Bucket(bucket_name)
        bucket.create(CreateBucketConfiguration={"LocationConstraint": "ru-central1"})
        target = self.tier_target(prefix)
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
        return bucket

    def tier_target(self, prefix):
        return f"`{self.database_path}/tier`" + (f".`{prefix}`" if prefix is not None else "")

    def write_rows(self, offset=0, count=10000):
        columns = ydb.BulkUpsertColumns()
        columns.add_column("ts", ydb.PrimitiveType.Timestamp)
        columns.add_column("id", ydb.PrimitiveType.Uint64)
        columns.add_column("payload", ydb.PrimitiveType.String)
        rows = [{"ts": 1000000, "id": i, "payload": (str(i) + "x" * 1024).encode()}
                for i in range(offset, offset + count)]
        for batch in (rows[::2], rows[1::2]):
            self.driver.table_client.bulk_upsert(f"{self.database_path}/tiered", batch, columns)
        return rows

    def wait_evicted(self, count):
        def all_evicted():
            result = self.query(f"""
                SELECT TierName, Optimized, SUM(Rows) AS rows FROM `{self.database_path}/tiered/.sys/primary_index_portion_stats`
                WHERE Activity = 1 GROUP BY TierName, Optimized;
            """)
            logging.info("Tier portions: %s", result[0].rows)
            return sum(row.rows for row in result[0].rows if row.TierName != "__DEFAULT") == count

        self.wait_for(all_evicted, "Data did not reach the external tier")

    def check_rows(self, rows):
        result = self.query(f"""
            SELECT COUNT(*) AS count, SUM(id) AS checksum, SUM(LENGTH(payload)) AS bytes,
                SUM(CASE WHEN payload = CAST(id AS String) || "{'x' * 1024}" THEN 0 ELSE 1 END) AS invalid
            FROM tiered;
        """)
        assert result[0].rows[0].count == len(rows)
        assert result[0].rows[0].checksum == sum(row["id"] for row in rows)
        assert result[0].rows[0].bytes == sum(len(row["payload"]) for row in rows)
        assert result[0].rows[0].invalid == 0

    def check_keys(self, keys, prefix):
        assert keys
        if prefix is None:
            assert all("/" not in key for key in keys), keys
        elif prefix:
            assert all(key.startswith(prefix + "/") for key in keys), keys
        else:
            assert all(len(key.split("/")) == 5 and key.split("/")[0].isdigit() for key in keys), keys

    def active_keys(self, bucket):
        result = self.query(f"""
            SELECT DISTINCT BlobId FROM `{self.database_path}/tiered/.sys/primary_index_stats`
            WHERE Activity = 1 AND TierName != "__DEFAULT" AND BlobId != "__INPLACE";
        """)
        blob_ids = {"[" + row.BlobId.split("[", 1)[1] for row in result[0].rows}
        keys = {obj.key for obj in bucket.objects.all() if obj.key.rsplit("/", 1)[-1] in blob_ids}
        assert {key.rsplit("/", 1)[-1] for key in keys} == blob_ids, (keys, blob_ids)
        return keys

    def active_portions(self):
        result = self.query(f"""
            SELECT TabletId, PortionId FROM `{self.database_path}/tiered/.sys/primary_index_portion_stats`
            WHERE Activity = 1 AND TierName != "__DEFAULT";
        """)
        return {(row.TabletId, row.PortionId) for row in result[0].rows}


class TestTieringRestart(TieringTestBase):
    @pytest.mark.parametrize("tree", [False, True])
    def test_tiering_survives_version_change(self, tree):
        prefix = "archive/data" if tree else None
        bucket = self.create_table(prefix)
        rows = self.write_rows()
        self.wait_evicted(len(rows))
        self.check_keys(self.active_keys(bucket), prefix)
        self.change_cluster_version()
        self.check_rows(rows)
        target = self.tier_target(prefix)
        self.query(f"""
            ALTER TABLE tiered SET TTL
                Interval("PT1S") TO EXTERNAL DATA SOURCE {target}, Interval("PT2S") DELETE ON ts;
        """)
        self.wait_for(lambda: self.query("SELECT COUNT(*) AS count FROM tiered;")[0].rows[0].count == 0,
                      "Expired rows were not deleted after changing version")
        self.wait_for(lambda: not list(bucket.objects.all()), "Tier objects were not collected after changing version")


class TestTieringPrefixAlter(TieringTestBase):
    requires_prefix_alter = True

    def object_versions(self, bucket):
        versions = {}
        for obj in bucket.object_versions.all():
            assert obj.id != "null", "S3 versioning must be enabled before writing data"
            versions.setdefault(obj.object_key, set()).add(obj.id)
        return versions

    def check_preserved_versions(self, bucket, preserved):
        versions = self.object_versions(bucket)
        assert {key: versions.get(key) for key in preserved} == preserved, (preserved, versions)
        return versions

    @pytest.mark.parametrize("tree", [False, True])
    def test_prefix_changes_preserve_existing_objects(self, tree):
        prefix = "archive/data" if tree else None
        bucket = self.create_table(prefix)
        bucket.Versioning().enable()
        rows = self.write_rows()
        self.wait_evicted(len(rows))
        preserved_keys = self.active_keys(bucket)
        preserved_portions = self.active_portions()
        preserved_versions = self.object_versions(bucket)
        self.check_keys(preserved_keys, prefix)

        for prefix in ("archive/new", "", None, "archive/last"):
            self.query(f"""
                ALTER TABLE tiered SET TTL Interval("PT1S")
                TO EXTERNAL DATA SOURCE {self.tier_target(prefix)} ON ts;
            """)
            if prefix == "archive/new":
                self.change_cluster_version()
                self.check_rows(rows)
                assert self.active_keys(bucket) == preserved_keys
                assert self.active_portions() == preserved_portions
                assert self.object_versions(bucket) == preserved_versions
            rows += self.write_rows(offset=len(rows))
            self.wait_evicted(len(rows))
            keys = self.active_keys(bucket)
            portions = self.active_portions()
            assert preserved_portions <= portions, (preserved_portions - portions, portions)
            assert preserved_keys <= keys, (preserved_keys - keys, keys)
            self.check_keys(keys - preserved_keys, prefix)
            preserved_versions = self.check_preserved_versions(bucket, preserved_versions)
            preserved_keys = keys
            preserved_portions = portions
            self.check_rows(rows)

        self.config.yaml_config["feature_flags"]["enable_tiering_object_key_tree"] = False
        self.change_cluster_version()
        self.check_rows(rows)
        assert self.active_keys(bucket) == preserved_keys
        assert self.active_portions() == preserved_portions
        assert self.object_versions(bucket) == preserved_versions
        keys_before_write = self.active_keys(bucket)
        rows += self.write_rows(offset=len(rows))
        self.wait_evicted(len(rows))
        self.check_rows(rows)
        self.check_keys(self.active_keys(bucket) - keys_before_write, "archive/last")
        assert preserved_portions <= self.active_portions()
        assert preserved_keys <= self.active_keys(bucket)
        self.check_preserved_versions(bucket, preserved_versions)

        self.query('ALTER TABLE tiered SET TTL Interval("PT2S") DELETE ON ts;')
        self.wait_for(lambda: self.query("SELECT COUNT(*) AS count FROM tiered;")[0].rows[0].count == 0,
                      "Expired rows from historical prefixes were not deleted")
        self.wait_for(lambda: not list(bucket.objects.all()),
                      "Objects from historical prefixes were not collected after TTL deletion")
