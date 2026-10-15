import json
import logging
import os
import re
import time

import pytest
import requests
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestCompactionConfig(object):
    test_name = "compaction_config"

    @classmethod
    def setup_class(cls):
        pass

    def init(self, config):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        self.cluster = KiKiMR(config)
        self.cluster.start()
        node = self.cluster.nodes[1]
        self.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        self.ydb_client.wait_connection()

    def check(self, name):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/name"
        statement = f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64 NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        self.ydb_client.session_pool.execute_with_retries(statement, retry_settings=ydb.RetrySettings(max_retries=0))
        return self.ydb_client.driver.table_client.session().create().describe_table(table_path)

    def test_lc_buckets(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "lc-buckets",
            })

        self.init(config)
        self.check("test_lc_buckets")

    def test_tiling(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "tiling",
            })

        self.init(config)
        self.check("test_tiling")

    def test_tiling_plus_plus(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "tiling++",
            })

        self.init(config)
        self.check("test_tiling_plus_plus")

    def test_wrong_preset(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "ydb",
            })

        with pytest.raises(Exception, match=r"Socket closed|recvmsg:Connection reset by peer"):
            self.init(config)
            self.check("test_wrong_preset")

    def test_default(self):
        config = KikimrConfigGenerator()

        self.init(config)
        self.check("test_default")

    def test_tiling_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "tiling",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
            })

        self.init(config)
        self.check("test_tiling_constructor")

    def test_lc_buckets_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "lc-buckets",
                    "node_portions_count_limit" : 6000000,
                    "weight_kff" : 1,
                    "lcbuckets" : {
                        "levels" : [
                            {
                                "class_name" : "Zero",
                                "zero_level": {
                                    "portions_count_limit": 4000000,
                                    "expected_blobs_size": 2000000,
                                    "portions_live_duration_seconds": 180
                                }
                            },
                            {
                                "class_name" : "Zero",
                                "zero_level": {
                                    "portions_count_limit": 2000000,
                                    "expected_blobs_size": 18000000
                                }
                            },
                            {
                                "class_name" : "Zero",
                                "zero_level": {
                                    "portions_count_limit": 1000000,
                                    "expected_blobs_size": 8000000
                                }
                            },
                        ]
                    }
                },
            })
        self.init(config)
        self.check("test_lc_buckets_constructor")

    def test_wrong_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "ydb",
                },
            })

        with pytest.raises(Exception, match=r"Socket closed|recvmsg:Connection reset by peer"):
            self.init(config)
            self.check("test_wrong_constructor")

    def test_mix_constructor(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "lc-buckets",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
            })
        with pytest.raises(Exception, match=r"Socket closed|recvmsg:Connection reset by peer"):
            self.init(config)
            self.check("test_mix_constructor")

    def test_constructor_overrides_preset(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_preset": "tiling",
                "default_compaction_constructor": {
                    "class_name" : "tiling",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
            })

        self.init(config)
        self.check("test_constructor_overrides_preset")

    def test_constructor_still_overrides_preset(self):
        config = KikimrConfigGenerator(
            column_shard_config={
                "default_compaction_constructor": {
                    "class_name" : "tiling",
                    "tiling" : {
                        "json" : "{}"
                    }
                },
                "default_compaction_preset": "tiling",
            })

        self.init(config)
        self.check("test_constructor_still_overrides_preset")

    def list_column_shard_ids(self, table_path, mon_url):
        response = requests.get(
            mon_url
            + f"/viewer/json/describe?database={self.ydb_client.database}&path={table_path}&enums=true&partition_stats=true&subs=0"
        )
        response.raise_for_status()
        path_description = response.json()["PathDescription"]
        assert "ColumnTableDescription" in path_description, path_description.keys()
        return path_description["ColumnTableDescription"]["Sharding"]["ColumnShards"]

    @staticmethod
    def compaction_page_html(mon_url, tablet_id):
        response = requests.get(f"{mon_url}/tablets/app?TabletID={tablet_id}&page=compaction")
        response.raise_for_status()
        return response.text

    @staticmethod
    def compaction_visual_objects(html):
        result = []
        for m in re.finditer(r"<pre>(.*?)</pre>", html, re.DOTALL):
            raw = m.group(1).strip()
            try:
                result.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
        return result

    def wait_compaction_predicate(self, mon_url, table_path, predicate, timeout_sec=60):
        shard_ids = self.list_column_shard_ids(table_path, mon_url)
        assert shard_ids, "expected at least one column shard"
        tablet_id = shard_ids[0]
        deadline = time.time() + timeout_sec
        last_html = ""
        while time.time() < deadline:
            last_html = self.compaction_page_html(mon_url, tablet_id)
            objs = self.compaction_visual_objects(last_html)
            if predicate(objs):
                return
            time.sleep(0.5)
        assert False, f"compaction predicate not satisfied within {timeout_sec}s, last HTML snippet:\n{last_html[:4000]}"

    @staticmethod
    def _is_tiling_visual(objs):
        return any(isinstance(o, dict) and o.get("1-Name") == "TILING" for o in objs)

    @staticmethod
    def _is_lc_buckets_visual(objs):
        return any(
            isinstance(o, dict) and "levels" in o and o.get("1-Name") is None for o in objs
        )

    def create_column_table(self, table_path):
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`")
        self.ydb_client.session_pool.execute_with_retries(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64 NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """,
            retry_settings=ydb.RetrySettings(max_retries=0),
        )

    def describe_schema_options(self, table_path, mon_url):
        response = requests.get(
            mon_url + f"/viewer/json/describe?database={self.ydb_client.database}&path={table_path}&enums=true&subs=0"
        )
        response.raise_for_status()
        path_description = response.json()["PathDescription"]
        assert "ColumnTableDescription" in path_description, path_description.keys()
        return path_description["ColumnTableDescription"]["Schema"].get("Options", {})

    def test_reset_options_compaction(self):
        # The node default is lc-buckets: after the reset the table must follow it again
        # instead of getting some hard-coded preset
        config = KikimrConfigGenerator(
            column_shard_config={
                "alter_object_enabled": True,
                "default_compaction_preset": "lc-buckets",
            }
        )
        self.init(config)
        node = self.cluster.nodes[1]
        mon_url = f"http://{node.host}:{node.mon_port}"

        table_path = f"{self.ydb_client.database}/{self.test_name}/reset_options_compaction"
        self.create_column_table(table_path)

        self.wait_compaction_predicate(mon_url, table_path, self._is_lc_buckets_visual)

        self.ydb_client.query(
            f"ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling`);"
        )

        self.wait_compaction_predicate(mon_url, table_path, self._is_tiling_visual)

        self.ydb_client.query(
            f"ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=RESET_OPTIONS, NAME=COMPACTION_PLANNER);"
        )

        self.wait_compaction_predicate(mon_url, table_path, self._is_lc_buckets_visual)
        assert "CompactionPlannerConstructor" not in self.describe_schema_options(table_path, mon_url)

    def test_reset_options_all(self):
        config = KikimrConfigGenerator(column_shard_config={"alter_object_enabled": True})
        self.init(config)
        node = self.cluster.nodes[1]
        mon_url = f"http://{node.host}:{node.mon_port}"

        table_path = f"{self.ydb_client.database}/{self.test_name}/reset_options_all"
        self.create_column_table(table_path)

        self.ydb_client.query(
            f"""
            ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS,
                SCHEME_NEED_ACTUALIZATION=`true`,
                SCAN_READER_POLICY_NAME=`SIMPLE`,
                DEDUPLICATION_ENABLED=`false`,
                CACHE_BLOBS_AFTER_WRITE=`true`,
                `INSERT_OPTIONS.BUILD_INDEXES_ENABLED`=`true`,
                `INSERT_OPTIONS.BUILD_INDEXES_MIN_BLOB_BYTES`=`1024`,
                `COMPACTION_PLANNER.CLASS_NAME`=`tiling`,
                `METADATA_MEMORY_MANAGER.CLASS_NAME`=`local_db`);
            """
        )
        options = self.describe_schema_options(table_path, mon_url)
        assert options.get("SchemeNeedActualization") is True, options
        assert options.get("ScanReaderPolicyName") == "SIMPLE", options
        assert options.get("DeduplicationEnabled") is False, options
        assert options.get("CacheBlobsAfterWrite") is True, options
        assert options.get("InsertOptions", {}).get("BuildIndexesEnabled") is True, options
        assert "CompactionPlannerConstructor" in options, options
        assert "MetadataManagerConstructor" in options, options

        # options not mentioned in UPSERT_OPTIONS keep their values
        self.ydb_client.query(
            f"ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCAN_READER_POLICY_NAME=`PLAIN`);"
        )
        options = self.describe_schema_options(table_path, mon_url)
        assert options.get("SchemeNeedActualization") is True, options
        assert options.get("ScanReaderPolicyName") == "PLAIN", options

        self.ydb_client.query(
            f"ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=RESET_OPTIONS, NAME=`SCHEME_NEED_ACTUALIZATION, DEDUPLICATION_ENABLED`);"
        )
        options = self.describe_schema_options(table_path, mon_url)
        assert not options.get("SchemeNeedActualization", False), options
        assert "DeduplicationEnabled" not in options, options
        assert options.get("ScanReaderPolicyName") == "PLAIN", options
        assert "CompactionPlannerConstructor" in options, options

        self.ydb_client.query(
            f"ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=RESET_OPTIONS, "
            "NAME=`SCAN_READER_POLICY_NAME, INSERT_OPTIONS, COMPACTION_PLANNER, METADATA_MEMORY_MANAGER, CACHE_BLOBS_AFTER_WRITE`);"
        )
        options = self.describe_schema_options(table_path, mon_url)
        assert not options.get("SchemeNeedActualization", False), options
        assert set(options.keys()) <= {"SchemeNeedActualization"}, options

        # Protobuf text formatting may escape apostrophes in SDK error messages.
        with pytest.raises(ydb.issues.GenericError, match=r"unknown option \\?'UNKNOWN\\?'"):
            self.ydb_client.query(f"ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=RESET_OPTIONS, NAME=UNKNOWN);")
        with pytest.raises(ydb.issues.GenericError, match=r"can\\?'t find alter parameter NAME"):
            self.ydb_client.query(f"ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=RESET_OPTIONS);")
