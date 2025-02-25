import yatest.common
import os
import time
import ydb
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels


logger = logging.getLogger(__name__)


class YdbClient:
    def __init__(self, endpoint, database):
        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.database = database
        self.session_pool = ydb.QuerySessionPool(self.driver)

    def stop(self):
        self.session_pool.stop()
        self.driver.stop()

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement):
        return self.session_pool.execute_with_retries(statement)


class ColumnTableHelper:
    def __init__(self, ydb_client: YdbClient, path: str):
        self.ydb_client = ydb_client
        self.path = path

    def get_row_count(self) -> int:
        count_row: int = 0
        result_set = self.ydb_client.query(f"SELECT COUNT(*) AS Rows FROM `{self.path}`")
        for result in result_set:
            for row in result.rows:
                count_row += row["Rows"]
        return count_row

    def get_portion_stat_by_tier(self) -> dict[str, dict[str, int]]:
        results = self.ydb_client.query(
            f"select TierName, sum(Rows) as Rows, count(*) as Portions from `{self.path}/.sys/primary_index_portion_stats` group by TierName"
        )
        return {
            row["TierName"]: {"Rows": row["Rows"], "Portions": row["Portions"]}
            for result_set in results
            for row in result_set.rows
        }

    def get_blob_stat_by_tier(self) -> dict[str, (int, int)]:
        stmt = f"""
            select TierName, count(*) as Portions, sum(BlobSize) as BlobSize, sum(BlobCount) as BlobCount from (
                select TabletId, PortionId, TierName, sum(BlobRangeSize) as BlobSize, count(*) as BlobCount from `{self.path}/.sys/primary_index_stats` group by TabletId, PortionId, TierName
            ) group by TierName
        """
        results = self.ydb_client.query(stmt)
        return {
            row["TierName"]: {"Portions": row["Portions"], "BlobSize": row["BlobSize"], "BlobCount": row["BlobCount"]}
            for result_set in results
            for row in result_set.rows
        }

    def _coollect_volumes_column(self, column_name: str) -> tuple[int, int]:
        query = f'SELECT * FROM `{self.path}/.sys/primary_index_stats` WHERE Activity == 1 AND EntityName = \"{column_name}\"'
        result_set = self.ydb_client.query(query)
        raw_bytes, bytes = 0, 0
        for result in result_set:
            for rows in result.rows:
                raw_bytes += rows["RawBytes"]
                bytes += rows["BlobRangeSize"]
        return raw_bytes, bytes

    def get_volumes_column(self, column_name: str) -> tuple[int, int]:
        pred_raw_bytes, pred_bytes = 0, 0
        raw_bytes, bytes = self._coollect_volumes_column(column_name)
        while pred_raw_bytes != raw_bytes and pred_bytes != bytes:
            pred_raw_bytes = raw_bytes
            pred_bytes = bytes
            time.sleep(10)
            raw_bytes, bytes = self._coollect_volumes_column(column_name)
        logging.info(f"Table `{self.path}`, volumes `{column_name}` ({raw_bytes}, {bytes})")
        return raw_bytes, bytes


class ColumnFamilyTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_olap_compression": True},
            column_shard_config={
                "lag_for_compaction_before_tierings_ms": 0,
                "compaction_actualization_lag_ms": 0,
                "optimizer_freshness_check_duration_ms": 0,
                "small_portion_detect_size_limit": 0,
                "max_read_staleness_ms": 5000,
            },
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    @staticmethod
    def wait_for(condition_func, timeout_seconds):
        t0 = time.time()
        while time.time() - t0 < timeout_seconds:
            if condition_func():
                return True
            time.sleep(1)
        return False
