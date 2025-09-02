import logging
import time
from .ydb_client import YdbClient


logger = logging.getLogger(__name__)


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

    def get_portion_count(self) -> int:
        return self.ydb_client.query(f"select count(*) as Rows from `{self.path}/.sys/primary_index_portion_stats`")[0].rows[0]["Rows"]

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

    def set_fast_compaction(self):
        self.ydb_client.query(
            f"""
            ALTER OBJECT `{self.path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {{"levels" : [{{"class_name" : "Zero", "portions_live_duration" : "5s", "expected_blobs_size" : 1572864, "portions_count_available" : 2}},
                                {{"class_name" : "Zero"}}]}}`);
            """
        )

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

    def portions_actualized_in_sys(self):
        portions = self.get_portion_stat_by_tier()
        logger.info(f"portions: {portions}, blobs: {self.get_blob_stat_by_tier()}")
        return "__DEFAULT" in portions
