from .base import ChunksTestBase
import yatest.common
import logging
import time


logger = logging.getLogger(__name__)


class TestSelectChunkedData(ChunksTestBase):
    test_name = "select_chunked_data"

    @classmethod
    def setup_class(self):
        super(TestSelectChunkedData, self).setup_class()

    def _get_test_dir(self):
        return f"{self.ydb_client.database}/{self.test_name}"

    def _full_data_csv_path(self):
        return '/home/vladilenmuz/full_data.csv'

    def _create_test_table(self):
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{self.test_table_path}`;")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{self.test_table_path}` (
                timestamp Timestamp NOT NULL,
                resource_type Utf8 NOT NULL,
                resource_id Utf8 NOT NULL,
                stream_name Utf8 NOT NULL,
                partition Uint32 NOT NULL,
                offset Uint64 NOT NULL,
                index Uint32 NOT NULL,
                level Int32,
                message Utf8,
                json_payload JsonDocument,
                ingested_at Timestamp,
                saved_at Timestamp,
                request_id Utf8,
                PRIMARY KEY (timestamp, resource_type, resource_id, stream_name, partition, offset, index)
            ) PARTITION BY HASH (
                timestamp, partition, offset, index
            ) WITH (
                STORE = COLUMN,
                PARTITION_COUNT = 1
            );

            ALTER OBJECT `{self.test_table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS,
                `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`{{"levels": [
                {{"class_name": "Zero", "portions_count_limit": 20000000, "expected_blobs_size": 20971520, "portions_live_duration": "1s"}},
                {{"class_name": "Zero", "portions_count_limit": 20000000, "expected_blobs_size": 20971520}},
                {{"class_name": "Zero", "portions_count_limit": 20000000, "expected_blobs_size": 20971520}}
            ]}}`);
            """
        )

    def _upload_data(self):
        command: list[str] = [
            self.ydb_path,
            "-e",
            self.endpoint,
            "-d",
            self.database,
            "import",
            "file",
            "csv",
            "--header",
            "--null-value",
            "",
            "--delimiter",
            "|",
            "--threads=20",
            "--batch-bytes=1MiB",
            "--path",
            self.test_table_path,
            self._full_data_csv_path()
        ]

        yatest.common.execute(command=command, wait=True, timeout=120)

    def test_select_chunked_data(self):
        self.test_table_path = f"{self._get_test_dir()}/chunks_test"
        self._create_test_table()
        self._upload_data()

        logger.info(
            self.ydb_client.query(f"""SELECT COUNT(*) FROM `{self.test_table_path}`;""")[0].rows
        )

        compacted = False
        for _ in range(10):
            if self.ydb_client.query(f"""SELECT COUNT(*) AS rows FROM `{self.test_table_path}/.sys/primary_index_stats`
                                         WHERE ChunkIdx != 0;""")[0].rows[0]["rows"] == 0 or \
               self.ydb_client.query(f"""SELECT COUNT(*) AS rows FROM `{self.test_table_path}/.sys/primary_index_stats`
                                         WHERE Kind != 'SPLIT_COMPACTED';""")[0].rows[0]["rows"] != 0:

                logger.info(
                    self.ydb_client.query(f"""SELECT * FROM `{self.test_table_path}/.sys/primary_index_stats`""")[0].rows
                )

                time.sleep(1)
            else:
                compacted = True
                break

        logger.info(
            self.ydb_client.query(f"""SELECT * FROM `{self.test_table_path}/.sys/primary_index_stats`""")[0].rows
        )

        assert compacted, "Table is not compacted"

        assert self.ydb_client.query(f"""SELECT Rows FROM `{self.test_table_path}/.sys/primary_index_stats`
                                         WHERE EntityName = 'json_payload' AND ChunkIdx = 0;""")[0].rows[0]["Rows"] == 13281

        res1 = self.ydb_client.query(f"""SELECT timestamp, message FROM `{self.test_table_path}`
                                         WHERE timestamp > Timestamp("2025-10-08T11:00:02.182598Z")
                                         ORDER BY timestamp ASC
                                         LIMIT 20""")[0]
        res2 = self.ydb_client.query(f"""SELECT timestamp, message, json_payload FROM `{self.test_table_path}`
                                         WHERE timestamp > Timestamp("2025-10-08T11:00:02.182598Z")
                                         ORDER BY timestamp ASC
                                         LIMIT 20""")[0]
        logger.info(
            res1.rows
        )
        logger.info(
            res2.rows
        )
        rows_count = len(res1.rows)
        assert rows_count == len(res2.rows)
        for i in range(rows_count):
            assert res1.rows[i]["timestamp"] == res2.rows[i]["timestamp"] and res1.rows[i]["message"] == res2.rows[i]["message"]
