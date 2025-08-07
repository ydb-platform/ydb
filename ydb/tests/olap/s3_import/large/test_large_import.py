import boto3
import datetime
import logging
import os
import traceback
import time
import ydb
from typing import Optional

from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cluster import YdbCluster

logger = logging.getLogger("TestLargeS3Import")


class TestLargeS3Import:
    class Results:
        def __init__(self, suite: str):
            self.suite = suite
            self.stats = dict()
            self.start_time: Optional[float] = None
            self.test_name: Optional[str] = None
            self.stage_starts: dict[str, float] = dict()
            self.finished = False

        def __report_results(self, is_successful: bool = False):
            assert self.start_time is not None, "Results is not setupped"
            assert not self.finished, "Results is already reported"
            logger.info(f"reporting result stats:\n{self.stats}\nis_successful: {is_successful}")
            ResultsProcessor.upload_results(
                kind="Import/Export",
                suite=self.suite,
                test=self.test_name,
                timestamp=self.start_time,
                is_successful=is_successful,
                statistics=self.stats,
                duration=(time.time() - self.start_time) / 1000000
            )

        def setup(self, test_name: str):
            self.start_time = time.time()
            self.test_name = test_name
            self.stats = {
                "stage": "setup",
                "stage_duration_seconds": dict()
            }
            self.stage_starts = dict()
            self.finished = False
            self.__report_results()

        def on_stage_start(self, stage: str):
            self.stats["stage"] = f"{stage}-RUNNING"
            self.stage_starts[stage] = time.time()
            self.__report_results()

        def on_stage_finish(self, stage: str):
            self.stats["stage"] = f"{stage}-FINISHED"
            self.stats["stage_duration_seconds"][stage] = time.time() - self.stage_starts[stage]
            self.__report_results()

        def report_finish(self):
            if not self.finished:
                self.__report_results(is_successful=True)
                self.finished = True

        def report_fail(self, error: str):
            if not self.finished:
                self.stats["error"] = error
                self.__report_results()
                self.finished = True

    class ReportTime:
        def __init__(self, results, stage: str):
            self.results = results
            self.stage = stage

        def __enter__(self):
            logger.info(f"starting {self.stage}...")
            self.results.on_stage_start(self.stage)

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                error = f"exception[{exc_type}]: {exc_val}, traceback:\n{traceback.format_exc()}"
                logger.error(f"{self.stage} failed, {error}")
                self.results.report_fail(error)
                return

            logger.info(f"{self.stage} finished")
            self.results.on_stage_finish(self.stage)

    @classmethod
    def setup_class(cls):
        cls.sink_access_key_id = os.getenv("S3_ACCESS_KEY_ID", None)
        assert cls.sink_access_key_id is not None, "S3_ACCESS_KEY_ID is not set for sink bucket"

        cls.sink_access_key_secret = os.getenv("S3_ACCESS_KEY_SECRET", None)
        assert cls.sink_access_key_secret is not None, "S3_ACCESS_KEY_SECRET is not set for sink bucket"

        cls.scale = get_external_param("scale", "1000")
        assert cls.scale in ["1", "10", "100", "1000"], f"Invalid scale: {cls.scale}"

        cls.s3_url = "https://storage.yandexcloud.net"
        cls.s3_sink_bucket = "olap-exp-private"
        cls.external_source_path = YdbCluster.get_tables_path("tpc_h_s3_parquet_import")
        cls.external_sink_path = YdbCluster.get_tables_path("tpc_h_s3_parquet_export")
        cls.external_table_path = YdbCluster.get_tables_path(f"s{cls.scale}/tpc_h_lineitem_s3_parquet_import")
        cls.external_sink_table_path = YdbCluster.get_tables_path(f"s{cls.scale}/tpc_h_lineitem_s3_parquet_export")
        cls.olap_table_path = YdbCluster.get_tables_path(f"s{cls.scale}/tpc_h_lineitem_olap")
        cls.table_size = {
            "1": 6001215,
            "10": 59986052,
            "100": 600037902,
            "1000": 5999989709,
        }[cls.scale]

        logger.info(f"test configuration, scale: {cls.scale}, "
                    f"external source: {cls.external_source_path}, "
                    f"external table: {cls.external_table_path}, "
                    f"olap table: {cls.olap_table_path}, "
                    f"external sink: {cls.external_sink_path}, "
                    f"external sink table: {cls.external_sink_table_path}")
        logger.info(f"target claster info, endpoint: {YdbCluster.ydb_endpoint}, "
                    f"database: {YdbCluster.ydb_database}, "
                    f"tables path: {YdbCluster.get_tables_path()}, "
                    f"has key {'YES' if os.getenv('OLAP_YDB_OAUTH', None) else 'NO'}")
        logger.info(f"results info, send-results: {ResultsProcessor.send_results}, "
                    f"endpoints: {get_external_param('results-endpoint', '-')}, "
                    f"dbs: {get_external_param('results-db', '-')}, "
                    f"tables: {get_external_param('results-table', '-')}, "
                    f"has key {'YES' if os.getenv('RESULT_YDB_OAUTH', None) else 'NO'}")

        YdbCluster._dyn_nodes_count = 0  # Disable node count check
        health_errors, health_warnings = YdbCluster.check_if_ydb_alive()
        logger.info(f"ydb health warnings: {health_warnings}")
        assert health_errors is None, f"ydb is not alive: {health_errors}"

        cls.session_pool = ydb.QuerySessionPool(YdbCluster.get_ydb_driver())
        cls.results = cls.Results(suite="TestLargeS3Import")

    def query(self, statement, log_query=True, max_retries=10):
        if log_query:
            logger.info(f"running query:\n{statement}")

        return self.session_pool.execute_with_retries(
            statement,
            settings=ydb.BaseRequestSettings().with_timeout(21600),
            retry_settings=ydb.RetrySettings(max_retries=max_retries)
        )

    def cleanup_tables(self):
        logger.info(f"cleaning up table `{self.olap_table_path}`...")
        self.query(f"DROP TABLE IF EXISTS `{self.olap_table_path}`;")

    def setup_datasource(self):
        logger.info(f"setupping datasource by path `{YdbCluster.get_tables_path()}/`...")
        self.query(f"""
            CREATE OR REPLACE EXTERNAL DATA SOURCE `{self.external_source_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_url}/tpc/",
                AUTH_METHOD="NONE"
            );

            CREATE OR REPLACE EXTERNAL TABLE `{self.external_table_path}` (
                l_orderkey Int64 NOT NULL,
                l_partkey Int64,
                l_suppkey Int64,
                l_linenumber Int64 NOT NULL,
                l_quantity Double,
                l_extendedprice Double,
                l_discount Double,
                l_tax Double,
                l_returnflag String,
                l_linestatus String,
                l_shipdate Date,
                l_commitdate Date,
                l_receiptdate Date,
                l_shipinstruct String,
                l_shipmode String,
                l_comment String
            ) WITH (
                DATA_SOURCE="{self.external_source_path}",
                LOCATION="/h/s{self.scale}/parquet/lineitem/",
                FORMAT="parquet"
            );
        """)

    def setup_datasink(self, output_path):
        logger.info(f"setupping detasink to `{output_path}`...")

        # Used to avoid secret name clashing than test runs from different user SIDS
        secret_prefix = os.getenv("S3_ACCESS_SECRET_PREFIX", "")

        access_key_id_name = f"{secret_prefix}test_olap_s3_import_aws_access_key_id"
        access_key_secret_name = f"{secret_prefix}test_olap_s3_import_aws_access_key_secret"
        self.query(f"""
            UPSERT OBJECT {access_key_id_name} (TYPE SECRET) WITH (value = "{self.sink_access_key_id}");
            UPSERT OBJECT {access_key_secret_name} (TYPE SECRET) WITH (value = "{self.sink_access_key_secret}");
        """, log_query=False)

        self.query(f"""
            CREATE OR REPLACE EXTERNAL DATA SOURCE `{self.external_sink_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_url}/{self.s3_sink_bucket}/",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_name}",
                AWS_REGION="ru-central-1"
            );

            CREATE OR REPLACE EXTERNAL TABLE `{self.external_sink_table_path}` (
                l_orderkey Int64 NOT NULL,
                l_partkey Int64,
                l_suppkey Int64,
                l_linenumber Int64 NOT NULL,
                l_quantity Double,
                l_extendedprice Double,
                l_discount Double,
                l_tax Double,
                l_returnflag String,
                l_linestatus String,
                l_shipdate Date,
                l_commitdate Date,
                l_receiptdate Date,
                l_shipinstruct String,
                l_shipmode String,
                l_comment String
            ) WITH (
                DATA_SOURCE="{self.external_sink_path}",
                LOCATION="{output_path}",
                FORMAT="parquet"
            );
        """)

    def validate_tables(self, first_table, second_table, stage_name):
        with self.ReportTime(self.results, stage_name):
            logger.info(f"validating tables {first_table} and {second_table}...")
            result_sets = self.query(f"""
                SELECT
                    String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS first_hash,
                    COUNT(*) AS first_size
                FROM `{first_table}`;

                SELECT
                    String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS second_hash,
                    COUNT(*) AS second_size
                FROM `{second_table}`;
            """)

            assert len(result_sets) == 2

            first_result = result_sets[0].rows
            assert len(first_result) == 1
            first_result = first_result[0]

            second_result = result_sets[1].rows
            assert len(second_result) == 1
            second_result = second_result[0]

            assert first_result.first_size == self.table_size
            assert first_result.first_size == second_result.second_size
            assert first_result.first_hash == second_result.second_hash

    def run_import_from_s3(self):
        with self.ReportTime(self.results, "import"):
            self.cleanup_tables()
            self.query(f"""
                CREATE TABLE `{self.olap_table_path}` (
                    PRIMARY KEY (l_orderkey, l_linenumber)
                ) WITH (
                    STORE = COLUMN
                ) AS SELECT * FROM `{self.external_table_path}`;
            """, max_retries=0)

    def run_export_to_s3(self):
        with self.ReportTime(self.results, "export"):
            self.query(f"""
                INSERT INTO `{self.external_sink_table_path}`
                SELECT * FROM `{self.olap_table_path}`;
            """, max_retries=0)

    def clear_bucket(self, output_path):
        logger.info(f"cleaning up s3 by path `{output_path}`...")
        resource = boto3.resource(
            "s3",
            endpoint_url=self.s3_url,
            aws_access_key_id=self.sink_access_key_id,
            aws_secret_access_key=self.sink_access_key_secret
        )
        bucket = resource.Bucket(self.s3_sink_bucket)
        bucket.objects.filter(Prefix=output_path).delete()

    def test_import_and_export(self):
        output_path = f"test_import/s{self.scale}/{datetime.datetime.now()}/"
        self.results.setup(f"test_import_and_export[scale={self.scale}]")

        with self.ReportTime(self.results, "global"):
            self.setup_datasource()
            self.setup_datasink(output_path)

            self.run_import_from_s3()
            self.validate_tables(self.external_table_path, self.olap_table_path, "validate_import")

            self.run_export_to_s3()
            self.validate_tables(self.olap_table_path, self.external_sink_table_path, "validate_export")

            self.cleanup_tables()
            self.clear_bucket(output_path)

        self.results.report_finish()
