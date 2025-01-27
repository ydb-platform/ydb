import yatest.common
import os
import time
import logging
import boto3
import requests
from library.recipes import common as recipes_common
from ydb.tests.olap.helpers.ydb_client import YdbClient

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self, endpoint, region, key_id, key_secret):
        self.endpoint = endpoint
        self.region = region
        self.key_id = key_id
        self.key_secret = key_secret

        session = boto3.session.Session()
        self.s3 = session.resource(
            service_name="s3",
            aws_access_key_id=key_id,
            aws_secret_access_key=key_secret,
            region_name=region,
            endpoint_url=endpoint
        )
        self.client = session.client(
            service_name="s3",
            aws_access_key_id=key_id,
            aws_secret_access_key=key_secret,
            region_name=region,
            endpoint_url=endpoint
        )

    def create_bucket(self, name: str):
        self.client.create_bucket(Bucket=name)

    def get_bucket_stat(self, bucket_name: str) -> (int, int):
        bucket = self.s3.Bucket(bucket_name)
        count = 0
        size = 0
        for obj in bucket.objects.all():
            count += 1
            size += obj.size
        return (count, size)


class ColumnTableHelper:
    def __init__(self, ydb_client: YdbClient, path: str):
        self.ydb_client = ydb_client
        self.path = path

    def get_row_count(self) -> int:
        return self.ydb_client.query(f"select count(*) as Rows from `{self.path}`")[0].rows[0]["Rows"]

    def get_portion_count(self) -> int:
        return self.ydb_client.query(f"select count(*) as Rows from `{self.path}/.sys/primary_index_portion_stats`")[0].rows[0]["Rows"]

    def get_portion_stat_by_tier(self) -> dict[str, dict[str, int]]:
        results = self.ydb_client.query(f"select TierName, sum(Rows) as Rows, count(*) as Portions from `{self.path}/.sys/primary_index_portion_stats` group by TierName")
        return {row["TierName"]: {"Rows": row["Rows"], "Portions": row["Portions"]} for result_set in results for row in result_set.rows}

    def get_blob_stat_by_tier(self) -> dict[str, (int, int)]:
        stmt = f"""
            select TierName, count(*) as Portions, sum(BlobSize) as BlobSize, sum(BlobCount) as BlobCount from (
                select TabletId, PortionId, TierName, sum(BlobRangeSize) as BlobSize, count(*) as BlobCount from `{self.path}/.sys/primary_index_stats` group by TabletId, PortionId, TierName
            ) group by TierName
        """
        results = self.ydb_client.query(stmt)
        return {row["TierName"]: {"Portions": row["Portions"], "BlobSize": row["BlobSize"], "BlobCount": row["BlobCount"]} for result_set in results for row in result_set.rows}


class TllTieringTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()
        cls._setup_s3()

    @classmethod
    def teardown_class(cls):
        recipes_common.stop_daemon(cls.s3_pid)
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_tiering_in_column_shard": True
            },
            column_shard_config={
                "lag_for_compaction_before_tierings_ms": 0,
                "compaction_actualization_lag_ms": 0,
                "optimizer_freshness_check_duration_ms": 0,
                "small_portion_detect_size_limit": 0,
                "max_read_staleness_ms": 5000,
                "alter_object_enabled": True,
            },
            additional_log_configs={
                "TX_COLUMNSHARD_TIERING": LogLevels.DEBUG,
                "TX_COLUMNSHARD_ACTUALIZATION": LogLevels.TRACE,
                "TX_COLUMNSHARD_BLOBS_TIER": LogLevels.DEBUG,
            },
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    @classmethod
    def _setup_s3(cls):
        s3_pid_file = "s3.pid"
        moto_server_path = os.environ["MOTO_SERVER_PATH"]

        port_manager = yatest.common.network.PortManager()
        port = port_manager.get_port()
        endpoint = f"http://localhost:{port}"
        command = [yatest.common.binary_path(moto_server_path), "s3", "--port", str(port)]

        def is_s3_ready():
            try:
                response = requests.get(endpoint)
                response.raise_for_status()
                return True
            except requests.RequestException as err:
                logging.debug(err)
                return False

        recipes_common.start_daemon(
            command=command, environment=None, is_alive_check=is_s3_ready, pid_file_name=s3_pid_file
        )

        with open(s3_pid_file, 'r') as f:
            cls.s3_pid = int(f.read())

        cls.s3_client = S3Client(endpoint, "us-east-1", "fake_key_id", "fake_key_secret")

    @staticmethod
    def wait_for(condition_func, timeout_seconds):
        t0 = time.time()
        while time.time() - t0 < timeout_seconds:
            if condition_func():
                return True
            time.sleep(1)
        return False
