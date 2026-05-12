import os
import pytest
import time
import tempfile

import logging
import yatest.common
import ydb
import random
import requests

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class YdbWorkloadOverload:
    def __init__(
        self,
        endpoint: str,
        database: str,
        table_name: str,
        stderr: str = None,
        stdout: str = None,
    ):
        self.path: str = yatest.common.binary_path(os.environ["YDB_CLI_BINARY"])
        self.endpoint: str = endpoint
        self.database: str = database
        self.begin_command: list[str] = [
            self.path,
            "-e",
            self.endpoint,
            "-d",
            self.database,
            "workload",
            "log",
            "--path",
            table_name,
        ]
        self.stderr = stderr
        self.stdout = stdout
        self.table_name = table_name

    def _call(self, command: list[str], wait=False, timeout=None):
        logging.info(f'YdbWorkloadOverload execute {" ".join(command)} with wait = {wait}')
        yatest.common.execute(command=command, wait=wait, timeout=timeout, stderr=self.stderr, stdout=self.stdout)

    def create_table(self):
        command = self.begin_command + ["init", "--path", self.table_name, "--store", "column", "--ttl", "1000"]
        self._call(command=command, wait=True)

    def _insert_rows(self, operation_name: str, seconds: int, threads: int, rows: int, wait: bool):
        logging.info(f'YdbWorkloadOverload {operation_name}')
        command = self.begin_command + [
            "run",
            str(operation_name),
            "--seconds",
            str(seconds),
            "--threads",
            str(threads),
            "--rows",
            str(rows),
            "--quiet"
        ]

        self._call(command=command, wait=wait, timeout=5*seconds)

    # seconds - Seconds to run workload
    # threads - Number of parallel threads in workload
    # rows - Number of rows to upsert
    def bulk_upsert(self, seconds: int, threads: int, rows: int, wait: bool = False):
        self._insert_rows(operation_name="bulk-upsert", seconds=seconds, threads=threads, rows=rows, wait=wait)

    def __del__(self):
        command: list[str] = self.begin_command + ["clean"]
        try:
            yatest.common.execute(command=command, wait=True)
        except Exception:
            pass


class TestLogScenario(object):
    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls, writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_immediate_writing_on_bulk_upsert": True,
                                 "enable_cs_overloads_subscription_retries": True},
            column_shard_config={"alter_object_enabled": True,
                                 "writing_in_flight_requests_count_limit": writing_in_flight_requests_count_limit,
                                 "writing_in_flight_request_bytes_limit": writing_in_flight_request_bytes_limit},
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(endpoint=f"grpc://{node.host}:{node.port}", database=f"/{config.domain_name}")
        cls.ydb_client.wait_connection(timeout=60)

    @classmethod
    def _setup_ydb_rp(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_olap_reject_probability": True},
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(endpoint=f"grpc://{node.host}:{node.port}", database=f"/{config.domain_name}")
        cls.ydb_client.wait_connection(timeout=60)
        cls.mon_url = f"http://{node.host}:{node.mon_port}"

    def get_row_count(self) -> int:
        return self.ydb_client.query(f"select count(*) as Rows from `{self.table_name}`")[0].rows[0]["Rows"]

    @pytest.mark.parametrize('writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit, partitions_count',
                             [(1, 10000, 1), (2, 10000, 1), (1000, 1, 1), (1000, 2, 1), (1, 1, 1), (2, 2, 1),
                              (1, 10000, 2), (2, 10000, 2), (1000, 1, 2), (1000, 2, 2), (1, 1, 2), (2, 2, 2),
                              (1, 10000, 8), (2, 10000, 8), (1000, 1, 8), (1000, 2, 8), (1, 1, 8), (2, 2, 8),
                              (1, 10000, 64), (2, 10000, 64), (1000, 1, 64), (1000, 2, 64), (1, 1, 64), (2, 2, 64),
                              (1, 10000, 128), (2, 10000, 128), (1000, 1, 128), (1000, 2, 128), (1, 1, 128), (2, 2, 128)])
    def test_overloads_bulk_upsert(self, writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit, partitions_count):
        test_name = f"test_overloads_bulk_upsert_{writing_in_flight_requests_count_limit}_{writing_in_flight_request_bytes_limit}_{partitions_count}"
        self._setup_ydb(writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit)

        test_dir = f"{self.ydb_client.database}/{test_name}"
        table_path = f"{test_dir}/table"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                val Uint64,
                PRIMARY KEY(id)
            )
            WITH (
                STORE = COLUMN,
                PARTITION_COUNT = {partitions_count}
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("val", ydb.PrimitiveType.Uint64)

        rows_count = 10

        data = [
            {
                "id": i,
                "val": i * rows_count,
            }
            for i in range(rows_count)
        ]

        max_retries = 10
        retry_delay = 0.5
        for attempt in range(max_retries):
            try:
                self.ydb_client.bulk_upsert(
                    table_path,
                    column_types,
                    data,
                )

                break
            except ydb.issues.Overloaded:
                if attempt == max_retries - 1:
                    raise

                time.sleep(retry_delay)
                retry_delay *= 1.5

        assert self.ydb_client.query(f"select count(*) as Rows from `{table_path}`")[0].rows[0]["Rows"] == rows_count

    @pytest.mark.parametrize('writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit', [(1, 10000), (2, 10000), (1000, 1), (1000, 2), (1, 1), (2, 2)])
    def test_overloads_workload(self, writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit):
        self._setup_ydb(writing_in_flight_requests_count_limit, writing_in_flight_request_bytes_limit)

        wait_time: int = 60
        self.table_name: str = f"log_{writing_in_flight_requests_count_limit}_{writing_in_flight_request_bytes_limit}"

        output_path = yatest.common.test_output_path()
        stdout_path = os.path.join(output_path, "command_stdout.log")

        with open(stdout_path, "w") as output_stdout:
            ydb_workload: YdbWorkloadOverload = YdbWorkloadOverload(
                endpoint=self.ydb_client.endpoint,
                database=self.ydb_client.database,
                table_name=self.table_name,
                stdout=output_stdout
            )
            ydb_workload.create_table()

            ydb_workload.bulk_upsert(seconds=wait_time, threads=10, rows=10, wait=True)

        keys = None
        values = None
        with open(stdout_path, "r") as file:
            for line in file:
                if line.startswith("Total"):
                    keys = line.split()
                elif keys is not None:
                    values = line.split()
                    break

        assert keys is not None and values is not None

        stats = dict(zip(keys, values))

        assert stats["Errors"] == "0"

        logging.info(f"Count rows after insert {self.get_row_count()}")
        assert self.get_row_count() != 0

    def tune_icb(self):
        response = requests.post(
            self.mon_url + "/actors/icb",
            data="TabletControls.MaxTxInFly=0"
        )
        response.raise_for_status()

    def test_overloads_reject_probability(self):
        self._setup_ydb_rp()
        self.tune_icb()

        table_path = f"{self.ydb_client.database}/table_for_test_overloads_reject_probability"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                v1 Int64,
                v2 Int64,
                PRIMARY KEY(id)
            )
            WITH (
                STORE = COLUMN,
                PARTITION_COUNT = 1,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("v1", ydb.PrimitiveType.Int64)
        column_types.add_column("v2", ydb.PrimitiveType.Int64)

        rows_count = 1000

        data = [
            {
                "id": i,
                "v1": 1,
                "v2": -1,
            }
            for i in range(rows_count)
        ]

        max_retries = 10
        retry_delay = 0.5
        for attempt in range(max_retries):
            try:
                self.ydb_client.bulk_upsert(table_path, column_types, data)
                break
            except ydb.issues.Overloaded:
                if attempt == max_retries - 1:
                    raise

                time.sleep(retry_delay)
                retry_delay *= 1.5

        futures = []

        for _ in range(19):
            lb = random.randint(0, rows_count)
            futures.append(self.ydb_client.query_async(f"UPDATE `{table_path}` SET v1 = v1 + 1, v2 = v2 - 1 WHERE id > {lb};"))

        for future in futures:
            future.result()

        monitor = self.cluster.monitors[0].fetch()
        _, rejectProbabilityCount = monitor.get_by_name('Deriviative/Overload/RejectProbability/Count')[0]

        assert rejectProbabilityCount > 0


class TestSqlExportImportFormats(object):

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_immediate_writing_on_bulk_upsert": True,
                "enable_cs_overloads_subscription_retries": True,
                "enable_columnshard_bool": True,
            },
            column_shard_config={
                "alter_object_enabled": True,
                "writing_in_flight_requests_count_limit": 1000,
                "writing_in_flight_request_bytes_limit": 10000,
            },
        )

        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(endpoint=f"grpc://{node.host}:{node.port}", database=f"/{config.domain_name}")
        cls.ydb_client.wait_connection(timeout=60)

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    def _ydb_cli_base_cmd(self):
        return [
            yatest.common.binary_path(os.environ["YDB_CLI_BINARY"]),
            "-e",
            self.ydb_client.endpoint,
            "-d",
            self.ydb_client.database,
        ]

    def _create_table(self, path: str):
        self.ydb_client.query(
            f"""
            CREATE TABLE `{path}` (
                id Uint64 NOT NULL,
                name Utf8,
                val Int32,
                flag Bool,
                PRIMARY KEY (id)
            )
            WITH (
                STORE = COLUMN,
                PARTITION_COUNT = 1
            )
            """
        )

    def _drop_table(self, path: str):
        self.ydb_client.query(f"DROP TABLE `{path}`")

    def _fetch_ordered_rows(self, path: str):
        rs = self.ydb_client.query(
            f"SELECT id, name, val, flag FROM `{path}` ORDER BY id"
        )[0]
        return [dict(r) for r in rs.rows]

    @pytest.mark.parametrize("export_kind", ["csv", "json"])
    def test_sql_export_then_import_roundtrip(self, export_kind):
        base = f"{self.ydb_client.database}/sql_export_import_{export_kind}"
        source = f"{base}/source"
        dest = f"{base}/dest"

        self._create_table(source)

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("name", ydb.PrimitiveType.Utf8)
        column_types.add_column("val", ydb.PrimitiveType.Int32)
        column_types.add_column("flag", ydb.PrimitiveType.Bool)

        expected_rows = [
            {"id": 1, "name": "a", "val": 10, "flag": True},
            {"id": 2, "name": "b", "val": 20, "flag": False},
            {"id": 3, "name": "c", "val": 30, "flag": True},
        ]

        self.ydb_client.bulk_upsert(source, column_types, expected_rows)

        assert len(self._fetch_ordered_rows(source)) == len(expected_rows)

        select_sql = f"SELECT `id`, `name`, `val`, `flag` FROM `{source}` ORDER BY `id`"
        fmt = "csv" if export_kind == "csv" else "json-unicode"
        export_cmd = self._ydb_cli_base_cmd() + ["sql", "-s", select_sql, "--format", fmt]
        export_res = yatest.common.execute(export_cmd, wait=True)
        assert export_res.returncode == 0, export_res.stderr.decode("utf-8")
        payload = export_res.stdout.decode("utf-8")
        assert payload.strip(), f"empty export for format {export_kind}"

        suffix = ".csv" if export_kind == "csv" else ".json"
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False, encoding="utf-8") as tmp:
            tmp.write(payload)
            export_path = tmp.name

        try:
            self._create_table(dest)
            if export_kind == "csv":
                import_cmd = self._ydb_cli_base_cmd() + [
                    "import",
                    "file",
                    "csv",
                    "-p",
                    dest,
                    export_path,
                ]
            else:
                import_cmd = self._ydb_cli_base_cmd() + [
                    "import",
                    "file",
                    "json",
                    "-p",
                    dest,
                    export_path,
                ]

            import_res = yatest.common.execute(import_cmd, wait=True)
            assert import_res.returncode == 0, import_res.stderr.decode("utf-8")

            imported = self._fetch_ordered_rows(dest)
            assert len(imported) == len(expected_rows)
            for got, exp in zip(imported, expected_rows):
                assert got["id"] == exp["id"]
                assert got["name"] == exp["name"]
                assert got["val"] == exp["val"]
                assert got["flag"] == exp["flag"]
        finally:
            try:
                os.unlink(export_path)
            except OSError:
                pass
            self._drop_table(dest)
            self._drop_table(source)
