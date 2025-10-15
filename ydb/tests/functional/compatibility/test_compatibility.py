# -*- coding: utf-8 -*-
import boto3
import tempfile
import time
import pytest
import logging
import yatest
import os
import json
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb

from decimal import Decimal


ydbd_25_1 = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-25-1")
ydbd_24_4 = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-24-4")
current_binary_path = kikimr_driver_path()

all_binary_combinations = [
    [ydbd_25_1, current_binary_path],
    [ydbd_25_1, [ydbd_25_1, current_binary_path]],
    [current_binary_path, ydbd_25_1],
    [current_binary_path, current_binary_path],
    [ydbd_24_4, current_binary_path],
    [ydbd_24_4, [ydbd_24_4, current_binary_path]],
    [current_binary_path, ydbd_24_4],
]
all_binary_combinations_ids = [
    "stable_25_1_to_current",
    "stable_25_1_to_current_mixed",
    "current_to_stable_25_1",
    "current_to_current",
    "stable_24_4_to_current",
    "stable_24_4_to_current_mixed",
    "current_to_stable_24_4",
]

logger = logging.getLogger(__name__)


class TestCompatibility(object):
    @pytest.fixture(autouse=True, params=all_binary_combinations, ids=all_binary_combinations_ids)
    def setup(self, request):
        self.all_binary_paths = request.param
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=[self.all_binary_paths[0]],
            use_in_memory_pdisks=False,

            extra_feature_flags={
                "suppress_compatibility_check": True,
                # "enable_table_datetime64": True # uncomment for 64 datetime in tpc-h/tpc-ds
                },
            column_shard_config={
                'disabled_on_scheme_shard': False,
            },
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")
        self.s3_config = self.setup_s3()

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        self.driver.wait()
        yield
        self.cluster.stop()

    @staticmethod
    def setup_s3():
        s3_endpoint = os.getenv("S3_ENDPOINT")
        s3_access_key = "minio"
        s3_secret_key = "minio123"
        s3_bucket = "export_test_bucket"

        resource = boto3.resource("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

        bucket = resource.Bucket(s3_bucket)
        bucket.create()
        bucket.objects.all().delete()

        return s3_endpoint, s3_access_key, s3_secret_key, s3_bucket

    def _execute_command_and_get_result(self, command):
        with tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp_file:
            yatest.common.execute(command, wait=True, stdout=temp_file, stderr=temp_file)
            temp_file.flush()
            temp_file.seek(0)
            result = json.load(temp_file)
            self.output_f.write(str(result) + "\n")
            return result

    def change_cluster_version(self, new_binary_paths):
        binary_path_before = self.config.get_binary_paths()
        versions_on_before = self.get_nodes_version()
        if isinstance(new_binary_paths, str):
            new_binary_paths = [new_binary_paths]
        elif not isinstance(new_binary_paths, list):
            raise ValueError("binary_paths must be a string or a list of strings")
        self.config.set_binary_paths(new_binary_paths)
        self.cluster.update_nodes_configurator(self.config)
        time.sleep(60)
        versions_on_after = self.get_nodes_version()
        if binary_path_before != new_binary_paths:
            assert versions_on_before != versions_on_after, f'Versions on before and after should be different: {versions_on_before} {versions_on_after}'
        else:
            assert versions_on_before == versions_on_after, f'Versions on before and after should be the same: {versions_on_before} {versions_on_after}'

    def get_nodes_version(self):
        versions = []
        for node_id, node in enumerate(self.cluster.nodes.values()):
            node.get_config_version()
            get_version_command = [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "--endpoint",
                "grpc://localhost:%d" % node.grpc_port,
                "--database=/Root",
                "yql",
                "--script",
                f'select version() as node_{node_id}_version',
                '--format',
                'json-unicode'
            ]
            result = yatest.common.execute(get_version_command, wait=True)
            result_data = json.loads(result.std_out.decode('utf-8'))
            logger.debug(f'node_{node_id}_version": {result_data}')
            node_version_key = f"node_{node_id}_version"
            if node_version_key in result_data:
                node_version = result_data[node_version_key]
                versions.append(node_version)
            else:
                print(f"Key {node_version_key} not found in the result.")
        return versions

    def check_table_exists(driver, table_path):
        try:
            driver.scheme_client.describe_table(table_path)
            return True
        except ydb.SchemeError as e:
            if e.issue_code == ydb.IssueCode.SCHEME_ERROR_NO_SUCH_TABLE:
                return False
            else:
                raise

    def exec_query(self, query: str):
        command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "-e",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "-d",
            "/Root",
            "yql",
            "--script",
            f"{query}"
        ]
        yatest.common.execute(command, wait=True, stdout=self.output_f)

    def execute_scan_query(self, query_body):
        query = ydb.ScanQuery(query_body, {})
        it = self.driver.table_client.scan_query(query)
        result_set = []

        try:
            while True:
                result = next(it)
                result_set.extend(result.result_set.rows)
        except StopIteration:
            pass

        return result_set

    def log_database_scheme(self):
        get_scheme_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "-e",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "-d",
            "/Root",
            "scheme",
            "ls",
            "-l",
            "-R"
        ]
        yatest.common.execute(get_scheme_command, wait=True, stdout=self.output_f)

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_simple(self, store_type):
        def read_update_data(self, iteration_count=1, start_index=0):
            id_ = start_index

            upsert_count = 200
            iteration_count = iteration_count
            for i in range(iteration_count):
                rows = []
                for j in range(upsert_count):
                    row = {}
                    row["id"] = id_
                    row["value"] = 1
                    row["payload"] = "DEADBEEF" * 1024 * 16  # 128 kb
                    row["income"] = Decimal("123.001").quantize(Decimal('0.000000000'))

                    rows.append(row)
                    id_ += 1

                column_types = ydb.BulkUpsertColumns()
                column_types.add_column("id", ydb.PrimitiveType.Uint64)
                column_types.add_column("value", ydb.PrimitiveType.Uint64)
                column_types.add_column("payload", ydb.PrimitiveType.Utf8)
                column_types.add_column("income", ydb.DecimalType())
                self.driver.table_client.bulk_upsert(
                    "Root/sample_table", rows, column_types
                )

            query_body = "SELECT SUM(value) as sum_value from `sample_table`"
            query = ydb.ScanQuery(query_body, {})
            it = self.driver.table_client.scan_query(query)
            result_set = []

            while True:
                try:
                    result = next(it)
                    result_set = result_set + result.result_set.rows
                except StopIteration:
                    break

            for row in result_set:
                print(" ".join([str(x) for x in list(row.values())]))

            assert len(result_set) == 1
            assert len(result_set[0]) == 1
            result = list(result_set)
            assert len(result) == 1
            assert result[0]['sum_value'] == upsert_count * iteration_count + start_index

        def create_table_column(self):
            with ydb.SessionPool(self.driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(
                        """create table `sample_table` (
                            id Uint64 NOT NULL, value Uint64,
                            payload Utf8, income Decimal(22,9),
                            PRIMARY KEY(id)
                            ) WITH (
                            STORE = COLUMN,
                            AUTO_PARTITIONING_BY_SIZE = ENABLED,
                            AUTO_PARTITIONING_PARTITION_SIZE_MB = 1);"""
                    )

        def create_table_row(self):
            with ydb.SessionPool(self.driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(
                        """create table `sample_table` (
                            id Uint64, value Uint64,
                            payload Utf8, income Decimal(22,9),
                            PRIMARY KEY(id)
                            ) WITH (
                            AUTO_PARTITIONING_BY_SIZE = ENABLED,
                            AUTO_PARTITIONING_PARTITION_SIZE_MB = 1);"""
                    )

        create_table_row(self) if store_type == "row" else create_table_column(self)
        read_update_data(self)
        self.change_cluster_version(self.all_binary_paths[1])
        assert self.execute_scan_query('select count(*) as row_count from `sample_table`')[0]['row_count'] == 200, 'Expected 200 rows after update version'
        read_update_data(self, iteration_count=2, start_index=100)
        assert self.execute_scan_query('select count(*) as row_count from `sample_table`')[0]['row_count'] == 500, 'Expected 500 rows: update 100-200 rows and added 300 rows'

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test_tpch1(self, store_type):
        result_json_path = os.path.join(yatest.common.test_output_path(), "result.json")
        query_output_path = os.path.join(yatest.common.test_output_path(), "query_output.json")
        init_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "init",
            "--store={}".format(store_type),
            "--datetime",  # use 32 bit dates instead of 64 (not supported in 24-4)
            # "--partition-size=25",  # not supported in stable yet
        ]
        import_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "import",
            "generator",
            "--scale=1",
        ]
        run_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "run",
            "--scale=1",
            "--check-canonical",
            # "--retries", # not supported in stable yet
            # "5",  # in row tables we have to retry query by design
            "--json",
            result_json_path,
            "--output",
            query_output_path,
        ]
        if store_type == "row":
            run_command.append("--exclude=12,17")

        clean_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "workload",
            "tpch",
            "-p",
            "tpch",
            "clean"
        ]

        yatest.common.execute(init_command, wait=True, stdout=self.output_f)
        yatest.common.execute(import_command, wait=True, stdout=self.output_f)
        yatest.common.execute(run_command, wait=True, stdout=self.output_f)
        self.change_cluster_version(self.all_binary_paths[1])
        yatest.common.execute(run_command, wait=True, stdout=self.output_f)
        yatest.common.execute(clean_command, wait=True, stdout=self.output_f)

    def test_export(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config

        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                for table_num in range(1, 6):
                    table_name = f"sample_table_{table_num}"
                    session.execute_scheme(
                        f"create table `{table_name}` (id Uint64, payload Utf8, PRIMARY KEY(id));"
                    )

                    query = f"""INSERT INTO `{table_name}` (id, payload) VALUES
                        (1, 'Payload 1 for table {table_num}'),
                        (2, 'Payload 2 for table {table_num}'),
                        (3, 'Payload 3 for table {table_num}'),
                        (4, 'Payload 4 for table {table_num}'),
                        (5, 'Payload 5 for table {table_num}');"""
                    session.transaction().execute(
                        query, commit_tx=True
                    )

        export_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "export",
            "s3",
            "--s3-endpoint",
            s3_endpoint,
            "--bucket",
            s3_bucket,
            "--access-key",
            s3_access_key,
            "--secret-key",
            s3_secret_key,
            "--item",
            "src=/Root,dst=.",
            "--format",
            "proto-json-base64"
        ]

        result_export = self._execute_command_and_get_result(export_command)

        export_id = result_export["id"]
        status_export = result_export["status"]
        progress_export = result_export["metadata"]["progress"]

        assert status_export == "SUCCESS"
        assert progress_export in ["PROGRESS_PREPARING", "PROGRESS_DONE"]

        operation_get_command = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "operation",
            "get",
            "%s" % export_id,
            "--format",
            "proto-json-base64"
        ]

        while progress_export != "PROGRESS_DONE":
            result_get = self._execute_command_and_get_result(operation_get_command)
            progress_export = result_get["metadata"]["progress"]

        s3_resource = boto3.resource("s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

        keys_expected = set()
        for table_num in range(1, 6):
            table_name = f"sample_table_{table_num}"
            keys_expected.add(table_name + "/data_00.csv")
            keys_expected.add(table_name + "/metadata.json")
            keys_expected.add(table_name + "/scheme.pb")

        bucket = s3_resource.Bucket(s3_bucket)
        keys = set()
        for x in list(bucket.objects.all()):
            keys.add(x.key)

        assert keys_expected <= keys
