# -*- coding: utf-8 -*-
import boto3

import os

import yatest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb


class TestCompatibility(object):
    @classmethod
    def setup_class(cls):
        last_stable_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-last-stable")
        binary_paths = [kikimr_driver_path(), last_stable_path]
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC, binary_paths=binary_paths))
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (
            cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
        )
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=cls.endpoint
            )
        )
        cls.driver.wait()
        output_path = yatest.common.test_output_path()
        cls.output_f = open(os.path.join(output_path, "out.log"), "w")

        cls.s3_config = cls.setup_s3()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop(kill=True)  # TODO fix

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

    def test_simple(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    "create table `sample_table` (id Uint64, value Uint64, payload Utf8, PRIMARY KEY(id)) WITH (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 1);"
                )
                id_ = 0

                upsert_count = 200
                iteration_count = 1
                for i in range(iteration_count):
                    rows = []
                    for j in range(upsert_count):
                        row = {}
                        row["id"] = id_
                        row["value"] = 1
                        row["payload"] = "DEADBEEF" * 1024 * 16  # 128 kb
                        rows.append(row)
                        id_ += 1

                    column_types = ydb.BulkUpsertColumns()
                    column_types.add_column("id", ydb.PrimitiveType.Uint64)
                    column_types.add_column("value", ydb.PrimitiveType.Uint64)
                    column_types.add_column("payload", ydb.PrimitiveType.Utf8)
                    self.driver.table_client.bulk_upsert(
                        "Root/sample_table", rows, column_types
                    )

                query = "SELECT SUM(value) from sample_table"
                result_sets = session.transaction().execute(
                    query, commit_tx=True
                )
                for row in result_sets[0].rows:
                    print(" ".join([str(x) for x in list(row.values())]))

                assert len(result_sets) == 1
                assert len(result_sets[0].rows) == 1
                result = list(result_sets[0].rows[0].values())
                assert len(result) == 1
                assert result[0] == upsert_count * iteration_count

    def test_export(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config

        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

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
            "src=/Root,dst=."
        ]

        yatest.common.execute(export_command, wait=True, stdout=self.output_f, stderr=self.output_f)

        s3_resource = boto3.resource("s3", endpoint_url=s3_endpoint, 
                                   aws_access_key_id=s3_access_key,
                                   aws_secret_access_key=s3_secret_key)

        keys_expected = []
        for table_num in range(1, 6):
            table_name = f"sample_table_{table_num}"
            keys_expected.append(table_name + "/data_00.csv")
            keys_expected.append(table_name + "/metadata.json")
            keys_expected.append(table_name + "/scheme.pb")
        
        def check_export_progress():
            bucket = s3_resource.Bucket(s3_bucket)
            keys = [x.key for x in list(bucket.objects.all())]
            keys.sort()
            return keys == keys_expected
        
        yatest.common.wait_for(check_function=check_export_progress, timeout=600)
