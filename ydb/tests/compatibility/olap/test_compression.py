# -*- coding: utf-8 -*-
import logging
import pytest
from ydb.tests.library.common.protobuf_ss import SchemeDescribeRequest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestCompressionRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):

        yield from self.setup_cluster(extra_feature_flags={
            "enable_olap_compression": True,
        })

    def _create_table_with_family_compression(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{name}` (
                    id Uint64 NOT NULL,
                    val Int64 FAMILY fam1,
                    PRIMARY KEY(id),
                    Family default (
                        COMPRESSION = "zstd"
                    ),
                    Family fam1 (
                        COMPRESSION = "zstd"
                    ),
                ) WITH (
                    STORE=COLUMN
                );
            """
            )

    def _create_table_with_column_compression(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{name}` (
                    id Uint64 NOT NULL COMPRESSION(algorithm=zstd),
                    val Int64 COMPRESSION(algorithm=zstd),
                    PRIMARY KEY(id),
                ) WITH (
                    STORE=COLUMN
                );
            """
            )

    def _write_to_table(self, name):
        data = []
        for i in range(100):
            data.append({
                'id': i,
                'val': i,
            })

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("val", ydb.PrimitiveType.Int64)

        self.driver.table_client.bulk_upsert(f"{self.database_path}/{name}", data, column_types)

    def test_compress_table(self):
        logger.info(f"Testing versions {self.versions[0]} -> {self.versions[1]}")

        if self.versions[0] < (25, 5):
            self._create_table_with_family_compression("table1")
        else:
            self._create_table_with_column_compression("table1")

        self._write_to_table("table1")

        self.change_cluster_version()

        columns = self.cluster.client.send(
            SchemeDescribeRequest(f"{self.database_path}/table1").protobuf,
            'SchemeDescribe').PathDescription.ColumnTableDescription.Schema.Columns

        for column in columns:
            assert column.Serializer.ArrowCompression.Codec == 2
