# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


class TestAutomaticPartitioningDocumentationAudit:
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(erasure=Erasure.NONE)
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database="/Root",
                endpoint=cls.cluster.nodes[1].endpoint,
            )
        )
        cls.driver.wait(timeout=60)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def execute_scheme_query(self, query):
        return ydb.ScriptingClient(self.driver).execute_yql(query)

    def test_null_partition_boundary_is_rejected_without_internal_error(self):
        with pytest.raises(ydb.Error) as caught:
            self.execute_scheme_query(
                """
                    CREATE TABLE null_partition_boundary (
                        id Uint64 NOT NULL,
                        PRIMARY KEY (id)
                    ) WITH (
                        PARTITION_AT_KEYS = (NULL)
                    );
                """
            )

        assert not isinstance(caught.value, ydb.InternalError), str(caught.value)

    @pytest.mark.parametrize("operation", ["create", "alter"])
    def test_min_partitions_count_cannot_exceed_max_partitions_count(self, operation):
        if operation == "create":
            query = """
                CREATE TABLE invalid_min_max_create (
                    id Uint64 NOT NULL,
                    PRIMARY KEY (id)
                ) WITH (
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
                    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 3
                );
            """
        else:
            self.execute_scheme_query(
                """
                    CREATE TABLE invalid_min_max_alter (
                        id Uint64 NOT NULL,
                        PRIMARY KEY (id)
                    );
                """
            )
            query = """
                ALTER TABLE invalid_min_max_alter SET (
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
                    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 3
                );
            """

        with pytest.raises(ydb.Error):
            self.execute_scheme_query(query)
