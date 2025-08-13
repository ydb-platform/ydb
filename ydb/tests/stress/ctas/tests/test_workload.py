# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            table_service_config={
                "enable_oltp_sink": True,
                "enable_olap_sink": True,
                "enable_create_table_as": True,
            }, extra_feature_flags={
                "enable_temp_tables": True,
                "enable_olap_schema_operations": True,
                "enable_move_column_table": True,
            }, column_shard_config={
                "disabled_on_scheme_shard": False,
            }
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "120",
        ])
