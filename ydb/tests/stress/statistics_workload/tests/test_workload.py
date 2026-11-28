# -*- coding: utf-8 -*-
import os
import pytest
import yatest
import ydb
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_cluster import KiKiMR
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.stress.statistics_workload.workload import Workload


class TestStatisticsWorkload:
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.all_binary_paths = [kikimr_driver_path()]
        self.base_duration = yatest.common.get_param('stress_default_duration', default='60')

        self.config = KikimrConfigGenerator(
            binary_paths=self.all_binary_paths,
            erasure=Erasure.NONE,
            extra_feature_flags=[
                "enable_statistics",
                "enable_column_statistics",
                "enable_analyze_long_running_operation",
            ],
            additional_log_configs={
                'STATISTICS': LogLevels.DEBUG,
            },
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)

        with ydb_database_ctx(self.cluster, "/Root/statistics_workload", node_count=1) as db_path:
            self.database = db_path
            self.driver = ydb.Driver(
                ydb.DriverConfig(
                    database=self.database,
                    endpoint=self.endpoint,
                )
            )
            self.driver.wait(timeout=60)
            yield
            self.driver.stop()

        self.cluster.stop()

    def test(self):
        log_file = yatest.common.output_path("statistics_workload.log")
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--host", "localhost",
            "--port", str(self.cluster.nodes[1].port),
            "--database", self.database,
            "--duration", self.base_duration,
            "--log_file", log_file,
        ])

    def test_statistics_count_excludes_traversal_fence(self):
        with ydb.QuerySessionPool(self.driver) as pool:
            pool.execute_with_retries("""
                CREATE TABLE stats (
                    local_path_id Uint64, stat_type Uint32, column_tags String,
                    PRIMARY KEY (local_path_id, stat_type, column_tags)
                )
            """)
            pool.execute_with_retries("""
                UPSERT INTO stats (local_path_id, stat_type, column_tags) VALUES
                    (1ul, 4294967295u, ''),
                    (2ul, 4294967295u, ''), (2ul, 4u, ''), (2ul, 1u, '1'), (2ul, 5u, '1,2')
            """)
        with Workload('localhost', self.cluster.nodes[1].port, self.database,
                      duration=0, batch_size=1, batch_count=1, prefix='') as workload:
            workload.driver.wait(timeout=60)
            assert workload.statistics_count("stats", 1) == 0
            assert workload.statistics_count("stats", 2) == 2
