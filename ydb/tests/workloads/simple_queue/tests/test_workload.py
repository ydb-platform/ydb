# -*- coding: utf-8 -*-
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        config_generator = KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC)
        config_generator.yaml_config["table_service_config"]["allow_olap_data_query"] = True
        cls.cluster = KiKiMR(config_generator)
        cls.cluster.start()
        workload_path = yatest.common.build_path("ydb/tests/workloads/simple_queue/simple_queue")
        cls.workload_command_prefix = [
            workload_path,
            "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "--duration", "60",
        ]

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test_row(self):
        command = self.workload_command_prefix
        command.extend(["--mode", "row"])
        yatest.common.execute(command, wait=True)

    def test_column(self):
        command = self.workload_command_prefix
        command.extend(["--mode", "column"])
        yatest.common.execute(command, wait=True)

