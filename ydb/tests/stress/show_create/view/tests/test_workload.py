# -*- coding: utf-8 -*-
import pytest
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.stress.show_create.view.workload import ShowCreateViewWorkload

logger = logging.getLogger("TestShowCreateViewWorkload")


class TestShowCreateViewWorkload(object):
    @classmethod
    def setup_class(cls):
        logger.info("Setting up KiKiMR cluster...")
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                extra_feature_flags={
                    "enable_show_create": True,
                }
            )
        )
        cls.cluster.start()
        logger.info(f"KiKiMR cluster started successfully. GRPC port: {cls.cluster.nodes[1].grpc_port}")

    @classmethod
    def teardown_class(cls):
        logger.info("Tearing down KiKiMR cluster...")
        cls.cluster.stop()
        logger.info("KiKiMR cluster stopped successfully.")

    @pytest.mark.parametrize(
        "duration, path_prefix",
        [
            (30, None),
            (30, "test_scv"),
        ],
    )
    def test_show_create_view_workload(self, duration, path_prefix):
        endpoint = f"grpc://localhost:{self.cluster.nodes[1].grpc_port}"
        database = "/Root"

        logger.info(
            f"Running test_show_create_view_workload with parameters: "
            f"duration={duration}s, "
            f"path_prefix='{path_prefix if path_prefix else 'None'}'"
        )

        with ShowCreateViewWorkload(
            endpoint=endpoint, database=database, duration=duration, path_prefix=path_prefix
        ) as workload:
            workload.loop()

            assert workload.failed_cycles == 0, (
                f"Test failed: Workload reported {workload.failed_cycles} failed cycles. Check logs for errors."
            )

            assert workload.successful_cycles > 0, (
                "Test completed with zero successful cycles, which is unexpected. "
                f"Successful: {workload.successful_cycles}, Failed: {workload.failed_cycles}"
            )

            logger.info(
                f"Test scenario completed successfully. "
                f"Successful_cycles: {workload.successful_cycles}, "
                f"Failed_cycles: {workload.failed_cycles}"
            )
