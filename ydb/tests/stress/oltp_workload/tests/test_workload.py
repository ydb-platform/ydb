# -*- coding: utf-8 -*-
import pytest

from ydb.tests.stress.oltp_workload.workload import WorkloadRunner
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.library.harness.util import LogLevels


class TestYdbWorkload(StressFixture):
    @staticmethod
    def _feature_flags():
        return {
            "enable_parameterized_decimal": True,
            "enable_table_datetime64": True,
            "enable_vector_index": True,
            "enable_fulltext_index": True,
        }

    @pytest.fixture(scope="function")
    def setup_common(self):
        yield from self.setup_cluster(
            extra_feature_flags=self._feature_flags()
        )

    @pytest.fixture(scope="function")
    def setup_tli(self):
        yield from self.setup_cluster(
            additional_log_configs={
                "TLI": LogLevels.INFO,
            },
            extra_feature_flags=self._feature_flags()
        )

    def test(self, setup_common):
        client = InstrumentedYdbClient(self.endpoint, self.database, True)
        client.wait_connection()
        try:
            with WorkloadRunner(client, 'oltp_workload', 120) as runner:
                runner.run(disabled_workloads={"tli"})
        finally:
            client.close()

    def test_tli(self, setup_tli):
        client = InstrumentedYdbClient(self.endpoint, self.database, True)
        client.wait_connection()
        try:
            with WorkloadRunner(client, 'oltp_workload', 120) as runner:
                runner.run(enabled_workloads={"tli"})
        finally:
            client.close()
