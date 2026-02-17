# -*- coding: utf-8 -*-
import pytest

from ydb.tests.stress.oltp_workload.workload import WorkloadRunner
from ydb.tests.stress.common.common import YdbClient
from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_parameterized_decimal": True,
                "enable_table_datetime64": True,
                "enable_vector_index": True,
                "enable_fulltext_index": True,
            }
        )

    def test(self):
        client = YdbClient(self.endpoint, self.database, True)
        client.wait_connection()
        try:
            with WorkloadRunner(client, 'oltp_workload', 120) as runner:
                runner.run()
        finally:
            client.close()
