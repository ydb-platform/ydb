# -*- coding: utf-8 -*-
import pytest

from ydb.tests.stress.simple_queue.workload import Workload
from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            table_service_config={
                "allow_olap_data_query": True,
                "enable_oltp_sink": True,
                "enable_batch_updates": True,
            }
        )

    @pytest.mark.parametrize('mode', ['row', 'column'])
    def test(self, mode: str):
        with Workload(self.endpoint, self.database, 60, mode) as workload:
            workload.start()
