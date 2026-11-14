# -*- coding: utf-8 -*-
import threading
import time
import traceback

import pytest

from ydb.tests.stress.simple_queue.workload import Workload
from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags=[
                "enable_add_colums_with_defaults",
                "enable_table_cache_modes",
                "enable_set_drop_default_value",
                "enable_forced_compactions",
            ],
            table_service_config={
                "allow_olap_data_query": True,
                "enable_batch_updates": True,
            }
        )

    def _dead_nodes(self):
        return [node_id for node_id, node in self.cluster.nodes.items() if not node.is_alive()]

    @pytest.mark.parametrize('mode', ['row', 'column'])
    def test(self, mode: str):
        duration = int(self.base_duration)
        errors = []

        with Workload(self.endpoint, self.database, duration, mode) as workload:
            def run_workload():
                try:
                    workload.start()
                except Exception:
                    errors.append(traceback.format_exc())

            thread = threading.Thread(target=run_workload, name="workload", daemon=True)
            thread.start()
            deadline = time.time() + duration + 300
            cluster_dead = []
            try:
                while time.time() < deadline and thread.is_alive():
                    cluster_dead = self._dead_nodes()
                    if cluster_dead:
                        break
                    time.sleep(2)
            finally:
                workload.stop()
                thread.join(timeout=120)

        if not cluster_dead:
            cluster_dead = self._dead_nodes()

        problems = []
        if cluster_dead:
            problems.append("cluster node(s) died during '{}' workload (VERIFY/crash?): nodes={}".format(mode, cluster_dead))
        if thread.is_alive():
            problems.append("workload thread did not finish (hang?)")
        if errors:
            problems.append("workload thread(s) failed:\n" + "\n\n".join(errors))
        assert not problems, "\n\n".join(problems)
