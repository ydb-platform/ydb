# -*- coding: utf-8 -*-
import threading
import time
import traceback

import pytest
import yatest.common

from ydb.tests.stress.simple_queue.workload import Workload, BLOB_MIN_SIZE
from ydb.tests.library.stress.fixtures import StressFixture


# Two profiles, both running several workloads concurrently against the same database
# (each workload is still single-threaded internally, matching the standalone CLI; the
# concurrency reproduces the contention of running several instances on one host).
#
#   heavy - large, real-sized payloads; a slow soak that mostly stresses I/O.
#   light - tiny payloads, so workers churn through steps fast and reliably exercise the
#           merge-on-write path (partial upserts after ADD_COLUMN). This is the one that
#           guards the conflict-detection fix.
#
# Any field can be overridden from the command line and applies to every profile, e.g.:
#   ya test ... --test-param stress_default_duration=600 \
#               --test-param stress_workers=8 \
#               --test-param stress_blob_min_bytes=4096
WORKLOAD_PROFILES = {
    'heavy': {'duration': 60, 'workers': 4, 'blob_min_bytes': BLOB_MIN_SIZE},
    'light': {'duration': 120, 'workers': 4, 'blob_min_bytes': 128},
}


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
    @pytest.mark.parametrize('profile', ['heavy', 'light'])
    def test(self, mode: str, profile: str):
        cfg = WORKLOAD_PROFILES[profile]
        duration = int(yatest.common.get_param('stress_default_duration', default=cfg['duration']))
        workers = int(yatest.common.get_param('stress_workers', default=cfg['workers']))
        blob_min_bytes = int(yatest.common.get_param('stress_blob_min_bytes', default=cfg['blob_min_bytes']))

        errors = []
        errors_lock = threading.Lock()

        def run_workload():
            try:
                with Workload(self.endpoint, self.database, duration, mode, blob_min_bytes) as workload:
                    workload.start()
            except Exception:
                with errors_lock:
                    errors.append(traceback.format_exc())

        threads = [
            threading.Thread(target=run_workload, name="workload-{}".format(i), daemon=True)
            for i in range(workers)
        ]
        for t in threads:
            t.start()

        # While the workloads run, poll cluster health so we fail fast on a node crash
        # (a tablet VERIFY aborts the ydbd process) instead of waiting out the duration.
        deadline = time.time() + duration + 300
        cluster_dead = []
        try:
            while time.time() < deadline and any(t.is_alive() for t in threads):
                cluster_dead = self._dead_nodes()
                if cluster_dead:
                    break
                time.sleep(2)
        finally:
            # Always join, even after spotting a dead node: when a crash was triggered by a
            # specific workload op, that worker's own exception (captured in `errors`) is the
            # actual root cause, and we don't want it lost behind the cluster-death signal.
            for t in threads:
                t.join(timeout=120)

        if not cluster_dead:
            cluster_dead = self._dead_nodes()
        stuck = [t.name for t in threads if t.is_alive()]

        # Report everything we found together, so a node crash and the worker error that caused
        # it show up in the same failure message.
        problems = []
        if cluster_dead:
            problems.append("cluster node(s) died during '{}' workload (VERIFY/crash?): nodes={}".format(mode, cluster_dead))
        if stuck:
            problems.append("workload thread(s) did not finish (hang?): {}".format(stuck))
        if errors:
            problems.append("workload thread(s) failed:\n" + "\n\n".join(errors))
        assert not problems, "\n\n".join(problems)
