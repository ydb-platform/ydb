import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbAlterStorageWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        cluster_generator = self.setup_cluster(extra_feature_flags=['enable_cut_history'])
        next(cluster_generator)
        self.database = self.database + "/db1"
        self.cluster.create_database(self.database, {'hdd': 3})
        slots = self.cluster.register_and_start_slots(self.database, 1)
        self.cluster.wait_tenant_up(self.database)
        yield
        try:
            self.cluster.unregister_and_stop_slots(slots)
            self.cluster.remove_database(self.database)
        finally:
            try:
                next(cluster_generator)
            except StopIteration:
                pass

    def test(self):
        cmd = [
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
        ]
        yatest.common.execute(cmd)
