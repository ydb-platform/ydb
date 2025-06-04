# -*- coding: utf-8 -*-
from ydb.tests.stress.reconfig_state_storage_workload.workload import WorkloadRunner
from reconfig_state_storage_workload_test import ReconfigStateStorageWorkloadTest


class TestReconfigStateStorageWorkload(ReconfigStateStorageWorkloadTest):
    def test_state_storage(self):
        with WorkloadRunner(self.client, self.cluster, 'reconfig_state_storage_workload', 120, True, "StateStorage") as runner:
            runner.run()
