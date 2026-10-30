# -*- coding: utf-8 -*-
from reconfig_state_storage_workload_test import ReconfigStateStorageWorkloadTest


class TestReconfigAllStateStorageWorkload(ReconfigStateStorageWorkloadTest):
    def test_all_subsystems(self):
        self.do_test("All")
