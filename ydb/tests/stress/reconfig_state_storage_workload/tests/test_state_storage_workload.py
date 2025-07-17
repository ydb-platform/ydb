# -*- coding: utf-8 -*-
from reconfig_state_storage_workload_test import ReconfigStateStorageWorkloadTest


class TestReconfigStateStorageWorkload(ReconfigStateStorageWorkloadTest):
    def test_state_storage(self):
        self.do_test("StateStorage")
