# -*- coding: utf-8 -*-
from reconfig_state_storage_workload_test import ReconfigStateStorageWorkloadTest


class TestReconfigStateStorageBoardWorkload(ReconfigStateStorageWorkloadTest):
    def test_state_storage_board(self):
        self.do_test("StateStorageBoard")
