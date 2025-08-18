# -*- coding: utf-8 -*-
from reconfig_state_storage_workload_test import ReconfigStateStorageWorkloadTest


class TestReconfigSchemeBoardWorkload(ReconfigStateStorageWorkloadTest):
    def test_scheme_board(self):
        self.do_test("SchemeBoard")
