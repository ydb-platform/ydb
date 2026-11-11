from ydb.tests.functional.sqs.merge_split_common_table.test import TestSqsSplitMergeTables
from ydb.tests.library.sqs.tables import QueueType


class TestSqsSplitMergeFifoTables(TestSqsSplitMergeTables):
    # Start with multiple Messages partitions to exercise leader redistribution on merge/split.
    message_table_partitions = {QueueType.FIFO: 10}

    def test_fifo_merge_split(self):
        self.run_test(is_fifo=True)
