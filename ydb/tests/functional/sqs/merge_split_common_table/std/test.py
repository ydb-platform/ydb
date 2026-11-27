from ydb.tests.functional.sqs.merge_split_common_table.test import TestSqsSplitMergeTables
from ydb.tests.library.sqs.tables import QueueType


class TestSqsSplitMergeStdTables(TestSqsSplitMergeTables):
    # Start with multiple Messages partitions to exercise leader redistribution on merge/split.
    message_table_partitions = {QueueType.STD: 10}

    def test_std_merge_split(self):
        self.run_test(is_fifo=False)
