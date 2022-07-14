from ydb.tests.functional.sqs.merge_split_common_table.test import TestSqsSplitMergeTables


class TestSqsSplitMergeStdTables(TestSqsSplitMergeTables):
    def test_std_merge_split(self):
        self.run_test(is_fifo=False)
