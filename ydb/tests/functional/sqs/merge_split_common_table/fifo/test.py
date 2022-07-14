from ydb.tests.functional.sqs.merge_split_common_table.test import TestSqsSplitMergeTables


class TestSqsSplitMergeFifoTables(TestSqsSplitMergeTables):
    def test_fifo_merge_split(self):
        self.run_test(is_fifo=True)
