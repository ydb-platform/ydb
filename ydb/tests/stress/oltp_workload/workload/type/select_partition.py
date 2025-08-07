import threading
import time

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadSelectPartition(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "insert_delete_all_types", stop)
        self.lock = threading.Lock()
        self.time_check = 2

    def get_stat(self):
        return ""

    def _loop(self):
        while not self.is_stop_requested():
            sql_select = f"""
                SELECT * FROM `.sys/partition_stats`
                WHERE Path = '{self.get_table_path('table')}'
            """
            result_set = self.client.query(sql_select, False)
            rows = result_set[0].rows
            if len(rows) != 1:
                raise Exception("partition > 1")
            time.sleep(self.time_check)

    def get_workload_thread_funcs(self):
        return [self._loop]
