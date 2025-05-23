from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, null_types, cleanup_type_name, format_sql_value

import threading


class WorkloadBatchDeleteAllTypes(WorkloadBase):
    def __init__(self, client, prefix, stop, batch_size=100, groups_cnt=3):
        super().__init__(client, prefix, "batch_delete_all_types", stop)
        self.deleted = 0
        self.table_name = "table"
        self.lock = threading.Lock()
        self.batch_size = batch_size
        self.groups_cnt = groups_cnt

    def get_stat(self):
        with self.lock:
            return f"Deleted: {self.deleted}"

    def _loop(self):
        non_pk_types_2 = {**non_pk_types}
        non_pk_types_2.pop("Json")
        non_pk_types_2.pop("JsonDocument")
        non_pk_types_2.pop("Yson")

        i = 0
        inflight = 10

        table_path = self.get_table_path(self.table_name)
        self.client.query(f"""
            CREATE TABLE `{table_path}` (
                pk Uint64,
                group_id Uint32,
                {", ".join(["pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                {", ".join(["null_pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in null_types.keys()])},
                {", ".join(["col_" + cleanup_type_name(type_name) + " " + type_name for type_name in non_pk_types_2.keys()])},
                {", ".join(["null_col_" + cleanup_type_name(type_name) + " " + type_name for type_name in null_types.keys()])},
                PRIMARY KEY(pk)
            );
        """, True)

        while not self.is_stop_requested():
            for j in range(self.batch_size):
                value = i * self.batch_size + j
                self.client.query(f"""
                    INSERT INTO `{table_path}` (
                        pk, group_id,
                        {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                        {", ".join(["null_pk_" + cleanup_type_name(type_name) for type_name in null_types.keys()])},
                        {", ".join(["col_" + cleanup_type_name(type_name) for type_name in non_pk_types_2.keys()])},
                        {", ".join(["null_col_" + cleanup_type_name(type_name) for type_name in null_types.keys()])}
                    ) VALUES (
                        {value}, {value % self.groups_cnt},
                        {", ".join([format_sql_value(pk_types[type_name](value), type_name) for type_name in pk_types.keys()])},
                        {", ".join(['NULL' for type_name in null_types.keys()])},
                        {", ".join([format_sql_value(non_pk_types_2[type_name](value), type_name) for type_name in non_pk_types_2.keys()])},
                        {", ".join(['NULL' for type_name in null_types.keys()])}
                    );
                """, False)

            if (i >= inflight):
                group_id = i % self.groups_cnt

                self.client.query(f"BATCH DELETE `{table_path}` WHERE group_id = {group_id};", False)

                actual = self.client.query(f"SELECT COUNT(*) AS cnt FROM `{table_path}` WHERE group_id = {group_id};", False)[0].rows[0]

                expected = {"cnt": 0}
                if actual != expected:
                    raise Exception(f"Incorrect result: expected: {expected}, actual: {actual}")

                with self.lock:
                    self.deleted += 1

            i += 1

    def get_workload_thread_funcs(self):
        return [self._loop]
