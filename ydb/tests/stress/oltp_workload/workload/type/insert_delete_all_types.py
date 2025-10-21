import threading
from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.datashard.lib.types_of_variables import pk_types, non_pk_types, null_types, cleanup_type_name, format_sql_value


class WorkloadInsertDeleteAllTypes(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "insert_delete_all_types", stop)
        self.inserted = 0
        self.table_name = "table"
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Inserted: {self.inserted}"

    def agent_work(self, agent_key):
        table_path = self.get_table_path(self.table_name)
        inflight = 10
        i = 0
        sum = 0
        while not self.is_stop_requested():
            value = i % 100
            insert_sql = f"""
                INSERT INTO `{table_path}` (
                pk,
                agent_key,
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                {", ".join(["null_pk_" + cleanup_type_name(type_name) for type_name in null_types.keys()])},
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in non_pk_types.keys()])},
                {", ".join(["null_col_" + cleanup_type_name(type_name) for type_name in null_types.keys()])}
                )
                VALUES
                (
                {i},
                {agent_key},
                {", ".join([format_sql_value(pk_types[type_name](value), type_name) for type_name in pk_types.keys()])},
                {", ".join(['NULL' for type_name in null_types.keys()])},
                {", ".join([format_sql_value(non_pk_types[type_name](value), type_name) for type_name in non_pk_types.keys()])},
                {", ".join(['NULL' for type_name in null_types.keys()])}
                )
                ;
            """
            self.client.query(insert_sql, False,)
            sum += i

            if (i >= inflight):
                self.client.query(
                    f"""
                    DELETE FROM `{table_path}`
                    WHERE pk == {i - inflight} AND null_pk_Int64 IS NULL AND agent_key == {agent_key}
                """,
                    False,
                )
                sum -= (i - inflight)

                actual = self.client.query(
                    f"""
                    SELECT COUNT(*) as cnt, SUM(pk) as sum FROM `{table_path}` WHERE agent_key == {agent_key}
                """,
                    False,
                )[0].rows[0]
                expected = {"cnt": inflight, "sum": sum}
                if actual != expected:
                    raise Exception(f"Incorrect result: expected:{expected}, actual:{actual}")
            i += 1
            with self.lock:
                self.inserted += 1

    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        create_sql = f"""
            CREATE TABLE `{table_path}` (
                pk Uint64,
                agent_key Uint64,
                {", ".join(["pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                {", ".join(["null_pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in null_types.keys()])},
                {", ".join(["col_" + cleanup_type_name(type_name) + " " + type_name for type_name in non_pk_types.keys()])},
                {", ".join(["null_col_" + cleanup_type_name(type_name) + " " + type_name for type_name in null_types.keys()])},
                PRIMARY KEY(
                agent_key,
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                {", ".join(["null_pk_" + cleanup_type_name(type_name) for type_name in null_types.keys()])}
                )
            )
        """

        self.client.query(create_sql, True,)

        for i in range(50):
            yield lambda: self.agent_work(i)

    def get_workload_thread_funcs(self):
        return self._loop()
