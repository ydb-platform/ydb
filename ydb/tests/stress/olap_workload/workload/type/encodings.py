# -*- coding: utf-8 -*-
import threading

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadEncodings(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "encodings", stop)
        self.iteration = 0
        self.total_rows = 0
        self.table_name = "table"
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Iteration: {self.iteration}; total rows in the table: {self.total_rows}; "

    def collect_distinct_records(self, query_result):
        res = []
        none_presented = False
        for chunk in query_result:
            for row in chunk.rows:
                if row['res'] is not None:
                    res.append(row['res'])
                else:
                    none_presented = True

        if not none_presented:
            raise Exception("No NULL in dictionary")
        return sorted(res)

    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        self.client.query(
            f"""
                CREATE TABLE `{table_path}` (
                    id Int64 NOT NULL,
                    i64Val Int64,
                    PRIMARY KEY(id)
                )
                PARTITION BY HASH(id)
                WITH (
                    STORE = COLUMN
                )
            """,
            True,
        )

        i = 1
        total_rows = 0
        i_values = list(range(1, 101))
        expected_values = list(range(2, 101, 2))

        states = ["DICT", "OFF", "", "DICT", "", "OFF"]
        current_state = 0
        while not self.is_stop_requested():
            query = f"INSERT INTO `{table_path}` (`id`, `i64Val`) VALUES "
            for v in i_values:
                query += f"({i}, {v}),"
                i += 1

            query += f"({i}, NULL);"
            i += 1
            self.client.query(query, False)

            total_rows += len(expected_values) + 1

            self.client.query(
                f"""
                    DELETE FROM `{table_path}`
                    WHERE i64Val % 2 == 1
                """,
                False,
            )

            distinct_no_optimization = self.client.query(
                f"""
                    PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "false";
                    SELECT SOME(i64Val) AS res FROM `{table_path}` GROUP BY i64Val;
                """,
                False,
            )

            distinct_with_optimization = self.client.query(
                f"""
                    PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
                    SELECT SOME(i64Val) AS res FROM `{table_path}` GROUP BY i64Val;
                """,
                False,
            )

            total_rows_actual = self.client.query(
                f"""
                    SELECT count(*) AS res FROM `{table_path}`;
                """,
                False,
            )[0].rows[0]['res']

            if total_rows != total_rows_actual:
                raise Exception(f"Actual total rows {total_rows_actual} differs from expected {total_rows}")

            actual_no_optimization = self.collect_distinct_records(distinct_no_optimization)
            actual_optimization = self.collect_distinct_records(distinct_with_optimization)

            if expected_values != actual_no_optimization:
                raise Exception(f"Actual dictionary without optimization differs from expected: {actual_no_optimization}")

            if expected_values != actual_optimization:
                raise Exception(f"Actual dictionary with optimization differs from expected: {actual_optimization}")

            with self.lock:
                self.iteration += 1
                self.total_rows = total_rows

            self.client.query(
                f"""
                    ALTER TABLE `{table_path}` ALTER COLUMN i64Val SET ENCODING({states[current_state]})
                """,
                True,
            )
            current_state += 1
            if current_state >= len(states):
                current_state = 0

    def get_workload_thread_funcs(self):
        return [self._loop]
