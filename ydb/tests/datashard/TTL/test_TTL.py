from datetime import datetime, timedelta
import time
import threading

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name
from ydb.tests.datashard.lib.create_table import CreateTables, pk_types, non_pk_types, index_first, index_second, unique, index_sync


ttl_types = {
    "Timestamp": "CAST({}000000 AS Timestamp)",
    "DyNumber": "CAST('{}' AS DyNumber)",
    "Uint32": "CAST({} AS Uint32)",
    "Uint64": "CAST({} AS Uint64)",
    "Date": "CAST('{}' AS Date)",
    "Datetime": "CAST('{}' AS Datetime)",
}


class TestTTL(TestBase, CreateTables):
    def test_TTL(self):
        threads = []
        for ttl in ttl_types.keys():
            for i in range(2):
                for uniq in unique:
                    for sync in index_sync:
                        if uniq != "UNIQUE" and sync != "ASYNC":
                            thr = threading.Thread(target=self.TTL, args=(
                                f"table_{ttl}_{i}_{uniq}_{sync}", pk_types, {
                                    **pk_types, **non_pk_types}, index_first if i == 0 else index_second, ttl, uniq, sync,), name=f"table_{ttl}_{i}_{uniq}_{sync}")
                            thr.start()
                            threads.append(thr)
                            
        for thread in threads:
            thread.join()


    def TTL(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        columns = {
            "pk_": pk_types.keys(),
            "col_": all_types.keys(),
            "col_index_": index.keys(),
            "ttl_": [ttl]
        }
        pk_columns = {
            "pk_": pk_types.keys()
        }
        index_columns = {
            "col_index_": index.keys()
        }
        sql_create_table = self.create_table(
            table_name, columns, pk_columns, index_columns, unique, sync)
        self.query(sql_create_table)
        sql_ttl = self.create_ttl(f"ttl_{cleanup_type_name(ttl)}", {"PT15M": ""}, "SECONDS" if ttl ==
                                  "Uint32" or ttl == "Uint64" or ttl == "DyNumber" else "", table_name)
        self.query(sql_ttl)
        self.insert(table_name, pk_types, all_types, index, ttl)
        self.select(table_name, pk_types, all_types, index)

    def insert(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str):
        now_date = datetime.now()
        old_date = now_date - timedelta(weeks=1)
        future_time_5_sec = now_date + timedelta(seconds=5)
        future_time = now_date + timedelta(weeks=2)
        if ttl == "Uint64" or ttl == "Uint32" or ttl == "DyNumber" or ttl == "Timestamp":
            now_date = int(datetime.timestamp(now_date))
            old_date = int(datetime.timestamp(old_date))
            future_time_5_sec = int(datetime.timestamp(future_time_5_sec))
            future_time = int(datetime.timestamp(future_time))
        if ttl == "Date":
            now_date = now_date.date()
            old_date = old_date.date()
            future_time_5_sec = future_time_5_sec.date()
            future_time = future_time.date()
        if ttl == "Datetime":
            now_date = now_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            old_date = old_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            future_time_5_sec = future_time_5_sec.strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            future_time = future_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        for i in range(1, 3):
            self.create_insert(table_name, pk_types,
                               all_types, index, ttl, i, old_date)
        for i in range(3, 5):
            self.create_insert(table_name, pk_types,
                               all_types, index, ttl, i, now_date)
        for i in range(5, 7):
            self.create_insert(table_name, pk_types, all_types,
                               index, ttl, i, future_time_5_sec)
        for i in range(7, 9):
            self.create_insert(table_name, pk_types,
                               all_types, index, ttl, i, future_time)
            
        for i in range(1, 9):
            self.create_select(table_name, pk_types, all_types, index, i, 1)

    def create_insert(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, value: int, date):
        insert_sql = f"""
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([pk_types[type_name].format(value) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([all_types[type_name].format(value) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([index[type_name].format(value) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {ttl_types[ttl].format(date) if ttl != "" else ""}
            );
        """
        self.query(insert_sql)

    def select(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str]):
        time.sleep(60*15)
        for i in range(1, 7):
            self.create_select(table_name, pk_types, all_types, index, i, 0)

        for i in range(7, 9):
            self.create_select(table_name, pk_types, all_types, index, i, 1)

    def create_select(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], value: int, expected_count_rows: int):
        sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE """

        for type_name in pk_types.keys():
            sql_select += f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(value)} and "
        for type_name in all_types.keys():
            if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                sql_select += f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(value)} and "
        for type_name in index.keys():
            sql_select += f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(value)} and "
        sql_select += f"""pk_Int64={pk_types["Int64"].format(value)}"""
        rows = self.query(sql_select)
        assert len(
            rows) == 1 and rows[0].count == expected_count_rows, f"Expected {expected_count_rows} rows, error when deleting {value} lines, table {table_name}"
