import threading

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name
from ydb.tests.datashard.lib.create_table import TestCreateTables, pk_types, non_pk_types, index_first, index_second, ttl_types, unique, index_sync


class TestDML(TestCreateTables, TestBase):
    def test_DML(self):
        threads = []
        # all type
        thr = threading.Thread(target=self.DML, args=(
            "table_all_types", pk_types, {**pk_types, **non_pk_types}, {}, "", "", "",))
        thr.start()
        threads.append(thr)

        # all ttl
        for ttl in ttl_types.keys():
            thr = threading.Thread(target=self.DML, args=(
                f"table_ttl_{ttl}", pk_types, {}, {}, "", "", ttl,))
            thr.start()
            threads.append(thr)

        # all index
        for i in range(2):
            for uniq in unique:
                for sync in index_sync:
                    if uniq != "UNIQUE" and sync != "ASYNC":
                        thr = threading.Thread(target=self.DML, args=(
                            f"table_index_{i}_{uniq}_{sync}", pk_types, {}, index_first if i == 0 else index_second, uniq, sync,))
                        thr.start()
                        threads.append(thr)

        for thread in threads:
            thread.join()

    def DML(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        columns = {
            "pk_": pk_types.keys(),
            "col_": all_types.keys(),
            "col_index_": index.keys(),
            "ttl_": [ttl]
        }
        sql_create_table = self.create_table(
            table_name, columns, unique, sync)
        self.query(sql_create_table)
        self.insert(table_name, all_types, pk_types, index, ttl)
        self.update(table_name, all_types, index, ttl)
        self.upsert(table_name, all_types, pk_types, index, ttl)
        self.delete(table_name, all_types, pk_types, index, ttl)

    def est_DML_all_types(self):
        table_name = "table_all_types"
        all_types = {**pk_types, **non_pk_types}
        columns = {
            "pk_": pk_types.keys(),
            "col_": all_types.keys(),
            "col_index_": [],
            "ttl_": [""]
        }
        sql_create_table = self.create_table(
            table_name, columns, "", "")
        self.query(sql_create_table)
        self.insert(table_name, all_types, pk_types, {}, "")
        self.update(table_name, all_types, {}, "")
        self.upsert(table_name, all_types, pk_types, {}, "")
        self.delete(table_name, all_types, pk_types, {}, "")

    def insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        for count in range(1, number_of_columns + 1):
            self.create_insetr(table_name, count, all_types,
                               pk_types, index, ttl)

        self.assert_table_after_insert_and_upsert(
            table_name, pk_types, all_types, index, ttl, number_of_columns)

    def create_insetr(self, table_name: str, value: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        insert_sql = f"""
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f", ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([pk_types[type_name].format(value) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([all_types[type_name].format(value) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([all_types[type_name].format(value) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {ttl_types[ttl].format(value) if ttl != "" else ""}
            );
        """
        self.query(insert_sql)

    def update(self, table_name: str, all_types: dict[str, str], index: dict[str, str], ttl: str):
        count = 1

        if ttl != "":
            self.create_update(
                count, type_name, f"ttl_{ttl}", ttl_types[ttl], table_name)
            count += 1

        for type_name in all_types.keys():
            self.create_update(
                count, f"col_{type_name}", all_types[type_name], table_name)
            count += 1

        for type_name in index.keys():
            self.create_update(
                count, type_name, f"col_index_{type_name}", index[type_name], table_name)
            count += 1

        count_assert = 1

        number_of_columns = len(pk_types) + len(all_types) + len(index)
        if ttl != "":
            number_of_columns += 1

        if ttl != "":
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count_assert)}")
            assert len(
                rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in ttl_{cleanup_type_name(ttl)}, table {table_name}"
            count_assert += 1

        for type_name in all_types.keys():
            if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_{cleanup_type_name(type_name)}={all_types[type_name].format(count_assert)}")
                assert len(
                    rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in col_{cleanup_type_name(type_name)}, table {table_name}"
            count_assert += 1

        for type_name in index.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count_assert)}")
            assert len(
                rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"
            count_assert += 1

    def create_update(self, value: int, type_name: str, key: str, table_name: str):
        update_sql = f"""
            UPDATE `{table_name}`
                SET {cleanup_type_name(type_name)} = {key.format(value)} 
        """
        self.query(update_sql)

    def upsert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index)
        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns+1):
            self.create_upsert(table_name, number_of_columns + 1 -
                               count, count, all_types, pk_types, index, ttl)

        for count in range(number_of_columns+1, 2*number_of_columns+1):
            self.create_upsert(table_name, count, count,
                               all_types, pk_types, index, ttl)

        self.assert_table_after_insert_and_upsert(
            table_name, pk_types, all_types, index, ttl, 2*number_of_columns)

    def create_upsert(self, table_name: str, value: int, search: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        upsert_sql = f"""
                    UPSERT INTO {table_name} (
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                    {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                    {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                    {f" ttl_{ttl}," if ttl != "" else ""}
                    )
                    VALUES
                    (
                    {", ".join([pk_types[type_name].format(search) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                    {", ".join([all_types[type_name].format(value) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                    {", ".join([all_types[type_name].format(value) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                    {ttl_types[ttl].format(value) if ttl != "" else ""}
                    )
                    ;
                """
        self.query(upsert_sql)

    def delete(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index) + 1

        if ttl != "":
            number_of_columns += 1

        if ttl != "":
            self.create_delete(number_of_columns,
                               f"ttl_{ttl}", ttl_types[ttl], table_name)
            number_of_columns += 1

        for type_name in pk_types.keys():
            if type_name != "Bool":
                self.create_delete(
                    number_of_columns, f"pk_{type_name}", pk_types[type_name], table_name)
            number_of_columns += 1

        for type_name in all_types.keys():
            if type_name != "Bool" and type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                self.create_delete(
                    number_of_columns, f"col_{type_name}", all_types[type_name], table_name)
            number_of_columns += 1

        for type_name in index.keys():
            if type_name != "Bool":
                self.create_delete(
                    number_of_columns, f"col_index_{type_name}", index[type_name], table_name)
            number_of_columns += 1

        number_of_columns = len(pk_types) + len(all_types) + len(index)
        if ttl != "":
            number_of_columns += 1

        self.assert_table_after_insert_and_upsert(
            table_name, pk_types, all_types, index, ttl, number_of_columns)

        for count in (number_of_columns + 1, 2*number_of_columns + 1):
            # primery key
            for type_name in pk_types.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)}")
                if type_name != "Bool":
                    assert len(
                        rows) == 1 and rows[0].count == 0, f"Expected zero rows, faild in pk_{cleanup_type_name(type_name)}, table {table_name}"

            # all types
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument" and type_name != "Bool":
                    rows = self.query(
                        f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_{cleanup_type_name(type_name)}={all_types[type_name].format(count)}")
                    assert len(
                        rows) == 1 and rows[0].count == 0, f"Expected zero rows, faild in col_{cleanup_type_name(type_name)}, table {table_name}"

            # index
            for type_name in index.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count)}")
                if type_name != "Bool":
                    assert len(
                        rows) == 1 and rows[0].count == 0, f"Expected zero rows, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"

            # ttl
            if ttl != "":
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count)}")
                assert len(
                    rows) == 1 and rows[0].count == 0, f"Expected zero rows, faild in ttl_{cleanup_type_name(ttl)}, table {table_name}"

    def create_delete(self, value: int, type_name: str, key: str, table_name: str):
        delete_sql = f"""
            DELETE FROM {table_name} WHERE {type_name} = {key.format(value)};
        """
        self.query(delete_sql)

    def assert_table_after_insert_and_upsert(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, number_of_columns: int):
        for count in range(1, number_of_columns + 1):
            # primery key
            for type_name in pk_types.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)}")
                if type_name != "Bool" and (type_name != "Date" and type_name != "Datetime" or count < 106):
                    assert len(
                        rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in pk_{cleanup_type_name(type_name)}, table {table_name}"
                elif type_name == "Bool":
                    assert len(
                        rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, faild in pk_{cleanup_type_name(type_name)}, table {table_name}"
                else:
                    assert len(
                        rows) == 1 and rows[0].count == 0, f"Expected {0} rows, because the date cannot be more than 2106, faild in pk_{cleanup_type_name(type_name)}, table {table_name}"

            # all types
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                    rows = self.query(
                        f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_{cleanup_type_name(type_name)}={all_types[type_name].format(count)}")
                    if type_name != "Bool" and (type_name != "Date" and type_name != "Datetime" or count < 106):
                        assert len(
                            rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in col_{cleanup_type_name(type_name)}, table {table_name}"
                    elif type_name == "Bool":
                        assert len(
                            rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, faild in col_{cleanup_type_name(type_name)}, table {table_name}"
                    else:
                        assert len(
                            rows) == 1 and rows[0].count == 0, f"Expected {0} rows, because the date cannot be more than 2106, faild in col_{cleanup_type_name(type_name)}, table {table_name}"

            # index
            for type_name in index.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count)}")
                if type_name != "Bool" and (type_name != "Date" and type_name != "Datetime" or count < 106):
                    assert len(
                        rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"
                elif type_name == "Bool":
                    assert len(
                        rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"
                else:
                    assert len(
                        rows) == 1 and rows[0].count == 0, f"Expected {0} rows, because the date cannot be more than 2106, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"

            # ttl
            if ttl != "":
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count)}")
                if (ttl == "Date" or ttl == "Datetime") and count > 105:
                    assert len(
                        rows) == 1 and rows[0].count == 0, f"Expected {0} rows, because the date cannot be more than 2106, faild in ttl{cleanup_type_name(type_name)}, table {table_name}"
                else:
                    assert len(
                        rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in ttl_{cleanup_type_name(ttl)}, table {table_name}"
