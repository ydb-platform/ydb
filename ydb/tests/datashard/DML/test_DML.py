from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name
from ydb.tests.datashard.lib.create_table import TestCreateTables, pk_types, non_pk_types, index_first, index_second, ttl_types, unique, index_sync


class TestDML(TestCreateTables, TestBase):
    def test_DML(self):
        for ttl in ttl_types.keys():
            for i in range(2):
                self.table_name = f"table_{ttl}_{i+1}_UNIQUE_SYNC"
                sql_create_table = self.create_table(
                    ttl, index_first if i == 0 else index_second, "UNIQUE", "SYNC")
                self.query(sql_create_table)
                self.inserts(index_first if i ==
                             0 else index_second, ttl)
                self.update(index_first if i ==
                             0 else index_second, ttl)
                self.upsert(index_first if i ==
                             0 else index_second, ttl)
                self.delete(index_first if i ==
                             0 else index_second, ttl)
                self.query(f"DROP TABLE {self.table_name}")

    def inserts(self, index: dict[str, str], ttl: str):
        count = 10

        # insetr
        for type_name in pk_types.keys():
            self.create_insert(
                count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name])
            count += 1

        for type_name in non_pk_types.keys():
            self.create_insert(
                count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name])
            count += 1

        for type_name in index.keys():
            self.create_insert(
                count, f"col_index_{cleanup_type_name(type_name)}", index[type_name])
            count += 1

        self.create_insert(
            count, f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl])

        rows = self.query(
            f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count)}")
        assert len(
            rows) == 1 and rows[0].count == 1, f"Expected one row after insert, faild in ttl_{cleanup_type_name(ttl)}, {count} {ttl_types[ttl].format(count)},table {self.table_name}"

        count += 1

        # check after insert
        rows = self.query(
            f"""
            SELECT COUNT(*) as count FROM `{self.table_name}`;
            """
        )
        assert len(
            rows) == 1 and rows[0].count == count - 10, f"Expected {count - 10} rows, found: {rows[0].count}"

        count_assert = 10
        for type_name in pk_types.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_{cleanup_type_name(type_name)}={pk_types[type_name].format(count_assert)}")
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one row after insert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        for type_name in non_pk_types.keys():
            if type_name != "Json" and type_name != "JsonDocument" and type_name != "Yson":
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_{cleanup_type_name(type_name)}={non_pk_types[type_name].format(count_assert)}")
                assert len(
                    rows) == 1 and rows[0].count == 1, f"Expected one row after insert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        for type_name in index.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count_assert)}")
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one row after insert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        assert count_assert + 1 == count, "vsdvsdvsdvsdv"

        rows = self.query(
            f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count_assert)}")
        assert len(
            rows) == 1 and rows[0].count == 1, f"Expected one row after insert, faild in ttl_{cleanup_type_name(ttl)}, {count_assert} {ttl_types[ttl].format(count_assert)},table {self.table_name}"
        count_assert += 1
        assert count == count_assert, f"Expected {count} select after insert, table {self.table_name}"

        for i in range(10, count_assert):
            for type_name in pk_types.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(i)}")
                if type_name != "Bool":
                    assert len(
                        rows) == 1 and rows[0].count == 1, f"Expected one row after insert, faild in pk_{cleanup_type_name(type_name)}, table {self.table_name}"
                else:
                    assert len(
                        rows) == 1 and rows[0].count == count_assert-10, f"Expected {count_assert-10} row after insert, faild in pk_{cleanup_type_name(type_name)}, table {self.table_name}"

    def create_insert(self, value: int, name: str, key: str):
        insert_sql = f"""
                    INSERT INTO {self.table_name} (
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                    {name}
                    )
                    VALUES
                    (
                    {", ".join([pk_types[type_name].format(value) for type_name in pk_types.keys()])},
                    {key.format(value)}
                    )
                    ;
                """
        self.query(insert_sql)

    def update(self, index: dict[str, str], ttl: str):
        count = 10

        for type_name in pk_types.keys():
            for change_name in pk_types.keys():
                self.create_update(count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name],
                                   f"col_{cleanup_type_name(change_name)}", pk_types[change_name])
            for change_name in non_pk_types.keys():
                self.create_update(count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name],
                                   f"col_{cleanup_type_name(change_name)}", non_pk_types[change_name])
            for change_name in index.keys():
                self.create_update(count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name],
                                   f"col_index_{cleanup_type_name(change_name)}", index[change_name])
            self.create_update(count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name],
                               f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl])
            count += 1

        for type_name in non_pk_types.keys():
            if type_name != "Json" and type_name != "JsonDocument" and type_name != "Yson":
                for change_name in pk_types.keys():
                    self.create_update(count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name],
                                       f"col_{cleanup_type_name(change_name)}", pk_types[change_name])
                for change_name in non_pk_types.keys():
                    self.create_update(count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name],
                                       f"col_{cleanup_type_name(change_name)}", non_pk_types[change_name])
                for change_name in index.keys():
                    self.create_update(count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name],
                                       f"col_index_{cleanup_type_name(change_name)}", index[change_name])
                self.create_update(count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name],
                                   f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl])
            count += 1

        for type_name in index.keys():
            for change_name in pk_types.keys():
                self.create_update(count, f"col_index_{cleanup_type_name(type_name)}", index[type_name],
                                   f"col_{cleanup_type_name(change_name)}", pk_types[change_name])
            for change_name in non_pk_types.keys():
                self.create_update(count, f"col_index_{cleanup_type_name(type_name)}", index[type_name],
                                   f"col_{cleanup_type_name(change_name)}", non_pk_types[change_name])
            for change_name in index.keys():
                self.create_update(count, f"col_index_{cleanup_type_name(type_name)}", index[type_name],
                                   f"col_{cleanup_type_name(change_name)}", index[change_name])
            self.create_update(count, f"col_index_{cleanup_type_name(type_name)}", index[type_name],
                               f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl])
            count += 1

        for change_name in pk_types.keys():
            self.create_update(count, f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl],
                               f"col_{cleanup_type_name(change_name)}", pk_types[change_name])
        for change_name in non_pk_types.keys():
            self.create_update(count, f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl],
                               f"col_{cleanup_type_name(change_name)}", non_pk_types[change_name])
        for change_name in index.keys():
            self.create_update(count, f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl],
                               f"col_index_{cleanup_type_name(change_name)}", index[change_name])
        count += 1

        # check after update

        for i in range(10, count):
            for type_name in pk_types.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_{cleanup_type_name(type_name)}={pk_types[type_name].format(i)}")
                assert len(
                    rows) == 1 and rows[0].count == 1, f"Expected one row after update, faild in col_{cleanup_type_name(type_name)}, {i} {pk_types[type_name].format(i)}, table {self.table_name}"
            for type_name in non_pk_types.keys():
                if type_name != "Json" and type_name != "JsonDocument" and type_name != "Yson":
                    rows = self.query(
                        f"""SELECT COUNT(*) as count FROM `{self.table_name}`
                        WHERE col_{cleanup_type_name(type_name)}={non_pk_types[type_name].format(i)}""")
                    assert len(
                        rows) == 1 and rows[0].count == 1, f"""Expected one row after update, faild in col_{cleanup_type_name(type_name)}, 
                        {i} {non_pk_types[type_name].format(i)}, table {self.table_name}"""
            for type_name in index.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(i)}")
                assert len(
                    rows) == 1 and rows[0].count == 1, f"Expected one row after update, faild in col_index_{cleanup_type_name(type_name)}, {i} {index[type_name].format(i)}, table {self.table_name}"
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(i)}")
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one row after update, faild in ttl_{cleanup_type_name(type_name)}, table {self.table_name}"

    def create_update(self, value: int, limit_name: str, key_limit_name: str, change_name: str, key_change_name: str):
        update_sql = f"""
                    UPDATE {self.table_name}
                    SET {change_name} = {key_change_name.format(value)} where {limit_name} = {key_limit_name.format(value)};
                """
        self.query(update_sql)

    def upsert(self, index: dict[str, str], ttl: str):
        count = 10
        for type_name in pk_types.keys():
            self.create_upsert(
                count, count-1, f"col_{cleanup_type_name(type_name)}", pk_types[type_name])
            count += 1

        for type_name in non_pk_types.keys():
            self.create_upsert(
                count, count-1, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name])
            count += 1

        for type_name in index.keys():
            self.create_upsert(
                count, count-1, f"col_index_{cleanup_type_name(type_name)}", index[type_name])
            count += 1

        self.create_upsert(
            count, count-1, f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl])

        for type_name in pk_types.keys():
            self.create_upsert(
                count, count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name])
            count += 1

        for type_name in non_pk_types.keys():
            self.create_upsert(
                count, count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name])
            count += 1

        for type_name in index.keys():
            self.create_upsert(
                count, count, f"col_index_{cleanup_type_name(type_name)}", index[type_name])
            count += 1
        self.create_upsert(
            count, count, f"ttl_{cleanup_type_name(ttl)}", ttl_types[ttl])
        count += 1

        # check after upsert
        count_assert = 9
        for type_name in pk_types.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_{cleanup_type_name(type_name)}={pk_types[type_name].format(count_assert)}")
            if count_assert == 9:
                assert len(
                    rows) == 1 and rows[0].count == 1, f"Expected one row after upsert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
            else:
                assert len(
                    rows) == 1 and rows[0].count == 2, f"Expected two row after upsert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        for type_name in non_pk_types.keys():
            if type_name != "Json" and type_name != "JsonDocument" and type_name != "Yson":
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_{cleanup_type_name(type_name)}={non_pk_types[type_name].format(count_assert)}")
                assert len(
                    rows) == 1 and rows[0].count == 2, f"Expected two row after upsert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
                count_assert += 1

        for type_name in index.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count_assert)}")
            assert len(
                rows) == 1 and rows[0].count == 2, f"Expected two row after upsert, faild in col_index_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        rows = self.query(
            f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count_assert)}")
        assert len(
            rows) == 1 and rows[0].count == 2, f"Expected two row after upsert, faild in ttl_{cleanup_type_name(ttl)}, table {self.table_name}"
        count_assert += 1

        for type_name in pk_types.key():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count_assert)}")
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected two row after upsert, faild in pk_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        rows = self.query(
            f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE ttl_{cleanup_type_name(ttl)}={ttl_types[ttl].format(count_assert)}")
        assert len(
            rows) == 1 and rows[0].count == 1, f"Expected two row after upsert, faild in pk_{cleanup_type_name(ttl)}, table {self.table_name}"

    def create_upsert(self, value_pk: int, value: int, name: str, key: str):
        insert_sql = f"""
                    UPSERT INTO {self.table_name} (
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                    {name}
                    )
                    VALUES
                    (
                    {", ".join([pk_types[type_name].format(value_pk) for type_name in pk_types.keys()])},
                    {key.format(value)}
                    )
                    ;
                """
        self.query(insert_sql)

    def delete(self, index: dict[str, str], ttl: str):
        count = len(index) + len(pk_types) + len(non_pk_types) + 11

        for type_name in pk_types.keys():
            self.create_delete(
                count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name])
            count += 1

        for type_name in non_pk_types.keys():
            self.create_delete(
                count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name])
            count += 1

        for type_name in index.keys():
            self.create_delete(
                count, f"col_index_{cleanup_type_name(type_name)}", index[type_name])
            count += 1

        self.create_delete(
            count, f"ttl{cleanup_type_name(ttl)}", ttl_types[ttl])
        count += 1

        # check after delete
        count_assert = len(index) + len(pk_types) + len(non_pk_types) + 11

        for type_name in pk_types.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_{cleanup_type_name(type_name)}={pk_types[type_name].format(count_assert)}"
            )
            assert len(
                rows) == 1 and rows[0].count == 0, f"Expected zero row after upsert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        for type_name in non_pk_types.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_{cleanup_type_name(type_name)}={non_pk_types[type_name].format(count_assert)}"
            )
            assert len(
                rows) == 1 and rows[0].count == 0, f"Expected zero row after upsert, faild in col_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

        for type_name in index.keys():
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{self.table_name}` WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count_assert)}"
            )
            assert len(
                rows) == 1 and rows[0].count == 0, f"Expected zero row after upsert, faild in col_index_{cleanup_type_name(type_name)}, table {self.table_name}"
            count_assert += 1

    def create_delete(self, value: int, type_name: str, key: str):
        delete_sql = f"""
            DELETE FROM my_table WHERE {type_name} = {key.format(value)}
        """
        self.query(delete_sql)
