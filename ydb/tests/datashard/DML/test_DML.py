from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name
from ..lib.create_table import TestCreateTables, pk_types, non_pk_types, index_first, index_second, ttl_types, index_unique, index_sync

from ydb.ydb.tests.sql import test_inserts # type: ignore

class TestDML(TestCreateTables, TestBase):
    def test_DML(self):
        for ttl in  ttl_types.keys():
            for i in range(2):
                for index_uniq in index_unique:
                    for sync in index_sync:
                        table_name = f"table_{ttl}_{i+1}_{index_uniq}_{index_sync}"
                        sql_create_table = self.create_table(ttl, table_name, index_first if i == 0 else index_second, index_uniq, sync)
                        self.query(sql_create_table)
                        test_inserts(self, table_name, index_first if i == 0 else index_second)
                        

    def test_insert(self, table_name:str, index):
        count = 0

        # insetr
        for type_name in pk_types.keys():
            self.create_insert(count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name], table_name)
            count+=1

        for type_name in non_pk_types.keys():
            self.create_insert(count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name], table_name)
            count+=1

        for type_name in index.keys():
            self.create_insert(count, f"col_index_{cleanup_type_name(type_name)}", index[type_name], table_name)
            count+=1

        #check after insert
        rows = self.query(f"SELECT * FROM {table_name}")
        assert len(rows) == count, f"Expected {count} row after insert, {len(rows)} row after insert"

        count_assert = 0
        for type_name in pk_types.keys():
                rows = self.query(f"SELECT * FROM {table_name} WHERE col_{cleanup_type_name(type_name)}={pk_types[type_name].format(count_assert)}")
                assert len(rows) == 1, f"Expected one row after insert, faild in col_{cleanup_type_name(type_name)}"
                count_assert+=1
        for type_name in non_pk_types.keys():
                rows = self.query(f"SELECT * FROM {table_name} WHERE col_{cleanup_type_name(type_name)}={non_pk_types[type_name].format(count_assert)}")
                assert len(rows) == 1, f"Expected one row after insert, faild in col_{cleanup_type_name(type_name)}"
                count_assert+=1
        for type_name in index.keys():
                rows = self.query(f"SELECT * FROM {table_name} WHERE col_index_{cleanup_type_name(type_name)}={index[type_name].format(count_assert)}")
                assert len(rows) == 1, f"Expected one row after insert, faild in col_{cleanup_type_name(type_name)}"
                count_assert+=1

        assert count == count_assert, f"Expected {count} select after insert"

    
    def create_insert(self, value: int, name: str, key: str, table_name: str): 
        insert_sql = f"""
                    INSERT INTO {table_name} (
                    pk,
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                    {name}
                    )
                    VALUES
                    (
                    {value},
                    {", ".join([pk_types[type_name].format(value) for type_name in pk_types.keys()])},
                    {key.format(value)}
                    )
                    ;
                """
        self.query(insert_sql)

    def test_update(self, table_name:str, index):
        count = 0

        for type_name in pk_types.keys():
            for change_name in pk_types.keys():
                self.create_update(self, table_name, count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name], # type: ignore
                               f"col_{cleanup_type_name(change_name)}", pk_types[change_name])
            for change_name in non_pk_types.keys():
                self.create_update(self, table_name, count, f"col_{cleanup_type_name(type_name)}", pk_types[type_name],
                               f"col_{cleanup_type_name(change_name)}", non_pk_types[change_name])
            for change_name in index.keys():
                self.create_update(self, table_name, count, f"col_index_{cleanup_type_name(type_name)}", pk_types[type_name],
                               f"col_{cleanup_type_name(change_name)}", index[change_name])
            count+=1

        for type_name in non_pk_types.keys():
            for change_name in pk_types.keys():
                self.create_update(self, table_name, count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name],
                               f"col_{cleanup_type_name(change_name)}", pk_types[change_name])
            for change_name in non_pk_types.keys():
                self.create_update(self, table_name, count, f"col_{cleanup_type_name(type_name)}", non_pk_types[type_name],
                               f"col_{cleanup_type_name(change_name)}", non_pk_types[change_name])
            for change_name in index.keys():
                self.create_update(self, table_name, count, f"col_index_{cleanup_type_name(type_name)}", non_pk_types[type_name],
                               f"col_{cleanup_type_name(change_name)}", index[change_name])
            count+=1
        
        for type_name in index.keys():
            for change_name in pk_types.keys():
                self.create_update(self, table_name, count, f"col_{cleanup_type_name(type_name)}", index[type_name],
                               f"col_{cleanup_type_name(change_name)}", pk_types[change_name])
            for change_name in non_pk_types.keys():
                self.create_update(self, table_name, count, f"col_{cleanup_type_name(type_name)}", index[type_name],
                               f"col_{cleanup_type_name(change_name)}", non_pk_types[change_name])
            for change_name in index.keys():
                self.create_update(self, table_name, count, f"col_index_{cleanup_type_name(type_name)}", index[type_name],
                               f"col_{cleanup_type_name(change_name)}", index[change_name])
            count+=1
        
        

    def create_update(self, table_name: str, value: int, limit_name: str, key_limit_name: str, change_name: str, key_change_name: str):
        update_sql = f"""
                    UPDATE {table_name}
                    SET {change_name} = {key_change_name.format(value)} where {limit_name} = {key_limit_name.format(value)}
                """
        self.query(update_sql)