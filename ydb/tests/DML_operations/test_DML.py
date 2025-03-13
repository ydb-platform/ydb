from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.stress.oltp_workload.workload import cleanup_type_name, pk_types, non_pk_types

serial_types = {
    "SmallSerial": "CAST({} AS SmallSerial)",
    "Serial2": "CAST({} AS Serial2)",
    "Serial": "CAST({} AS Serial)",
    "Serial4": "CAST({} AS Serial4)",
    "Serial8": "CAST({} AS Serial8)",
    "BigSerial": "CAST({} AS BigSerial)",
}

ttl_types = {
    "Date": "CAST('30{:02}-01-01' AS Date)",
    "Datetime": "CAST('30{:02}-10-02T11:00:00Z' AS Datetime)",
    "Timestamp": "CAST(269624{:02}00000000 AS Timestamp)",
    "Uint32": "CAST({} AS Uint32)",
    "Uint64": "CAST({} AS Uint64)",
    "DyNumber": "CAST({} AS DyNumber)",
}

class TestYdbDMLOperator(TestBase):
    def create_insert(self, value: int, name: str, key: str, ttl: str): 
        insert_sql = f"""
                    INSERT INTO DML_{ttl} (
                    pk,
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                    {cleanup_type_name(name)}
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
    
    def update(self, value: int, changeable_name: str, type_name: str, key: str, ttl: str):
        update_sql = f"""
                    UPDATE DML_{ttl}
                    SET {changeable_name} = {key.format(value)} where {type_name} = {key.format(value)}
                """
        self.query(update_sql)

    def test_DML_operator(self):
        for ttl in ttl_types.keys():
            create_sql = f"""
                CREATE TABLE DML_{ttl} (
                    pk Uint64,
                    {", ".join(["pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                    {", ".join(["pk_" + cleanup_type_name(type_name) + " " + type_name for type_name in serial_types.keys()])},
                    {", ".join(["col_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                    {", ".join(["col_" + cleanup_type_name(type_name) + " " + type_name for type_name in non_pk_types.keys()])},
                    {", ".join(["col_index_" + cleanup_type_name(type_name) + " " + type_name for type_name in pk_types.keys()])},
                    {", ".join(["col_index_" + cleanup_type_name(type_name) + " " + type_name for type_name in non_pk_types.keys()])},
                    ttl_{ttl} {ttl},
                    PRIMARY KEY(
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in serial_types.keys()])},
                    )
                ) with (
                    TTL = Interval("PT0S") ON ttl_{ttl}
                )
            """
            self.query(create_sql)
            index_sql = f"""
                CREATE UNIQUE INDEX pk ,
                {", ".join(["col_index_" + cleanup_type_name(type_name)for type_name in pk_types.keys()])},
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in non_pk_types.keys()])},
                ON DML_{ttl}(
                {", ".join(["idx_" + cleanup_type_name(type_name)for type_name in pk_types.keys()])},
                {", ".join(["idx_" + cleanup_type_name(type_name) for type_name in non_pk_types.keys()])},
                )
            """
            self.query(index_sql)
            i = 0
            
            # insetr
            for type_name in pk_types.keys():
                self.create_insert(i, f"col_{type_name}", pk_types[type_name], ttl)
                i+=1
                self.create_insert(i, f"col_index_{type_name}", pk_types[type_name], ttl)
                i+=1

            for type_name in non_pk_types.keys():
                self.create_insert(i, f"col_{type_name}", non_pk_types[type_name], ttl)
                i+=1
                self.create_insert(i, f"col_index_{type_name}", non_pk_types[type_name], ttl)
                i+=1

            #check after insert
            rows = self.query(f"SELECT * FROM DML_{ttl}")
            assert len(rows) == i, f"Expected {i} row after insert, {len(rows)} row after insert"

            count = 0
            for type_name in pk_types.keys():
                rows = self.query(f"SELECT * FROM DML_{ttl} WHERE col_{type_name}={pk_types[type_name].format(count)}")
                assert len(rows) == 1, f"Expected one row after insert, faild in col_{type_name}"
                count+=1
                rows = self.query(f"SELECT * FROM DML_{ttl} WHERE col_index_{type_name}={pk_types[type_name].format(count)}")
                assert len(rows) == 1, f"Expected one row after insert, faild in col_index_{type_name}"
                count+=1

            for type_name in non_pk_types.keys():
                rows = self.query(f"SELECT * FROM DML_{ttl} WHERE col_{type_name}={non_pk_types[type_name].format(count)}")
                assert len(rows) == 1, f"Expected one row after insert, faild in col_{type_name}"
                count+=1
                rows = self.query(f"SELECT * FROM DML_{ttl} WHERE col_index_{type_name}={non_pk_types[type_name].format(count)}")
                assert len(rows) == 1, f"Expected one row after insert, faild in col_index_{type_name}"
                count+=1

            #update
            for type_name in non_pk_types.keys():
                for changeable_name in non_pk_types.keys():
                    self.update(self, i, f"col_{changeable_name}", f"col_{type_name}", non_pk_types[type_name], ttl)
                    self.update(self, i-1, f"col_index_{changeable_name}", f"col_index_{type_name}", non_pk_types[type_name], ttl)
                for changeable_name in pk_types.keys():
                    self.update(self, i, f"col_{changeable_name}", f"col_{type_name}", non_pk_types[type_name], ttl)
                    self.update(self, i-1, f"col_index_{changeable_name}", f"col_index_{type_name}", non_pk_types[type_name], ttl)
                i-=2


            

