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
    def create_insert(self, value: int, name_table: str, key: str, ttl: str): 
        insert_sql = f"""
                    INSERT INTO DML_{ttl} (
                    pk,
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])},
                    {cleanup_type_name(name_table)}
                    )
                    VALUES
                    (
                    {i},
                    {", ".join([pk_types[type_name].format(value) for type_name in pk_types.keys()])},
                    {key.format(value)}
                    )
                    ;
                """
        self.query(insert_sql)

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
            for type_name in pk_types.keys():
                self.create_insert(i, f"col_{type_name}", pk_types[type_name], ttl)
                i+=1

            for type_name in non_pk_types.keys():
                self.create_insert(i, f"col_{type_name}", non_pk_types[type_name], ttl)
                i+=1

            for type_name in pk_types.keys():
                self.create_insert(i, f"col_index_{type_name}", pk_types[type_name], ttl)
                i+=1
            
            for type_name in non_pk_types.keys():
                self.create_insert(i, f"col_index_{type_name}", non_pk_types[type_name], ttl)
                i+=1

            

