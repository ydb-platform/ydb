import pytest
import random

from ydb.tests.datashard.lib.vectore_base import VectoreBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_ttl_sql_request, create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, format_sql_value, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync


class TestVectorIndex(VectoreBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync, vector_type",
        [
            ("table_index_4_UNIQUE_SYNC_float", pk_types, {},
             index_four_sync, "", "UNIQUE", "SYNC", "float"),
            ("table_index_3_UNIQUE_SYNC_float", pk_types, {},
             index_three_sync_not_Bool, "", "UNIQUE", "SYNC", "float"),
            ("table_index_2_UNIQUE_SYNC_float", pk_types, {},
             index_second_sync, "", "UNIQUE", "SYNC", "float"),
            ("table_index_1_UNIQUE_SYNC_float", pk_types, {},
             index_first_sync, "", "UNIQUE", "SYNC", "float"),
            ("table_index_0_UNIQUE_SYNC_float", pk_types, {},
             index_zero_sync, "", "UNIQUE", "SYNC", "float"),
            ("table_index_4__SYNC_float", pk_types, {},
             index_four_sync, "", "", "SYNC", "float"),
            ("table_index_3__SYNC_float", pk_types, {},
             index_three_sync, "", "", "SYNC", "float"),
            ("table_index_2__SYNC_float", pk_types, {},
             index_second_sync, "", "", "SYNC", "float"),
            ("table_index_1__SYNC_float", pk_types, {},
             index_first_sync, "", "", "SYNC", "float"),
            ("table_index_0__SYNC_float", pk_types, {},
             index_zero_sync, "", "", "SYNC", "float"),
            ("table_index_1__ASYNC_float", pk_types, {},
             index_second, "", "", "ASYNC", "float"),
            ("table_index_0__ASYNC_float", pk_types, {},
             index_first, "", "", "ASYNC", "float"),
            ("table_all_types_float", pk_types, {
             **pk_types, **non_pk_types}, {}, "", "", "", "float"),
            ("table_ttl_DyNumber_float", pk_types, {}, {}, "DyNumber", "", "", "float"),
            ("table_ttl_Uint32_float", pk_types, {}, {}, "Uint32", "", "", "float"),
            ("table_ttl_Uint64_float", pk_types, {}, {}, "Uint64", "", "", "float"),
            ("table_ttl_Datetime_float", pk_types, {}, {}, "Datetime", "", "", "float"),
            ("table_ttl_Timestamp_float", pk_types, {},
             {}, "Timestamp", "", "", "float"),
            ("table_ttl_Date_float", pk_types, {}, {}, "Date", "", "", "float"),
            
            ("table_index_4_UNIQUE_SYNC", pk_types, {},
             index_four_sync, "", "UNIQUE", "SYNC", "uint8"),
            ("table_index_3_UNIQUE_SYNC", pk_types, {},
             index_three_sync_not_Bool, "", "UNIQUE", "SYNC", "uint8"),
            ("table_index_2_UNIQUE_SYNC", pk_types, {},
             index_second_sync, "", "UNIQUE", "SYNC", "uint8"),
            ("table_index_1_UNIQUE_SYNC", pk_types, {},
             index_first_sync, "", "UNIQUE", "SYNC", "uint8"),
            ("table_index_0_UNIQUE_SYNC", pk_types, {},
             index_zero_sync, "", "UNIQUE", "SYNC", "uint8"),
            ("table_index_4__SYNC", pk_types, {},
             index_four_sync, "", "", "SYNC", "uint8"),
            ("table_index_3__SYNC", pk_types, {},
             index_three_sync, "", "", "SYNC", "uint8"),
            ("table_index_2__SYNC", pk_types, {},
             index_second_sync, "", "", "SYNC", "uint8"),
            ("table_index_1__SYNC", pk_types, {},
             index_first_sync, "", "", "SYNC", "uint8"),
            ("table_index_0__SYNC", pk_types, {},
             index_zero_sync, "", "", "SYNC", "uint8"),
            ("table_index_1__ASYNC", pk_types, {},
             index_second, "", "", "ASYNC", "uint8"),
            ("table_index_0__ASYNC", pk_types, {},
             index_first, "", "", "ASYNC", "uint8"),
            ("table_all_types", pk_types, {
             **pk_types, **non_pk_types}, {}, "", "", "", "uint8"),
            ("table_ttl_DyNumber", pk_types, {}, {}, "DyNumber", "", "", "uint8"),
            ("table_ttl_Uint32", pk_types, {}, {}, "Uint32", "", "", "uint8"),
            ("table_ttl_Uint64", pk_types, {}, {}, "Uint64", "", "", "uint8"),
            ("table_ttl_Datetime", pk_types, {}, {}, "Datetime", "", "", "uint8"),
            ("table_ttl_Timestamp", pk_types, {},
             {}, "Timestamp", "", "", "uint8"),
            ("table_ttl_Date", pk_types, {}, {}, "Date", "", "", "uint8"),
        ]
    )
    def test_vector_index(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str, vector_type: str):
        self.n = 2
        dml = DMLOperations(self)
        all_types["String"] = lambda i: f"String {i}"
        dml.create_table(table_name, pk_types, all_types,
                         index, ttl, unique, sync)
        self.vectors = []
        self.insert(table_name, all_types, pk_types, index, ttl, vector_type)
        sql_create_vector_index = create_vector_index_sql_request(
            table_name, "col_String", vector_type, self.n)
        print(sql_create_vector_index)
        dml.query(sql_create_vector_index)
        self.select(table_name, "col_String", vector_type)

    def get_vector(self, type, size, numb):
        if type == "float":
            values = [float(numb) for _ in range(size)]
            return ",".join(f'{val}f' for val in values)

        values = [numb for _ in range(size)]
        return ",".join(str(val) for val in values)
    
    
    def insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, vector_type: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        for count in range(1, number_of_columns + 1):
            self.create_insert(table_name, count, all_types,
                               pk_types, index, ttl, vector_type)

    def create_insert(self, table_name: str, value: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, vector_type: str):
        vector = self.get_vector(vector_type, self.n, value)
        self.vectors.append(vector)
        statements_all_type = []
        statements_all_type_value = []
        for type_name in all_types.keys():
            if type_name != "String":
                statements_all_type.append("col_" + cleanup_type_name(type_name))
                statements_all_type_value.append(format_sql_value(all_types[type_name](value), type_name))
        insert_sql = f"""
            INSERT INTO {table_name}(
                col_String,
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(statements_all_type) != 0 else ""}
                {", ".join(statements_all_type)}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {format_sql_value(vector, "String")},
                {", ".join([format_sql_value(pk_types[type_name](value), type_name) for type_name in pk_types.keys()])}{", " if len(statements_all_type_value) != 0 else ""}
                {", ".join(statements_all_type_value)}{", " if len(index) != 0 else ""}
                {", ".join([format_sql_value(index[type_name](value), type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {format_sql_value(ttl_types[ttl](value), ttl) if ttl != "" else ""}
            );
        """
        print(insert_sql)
        self.query(insert_sql)
        
    def select(self, table_name, col_name, vector_type):
        knn_type = {
            "float": "ToBinaryStringFloat",
            "uint8": "ToBinaryStringUint8"
        }
        for vector in self.vectors:
            print(f"""
                              $Target = Knn::{knn_type[vector_type]}(Cast([{vector}] AS List<{vector_type}>));
                              select col_String
                              from {table_name} view idx_vector_{col_name}
                              order by Knn::CosineDistance(col_String, $Target)
                              limit 10;
                              """)
            rows = self.query(f"""
                              $Target = Knn::{knn_type[vector_type]}(Cast([{vector}] AS List<{vector_type}>));
                              select col_String
                              from {table_name} view idx_vector_{col_name}
                              order by Knn::CosineDistance(col_String, $Target)
                              limit 10;
                              """)
            print(rows)
