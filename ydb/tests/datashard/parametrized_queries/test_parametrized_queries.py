import pytest
import ydb

from  uuid import UUID
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_ttl_sql_request
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, pk_types, non_pk_types, index_first, index_second, ttl_types, \
    index_first_sync, index_second_sync, index_three_sync, index_three_sync_not_Bool, index_four_sync, index_zero_sync


class TestParametrizedQueries(TestBase):
    def test_t(self):
        self.query("create table a(pk Int64, p Int64, primary key(pk))")
        self.query("""
                   DECLARE $pk AS Int64;
                   DECLARE $p AS Int64;  
                   insert into a(pk, p) values($pk, $p)""",
                   parameters={"$pk": ydb.TypedValue(1, ydb.PrimitiveType.Int64),
                               "$p": ydb.TypedValue(1, ydb.PrimitiveType.Int64),
                               })
        rows = self.query("select count(*) as count from a")
        assert rows[0].count == 1, "cdscd"

    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync",
        [
            ("table_index_4_UNIQUE_SYNC", pk_types, {},
             index_four_sync, "", "UNIQUE", "SYNC"),
            ("table_index_3_UNIQUE_SYNC", pk_types, {},
             index_three_sync_not_Bool, "", "UNIQUE", "SYNC"),
            ("table_index_2_UNIQUE_SYNC", pk_types, {},
             index_second_sync, "", "UNIQUE", "SYNC"),
            ("table_index_1_UNIQUE_SYNC", pk_types, {},
             index_first_sync, "", "UNIQUE", "SYNC"),
            ("table_index_0_UNIQUE_SYNC", pk_types, {},
             index_zero_sync, "", "UNIQUE", "SYNC"),
            ("table_index_4__SYNC", pk_types, {},
             index_four_sync, "", "", "SYNC"),
            ("table_index_3__SYNC", pk_types, {},
             index_three_sync, "", "", "SYNC"),
            ("table_index_2__SYNC", pk_types, {},
             index_second_sync, "", "", "SYNC"),
            ("table_index_1__SYNC", pk_types, {},
             index_first_sync, "", "", "SYNC"),
            ("table_index_0__SYNC", pk_types, {},
             index_zero_sync, "", "", "SYNC"),
            ("table_index_1__ASYNC", pk_types, {}, index_second, "", "", "ASYNC"),
            ("table_index_0__ASYNC", pk_types, {}, index_first, "", "", "ASYNC"),
            ("table_all_types", pk_types, {
             **pk_types, **non_pk_types}, {}, "", "", ""),
            ("table_ttl_DyNumber", pk_types, {}, {}, "DyNumber", "", ""),
            ("table_ttl_Uint32", pk_types, {}, {}, "Uint32", "", ""),
            ("table_ttl_Uint64", pk_types, {}, {}, "Uint64", "", ""),
            ("table_ttl_Datetime", pk_types, {}, {}, "Datetime", "", ""),
            ("table_ttl_Timestamp", pk_types, {}, {}, "Timestamp", "", ""),
            ("table_ttl_Date", pk_types, {}, {}, "Date", "", ""),
        ]
    )
    def test_dml(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
        self.create_table(table_name, pk_types, all_types,
                          index, ttl, unique, sync)
        self.insert(table_name, all_types, pk_types, index, ttl)
        self.select_after_insert(table_name, all_types, pk_types, index, ttl)

    def create_table(self, table_name: str, pk_types: dict[str, str], all_types: dict[str, str], index: dict[str, str], ttl: str, unique: str, sync: str):
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
        sql_create_table = create_table_sql_request(
            table_name, columns, pk_columns, index_columns, unique, sync)
        self.query(sql_create_table)
        if ttl != "":
            sql_ttl = create_ttl_sql_request(f"ttl_{cleanup_type_name(ttl)}", {"P18262D": ""}, "SECONDS" if ttl ==
                                             "Uint32" or ttl == "Uint64" or ttl == "DyNumber" else "", table_name)
            self.query(sql_ttl)

    def insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        for count in range(1, number_of_columns + 1):
            print(count)
            self.create_insert(table_name, count, all_types,
                               pk_types, index, ttl)

    def create_insert(self, table_name: str, value: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        insert_sql = f"""
            {" ".join([f"DECLARE $pk_{cleanup_type_name(type_name)} AS {type_name};" for type_name in pk_types.keys()])}
            {" ".join([f"DECLARE $col_{cleanup_type_name(type_name)} AS {type_name };" for type_name in all_types.keys()])}
            {" ".join([f"DECLARE $col_index_{cleanup_type_name(type_name)} AS {type_name};" for type_name in index.keys()])}
            {f"DECLARE $ttl_{ttl} AS {ttl};" if ttl != "" else ""}
        
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([f"$pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([f"$col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([f"$col_index_{cleanup_type_name(type_name)}" for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"$ttl_{ttl}" if ttl != "" else ""}
            );
        """
        parameters = {}
        for type_name in pk_types.keys():
            parameters[f"$pk_{cleanup_type_name(type_name)}"] = self.create_paramerts(type_name, value, pk_types)
        for type_name in all_types.keys():
            parameters[f"$col_{cleanup_type_name(type_name)}"] = self.create_paramerts(type_name, value, all_types)
        for type_name in index.keys():
            parameters[f"$col_index_{cleanup_type_name(type_name)}"] = self.create_paramerts(type_name, value, index)
        if ttl != "":
            parameters[f"$ttl_{cleanup_type_name(ttl)}"] = self.create_paramerts(ttl, value, ttl_types)
        self.query(insert_sql, parameters=parameters)

    def select_after_insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):

        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns + 1):
            create_all_type = []
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                    create_all_type.append(
                        f"col_{cleanup_type_name(type_name)}={all_types[type_name].format(count)}")
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE 
                {" and ".join([f"pk_{cleanup_type_name(type_name)}={pk_types[type_name].format(count)}" for type_name in pk_types.keys()])}
                {" and " if len(index) != 0 else ""}
                {" and ".join([f"col_index_{cleanup_type_name(type_name)}={index[type_name].format(count)}" for type_name in index.keys()])}
                {" and " if len(create_all_type) != 0 else ""}
                {" and ".join(create_all_type)}
                {f" and  ttl_{ttl}={ttl_types[ttl].format(count)}" if ttl != "" else ""}
                """
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

        rows = self.query(f"SELECT COUNT(*) as count FROM `{table_name}`")
        assert len(
            rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, after select all line"

    def create_paramerts(self, type_name: str, value: int, key):
        if type_name == "Int64":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Int64)
        if type_name == "Int32":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Int32)
        if type_name == "Int16":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Int16)
        if type_name == "Int8":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Int8)
        if type_name == "Uint64":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Uint64)
        if type_name == "Uint32":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Uint32)
        if type_name == "Uint16":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Uint16)
        if type_name == "Uint8":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Uint8)
        if type_name == "Bool":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Bool)
        if type_name == "Date" or type_name == "Date32":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Date)
        if type_name == "Double":
            return ydb.TypedValue(value + 0.2, ydb.PrimitiveType.Double)
        if type_name == "Datetime" or type_name == "Datetime64":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Datetime)
        if type_name == "DyNumber":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.DyNumber)
        if type_name == "UUID":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.UUID)
        if type_name == "Float":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Float)
        if type_name == "Interval" or type_name == "Interval64":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Interval)
        if type_name == "Json":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Json)
        if type_name == "JsonDocument":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.JsonDocument)
        if type_name == "Yson":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Yson)
        if type_name == "String":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.String)
        if type_name == "Timestamp" or type_name == "Timestamp64":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Timestamp)
        if type_name == "Utf8":
            return ydb.TypedValue(key[type_name](value), ydb.PrimitiveType.Utf8)
        if type_name == "Decimal(15,0)":
            return ydb.TypedValue(key[type_name](value), ydb.DecimalType(15, 0))
        if type_name == "Decimal(22,9)":
            return ydb.TypedValue(key[type_name](value), ydb.DecimalType(22, 9))
        if type_name == "Decimal(35,10)":
            return ydb.TypedValue(key[type_name](value), ydb.DecimalType(35, 10))
