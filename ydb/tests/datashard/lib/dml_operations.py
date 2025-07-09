import math
from datetime import datetime, timedelta

from ydb.tests.sql.lib.test_query import Query
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_ttl_sql_request
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, format_sql_value, ttl_types


class DMLOperations():
    def __init__(self, query_object: Query):
        self.query_object = query_object

    def query(self, text):
        return self.query_object.query(text)

    def transactional(self, process):
        return self.query_object.transactional(process)

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
            self.create_insert(table_name, count, all_types,
                               pk_types, index, ttl)

    def create_insert(self, table_name: str, value: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        insert_sql = f"""
            INSERT INTO {table_name}(
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                {", ".join([format_sql_value(pk_types[type_name](value), type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                {", ".join([format_sql_value(all_types[type_name](value), type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                {", ".join([format_sql_value(index[type_name](value), type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {format_sql_value(ttl_types[ttl](value), ttl) if ttl != "" else ""}
            );
        """
        self.query(insert_sql)

    def select_after_insert(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):

        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns + 1):
            sql_select = self.create_select_sql_request(
                table_name, all_types, count, pk_types, count, index, count, ttl, count)
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, failed in {count} value, table {table_name}"

        rows = self.query(f"SELECT COUNT(*) as count FROM `{table_name}`")
        assert len(
            rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, after select all line"

    def update(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str, unique: str):
        count = 1

        if ttl != "":
            self.create_update(
                count, "ttl_", ttl, ttl_types[ttl], table_name)
            count += 1

        for type_name in all_types.keys():
            self.create_update(
                count, "col_", type_name, all_types[type_name], table_name)
            count += 1

        if unique == "":
            for type_name in index.keys():
                self.create_update(
                    count, "col_index_", type_name, index[type_name], table_name)
                count += 1
        else:
            number_of_columns = len(pk_types) + len(all_types) + len(index)+1
            if ttl != "":
                number_of_columns += 1
            for i in range(1, number_of_columns + 1):
                self.create_update_unique(
                    number_of_columns + i, i, index, table_name)

        count_assert = 1

        number_of_columns = len(pk_types) + len(all_types) + len(index)
        if ttl != "":
            number_of_columns += 1

        if ttl != "":
            rows = self.query(
                f"SELECT COUNT(*) as count FROM `{table_name}` WHERE ttl_{cleanup_type_name(ttl)}={format_sql_value(ttl_types[ttl](count_assert), ttl)}")
            assert len(
                rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in ttl_{cleanup_type_name(ttl)}, table {table_name}"
            count_assert += 1

        for type_name in all_types.keys():
            if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_{cleanup_type_name(type_name)}={format_sql_value(all_types[type_name](count_assert), type_name)}")
                assert len(
                    rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in col_{cleanup_type_name(type_name)}, table {table_name}"
            count_assert += 1
        if unique == "":
            for type_name in index.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_index_{cleanup_type_name(type_name)}={format_sql_value(index[type_name](count_assert), type_name)}")
                assert len(
                    rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows after insert, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"
                count_assert += 1
        else:
            number_of_columns = len(pk_types) + len(all_types) + len(index) + 2
            if ttl != "":
                number_of_columns += 1
            for type_name in index.keys():
                rows = self.query(
                    f"SELECT COUNT(*) as count FROM `{table_name}` WHERE col_index_{cleanup_type_name(type_name)}={format_sql_value(index[type_name](number_of_columns), type_name)}")
                assert len(
                    rows) == 1 and rows[0].count == 1, f"Expected {1} rows after insert, faild in col_index_{cleanup_type_name(type_name)}, table {table_name}"
                number_of_columns += 1

    def create_update(self, value: int, prefix: str, type_name: str, key: str, table_name: str):
        update_sql = f""" UPDATE `{table_name}` SET {prefix}{cleanup_type_name(type_name)} = {format_sql_value(key(value), type_name)} """
        self.query(update_sql)

    def create_update_unique(self, value: int, search: int, index: dict[str, str], table_name: str):
        update_sql = f""" UPDATE `{table_name}` SET 
            {", ".join([f"col_index_{cleanup_type_name(type_name)} = {format_sql_value(index[type_name](value), type_name)}" for type_name in index.keys()])}
            WHERE
            {" and ".join(f"col_index_{cleanup_type_name(type_name)} = {format_sql_value(index[type_name](search), type_name)}" for type_name in index.keys())}
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

        for count in range(1, number_of_columns + 1):
            sql_select = self.create_select_sql_request(table_name, all_types, number_of_columns - count + 1,
                                                        pk_types, count, index, number_of_columns - count + 1, ttl, number_of_columns - count + 1)
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

        for count in range(number_of_columns + 1, 2*number_of_columns + 1):
            create_all_type = []
            for type_name in all_types.keys():
                if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument" and ((type_name != "Date" and type_name != "Datetime") or count < 106):
                    create_all_type.append(
                        f"col_{cleanup_type_name(type_name)}={format_sql_value(all_types[type_name](count), type_name)}")
            create_pk = []
            for type_name in pk_types.keys():
                if (type_name != "Date" and type_name != "Datetime") or count < 106:
                    create_pk.append(
                        f"pk_{cleanup_type_name(type_name)}={format_sql_value(pk_types[type_name](count), type_name)}")
            create_index = []
            for type_name in index.keys():
                if (type_name != "Date" and type_name != "Datetime") or count < 106:
                    create_index.append(
                        f"col_index_{cleanup_type_name(type_name)}={format_sql_value(index[type_name](count), type_name)}")
            sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE 
                {" and ".join(create_pk)}
                {" and " if len(create_index) != 0 else ""}
                {" and ".join(create_index)}
                {" and " if len(create_all_type) != 0 else ""}
                {" and ".join(create_all_type)}
                {f" and  ttl_{ttl}={format_sql_value(ttl_types[ttl](count), ttl)}" if ttl != "" and ((type_name != "Date" and type_name != "Datetime") or count < 106) else ""}
                """
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"
        rows = self.query(f"SELECT COUNT(*) as count FROM `{table_name}`")
        assert len(
            rows) == 1 and rows[0].count == 2*number_of_columns, f"Expected {2*number_of_columns} rows, after select all line"

    def create_upsert(self, table_name: str, value: int, search: int, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        upsert_sql = f"""
                    UPSERT INTO {table_name} (
                    {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                    {", ".join(["col_" + cleanup_type_name(type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                    {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                    {f" ttl_{ttl}" if ttl != "" else ""}
                    )
                    VALUES
                    (
                    {", ".join([format_sql_value(pk_types[type_name](search), type_name) for type_name in pk_types.keys()])}{", " if len(all_types) != 0 else ""}
                    {", ".join([format_sql_value(all_types[type_name](value), type_name) for type_name in all_types.keys()])}{", " if len(index) != 0 else ""}
                    {", ".join([format_sql_value(index[type_name](value), type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                    {format_sql_value(ttl_types[ttl](value), ttl) if ttl != "" else ""}
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
                               "ttl_", ttl, ttl_types[ttl], table_name)
            number_of_columns += 1

        for type_name in pk_types.keys():
            if type_name != "Bool":
                self.create_delete(
                    number_of_columns, "pk_", type_name, pk_types[type_name], table_name)
            else:
                self.create_delete(
                    number_of_columns, "pk_", "Int64", pk_types["Int64"], table_name)
            number_of_columns += 1

        for type_name in all_types.keys():
            if type_name != "Bool" and type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                self.create_delete(
                    number_of_columns, "col_", type_name, all_types[type_name], table_name)
            else:
                self.create_delete(
                    number_of_columns, "pk_", "Int64", pk_types["Int64"], table_name)
            number_of_columns += 1

        for type_name in index.keys():
            if type_name != "Bool":
                self.create_delete(
                    number_of_columns, "col_index_", type_name, index[type_name], table_name)
            else:
                self.create_delete(
                    number_of_columns, "pk_", "Int64", pk_types["Int64"], table_name)
            number_of_columns += 1

        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1

        for count in range(1, number_of_columns + 1):
            sql_select = self.create_select_sql_request(table_name, all_types, number_of_columns - count + 1,
                                                        pk_types, count, index, number_of_columns - count + 1, ttl, number_of_columns - count + 1)
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 1, f"Expected one rows, faild in {count} value, table {table_name}"

        for count in range(number_of_columns + 1, 2*number_of_columns + 1):
            sql_select = self.create_select_sql_request(table_name, all_types, count, pk_types, count, index, count, ttl, count)
            rows = self.query(sql_select)
            assert len(
                rows) == 1 and rows[0].count == 0, f"Expected one rows, faild in {count} value, table {table_name}"
        rows = self.query(f"SELECT COUNT(*) as count FROM `{table_name}`")
        assert len(
            rows) == 1 and rows[0].count == number_of_columns, f"Expected {number_of_columns} rows, after select all line"

    def create_delete(self, value: int, prefix: str, type_name: str, key: str, table_name: str):
        delete_sql = f"""
            DELETE FROM {table_name} WHERE {prefix}{cleanup_type_name(type_name)} = {format_sql_value(key(value), type_name)};
        """
        self.query(delete_sql)

    def select_all_type(self, table_name: str, all_types: dict[str, str], pk_types: dict[str, str], index: dict[str, str], ttl: str):
        statements = self.create_statements(pk_types, all_types, index, ttl)

        rows = self.query(f"select {", ".join(statements)} from {table_name}")
        count = 0
        for data_type in all_types.keys():
            if data_type != "Date32" and data_type != "Datetime64" and data_type != "Timestamp64" and data_type != 'Interval64':
                for i in range(len(rows)):
                    self.assert_type(all_types, data_type, i+1, rows[i][count])
                count += 1
        for data_type in pk_types.keys():
            if data_type != "Date32" and data_type != "Datetime64" and data_type != "Timestamp64" and data_type != 'Interval64':
                for i in range(len(rows)):
                    self.assert_type(pk_types, data_type, i+1, rows[i][count])
                count += 1
        for data_type in index.keys():
            if data_type != "Date32" and data_type != "Datetime64" and data_type != "Timestamp64" and data_type != 'Interval64':
                for i in range(len(rows)):
                    self.assert_type(index, data_type, i+1, rows[i][count])
                count += 1
        if ttl != "":
            for i in range(len(rows)):
                self.assert_type(ttl_types, ttl, i+1, rows[i][count])
            count += 1

    def create_statements(self, pk_types, all_types, index, ttl):
        # delete if after https://github.com/ydb-platform/ydb/issues/16930
        statements = []
        for data_type in all_types.keys():
            if data_type != "Date32" and data_type != "Datetime64" and data_type != "Timestamp64" and data_type != 'Interval64':
                statements.append(f"col_{cleanup_type_name(data_type)}")
        for data_type in pk_types.keys():
            if data_type != "Date32" and data_type != "Datetime64" and data_type != "Timestamp64" and data_type != 'Interval64':
                statements.append(f"pk_{cleanup_type_name(data_type)}")
        for data_type in index.keys():
            if data_type != "Date32" and data_type != "Datetime64" and data_type != "Timestamp64" and data_type != 'Interval64':
                statements.append(f"col_index_{cleanup_type_name(data_type)}")
        if ttl != "":
            statements.append(f"ttl_{cleanup_type_name(ttl)}")
        return statements

    def assert_type(self, key, data_type: str, values: int, values_from_rows):
        if data_type == "String" or data_type == "Yson":
            assert values_from_rows.decode(
                "utf-8") == key[data_type](values), f"{data_type}, expected {key[data_type](values)}, received {values_from_rows.decode("utf-8")}"
        elif data_type == "Float" or data_type == "DyNumber":
            assert math.isclose(float(values_from_rows), float(
                key[data_type](values)), rel_tol=1e-3), f"{data_type}, expected {key[data_type](values)}, received {values_from_rows}"
        elif data_type == "Interval" or data_type == "Interval64":
            assert values_from_rows == timedelta(
                microseconds=key[data_type](values)), f"{data_type}, expected {timedelta(microseconds=key[data_type](values))}, received {values_from_rows}"
        elif data_type == "Timestamp" or data_type == "Timestamp64":
            assert values_from_rows == datetime.fromtimestamp(
                key[data_type](values)/1_000_000), f"{data_type}, expected {datetime.fromtimestamp(key[data_type](values)/1_000_000)}, received {values_from_rows}"
        elif data_type == "Json" or data_type == "JsonDocument":
            assert str(values_from_rows).replace(
                "'", "\"") == str(key[data_type](values)), f"{data_type}, expected {key[data_type](values)}, received {values_from_rows}"
        else:
            assert str(values_from_rows) == str(
                key[data_type](values)), f"{data_type}, expected {key[data_type](values)}, received {values_from_rows}"

    def create_select_sql_request(self, table_name, all_types, all_types_value, pk_types, pk_types_value, index, index_value, ttl, ttl_value):
        create_all_type = []
        for type_name in all_types.keys():
            if type_name != "Json" and type_name != "Yson" and type_name != "JsonDocument":
                create_all_type.append(
                    f"col_{cleanup_type_name(type_name)}={format_sql_value(all_types[type_name](all_types_value), type_name)}")
        sql_select = f"""
                SELECT COUNT(*) as count FROM `{table_name}` WHERE 
                {" and ".join([f"pk_{cleanup_type_name(type_name)}={format_sql_value(pk_types[type_name](pk_types_value), type_name)}" for type_name in pk_types.keys()])}
                {" and " if len(index) != 0 else ""}
                {" and ".join([f"col_index_{cleanup_type_name(type_name)}={format_sql_value(index[type_name](index_value), type_name)}" for type_name in index.keys()])}
                {" and " if len(create_all_type) != 0 else ""}
                {" and ".join(create_all_type)}
                {f" and  ttl_{ttl}={format_sql_value(ttl_types[ttl](ttl_value), ttl)}" if ttl != "" else ""}
                """
        return sql_select
