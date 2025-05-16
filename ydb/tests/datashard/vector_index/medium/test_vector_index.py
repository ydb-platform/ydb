import pytest

from ydb.tests.datashard.lib.vector_base import VectorBase
from ydb.tests.datashard.lib.vector_index import get_vector, targets
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.create_table import create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
    pk_types,
    non_pk_types,
    ttl_types,
    index_first_sync,
    index_second_sync,
    index_three_sync,
    index_four_sync,
    index_zero_sync,
)


class TestVectorIndex(VectorBase):
    def setup_method(self):
        self.dimensions = [{"levels": 2, "cluster": 50}, {"levels": 1, "cluster": 100}]
        self.size_vector = 10
        self.knn_type = {"Float": "ToBinaryStringFloat", "Uint8": "ToBinaryStringUint8", "Int8": "ToBinaryStringInt8"}

    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, vector_type",
        [
            ("table_index_4_float", pk_types, {}, index_four_sync, "Float"),
            ("table_index_3_float", pk_types, {}, index_three_sync, "Float"),
            ("table_index_2_float", pk_types, {}, index_second_sync, "Float"),
            ("table_index_1_float", pk_types, {}, index_first_sync, "Float"),
            ("table_index_0_float", pk_types, {}, index_zero_sync, "Float"),
            (
                "table_all_types_float",
                pk_types,
                {**pk_types, **non_pk_types},
                {},
                "Float",
            ),
            ("table_index_4", pk_types, {}, index_four_sync, "Uint8"),
            ("table_index_3", pk_types, {}, index_three_sync, "Uint8"),
            ("table_index_2", pk_types, {}, index_second_sync, "Uint8"),
            ("table_index_1", pk_types, {}, index_first_sync, "Uint8"),
            ("table_index_0", pk_types, {}, index_zero_sync, "Uint8"),
            ("table_all_types", pk_types, {**pk_types, **non_pk_types}, {}, "Uint8"),
            ("table_index_4", pk_types, {}, index_four_sync, "Int8"),
            ("table_index_3", pk_types, {}, index_three_sync, "Int8"),
            ("table_index_2", pk_types, {}, index_second_sync, "Int8"),
            ("table_index_1", pk_types, {}, index_first_sync, "Int8"),
            ("table_index_0", pk_types, {}, index_zero_sync, "Int8"),
            ("table_all_types", pk_types, {**pk_types, **non_pk_types}, {}, "Int8"),
        ],
    )
    def test_vector_index(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        vector_type: str,
    ):
        dml = DMLOperations(self)
        all_types["String"] = lambda i: f"String {i}"
        dml.create_table(table_name, pk_types, all_types, index, "", "", "")
        self.upsert(table_name, all_types, pk_types, index, "", vector_type)
        cover = []
        for type_name in all_types.keys():
            if type_name != "String":
                cover.append("col_" + cleanup_type_name(type_name))
        for dimension in self.dimensions:
            for target, distances in targets.items():
                for distance, knn_func in distances.items():
                    sql_create_vector_index = create_vector_index_sql_request(
                        table_name,
                        f"{target}_{distance}_{dimension["levels"]}",
                        "col_String",
                        "",
                        target,
                        distance,
                        vector_type.lower(),
                        "",
                        self.size_vector,
                        dimension["levels"],
                        dimension["cluster"],
                        cover,
                    )
                    dml.query(sql_create_vector_index)
                    self.select_and_check(
                        table_name,
                        f"{target}_{distance}_{dimension["levels"]}",
                        "col_String",
                        vector_type,
                        pk_types,
                        all_types,
                        index,
                        "",
                        knn_func,
                        dml,
                    )

    def upsert(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        vector_type: str,
    ):
        number_of_columns = len(pk_types) + len(all_types) + len(index)

        if ttl != "":
            number_of_columns += 1
        for count in range(1, number_of_columns + 1):
            self.create_upsert(table_name, count, all_types, pk_types, index, ttl, vector_type)

    def create_upsert(
        self,
        table_name: str,
        value: int,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        vector_type: str,
    ):
        vector = get_vector(vector_type, value, self.size_vector)
        statements_all_type = []
        statements_all_type_value = []
        for type_name in all_types.keys():
            if type_name != "String":
                statements_all_type.append("col_" + cleanup_type_name(type_name))
                statements_all_type_value.append(format_sql_value(all_types[type_name](value), type_name))
        upsert_sql = f"""
            UPSERT INTO {table_name}(
                col_String,
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(statements_all_type) != 0 else ""}
                {", ".join(statements_all_type)}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                Untag(Knn::{self.knn_type[vector_type]}([{vector}]), "{vector_type}Vector"),
                {", ".join([format_sql_value(pk_types[type_name](value), type_name) for type_name in pk_types.keys()])}{", " if len(statements_all_type_value) != 0 else ""}
                {", ".join(statements_all_type_value)}{", " if len(index) != 0 else ""}
                {", ".join([format_sql_value(index[type_name](value), type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {format_sql_value(ttl_types[ttl](value), ttl) if ttl != "" else ""}
            );
        """
        self.query(upsert_sql)

    def select_and_check(
        self,
        table_name,
        vector_name,
        col_name,
        vector_type,
        pk_types,
        all_types,
        index,
        ttl,
        knn_func,
        dml: DMLOperations,
    ):
        statements = dml.create_statements(pk_types, all_types, index, ttl)
        statements.remove("col_String")
        statements.append(f"{knn_func}(col_String, $Target)")
        vector = get_vector(vector_type, 1, self.size_vector)
        sql_select_request = f"""
                                    $Target = Knn::{self.knn_type[vector_type]}(Cast([{vector}] AS List<{vector_type}>));
                                    select {", ".join(statements)}
                                    from {table_name} view {vector_name}
                                    order by {knn_func}({col_name}, $Target) {"DESC" if knn_func in targets["similarity"].values() else "ASC"}
                                    limit 100;
                                    """
        wait_for(self.wait_create_vector_index(sql_select_request, dml), timeout_seconds=150)
        rows = dml.query(sql_select_request)
        if knn_func == "Knn::InnerProductSimilarity":
            rows.reverse()
        count = 0
        for data_type in all_types.keys():
            if (
                data_type != "Date32"
                and data_type != "Datetime64"
                and data_type != "Timestamp64"
                and data_type != 'Interval64'
                and data_type != 'String'
            ):
                for i in range(len(rows)):
                    dml.assert_type(all_types, data_type, i + 1, rows[i][count])
                count += 1
        for data_type in pk_types.keys():
            if (
                data_type != "Date32"
                and data_type != "Datetime64"
                and data_type != "Timestamp64"
                and data_type != 'Interval64'
            ):
                for i in range(len(rows)):
                    dml.assert_type(pk_types, data_type, i + 1, rows[i][count])
                count += 1
        for data_type in index.keys():
            if (
                data_type != "Date32"
                and data_type != "Datetime64"
                and data_type != "Timestamp64"
                and data_type != 'Interval64'
            ):
                for i in range(len(rows)):
                    dml.assert_type(index, data_type, i + 1, rows[i][count])
                count += 1
        if ttl != "":
            for i in range(len(rows)):
                dml.assert_type(ttl_types, ttl, i + 1, rows[i][count])
            count += 1
        for i in range(len(rows)):
            if i != 0 or knn_func in targets["similarity"].values():
                assert rows[i][count] != 0, f"faild in {knn_func} == 0, rows = {i}"
            else:
                assert rows[i][count] == 0, f"faild in {knn_func} != 0, rows{i} = {rows[i][count]}"
        count += 1

    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, vector_type",
        [
            ("table_index_4_float", pk_types, {}, index_four_sync, "Float"),
            ("table_index_3_float", pk_types, {}, index_three_sync, "Float"),
            ("table_index_2_float", pk_types, {}, index_second_sync, "Float"),
            ("table_index_1_float", pk_types, {}, index_first_sync, "Float"),
            ("table_index_0_float", pk_types, {}, index_zero_sync, "Float"),
            (
                "table_all_types_float",
                pk_types,
                {**pk_types, **non_pk_types},
                {},
                "Float",
            ),
            ("table_index_4", pk_types, {}, index_four_sync, "Uint8"),
            ("table_index_3", pk_types, {}, index_three_sync, "Uint8"),
            ("table_index_2", pk_types, {}, index_second_sync, "Uint8"),
            ("table_index_1", pk_types, {}, index_first_sync, "Uint8"),
            ("table_index_0", pk_types, {}, index_zero_sync, "Uint8"),
            ("table_all_types", pk_types, {**pk_types, **non_pk_types}, {}, "Uint8"),
            ("table_index_4", pk_types, {}, index_four_sync, "Int8"),
            ("table_index_3", pk_types, {}, index_three_sync, "Int8"),
            ("table_index_2", pk_types, {}, index_second_sync, "Int8"),
            ("table_index_1", pk_types, {}, index_first_sync, "Int8"),
            ("table_index_0", pk_types, {}, index_zero_sync, "Int8"),
            ("table_all_types", pk_types, {**pk_types, **non_pk_types}, {}, "Int8"),
        ],
    )
    def test_vector_index_prefix(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        vector_type: str,
    ):
        dml = DMLOperations(self)
        index["String"] = lambda i: f"String {i}"
        all_types["String"] = lambda i: f"String {i}"
        dml.create_table(table_name, pk_types, all_types, index, "", "", "")
        self.upsert_for_prefix(table_name, all_types, pk_types, index, "", vector_type)
        cover = []
        for type_name in all_types.keys():
            if type_name != "String":
                cover.append("col_" + cleanup_type_name(type_name))
        for dimension in self.dimensions:
            for target, distances in targets.items():
                for distance, knn_func in distances.items():
                    sql_create_vector_index = create_vector_index_sql_request(
                        table_name,
                        f"{target}_{distance}_{dimension["levels"]}",
                        "col_String",
                        "col_index_String",
                        target,
                        distance,
                        vector_type.lower(),
                        "",
                        self.size_vector,
                        dimension["levels"],
                        dimension["cluster"],
                        cover,
                    )
                    dml.query(sql_create_vector_index)
                    self.select_and_check_for_prefix(
                        table_name,
                        f"{target}_{distance}_{dimension["levels"]}",
                        "col_String",
                        vector_type,
                        pk_types,
                        all_types,
                        index,
                        "",
                        knn_func,
                        dml,
                    )

    def upsert_for_prefix(
        self,
        table_name: str,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        vector_type: str,
    ):
        for count in range(1, 25 + 1):
            self.create_upsert_for_prefix(table_name, count % 5, count, all_types, pk_types, index, ttl, vector_type)

    def create_upsert_for_prefix(
        self,
        table_name: str,
        value_for_prefix: str,
        value: int,
        all_types: dict[str, str],
        pk_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        vector_type: str,
    ):
        if value_for_prefix == 0:
            value_for_prefix = 5
        vector = get_vector(vector_type, value, self.size_vector)
        statements_all_type = []
        statements_all_type_value = []
        for type_name in all_types.keys():
            if type_name != "String":
                statements_all_type.append("col_" + cleanup_type_name(type_name))
                statements_all_type_value.append(format_sql_value(all_types[type_name](value), type_name))
        statements_index_value = []
        for type_name in index.keys():
            if type_name != "String":
                statements_index_value.append(format_sql_value(index[type_name](value), type_name))
            else:
                statements_index_value.append(format_sql_value(index[type_name](value_for_prefix), type_name))
        upsert_sql = f"""
            UPSERT INTO {table_name}(
                col_String,
                {", ".join(["pk_" + cleanup_type_name(type_name) for type_name in pk_types.keys()])}{", " if len(statements_all_type) != 0 else ""}
                {", ".join(statements_all_type)}{", " if len(index) != 0 else ""}
                {", ".join(["col_index_" + cleanup_type_name(type_name) for type_name in index.keys()])}{", " if len(ttl) != 0 else ""}
                {f"ttl_{ttl}" if ttl != "" else ""}
            )
            VALUES(
                Untag(Knn::{self.knn_type[vector_type]}([{vector}]), "{vector_type}Vector"),
                {", ".join([format_sql_value(pk_types[type_name](value), type_name) for type_name in pk_types.keys()])}{", " if len(statements_all_type_value) != 0 else ""}
                {", ".join(statements_all_type_value)}{", " if len(index) != 0 else ""}
                {", ".join(statements_index_value)}{", " if len(ttl) != 0 else ""}
                {format_sql_value(ttl_types[ttl](value), ttl) if ttl != "" else ""}
            );
        """
        self.query(upsert_sql)

    def select_and_check_for_prefix(
        self,
        table_name,
        vector_name,
        col_name,
        vector_type,
        pk_types,
        all_types,
        index,
        ttl,
        knn_func,
        dml: DMLOperations,
    ):
        statements = dml.create_statements(pk_types, all_types, index, ttl)
        statements.remove("col_String")
        statements.remove("col_index_String")
        statements.append(f"{knn_func}(col_String, $Target)")
        for i in range(1, 6):
            vector = get_vector(vector_type, i, self.size_vector)
            sql_select_request = f"""
                                    $Target = Knn::{self.knn_type[vector_type]}(Cast([{vector}] AS List<{vector_type}>));
                                    select {", ".join(statements)}
                                    from {table_name} view {vector_name}
                                    WHERE col_index_String = {format_sql_value(pk_types["String"](i), "String")}
                                    order by {knn_func}({col_name}, $Target) {"DESC" if knn_func in targets["similarity"].values() else "ASC"}
                                    limit 5;
                                    """
            wait_for(self.wait_create_vector_index(sql_select_request, dml), timeout_seconds=100)
            rows = dml.query(sql_select_request)
            if knn_func == "Knn::InnerProductSimilarity":
                rows.reverse()
            count = 0
            for data_type in all_types.keys():
                if (
                    data_type != "Date32"
                    and data_type != "Datetime64"
                    and data_type != "Timestamp64"
                    and data_type != 'Interval64'
                    and data_type != 'String'
                ):
                    for j in range(len(rows)):
                        dml.assert_type(all_types, data_type, i + j * 5, rows[j][count])
                    count += 1
            for data_type in pk_types.keys():
                if (
                    data_type != "Date32"
                    and data_type != "Datetime64"
                    and data_type != "Timestamp64"
                    and data_type != 'Interval64'
                ):
                    for j in range(len(rows)):
                        dml.assert_type(pk_types, data_type, i + j * 5, rows[j][count])
                    count += 1
            for data_type in index.keys():
                if (
                    data_type != "Date32"
                    and data_type != "Datetime64"
                    and data_type != "Timestamp64"
                    and data_type != 'Interval64'
                    and data_type != 'String'
                ):
                    for j in range(len(rows)):
                        dml.assert_type(index, data_type, i + j * 5, rows[j][count])
                    count += 1
            if ttl != "":
                for j in range(len(rows)):
                    dml.assert_type(ttl_types, ttl, i + j * 5, rows[j][count])
                count += 1
            for j in range(len(rows)):
                if j != 0 or knn_func in targets["similarity"].values():
                    assert rows[j][count] != 0, f"faild in {knn_func} == 0, rows{j}"
                else:
                    assert rows[j][count] == 0, f"faild in {knn_func} != 0, rows{j} = {rows[j][count]}"
            count += 1

    def wait_create_vector_index(self, sql_request: str, dml: DMLOperations):
        def predicate():
            try:
                dml.query(sql_request)
            except Exception as ex:
                if "No global indexes for table" in str(ex):
                    return False
                raise ex
            return True

        return predicate
