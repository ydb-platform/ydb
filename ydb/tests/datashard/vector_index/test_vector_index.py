import pytest

from ydb.tests.datashard.lib.vector_base import VectorBase
from ydb.tests.datashard.lib.dml_operations import DMLOperations
from ydb.tests.datashard.lib.create_table import create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
    pk_types,
    non_pk_types,
    index_first,
    index_second,
    ttl_types,
    index_first_sync,
    index_second_sync,
    index_three_sync,
    index_three_sync_not_Bool,
    index_four_sync,
    index_zero_sync,
)


class TestVectorIndex(VectorBase):
    @pytest.mark.parametrize(
        "table_name, pk_types, all_types, index, ttl, unique, sync, vector_type",
        [
            ("table_index_4_UNIQUE_SYNC_float", pk_types, {}, index_four_sync, "", "UNIQUE", "SYNC", "Float"),
            ("table_index_3_UNIQUE_SYNC_float", pk_types, {}, index_three_sync_not_Bool, "", "UNIQUE", "SYNC", "Float"),
            ("table_index_2_UNIQUE_SYNC_float", pk_types, {}, index_second_sync, "", "UNIQUE", "SYNC", "Float"),
            ("table_index_1_UNIQUE_SYNC_float", pk_types, {}, index_first_sync, "", "UNIQUE", "SYNC", "Float"),
            ("table_index_0_UNIQUE_SYNC_float", pk_types, {}, index_zero_sync, "", "UNIQUE", "SYNC", "Float"),
            ("table_index_4__SYNC_float", pk_types, {}, index_four_sync, "", "", "SYNC", "Float"),
            ("table_index_3__SYNC_float", pk_types, {}, index_three_sync, "", "", "SYNC", "Float"),
            ("table_index_2__SYNC_float", pk_types, {}, index_second_sync, "", "", "SYNC", "Float"),
            ("table_index_1__SYNC_float", pk_types, {}, index_first_sync, "", "", "SYNC", "Float"),
            ("table_index_0__SYNC_float", pk_types, {}, index_zero_sync, "", "", "SYNC", "Float"),
            ("table_index_1__ASYNC_float", pk_types, {}, index_second, "", "", "ASYNC", "Float"),
            ("table_index_0__ASYNC_float", pk_types, {}, index_first, "", "", "ASYNC", "Float"),
            ("table_all_types_float", pk_types, {**pk_types, **non_pk_types}, {}, "", "", "", "Float"),
            ("table_index_4_UNIQUE_SYNC", pk_types, {}, index_four_sync, "", "UNIQUE", "SYNC", "Uint8"),
            ("table_index_3_UNIQUE_SYNC", pk_types, {}, index_three_sync_not_Bool, "", "UNIQUE", "SYNC", "Uint8"),
            ("table_index_2_UNIQUE_SYNC", pk_types, {}, index_second_sync, "", "UNIQUE", "SYNC", "Uint8"),
            ("table_index_1_UNIQUE_SYNC", pk_types, {}, index_first_sync, "", "UNIQUE", "SYNC", "Uint8"),
            ("table_index_0_UNIQUE_SYNC", pk_types, {}, index_zero_sync, "", "UNIQUE", "SYNC", "Uint8"),
            ("table_index_4__SYNC", pk_types, {}, index_four_sync, "", "", "SYNC", "Uint8"),
            ("table_index_3__SYNC", pk_types, {}, index_three_sync, "", "", "SYNC", "Uint8"),
            ("table_index_2__SYNC", pk_types, {}, index_second_sync, "", "", "SYNC", "Uint8"),
            ("table_index_1__SYNC", pk_types, {}, index_first_sync, "", "", "SYNC", "Uint8"),
            ("table_index_0__SYNC", pk_types, {}, index_zero_sync, "", "", "SYNC", "Uint8"),
            ("table_index_1__ASYNC", pk_types, {}, index_second, "", "", "ASYNC", "Uint8"),
            ("table_index_0__ASYNC", pk_types, {}, index_first, "", "", "ASYNC", "Uint8"),
            ("table_all_types", pk_types, {**pk_types, **non_pk_types}, {}, "", "", "", "Uint8"),
        ],
    )
    def test_vector_index(
        self,
        table_name: str,
        pk_types: dict[str, str],
        all_types: dict[str, str],
        index: dict[str, str],
        ttl: str,
        unique: str,
        sync: str,
        vector_type: str,
    ):
        self.size_vector = 10
        self.knn_type = {"Float": "ToBinaryStringFloat", "Uint8": "ToBinaryStringUint8"}
        self.targets = {
            "similarity": {"inner_product": "Knn::InnerProductSimilarity", "cosine": "Knn::CosineSimilarity"},
            "distance": {
                "cosine": "Knn::CosineDistance",
                "manhattan": "Knn::ManhattanDistance",
                "euclidean": "Knn::EuclideanDistance",
            },
        }
        dml = DMLOperations(self)
        all_types["String"] = lambda i: f"String {i}"
        self.sync = ["", "SYNC"]
        dimensions = [{"levels": 1, "claster": 100}, {"levels": 2, "claster": 50}]
        for dimension in dimensions:
            for sync in self.sync:
                for target in self.targets.keys():
                    for distance in self.targets[target].keys():
                        table_name_distance = f"{table_name}_{distance}_{target}"
                        dml.create_table(table_name_distance, pk_types, all_types, index, ttl, unique, sync)
                        self.vectors = []
                        self.upsert(table_name_distance, all_types, pk_types, index, ttl, vector_type)
                        cover = []
                        for type_name in all_types.keys():
                            if type_name != "String":
                                cover.append("col_" + cleanup_type_name(type_name))
                        sql_create_vector_index = create_vector_index_sql_request(
                            table_name_distance,
                            "col_String",
                            target,
                            distance,
                            vector_type.lower(),
                            self.size_vector,
                            dimension["levels"],
                            dimension["claster"],
                            cover,
                        )
                        print(sql_create_vector_index)
                        dml.query(sql_create_vector_index)
                        self.select(
                            table_name_distance,
                            "col_String",
                            vector_type,
                            pk_types,
                            all_types,
                            index,
                            ttl,
                            self.targets[target][distance],
                            dml,
                        )

    def get_vector(self, type, numb):
        if type == "Float":
            values = [float(i) for i in range(self.size_vector - 1)]
            values.append(float(numb))
            return ",".join(f'{val}f' for val in values)

        values = [i for i in range(self.size_vector - 1)]
        values.append(numb)
        return ",".join(str(val) for val in values)

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
        vector = self.get_vector(vector_type, value)
        self.vectors.append(vector)
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

    def select(self, table_name, col_name, vector_type, pk_types, all_types, index, ttl, knn_func, dml: DMLOperations):
        statements = dml.create_statements(pk_types, all_types, index, ttl)
        statements.remove("col_String")
        statements.append(f"{knn_func}(col_String, $Target)")
        vector = self.get_vector(vector_type, 1)
        rows = dml.query(
            f"""
                                    $Target = Knn::{self.knn_type[vector_type]}(Cast([{vector}] AS List<{vector_type}>));
                                    select {", ".join(statements)}
                                    from {table_name} view idx_vector_{col_name}
                                    order by {knn_func}(col_String, $Target) {"DESC" if knn_func in self.targets["similarity"].values() else "ASC"}
                                    limit 100;
                                    """
        )
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
            if i != 0 or knn_func in self.targets["similarity"].values():
                assert rows[i][count] != 0, f"faild in {knn_func} == 0, rows = {i}"
            else:
                assert rows[i][count] == 0, f"faild in {knn_func} != 0, rows{i} = {rows[i][count]}"
        count += 1
