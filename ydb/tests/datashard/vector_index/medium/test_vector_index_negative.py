import ydb

from ydb.tests.datashard.lib.vector_base import VectorBase
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_vector_index_sql_request
from ydb.tests.datashard.lib.vector_index import targets, get_vector, BinaryStringConverter
from ydb.tests.datashard.lib.types_of_variables import (
    cleanup_type_name,
    format_sql_value,
)


class TestVectorIndexNegative(VectorBase):
    def setup_method(self):
        self.table_name = "table"
        self.index_name = "idx_vector_vec_String"
        self.rows_count = 10
        self.count_prefix = 5
        self.to_binary_string_converters = {
            "float": BinaryStringConverter(
                name="Knn::ToBinaryStringFloat", data_type="Float", vector_type="FloatVector"
            ),
            "uint8": BinaryStringConverter(
                name="Knn::ToBinaryStringUint8", data_type="Uint8", vector_type="Uint8Vector"
            ),
            "int8": BinaryStringConverter(name="Knn::ToBinaryStringInt8", data_type="Int8", vector_type="Int8Vector"),
        }

    def test_vector_index_negative(self):
        prefix_data = {"String": lambda i: f"{i}"}
        vector = {"String": lambda i: f"{i}"}
        vector_type = "uint8"
        vector_dimension = 5
        all_types = {
            "Int64": lambda i: i,
            "Uint64": lambda i: i,
            "Int32": lambda i: i,
            "Uint32": lambda i: i,
        }
        pk_types = {
            "Int64": lambda i: i,
            "Uint64": lambda i: i,
            "Int32": lambda i: i,
            "Uint32": lambda i: i,
        }
        columns = {
            "pk_": pk_types.keys(),
            "col_": all_types.keys(),
            "prefix_": prefix_data.keys(),
            "vec_": vector.keys(),
        }
        pk_columns = {"pk_": pk_types.keys()}
        prefixes = ["", "prefix_String"]
        covers = [[], [f"col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()]]
        create_table_sql = create_table_sql_request(
            table_name=self.table_name,
            columns=columns,
            pk_columns=pk_columns,
            index_columns={},
            unique="",
            sync="",
        )
        self.query(create_table_sql)
        for prefix in prefixes:
            for cover in covers:
                self._upsert_values(
                    all_types=all_types,
                    prefix=prefix_data,
                    pk_types=pk_types,
                    vector_type=vector_type,
                    vector_dimension=vector_dimension,
                    to_binary_string_converters=self.to_binary_string_converters,
                )
                try:
                    self._select(
                        vector_type=vector_type,
                        vector_name="vec_String",
                        col_name="vec_String",
                        knn_func="Knn::ManhattanDistance",
                        numb=1,
                        prefix=prefix,
                        vector_dimension=vector_dimension,
                        to_binary_string_converters=self.to_binary_string_converters,
                    )
                except ydb.issues.SchemeError as ex:
                    if "No global indexes for table /Root/table" not in str(ex):
                        raise ex

                self._create_index(
                    table_path=self.table_name,
                    function="distance",
                    distance="manhattan",
                    vector_type=vector_type,
                    vector_dimension=vector_dimension,
                    levels=1,
                    clusters=10,
                    prefix=prefix,
                    cover=cover,
                )

                try:
                    self._select(
                        vector_type=vector_type,
                        vector_name="vec_String",
                        col_name="vec_String",
                        knn_func="Knn::EuclideanDistance",
                        numb=1,
                        prefix=prefix,
                        vector_dimension=vector_dimension,
                        to_binary_string_converters=self.to_binary_string_converters,
                    )
                except ydb.issues.InternalError as ex:
                    if "Given predicate is not suitable for used index: idx_vector_vec_String" not in str(ex):
                        raise ex
                self.drop_index()

    def _create_index(
        self, table_path, function, distance, vector_type, vector_dimension, levels, clusters, prefix, cover
    ):
        vector_index_sql_request = create_vector_index_sql_request(
            table_name=table_path,
            name_vector_index="idx_vector_vec_String",
            embedding="vec_String",
            prefix=prefix,
            function=function,
            distance=distance,
            vector_type=vector_type,
            sync="",
            vector_dimension=vector_dimension,
            levels=levels,
            clusters=clusters,
            cover=cover,
        )
        self.query(vector_index_sql_request)

    def _upsert_values(
        self,
        all_types,
        prefix,
        pk_types,
        vector_type,
        vector_dimension,
        to_binary_string_converters,
    ):
        converter = to_binary_string_converters[vector_type]
        values = []

        for key in range(1, self.rows_count + 1):
            vector = get_vector(vector_type, key % 127, vector_dimension)
            name = converter.name
            vector_type = converter.vector_type
            values.append(
                f'''(
                    {", ".join([format_sql_value(key, type_name) for type_name in pk_types.keys()])},
                    {", ".join([format_sql_value(key, type_name) for type_name in all_types.keys()])},
                    {", ".join([format_sql_value(key % self.count_prefix, type_name) for type_name in prefix.keys()])},
                    Untag({name}([{vector}]), "{vector_type}")
                    )
                    '''
            )
        upsert_sql = f"""
            UPSERT INTO `{self.table_name}` (
                {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])},
                {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()])},
                {", ".join([f"prefix_{cleanup_type_name(type_name)}" for type_name in prefix.keys()])},
                vec_String
            )
            VALUES {",".join(values)};
        """
        self.query(upsert_sql)

    def _select(
        self,
        vector_type,
        vector_name,
        col_name,
        knn_func,
        numb,
        prefix,
        vector_dimension,
        to_binary_string_converters,
    ):
        vector = get_vector(vector_type, numb, vector_dimension)
        select_sql = f"""
                                        $Target = {to_binary_string_converters[vector_type].name}(Cast([{vector}] AS List<{vector_type}>));
                                        select  *
                                        from {self.table_name} view idx_vector_{vector_name}
                                        {f"WHERE prefix_String = {prefix}" if prefix != "" else ""}
                                        order by {knn_func}({col_name}, $Target) {"DESC" if knn_func in targets["similarity"].values() else "ASC"}
                                        limit 10;
                                        """
        return self.query(select_sql)

    def drop_index(self):
        drop_index_sql = f"""
            ALTER TABLE `{self.table_name}`
            DROP INDEX `{self.index_name}`;
        """
        self.query(drop_index_sql)
