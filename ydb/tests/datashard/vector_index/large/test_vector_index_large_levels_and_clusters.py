import time

from ydb.tests.datashard.lib.vector_base import VectorBase
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.vector_index import BinaryStringConverter, get_vector, targets
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import format_sql_value, cleanup_type_name


class TestVectorIndexLargeLevelsAndClusters(VectorBase):
    def setup_method(self):
        self.table_name = "table"
        self.index_name = "idx_vector_vec_String"
        self.rows_count = 1000
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

    def test_vecot_index_large_levels_and_clusters(self):
        table_path = self.table_name
        prefix = {"String": lambda i: f"{i}"}
        vector = {"String": lambda i: f"{i}"}
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
        columns = {"pk_": pk_types.keys(), "col_": all_types.keys(), "prefix_": prefix.keys(), "vec_": vector.keys()}
        pk_columns = {"pk_": pk_types.keys()}
        clusters_data = [10, 30, 50, 100]
        levels_data = [3, 4, 5, 6]
        vector_dimension_data = [5]
        distance_data = ["cosine"]  # "cosine", "manhattan", "euclidean"
        similarity_data = ["cosine"]  # "inner_product", "cosine"
        vector_type_data = ["float", "int8"]
        prefixs = ["", "prefix_String"]
        covers = [[], [f"col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()]]
        for vector_type in vector_type_data:
            for vector_dimension in vector_dimension_data:
                create_table_sql = create_table_sql_request(
                    table_name=table_path,
                    columns=columns,
                    pk_colums=pk_columns,
                    index_colums={},
                    unique="",
                    sync="",
                )
                self.query(create_table_sql)
                self._upsert_values(
                    table_name=table_path,
                    all_types=all_types,
                    prefix=prefix,
                    pk_types=pk_types,
                    vector_type=vector_type,
                    vector_dimension=vector_dimension,
                )

                for cover in covers:
                    for prefix in prefixs:
                        for levels in levels_data:
                            for clusters in clusters_data:
                                for distance in distance_data:
                                    self._check_loop(
                                        table_path=table_path,
                                        function="distance",
                                        distance=distance,
                                        vector_type=vector_type,
                                        vector_dimension=vector_dimension,
                                        levels=levels,
                                        clusters=clusters,
                                        all_types=all_types,
                                        prefix=prefix,
                                        cover=cover,
                                    )

                for cover in covers:
                    for prefix in prefixs:
                        for levels in levels_data:
                            for clusters in clusters_data:
                                for similarity in similarity_data:
                                    self._check_loop(
                                        table_path=table_path,
                                        function="similarity",
                                        distance=similarity,
                                        vector_type=vector_type,
                                        vector_dimension=vector_dimension,
                                        levels=levels,
                                        clusters=clusters,
                                        all_types=all_types,
                                        prefix=prefix,
                                        cover=cover,
                                    )
                self._drop_table(table_path)

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

    def _check_loop(
        self, table_path, function, distance, vector_type, vector_dimension, levels, clusters, all_types, prefix, cover
    ):
        self._create_index(
            table_path=table_path,
            function=function,
            distance=distance,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            levels=levels,
            clusters=clusters,
            prefix=prefix,
            cover=cover,
        )
        self._wait_inddex_ready(
            table_path=table_path,
            vector_type=vector_type,
            knn_func=targets[function][distance],
            statements=self.create_statements(all_types),
            prefix=prefix,
            vector_dimension=vector_dimension,
        )
        self._select_top(
            table_path=table_path,
            vector_type=vector_type,
            knn_func=targets[function][distance],
            statements=self.create_statements(all_types),
            prefix=prefix,
            vector_dimension=vector_dimension,
        )
        self._drop_index(table_path)

    def _select(
        self, table_path, vector_type, vector_name, col_name, knn_func, statements, numb, prefix, vector_dimension
    ):
        vector = get_vector(vector_type, numb, vector_dimension)
        select_sql = f"""
                                    $Target = {self.to_binary_string_converters[vector_type].name}(Cast([{vector}] AS List<{vector_type}>));
                                    select {", ".join(statements)}
                                    from {table_path} view idx_vector_{vector_name}
                                    {f"WHERE prefix_String = {prefix}" if prefix != "" else ""}
                                    order by {knn_func}({col_name}, $Target) {"DESC" if knn_func in targets["similarity"].values() else "ASC"}
                                    limit {self.rows_count//self.count_prefix if prefix != "" else self.rows_count};
                                    """
        return self.query(select_sql)

    def _upsert_values(
        self,
        table_name,
        all_types,
        prefix,
        pk_types,
        vector_type,
        vector_dimension,
    ):
        converter = self.to_binary_string_converters[vector_type]
        values = []

        for key in range(1, self.rows_count + 1):
            vector = get_vector(vector_type, vector_dimension, vector_dimension)
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
            UPSERT INTO `{table_name}` (
                {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])},
                {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()])},
                {", ".join([f"prefix_{cleanup_type_name(type_name)}" for type_name in prefix.keys()])},
                vec_String
            )
            VALUES {",".join(values)};
        """
        self.query(upsert_sql)

    def create_statements(self, all_types):
        return [f"col_{type_name}" for type_name in all_types.keys()]

    def _wait_inddex_ready(self, table_path, vector_type, knn_func, statements, prefix, vector_dimension):
        def predicate():
            try:
                self._select(
                    table_path=table_path,
                    vector_type=vector_type,
                    vector_name="vec_String",
                    col_name="vec_String",
                    knn_func=knn_func,
                    statements=statements,
                    numb=1,
                    prefix=prefix,
                    vector_dimension=vector_dimension,
                )
            except Exception as ex:
                return False
            return True

        wait_for(predicate, timeout_seconds=200, step_seconds=5)
        try:
            self._select(
                table_path=table_path,
                vector_type=vector_type,
                vector_name="vec_String",
                col_name="vec_String",
                knn_func=knn_func,
                statements=statements,
                numb=1,
                prefix=prefix,
                vector_dimension=vector_dimension,
            )
        except Exception as ex:
            assert str(ex) == "Global index", str(ex)

    def _select_top(self, table_path, vector_type, knn_func, statements, prefix, vector_dimension):
        if prefix == "":
            self._select_assert(
                table_path=table_path,
                vector_type=vector_type,
                knn_func=knn_func,
                statements=statements,
                numb=1,
                prefix=prefix,
                vector_dimension=vector_dimension,
            )
        else:
            for numb in range(1, self.count_prefix + 1):
                self._select_assert(
                    table_path=table_path,
                    vector_type=vector_type,
                    knn_func=knn_func,
                    statements=statements,
                    numb=numb,
                    prefix=prefix,
                    vector_dimension=vector_dimension,
                )

    def _select_assert(self, table_path, vector_type, knn_func, statements, numb, prefix, vector_dimension):
        rows = self._select(
            table_path=table_path,
            vector_type=vector_type,
            vector_name="vec_String",
            col_name="vec_String",
            knn_func=knn_func,
            statements=statements,
            numb=numb,
            prefix=prefix,
            vector_dimension=vector_dimension,
        )

        assert len(rows) != 0, "Query returned an empty set"

        if knn_func == "Knn::InnerProductSimilarity":
            rows.reverse()
        for row in rows:
            cur = row[0]
            for val in row.values():
                assert cur == val, f"""incorrect data after the selection: cur {cur}, received {val}"""

    def _drop_index(self, table_path):
        drop_index_sql = f"""
            ALTER TABLE `{table_path}`
            DROP INDEX `{self.index_name}`;
        """
        self.query(drop_index_sql)

    def _drop_table(self, table_path):
        drop_table_sql = f"""
            DROP TABLE `{table_path}`;
        """
        self.query(drop_table_sql)
