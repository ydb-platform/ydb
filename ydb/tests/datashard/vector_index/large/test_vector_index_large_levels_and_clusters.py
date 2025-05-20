from ydb.tests.datashard.lib.vector_base import VectorBase
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.datashard.lib.vector_index import BinaryStringConverter, targets, VectorIndexOperations
from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name


class TestVectorIndexLargeLevelsAndClusters(VectorBase):
    def setup_method(self):
        self.vector_index = VectorIndexOperations(self)
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
        prefix_data = {"String": lambda i: f"{i}"}
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
        columns = {"pk_": pk_types.keys(), "col_": all_types.keys(), "prefix_": prefix_data.keys(), "vec_": vector.keys()}
        pk_columns = {"pk_": pk_types.keys()}
        dimensions = [(5, 10), (4, 50), (3, 100)]
        vector_dimension_data = [5]
        distance_data = ["cosine"]  # "cosine", "manhattan", "euclidean"
        similarity_data = ["cosine"]  # "inner_product", "cosine"
        vector_type_data = ["float", "int8"]
        prefixs = ["", "prefix_String"]
        covers = [[], [f"col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()]]
        for vector_type in vector_type_data:
            for vector_dimension in vector_dimension_data:
                print(f"vector_type: {vector_type}, vector_dimension: {vector_dimension}")
                create_table_sql = create_table_sql_request(
                    table_name=table_path,
                    columns=columns,
                    pk_colums=pk_columns,
                    index_colums={},
                    unique="",
                    sync="",
                )
                self.query(create_table_sql)
                self.vector_index._upsert_values(
                    table_name=table_path,
                    all_types=all_types,
                    prefix=prefix_data,
                    pk_types=pk_types,
                    vector_type=vector_type,
                    vector_dimension=vector_dimension,
                    to_binary_string_converters=self.to_binary_string_converters,
                    rows_count=self.rows_count,
                    count_prefix=self.count_prefix,
                )

                for cover in covers:
                    for prefix in prefixs:
                        for levels, clusters in dimensions:
                            for distance in distance_data:
                                print(
                                    f"cover: {cover}, prefix: {prefix}, levels: {levels}, clusters: {clusters}, distance: {distance}"
                                )
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
                        for levels, clusters in dimensions:
                            for similarity in similarity_data:
                                print(
                                    f"cover: {cover}, prefix: {prefix}, levels: {levels}, clusters: {clusters}, similarity: {similarity}"
                                )
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

    def create_statements(self, all_types):
        return [f"col_{type_name}" for type_name in all_types.keys()]

    def _wait_inddex_ready(self, table_path, vector_type, knn_func, statements, prefix, vector_dimension):
        def predicate():
            try:
                self.vector_index._select(
                    table_path=table_path,
                    vector_type=vector_type,
                    vector_name="vec_String",
                    col_name="vec_String",
                    knn_func=knn_func,
                    statements=statements,
                    numb=1,
                    prefix=prefix,
                    vector_dimension=vector_dimension,
                    to_binary_string_converters=self.to_binary_string_converters,
                    rows_count=self.rows_count,
                    count_prefix=self.count_prefix
                )
            except Exception:
                return False
            return True

        wait_for(predicate, timeout_seconds=200, step_seconds=5)
        try:
            self.vector_index._select(
                table_path=table_path,
                vector_type=vector_type,
                vector_name="vec_String",
                col_name="vec_String",
                knn_func=knn_func,
                statements=statements,
                numb=1,
                prefix=prefix,
                vector_dimension=vector_dimension,
                to_binary_string_converters=self.to_binary_string_converters,
                rows_count=self.rows_count,
                count_prefix=self.count_prefix
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
        rows = self.vector_index._select(
            table_path=table_path,
            vector_type=vector_type,
            vector_name="vec_String",
            col_name="vec_String",
            knn_func=knn_func,
            statements=statements,
            numb=numb,
            prefix=prefix,
            vector_dimension=vector_dimension,
            to_binary_string_converters=self.to_binary_string_converters,
            rows_count=self.rows_count,
            count_prefix=self.count_prefix
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
