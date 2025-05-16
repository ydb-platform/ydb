import logging
import time

from ydb.tests.datashard.lib.create_table import create_table_sql_request, create_vector_index_sql_request
from ydb.tests.datashard.lib.types_of_variables import format_sql_value, cleanup_type_name
from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.stress.oltp_workload.workload.type.vector_index import WorkloadVectorIndex

logger = logging.getLogger("VectorIndexLargeLevelsAndClustersWorkload")


class WorkloadVectorIndexLargeLevelsAndClusters(WorkloadVectorIndex, WorkloadBase):
    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        self.rows_count = 1000
        self.count_prefix = 1
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
        vector_dimension_data = [500, 1000]
        distance_data = ["cosine"]  # "cosine", "manhattan", "euclidean"
        similarity_data = ["cosine"]  # "inner_product", "cosine"
        vector_type_data = ["float", "int8"]
        prefixs = ["", "prefix_String"]
        covers = [[], [f"col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()]]
        try:
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
                    self.client.query(create_table_sql, True)
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
        except Exception as ex:
            logger.info(f"ERRROR {ex}")

    def _create_index(
        self, table_path, function, distance, vector_type, vector_dimension, levels, clusters, prefix, cover
    ):
        vector_index_sql_request = create_vector_index_sql_request(
            table_name=table_path,
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
        self.client.query(vector_index_sql_request, True)

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
            knn_func=self.targets[function][distance],
            statements=self.create_statements(all_types),
            prefix=prefix,
        )
        self._select_top(
            table_path=table_path,
            vector_type=vector_type,
            knn_func=self.targets[function][distance],
            statements=self.create_statements(all_types),
            prefix=prefix,
        )
        self._drop_index(table_path)

    def _select(self, table_path, vector_type, vector_name, col_name, knn_func, statements, numb, prefix):
        vector = self.get_vector(vector_type, numb)
        select_sql = f"""
                                    $Target = {self.to_binary_string_converters[vector_type].name}(Cast([{vector}] AS List<{vector_type}>));
                                    select {", ".join(statements)}
                                    from {table_path} view idx_vector_{vector_name}
                                    {f"WHERE prefix_String = {prefix}" if prefix != "" else ""}
                                    order by {knn_func}({col_name}, $Target) {"DESC" if knn_func in self.targets["similarity"].values() else "ASC"}
                                    limit {self.rows_count//self.count_prefix};
                                    """
        return self.client.query(select_sql, False)

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
            vector = self.get_vector(vector_type, vector_dimension)
            name = converter.name
            vector_type = converter.vector_type
            values.append(
                f'''(
                    {", ".join([format_sql_value(key, type_name) for type_name in pk_types.key()])},
                    {", ".join([format_sql_value(key, type_name) for type_name in all_types.key()])},
                    {", ".join([format_sql_value(key % self.count_prefix, type_name) for type_name in prefix.key()])}
                    Untag({name}([{vector}]), "{vector_type}"))
                    '''
            )
        upsert_sql = f"""
            UPSERT INTO `{table_name}` (
                {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in pk_types.key()])},
                {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in all_types.key()])},
                {", ".join([f"prefix_{cleanup_type_name(type_name)}" for type_name in prefix.key()])},
                vec_String
            )
            VALUES {",".join(values)};
        """
        self.client.query(upsert_sql, False)

    def create_statements(self, all_types):
        return [f"col_{type_name}" for type_name in all_types.keys()]

    def _wait_inddex_ready(self, table_path, vector_type, knn_func, statements, prefix):
        for _ in range(10):
            time.sleep(7)

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
                )
            except Exception as ex:
                if "No global indexes for table" in str(ex):
                    continue
                raise ex
            logger.info(f"Index {self.index_name} is ready")
            return
        raise Exception("Error getting index status")

    def _select_top(self, table_path, vector_type, knn_func, statements, prefix):
        logger.info("Select values from table")
        if prefix == "":
            try:
                self._select_assert(
                    table_path=table_path,
                    vector_type=vector_type,
                    knn_func=knn_func,
                    statements=statements,
                    numb=1,
                    prefix=prefix,
                )
            except Exception as ex:
                raise ex
        else:
            for numb in range(1, self.count_prefix + 1):
                try:
                    self._select_assert(
                        table_path=table_path,
                        vector_type=vector_type,
                        knn_func=knn_func,
                        statements=statements,
                        numb=numb,
                        prefix=prefix,
                    )
                except Exception as ex:
                    raise ex

    def _select_assert(self, table_path, vector_type, knn_func, statements, numb, prefix):
        result_set = self._select(
            table_path=table_path,
            vector_type=vector_type,
            vector_name="vec_String",
            col_name="vec_String",
            knn_func=knn_func,
            statements=statements,
            numb=numb,
            prefix=prefix,
        )
        if len(result_set) == 0:
            raise Exception("Query returned an empty set")

        rows = result_set[0].rows
        if knn_func == "Knn::InnerProductSimilarity":
            rows.reverse()
        logger.info(f"Rows count {len(rows)}")
        for row in rows:
            cur = row[0]
            for val in row.values():
                if cur != val:
                    raise Exception(f"""incorrect data after the selection: cur {cur}, received {val}""")

    def get_vector(self, type, numb):
        if type == "Float":
            values = [float(i) for i in range(self.size_vector - 1)]
            values.append(float(numb))
            return ",".join(f'{val}f' for val in values)

        values = [i for i in range(self.size_vector - 1)]
        values.append(numb)
        return ",".join(str(val) for val in values)
