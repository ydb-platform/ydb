import logging
import random
import time

from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("VectorIndexWorkload")


class BinaryStringConverter:
    def __init__(self, name, data_type, vector_type):
        self.name = name
        self.data_type = data_type
        self.vector_type = vector_type


class WorkloadVectorIndex(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "vector_index", stop)
        self.table_name = "table"
        self.index_name = "vector_idx"
        self.rows_count = 100
        self.limit = 10
        self.to_binary_string_converters = {
            "float": BinaryStringConverter(
                name="Knn::ToBinaryStringFloat",
                data_type="Float",
                vector_type="FloatVector"
            ),
            "uint8": BinaryStringConverter(
                name="Knn::ToBinaryStringUint8",
                data_type="Uint8",
                vector_type="Uint8Vector"
            ),
            "int8": BinaryStringConverter(
                name="Knn::ToBinaryStringInt8",
                data_type="Int8",
                vector_type="Int8Vector"
            ),
        }
        self.targets = {
            "similarity": {
                "inner_product": "Knn::InnerProductSimilarity",
                "cosine": "Knn::CosineSimilarity"
            },
            "distance": {
                "cosine": "Knn::CosineDistance",
                "manhattan": "Knn::ManhattanDistance",
                "euclidean": "Knn::EuclideanDistance"
            }
        }

    def _get_random_vector(self, type, size):
        if type == "float":
            values = [round(random.uniform(-100, 100), 2) for _ in range(size)]
            return ",".join(f'{val}f' for val in values)

        if type == "uint8":
            values = [random.randint(0, 255) for _ in range(size)]
        else:
            values = [random.randint(-127, 127) for _ in range(size)]
        return ",".join(str(val) for val in values)

    def _create_table(self, table_path):
        logger.info(f"Create table {table_path}")
        create_table_sql = f"""
            CREATE TABLE `{table_path}` (
                pk Uint64,
                embedding String,
                PRIMARY KEY(pk)
            );
        """
        self.client.query(create_table_sql, True)

    def _drop_table(self, table_path):
        logger.info(f"Drop table {table_path}")
        drop_table_sql = f"""
            DROP TABLE `{table_path}`;
        """
        self.client.query(drop_table_sql, True)

    def _drop_index(self, table_path):
        logger.info(f"Drop index {self.index_name}")
        drop_index_sql = f"""
            ALTER TABLE `{table_path}`
            DROP INDEX `{self.index_name}`;
        """
        self.client.query(drop_index_sql, True)

    def _create_index(self, table_path, vector_type,
                      vector_dimension, levels, clusters,
                      distance=None, similarity=None):
        logger.info(f"""Create index vector_type={vector_type},
                        vector_dimension={vector_dimension},
                        levels={levels}, clusters={clusters},
                        distance={distance},
                        similarity={similarity}""")
        if distance is not None:
            create_index_sql = f"""
                ALTER TABLE `{table_path}`
                ADD INDEX `{self.index_name}` GLOBAL USING vector_kmeans_tree
                ON (embedding)
                WITH (distance={distance},
                      vector_type={vector_type},
                      vector_dimension={vector_dimension},
                      levels={levels},
                      clusters={clusters}
                );
            """
        else:
            create_index_sql = f"""
                ALTER TABLE `{table_path}`
                ADD INDEX `{self.index_name}` GLOBAL USING vector_kmeans_tree
                ON (embedding)
                WITH (similarity={similarity},
                      vector_type={vector_type},
                      vector_dimension={vector_dimension},
                      levels={levels},
                      clusters={clusters}
                );
            """
        logger.info(create_index_sql)
        self.client.query(create_index_sql, True)

    def _upsert_values(self, table_path, vector_type, vector_dimension):
        logger.info("Upsert values")
        converter = self.to_binary_string_converters[vector_type]
        values = []

        for key in range(self.rows_count):
            vector = self._get_random_vector(vector_type, vector_dimension)
            name = converter.name
            vector_type = converter.vector_type
            values.append(
                f'({key}, Untag({name}([{vector}]), "{vector_type}"))'
            )

        upsert_sql = f"""
            UPSERT INTO `{table_path}` (pk, embedding)
            VALUES {",".join(values)};
        """
        self.client.query(upsert_sql, False)

    def _select(self, table_path, vector_type,
                vector_dimension, distance, similarity):
        if distance is not None:
            target = self.targets["distance"][distance]
        else:
            target = self.targets["similarity"][similarity]
        order = "ASC" if distance is not None else "DESC"
        vector = self._get_random_vector(vector_type, vector_dimension)
        converter = self.to_binary_string_converters[vector_type]
        name = converter.name
        data_type = converter.data_type
        select_sql = f"""
            $Target = {name}(Cast([{vector}] AS List<{data_type}>));
            SELECT pk, embedding, {target}(embedding, $Target) as target
            FROM `{table_path}`
            VIEW `{self.index_name}`
            ORDER BY {target}(embedding, $Target) {order}
            LIMIT {self.limit};
        """
        return self.client.query(select_sql, False)

    def _select_top(self, table_path, vector_type,
                    vector_dimension, distance, similarity):
        logger.info("Select values from table")
        result_set = self._select(
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            distance=distance,
            similarity=similarity
        )
        if len(result_set) == 0:
            raise Exception("Query returned an empty set")

        rows = result_set[0].rows
        logger.info(f"Rows count {len(rows)}")

        prev = 0.0 if distance is not None else 1.0
        for row in rows:
            cur = row['target']
            condition = prev <= cur if distance is not None else prev >= cur
            if not condition:
                raise Exception(f"""The set of rows does not satisfy the
                                    condition, prev: {prev}, cur: {cur}""")
            prev = cur

    def _wait_inddex_ready(self, table_path, vector_type,
                           vector_dimension, distance, similarity):
        for i in range(10):
            time.sleep(7)

            try:
                self._select(
                    table_path=table_path,
                    vector_type=vector_type,
                    vector_dimension=vector_dimension,
                    distance=distance,
                    similarity=similarity
                )
            except Exception as ex:
                if "No global indexes for table" in str(ex):
                    continue
                raise ex
            logger.info(f"Index {self.index_name} is ready")
            return
        raise Exception("Error getting index status")

    def _check_loop(self, table_path, vector_type,
                    vector_dimension, levels, clusters,
                    distance=None, similarity=None):
        self._create_index(
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            levels=levels,
            clusters=clusters,
            distance=distance,
            similarity=similarity
        )
        self._wait_inddex_ready(
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            distance=distance,
            similarity=similarity
        )
        self._select_top(
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            distance=distance,
            similarity=similarity
        )
        self._drop_index(table_path)
        logger.info('check was completed successfully')

    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        distance_data = ["cosine"]  # "cosine", "manhattan", "euclidean"
        similarity_data = ["cosine"]  # "inner_product", "cosine"
        vector_type_data = ["float", "int8"]
        levels_data = [1, 3]
        clusters_data = [1, 17]
        vector_dimension_data = [5]

        try:
            for vector_type in vector_type_data:
                for vector_dimension in vector_dimension_data:
                    self._create_table(table_path)

                    self._upsert_values(
                        table_path=table_path,
                        vector_type=vector_type,
                        vector_dimension=vector_dimension
                    )

                    for levels in levels_data:
                        for clusters in clusters_data:
                            for distance in distance_data:
                                self._check_loop(
                                    table_path=table_path,
                                    vector_type=vector_type,
                                    vector_dimension=vector_dimension,
                                    levels=levels,
                                    clusters=clusters,
                                    distance=distance
                                )

                    for levels in levels_data:
                        for clusters in clusters_data:
                            for similarity in similarity_data:
                                self._check_loop(
                                    table_path=table_path,
                                    vector_type=vector_type,
                                    vector_dimension=vector_dimension,
                                    levels=levels,
                                    clusters=clusters,
                                    similarity=similarity
                                )

                    self._drop_table(table_path)
        except Exception as ex:
            logger.info(f"ERRROR {ex}")

    def get_stat(self):
        return ""

    def get_workload_thread_funcs(self):
        return [self._loop]
