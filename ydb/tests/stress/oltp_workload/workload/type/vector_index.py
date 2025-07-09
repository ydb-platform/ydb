import logging
import random
import time
from itertools import cycle, product

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.datashard.lib.vector_index import targets, to_binary_string_converters

logger = logging.getLogger("VectorIndexWorkload")


class WorkloadVectorIndex(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "vector_index", stop)
        self.table_name = "table"
        self.index_name_prefix = "vector_idx"
        self.rows_count = 10
        self.limit = 10

    def _get_random_vector(self, type, size):
        logger.info(f"random vector type: {type}")
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

    def _drop_index(self, index_name, table_path):
        logger.info(f"Drop index {index_name}")
        drop_index_sql = f"""
            ALTER TABLE `{table_path}`
            DROP INDEX `{index_name}`;
        """
        self.client.query(drop_index_sql, True)

    def _create_index(
        self, index_name, table_path, vector_type, vector_dimension, levels, clusters, distance=None, similarity=None
    ):
        logger.info(
            f"""Create index vector_type={vector_type},
                        vector_dimension={vector_dimension},
                        levels={levels}, clusters={clusters},
                        distance={distance},
                        similarity={similarity}"""
        )
        if distance is not None:
            create_index_sql = f"""
                ALTER TABLE `{table_path}`
                ADD INDEX `{index_name}` GLOBAL USING vector_kmeans_tree
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
                ADD INDEX `{index_name}` GLOBAL USING vector_kmeans_tree
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
        converter = to_binary_string_converters[vector_type]
        values = []

        for key in range(self.rows_count):
            vector = self._get_random_vector(vector_type, vector_dimension)
            name = converter.name
            vector_types = converter.vector_type
            values.append(f'({key}, Untag({name}([{vector}]), "{vector_types}"))')

        upsert_sql = f"""
            UPSERT INTO `{table_path}` (pk, embedding)
            VALUES {",".join(values)};
        """
        self.client.query(upsert_sql, False)

    def _select(self, index_name, table_path, vector_type, vector_dimension, distance, similarity):
        if distance is not None:
            target = targets["distance"][distance]
        else:
            target = targets["similarity"][similarity]
        order = "ASC" if distance is not None else "DESC"
        vector = self._get_random_vector(vector_type, vector_dimension)
        converter = to_binary_string_converters[vector_type]
        name = converter.name
        data_type = converter.data_type
        select_sql = f"""
            $Target = {name}(Cast([{vector}] AS List<{data_type}>));
            SELECT pk, embedding, {target}(embedding, $Target) as target
            FROM `{table_path}`
            VIEW `{index_name}`
            ORDER BY {target}(embedding, $Target) {order}
            LIMIT {self.limit};
        """
        return self.client.query(select_sql, False)

    def _select_top(self, index_name, table_path, vector_type, vector_dimension, distance, similarity):
        logger.info("Select values from table")
        result_set = self._select(
            index_name=index_name,
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            distance=distance,
            similarity=similarity,
        )
        if len(result_set) == 0:
            raise Exception("Query returned an empty set")

        rows = result_set[0].rows
        logger.info(f"Rows count {len(rows)}")

        prev = rows[0]['target']
        for row in rows:
            cur = row['target']
            condition = prev <= cur if distance is not None else prev >= cur
            if not condition:
                raise Exception(
                    f"""The set of rows does not satisfy the
                                    condition, prev: {prev}, cur: {cur}"""
                )
            prev = cur

    def _wait_inddex_ready(self, index_name, table_path, vector_type, vector_dimension, distance, similarity):
        start_time = time.time()
        while time.time() - start_time < 60:
            time.sleep(5)
            try:
                res = self._select(
                    index_name=index_name,
                    table_path=table_path,
                    vector_type=vector_type,
                    vector_dimension=vector_dimension,
                    distance=distance,
                    similarity=similarity,
                )
                if len(res) == 0 or len(res[0].rows) == 0 or res[0].rows[0]['target'] is None:
                    continue
            except Exception as ex:
                if "No global indexes for table" in str(ex):
                    continue
                raise ex
            logger.info(f"Index {index_name} is ready")
            return
        raise Exception("Error getting index status")

    def _check_loop(self, table_path, vector_type, vector_dimension, levels, clusters, distance=None, similarity=None):
        index_name = f"{self.index_name_prefix}_{vector_type}_{vector_dimension}_{levels}_{clusters}_{distance}_{similarity}"
        self._create_index(
            index_name=index_name,
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            levels=levels,
            clusters=clusters,
            distance=distance,
            similarity=similarity,
        )
        self._wait_inddex_ready(
            index_name=index_name,
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            distance=distance,
            similarity=similarity,
        )
        self._select_top(
            index_name=index_name,
            table_path=table_path,
            vector_type=vector_type,
            vector_dimension=vector_dimension,
            distance=distance,
            similarity=similarity,
        )
        if random.randint(0, 1) == 0:
            self._create_index(
                index_name=index_name+'Rename',
                table_path=table_path,
                vector_type=vector_type,
                vector_dimension=vector_dimension,
                levels=levels,
                clusters=clusters,
                distance=distance,
                similarity=similarity,
            )
            self.client.replace_index(table_path, index_name+'Rename', index_name)
        self._drop_index(index_name, table_path)
        logger.info('check was completed successfully')

    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        distance_data = ["cosine", "manhattan", "euclidean"]
        similarity_data = ["cosine", "inner_product"]
        vector_type_data = ["float", "int8", "uint8"]
        levels_data = [1, 3]
        clusters_data = [1, 17]
        vector_dimension_data = [5]
        self._create_table(table_path)

        distance_index_data = list(product(vector_type_data, vector_dimension_data, levels_data, clusters_data, distance_data))
        similarity_index_data = list(product(vector_type_data, vector_dimension_data, levels_data, clusters_data, similarity_data))

        random.shuffle(distance_index_data)
        random.shuffle(similarity_index_data)
        distance_iter = cycle(distance_index_data)
        similarity_iter = cycle(similarity_index_data)

        while not self.is_stop_requested():
            distance_idx_data = next(distance_iter)
            try:
                self._upsert_values(
                    table_path=table_path, vector_type=distance_idx_data[0], vector_dimension=distance_idx_data[1]
                )

                self._check_loop(
                    table_path=table_path,
                    vector_type=distance_idx_data[0],
                    vector_dimension=distance_idx_data[1],
                    levels=distance_idx_data[2],
                    clusters=distance_idx_data[3],
                    distance=distance_idx_data[4])
                similarity_idx_data = next(similarity_iter)

                self._upsert_values(
                    table_path=table_path, vector_type=similarity_idx_data[0], vector_dimension=similarity_idx_data[1]
                )
                self._check_loop(
                    table_path=table_path,
                    vector_type=similarity_idx_data[0],
                    vector_dimension=similarity_idx_data[1],
                    levels=similarity_idx_data[2],
                    clusters=similarity_idx_data[3],
                    similarity=similarity_idx_data[4])
            except Exception as ex:
                logger.info(f"ERROR {ex}")
                raise ex
        self._drop_table(table_path)

    def get_stat(self):
        return ""

    def get_workload_thread_funcs(self):
        return [self._loop]
