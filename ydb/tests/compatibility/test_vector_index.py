import pytest
import time
import random

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestVectorIndex(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.table_name = "table"
        self.rows_count = 15
        self.index_name = "vector_idx"
        self.vector_dimension = 5
        yield from self.setup_cluster(extra_feature_flags={"enable_vector_index": True})

    def _get_random_vector(self, vector_type, size):
        if vector_type == "FloatVector":
            values = [round(random.uniform(-100, 100), 2) for _ in range(size)]
            return ",".join(f'{val}f' for val in values)

        if vector_type == "Uint8Vector":
            values = [random.randint(0, 255) for _ in range(size)]
        else:
            values = [random.randint(-127, 127) for _ in range(size)]
        return ",".join(str(val) for val in values)

    def _create_index(self, vector_type, distance=None, similarity=None):
        if distance is not None:
            create_index_sql = f"""
                ALTER TABLE {self.table_name}
                ADD INDEX `{self.index_name}` GLOBAL USING vector_kmeans_tree
                ON (vec)
                WITH (distance={distance},
                      vector_type={vector_type},
                      vector_dimension={self.vector_dimension},
                      levels=2,
                      clusters=10
                );
            """
        else:
            create_index_sql = f"""
                ALTER TABLE {self.table_name}
                ADD INDEX `{self.index_name}` GLOBAL USING vector_kmeans_tree
                ON (vec)
                WITH (similarity={similarity},
                      vector_type={vector_type},
                      vector_dimension={self.vector_dimension},
                      levels=2,
                      clusters=10
                );
            """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create_index_sql)

    def wait_index_ready(self, targets, vector_types, vector_type, distance, order, distance_func):
        def predicate():
            try:
                self.select_from_index(
                    target=targets[distance][distance_func],
                    name=vector_types[vector_type],
                    data_type=vector_type,
                    order=order,
                )
            except Exception as ex:
                if "No global indexes for table" in str(ex):
                    return False
                raise ex
            return True
        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Error getting index status"

    def _drop_index(self):
        drop_index_sql = f"""
            ALTER TABLE {self.table_name}
            DROP INDEX `{self.index_name}`;
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(drop_index_sql)

    def write_data(self, name, vector_type):
        values = []
        for key in range(self.rows_count):
            vector = self._get_random_vector(vector_type, self.vector_dimension)
            values.append(f'({key}, Untag({name}([{vector}]), "{vector_type}"))')

        upsert_sql = f"""
            UPSERT INTO `{self.table_name}` (key, vec)
            VALUES {",".join(values)};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(upsert_sql)

    def select_from_index(self, target, name, data_type, order):
        vector = self._get_random_vector(f"{data_type}Vector", self.vector_dimension)
        select_sql = f"""
            $Target = {name}(Cast([{vector}] AS List<{data_type}>));
            SELECT key, vec, {target}(vec, $Target) as target
            FROM {self.table_name}
            VIEW `{self.index_name}`
            ORDER BY {target}(vec, $Target) {order}
            LIMIT {self.rows_count};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result_sets = session_pool.execute_with_retries(select_sql)
            print(result_sets[0].rows)
            assert len(result_sets[0].rows) > 0, "Query returned an empty set"
            rows = result_sets[0].rows
            for row in rows:
                assert row['target'] is not None, "the distance is None"

    def create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE {self.table_name} (
                    key Int64 NOT NULL,
                    vec String NOT NULL,
                    PRIMARY KEY (key)
                )
                """
            session_pool.execute_with_retries(query)

    @pytest.mark.parametrize(
        "vector_type, distance, distance_func",
        [
            ("Uint8", "similarity", "inner_product"),
            ("Int8", "similarity", "inner_product"),
            ("Float", "similarity", "inner_product"),
            ("Uint8", "similarity", "cosine"),
            ("Int8", "similarity", "cosine"),
            ("Float", "similarity", "cosine"),
            ("Uint8", "distance", "cosine"),
            ("Int8", "distance", "cosine"),
            ("Float", "distance", "cosine"),
            ("Uint8", "distance", "manhattan"),
            ("Int8", "distance", "manhattan"),
            ("Float", "distance", "manhattan"),
            ("Uint8", "distance", "euclidean"),
            ("Int8", "distance", "euclidean"),
            ("Float", "distance", "euclidean"),
        ],
    )
    def test_vector_index(self, vector_type, distance, distance_func):
        vector_types = {
            "Uint8": "Knn::ToBinaryStringUint8",
            "Int8": "Knn::ToBinaryStringInt8",
            "Float": "Knn::ToBinaryStringFloat",
        }
        targets = {
            "similarity": {"inner_product": "Knn::InnerProductSimilarity", "cosine": "Knn::CosineSimilarity"},
            "distance": {
                "cosine": "Knn::CosineDistance",
                "manhattan": "Knn::ManhattanDistance",
                "euclidean": "Knn::EuclideanDistance",
            },
        }
        self.create_table()

        self.write_data(name=vector_types[vector_type], vector_type=f"{vector_type}Vector")
        if distance == "similarity":
            self._create_index(
                vector_type=vector_type,
                similarity=distance_func,
            )
        else:
            self._create_index(
                vector_type=vector_type,
                distance=distance_func,
            )
        order = "ASC" if distance != "similarity" else "DESC"
        self.wait_index_ready(
            targets=targets,
            vector_types=vector_types,
            vector_type=vector_type,
            distance=distance,
            order=order,
            distance_func=distance_func,
        )
        self.select_from_index(
            target=targets[distance][distance_func], name=vector_types[vector_type], data_type=vector_type, order=order
        )
        self.change_cluster_version()

        self.select_from_index(
            target=targets[distance][distance_func], name=vector_types[vector_type], data_type=vector_type, order=order
        )
        self._drop_index()

        self.write_data(name=vector_types[vector_type], vector_type=f"{vector_type}Vector")
        if distance == "similarity":
            self._create_index(
                vector_type=vector_type,
                similarity=distance_func,
            )
        else:
            self._create_index(
                vector_type=vector_type,
                distance=distance_func,
            )
        self.wait_index_ready(
            targets=targets,
            vector_types=vector_types,
            vector_type=vector_type,
            distance=distance,
            order=order,
            distance_func=distance_func,
        )
        self.select_from_index(
            target=targets[distance][distance_func], name=vector_types[vector_type], data_type=vector_type, order=order
        )
