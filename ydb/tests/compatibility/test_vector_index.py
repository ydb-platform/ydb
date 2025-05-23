import pytest
import random
import threading
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

TABLE_NAME = "table"


class TestStatisticsFollowers(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):

        yield from self.setup_cluster(extra_feature_flags={"enable_vector_index": True})

    def _get_random_vector(self, type, size):
        if type == "Float":
            values = [round(random.uniform(-100, 100), 2) for _ in range(size)]
            return ",".join(f'{val}f' for val in values)

        if type == "Uint8":
            values = [random.randint(0, 255) for _ in range(size)]
        else:
            values = [random.randint(-127, 127) for _ in range(size)]
        return ",".join(str(val) for val in values)

    def _create_index(self, vector_type, distance=None, similarity=None):
        if distance is not None:
            create_index_sql = f"""
                ALTER TABLE {TABLE_NAME}
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
                ALTER TABLE {TABLE_NAME}
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

    def _drop_index(self):
        drop_index_sql = f"""
            ALTER TABLE {TABLE_NAME}
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
            UPSERT INTO `{TABLE_NAME}` (key, vec)
            VALUES {",".join(values)};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(upsert_sql)

    def check_statistics(self, target, name, data_type, order):
        vector = self._get_random_vector(data_type, self.vector_dimension)
        select_sql = f"""
            $Target = {name}(Cast([{vector}] AS List<{data_type}>));
            SELECT key, vec, {target}(vec, $Target) as target
            FROM {TABLE_NAME}
            VIEW `{self.index_name}`
            ORDER BY {target}(vec, $Target) {order}
            LIMIT {self.rows_count};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result_sets = session_pool.execute_with_retries(select_sql)
            assert len(result_sets[0].rows) > 0

    def create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE {TABLE_NAME} (
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
        ]
    )
    def test_statistics_followers(self, vector_type, distance, distance_func):
        self.rows_count = 30
        self.index_name = "vector_idx"
        self.vector_dimension = 5
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
        self.check_statistics(
            target=targets[distance][distance_func], name=vector_types[vector_type], data_type=vector_type, order=order
        )
        self._drop_index()
        self.change_cluster_version()

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

        self.check_statistics(
            target=targets[distance][distance_func], name=vector_types[vector_type], data_type=vector_type, order=order
        )