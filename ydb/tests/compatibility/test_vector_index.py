import pytest
import ydb as ydbs

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestVectorIndex(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")
        self.rows_count = 5
        self.index_name = "vector_idx"
        self.vector_dimension = 3

        yield from self.setup_cluster(extra_feature_flags={"enable_vector_index": True})

    def get_vector(self, type, numb):
        if type == "FloatVector":
            values = [float(i) for i in range(self.vector_dimension - 1)]
            values.append(float(numb))
            return ",".join(f'{val}f' for val in values)

        values = [i for i in range(self.vector_dimension - 1)]
        values.append(numb)
        return ",".join(str(val) for val in values)

    def _create_index(self, vector_type, table_name, distance=None, similarity=None):
        if distance is not None:
            create_index_sql = f"""
                ALTER TABLE {table_name}
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
                ALTER TABLE {table_name}
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
            except ydbs.issues.SchemeError as ex:
                if "Required global index not found, index name" in str(ex):
                    return False
                raise ex
            return True

        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Error getting index status"

    def write_data(self, name, vector_type):
        qyerys = []
        for table_name in self.table_names:
            values = []
            for key in range(self.rows_count):
                vector = self.get_vector(vector_type, key + 1)
                values.append(f'({key}, Untag({name}([{vector}]), "{vector_type}"))')

            qyerys.append(
                f"""
                UPSERT INTO `{table_name}` (key, vec)
                VALUES {",".join(values)};
            """
            )
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for qyery in qyerys:
                session_pool.execute_with_retries(qyery)

    def select_from_index(self, target, name, data_type, order):
        qyerys = []
        for table_name in self.table_names:
            vector = self.get_vector(f"{data_type}Vector", 1)
            qyerys.append(
                f"""
                $Target = {name}(Cast([{vector}] AS List<{data_type}>));
                SELECT key, vec, {target}(vec, $Target) as target
                FROM {table_name}
                VIEW `{self.index_name}`
                ORDER BY {target}(vec, $Target) {order}
                LIMIT {self.rows_count};
            """
            )
        for _ in self.roll():
            with ydb.QuerySessionPool(self.driver) as session_pool:
                for qyery in qyerys:
                    result_sets = session_pool.execute_with_retries(qyery)
                    assert len(result_sets[0].rows) > 0, "Query returned an empty set"
                    rows = result_sets[0].rows
                    for row in rows:
                        assert row['target'] is not None, "the distance is None"

    def create_table(self):
        querys = []
        for table_name in self.table_names:
            querys.append(
                f"""
                CREATE TABLE {table_name} (
                    key Int64 NOT NULL,
                    vec String NOT NULL,
                    PRIMARY KEY (key)
                )
                """
            )
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in querys:
                session_pool.execute_with_retries(query)

    def test_vector_index(self):
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
        self.table_names = []
        for vector_type in vector_types.keys():
            for distance in targets.keys():
                for distance_func in targets[distance].keys():
                    self.table_names.append(f"{vector_type}_{distance}_{distance_func}")
        self.create_table()
        self.write_data(name=vector_types[vector_type], vector_type=f"{vector_type}Vector")
        for vector_type in vector_types.keys():
            for distance in targets.keys():
                for distance_func in targets[distance].keys():
                    if distance == "similarity":
                        self._create_index(
                            table_name=f"{vector_type}_{distance}_{distance_func}",
                            vector_type=vector_type,
                            similarity=distance_func,
                        )
                    else:
                        self._create_index(
                            table_name=f"{vector_type}_{distance}_{distance_func}",
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
            target=targets[distance][distance_func],
            name=vector_types[vector_type],
            data_type=vector_type,
            order=order,
        )
