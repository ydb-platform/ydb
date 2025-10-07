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
        self.rows_count = 9
        self.rows_per_user = 3
        self.index_name = "vector_idx"
        self.vector_dimension = 3
        self.vector_types = {
            "Uint8": "Knn::ToBinaryStringUint8",
            "Int8": "Knn::ToBinaryStringInt8",
            "Float": "Knn::ToBinaryStringFloat",
        }
        self.targets = {
            "similarity": {"inner_product": "Knn::InnerProductSimilarity", "cosine": "Knn::CosineSimilarity"},
            "distance": {
                "cosine": "Knn::CosineDistance",
                "manhattan": "Knn::ManhattanDistance",
                "euclidean": "Knn::EuclideanDistance",
            },
        }

        yield from self.setup_cluster(extra_feature_flags={"enable_vector_index": True})

    def get_vector(self, type, numb):
        if type == "FloatVector":
            values = [float(i) for i in range(self.vector_dimension - 1)]
            values.append(float(numb))
            return ",".join(f'{val}f' for val in values)

        values = [i for i in range(self.vector_dimension - 1)]
        values.append(numb)
        return ",".join(str(val) for val in values)

    def _create_index(self, vector_type, table_name, target, prefixed):
        prefix = ""
        if prefixed:
            prefix = "user, "
        create_index_sql = f"""
            ALTER TABLE {table_name}
            ADD INDEX `{self.index_name}` GLOBAL USING vector_kmeans_tree
            ON ({prefix}vec)
            WITH ({target},
                  vector_type={vector_type},
                  vector_dimension={self.vector_dimension},
                  levels=2,
                  clusters=10
            );
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create_index_sql)

    def wait_index_ready(self):
        def predicate():
            try:
                self.select_from_index_without_roll()
            except ydbs.issues.SchemeError as ex:
                if "Required global index not found, index name" in str(ex):
                    return False
                raise ex
            return True

        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Error getting index status"

    def _write_data(self, name, vector_type, table_name):
        values = []
        for key in range(self.rows_count):
            vector = self.get_vector(vector_type, key + 1)
            user = 1 + (key % self.rows_per_user)
            values.append(f'({key}, {user}, Untag({name}([{vector}]), "{vector_type}Vector"))')

        sql_upsert = f"""
                UPSERT INTO `{table_name}` (key, user, vec)
                VALUES {",".join(values)};
            """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(sql_upsert)

    def _get_queries(self):
        queries = []
        for prefixed in ['', '_pfx']:
            for vector_type in self.vector_types.keys():
                for distance in self.targets.keys():
                    for distance_func in self.targets[distance].keys():
                        table_name = f"{vector_type}_{distance}_{distance_func}{prefixed}"
                        order = "ASC" if distance != "similarity" else "DESC"
                        vector = self.get_vector(f"{vector_type}Vector", 1)
                        where = ""
                        if prefixed:
                            where = "WHERE user=1"
                        queries.append([
                            True, f"""
                            $Target = {self.vector_types[vector_type]}(Cast([{vector}] AS List<{vector_type}>));
                            SELECT key, vec, {self.targets[distance][distance_func]}(vec, $Target) as target
                            FROM `{table_name}`
                            VIEW `{self.index_name}`
                            {where}
                            ORDER BY {self.targets[distance][distance_func]}(vec, $Target) {order}
                            LIMIT {self.rows_count};"""
                        ])
                        # Insert, update, upsert, delete
                        key = self.rows_count+1
                        vector = self.get_vector(vector_type, key+1)
                        queries.append([
                            False, f"""
                            INSERT INTO `{table_name}` (key, user, vec)
                            VALUES ({key}, {1 + (key) % self.rows_per_user},
                                Untag({self.vector_types[vector_type]}([{vector}]), "{vector_type}Vector"))
                            """
                        ])
                        vector = self.get_vector(vector_type, key+2)
                        queries.append([
                            False, f"""
                            UPDATE `{table_name}` SET user=user+1,
                                vec=Untag({self.vector_types[vector_type]}([{vector}]), "{vector_type}Vector")
                            WHERE key={key}
                            """
                        ])
                        vector = self.get_vector(vector_type, key+3)
                        queries.append([
                            False, f"""
                            UPSERT INTO `{table_name}` (key, user, vec)
                            VALUES ({key}, {1 + (key) % self.rows_per_user},
                                Untag({self.vector_types[vector_type]}([{vector}]), "{vector_type}Vector"))
                            """
                        ])
                        queries.append([
                            False, f"""
                            DELETE FROM `{table_name}` WHERE key={key}
                            """
                        ])
        return queries

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for [is_select, query] in queries:
                result_sets = session_pool.execute_with_retries(query)
                if is_select:
                    assert len(result_sets[0].rows) > 0, "Query returned an empty set"
                    rows = result_sets[0].rows
                    for row in rows:
                        assert row['target'] is not None, "the distance is None"

    def select_from_index(self):
        queries = self._get_queries()
        for _ in self.roll():
            self._do_queries(queries)

    def select_from_index_without_roll(self):
        queries = self._get_queries()
        self._do_queries(queries)

    def create_table(self, table_name):
        query = f"""
                CREATE TABLE {table_name} (
                    key Int64 NOT NULL,
                    user Uint64 NOT NULL,
                    vec String NOT NULL,
                    PRIMARY KEY (key)
                )
                """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def test_vector_index(self):
        for prefixed in ['', '_pfx']:
            for vector_type in self.vector_types.keys():
                for distance in self.targets.keys():
                    for distance_func in self.targets[distance].keys():
                        table_name = f"{vector_type}_{distance}_{distance_func}{prefixed}"
                        self.create_table(table_name)
                        self._write_data(
                            name=self.vector_types[vector_type],
                            vector_type=vector_type,
                            table_name=table_name,
                        )
                        self._create_index(
                            table_name=table_name,
                            vector_type=vector_type,
                            target=f"{distance}={distance_func}",
                            prefixed=prefixed,
                        )
        self.wait_index_ready()
        self.select_from_index()
