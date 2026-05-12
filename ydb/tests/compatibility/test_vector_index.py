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
        # vector type: [ data type, conversion function ]
        self.vector_types = {
            "Uint8": ["Uint8", "Knn::ToBinaryStringUint8"],
            "Int8": ["Int8", "Knn::ToBinaryStringInt8"],
            "Float": ["Float", "Knn::ToBinaryStringFloat"],
        }
        if min(self.versions) >= (26, 1):
            self.vector_types["Bit"] = ["Float", "Knn::ToBinaryStringBit"]
        self.targets = {
            "similarity": {"inner_product": "Knn::InnerProductSimilarity", "cosine": "Knn::CosineSimilarity"},
            "distance": {
                "cosine": "Knn::CosineDistance",
                "manhattan": "Knn::ManhattanDistance",
                "euclidean": "Knn::EuclideanDistance",
            },
        }

        yield from self.setup_cluster(extra_feature_flags={"enable_vector_index": True})

    def get_vector(self, data_type, numb):
        if data_type == "Float":
            values = [float(i) for i in range(self.vector_dimension - 1)]
            values.append(float(numb))
            return ",".join(f'{val}f' for val in values)

        values = [i for i in range(self.vector_dimension - 1)]
        values.append(numb)
        return ",".join(str(val) for val in values)

    def _create_index_sql(self, vector_type, table_name, target, prefixed, overlap):
        prefix = ""
        if prefixed:
            prefix = "user, "
        if overlap:
            overlap = f", overlap_clusters={overlap}"
        else:
            overlap = ""
        return f"""
            ALTER TABLE {table_name}
            ADD INDEX `{self.index_name}` GLOBAL USING vector_kmeans_tree
            ON ({prefix}vec)
            WITH ({target},
                  vector_type={vector_type},
                  vector_dimension={self.vector_dimension},
                  levels=2,
                  clusters=10
                  {overlap}
            );
        """

    def _drop_index_sql(self, table_name):
        return f"""
            ALTER TABLE {table_name} DROP INDEX `{self.index_name}`;
        """

    def _write_data(self, vector_type, table_name):
        [data_type, converter] = self.vector_types[vector_type]
        values = []
        for key in range(self.rows_count):
            vector = self.get_vector(data_type, key + 1)
            user = 1 + (key % self.rows_per_user)
            values.append(f'({key}, {user}, Untag({converter}([{vector}]), "{vector_type}Vector"))')

        sql_upsert = f"""
                UPSERT INTO `{table_name}` (key, user, vec)
                VALUES {",".join(values)};
            """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(sql_upsert)

    def _get_knn_queries(self):
        """Get KNN search queries without vector index (brute force with pushdown)."""
        queries = []
        for prefixed in ['', '_pfx']:
            for vector_type in self.vector_types.keys():
                [data_type, converter] = self.vector_types[vector_type]
                for distance in self.targets.keys():
                    for distance_func in self.targets[distance].keys():
                        table_name = f"{vector_type}_{distance}_{distance_func}{prefixed}_"
                        order = "ASC" if distance != "similarity" else "DESC"
                        vector = self.get_vector(data_type, 1)
                        where = ""
                        if prefixed:
                            where = "WHERE user=1"
                        queries.append([
                            True, f"""
                            $Target = {converter}(Cast([{vector}] AS List<{data_type}>));
                            SELECT key, vec, {self.targets[distance][distance_func]}(vec, $Target) as target
                            FROM `{table_name}`
                            {where}
                            ORDER BY {self.targets[distance][distance_func]}(vec, $Target) {order}
                            LIMIT {self.rows_count};"""
                        ])
        return queries

    def _get_queries_for(self, prefixed, vector_type, distance, distance_func, overlap):
        queries = []
        [data_type, converter] = self.vector_types[vector_type]
        table_name = f"{vector_type}_{distance}_{distance_func}{prefixed}_{overlap}"
        order = "ASC" if distance != "similarity" else "DESC"
        vector = self.get_vector(data_type, 1)
        where = ""
        if prefixed:
            where = "WHERE user=1"
        queries.append([
            True, f"""
            $Target = {converter}(Cast([{vector}] AS List<{data_type}>));
            SELECT key, vec, {self.targets[distance][distance_func]}(vec, $Target) as target
            FROM `{table_name}`
            VIEW `{self.index_name}`
            {where}
            ORDER BY {self.targets[distance][distance_func]}(vec, $Target) {order}
            LIMIT {self.rows_count};"""
        ])
        # Insert, update, upsert, delete
        key = self.rows_count+1
        vector = self.get_vector(data_type, key+1)
        queries.append([
            False, f"""
            INSERT INTO `{table_name}` (key, user, vec)
            VALUES ({key}, {1 + (key) % self.rows_per_user},
                Untag({converter}([{vector}]), "{vector_type}Vector"))
            """
        ])
        vector = self.get_vector(data_type, key+2)
        queries.append([
            False, f"""
            UPDATE `{table_name}` SET user=user+1,
                vec=Untag({converter}([{vector}]), "{vector_type}Vector")
            WHERE key={key}
            """
        ])
        vector = self.get_vector(data_type, key+3)
        queries.append([
            False, f"""
            UPSERT INTO `{table_name}` (key, user, vec)
            VALUES ({key}, {1 + (key) % self.rows_per_user},
                Untag({converter}([{vector}]), "{vector_type}Vector"))
            """
        ])
        queries.append([
            False, f"""
            DELETE FROM `{table_name}` WHERE key={key}
            """
        ])
        return queries

    def _all_variants(self):
        overlaps = ['']
        if min(self.versions) >= (25, 4):
            overlaps = ['', '2']
        res = []
        for prefixed in ['', '_pfx']:
            for vector_type in self.vector_types.keys():
                for distance in self.targets.keys():
                    for distance_func in self.targets[distance].keys():
                        for overlap in overlaps:
                            res.append([prefixed, vector_type, distance, distance_func, overlap])
        return res

    def _do_queries(self, session_pool, queries):
        for [is_select, query] in queries:
            result_sets = session_pool.execute_with_retries(query)
            if is_select:
                assert len(result_sets[0].rows) > 0, "Query returned an empty set"
                rows = result_sets[0].rows
                for row in rows:
                    assert row['target'] is not None, "the distance is None"

    def _indexed_search(self):
        queries = []

        def predicate():
            try:
                session_pool.execute_with_retries(queries[0][1])
            except ydbs.issues.SchemeError as ex:
                if "Required global index not found, index name" in str(ex):
                    return False
                raise ex
            return True

        variants = self._all_variants()
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for [prefixed, vector_type, distance, distance_func, overlap] in variants:
                table_name = f"{vector_type}_{distance}_{distance_func}{prefixed}_{overlap}"
                session_pool.execute_with_retries(self._create_index_sql(
                    table_name=table_name,
                    vector_type=vector_type,
                    target=f"{distance}={distance_func}",
                    prefixed=prefixed,
                    overlap=overlap,
                ))
                queries = self._get_queries_for(prefixed, vector_type, distance, distance_func, overlap)
                assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Error getting index status"
                self._do_queries(session_pool, queries)
                session_pool.execute_with_retries(self._drop_index_sql(table_name))

    def _knn_search(self):
        """Perform KNN search without vector index during rolling upgrade/downgrade."""
        queries = self._get_knn_queries()
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self._do_queries(session_pool, queries)

    def _create_table(self, table_name):
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
        variants = self._all_variants()
        for [prefixed, vector_type, distance, distance_func, overlap] in variants:
            table_name = f"{vector_type}_{distance}_{distance_func}{prefixed}_{overlap}"
            self._create_table(table_name)
            self._write_data(
                vector_type=vector_type,
                table_name=table_name,
            )
        for _ in self.roll():
            self._knn_search()
        for _ in self.roll():
            self._indexed_search()
