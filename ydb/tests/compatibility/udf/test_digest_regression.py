# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestDigest(MixedClusterFixture):
    rows = 100
    table_name = 'digest_test'

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def generate_create_table(self):
        zfill_width = len(str(self.rows))
        partition_keys = []
        for i in range(1, self.rows):
            suffix = str(i).zfill(zfill_width)
            partition_keys.append(
                f'("Text1_{suffix}")'
            )

        return f"""
            CREATE TABLE `{self.table_name}` (
                id1 Utf8,
                id2 Int64,
                id3 Uint64,
                id4 Utf8,
                id5 Utf8,
                id6 Utf8,
                PRIMARY KEY (id1, id3, id2, id4, id5, id6)
            ) WITH (
                PARTITION_AT_KEYS = ({", ".join(partition_keys)}),
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {self.rows}
            );
        """

    def generate_insert(self):
        header = f"UPSERT INTO {self.table_name} (id1, id2, id3, id4, id5, id6) VALUES\n"
        rows = []

        zfill_width = len(str(self.rows))
        for i in range(1, self.rows + 1):
            suffix = str(i).zfill(zfill_width)
            text1 = f"Text1_{suffix}"
            text2 = f"Text2_{suffix}"
            text3 = f"Text3_{suffix}"
            text4 = f"Text4_{suffix}"
            number_signed = i
            number_unsigned = i
            rows.append(f'("{text1}", {number_signed}, {number_unsigned}, "{text2}", "{text3}", "{text4}")')

        return header + ",\n".join(rows) + ";"

    def q_real_life(self):
        return f"""
            DECLARE $id1 AS Utf8;
            DECLARE $id2s AS List<Int64>;
            DECLARE $limit AS Uint64;

            $id2_tuples = ListMap($id2s, ($id) -> (AsTuple(Digest::CityHash(CAST($id AS String)), $id)));

            SELECT
                id1,
                id3,
                id2,
                id4,
                id5,
                id6
            FROM {self.table_name}
            WHERE
                id1 = $id1
                AND (id3, id2) IN $id2_tuples
            ORDER BY id1 ASC, id3 ASC, id2 ASC, id4 ASC, id5 ASC, id6 ASC
            LIMIT $limit;
        """

    def test_digest_regression(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = self.generate_create_table()
            session_pool.execute_with_retries(query)

            query = self.generate_insert()
            session_pool.execute_with_retries(query)

        query = self.q_real_life()

        for i in range(1, 11):
            zfill_width = len(str(self.rows))
            sample_i = i
            suffix = str(sample_i).zfill(zfill_width)
            id1_value = f"Text1_{suffix}"
            id2_value = sample_i
            query_params = {
                "$id1": id1_value,
                "$id2s": [id2_value],
                "$limit": (1000, ydb.PrimitiveType.Uint64),
            }
            with ydb.QuerySessionPool(self.driver) as session_pool:
                session_pool.execute_with_retries(query, parameters=query_params)
