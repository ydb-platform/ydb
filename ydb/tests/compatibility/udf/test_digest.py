# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb
import random


class TestDigest(MixedClusterFixture):
    rows = 100
    table_name = 'digest_test'

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def generate_create_table(self):
        return f"""
            CREATE TABLE {self.table_name} (
                id Uint64,
                text String,
                number Uint64,
                PRIMARY KEY(id)
            ) WITH (
                PARTITION_AT_KEYS = ({", ".join(str(i) for i in range(1, self.rows))}),
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {self.rows}
            );
        """

    def generate_insert(self):
        header = f"UPSERT INTO {self.table_name} (id, text, number) VALUES\n"
        rows = []

        for i in range(1, self.rows + 1):
            text = f"Text_{random.randint(1000, 9999)}"
            number = random.randint(1_000_000, 9_999_999)
            rows.append(f'({i}, "{text}", {number})')

        return header + ",\n".join(rows) + ";"

    def q_digest(self):
        return f"""
        SELECT
            Digest::Crc32c(text),
            Digest::Crc64(text),

            Digest::Fnv32(text),
            Digest::Fnv64(text),

            Digest::MurMurHash(text),
            Digest::MurMurHash32(text),
            Digest::MurMurHash2A(text),
            Digest::MurMurHash2A32(text),

            Digest::CityHash(text),
            Digest::CityHash128(text),

            Digest::NumericHash(number),

            Digest::Md5Hex(text),
            Digest::Md5Raw(text),
            Digest::Md5HalfMix(text),

            Digest::Argon2(text, "1234567890"),

            Digest::Blake2B(text),
            Digest::Blake2B(text, "key"),

            Digest::SipHash(1, 2, text),
            Digest::HighwayHash(1, 2, 3, 4, text),

            Digest::FarmHashFingerprint(number),
            Digest::FarmHashFingerprint2(number, number),
            Digest::FarmHashFingerprint32(text),
            Digest::FarmHashFingerprint64(text),
            Digest::FarmHashFingerprint128(text),

            Digest::SuperFastHash(text),

            Digest::Sha1(text),
            Digest::Sha256(text),

            Digest::IntHash64(number),

            Digest::XXH3(text),
            Digest::XXH3_128(text)
        FROM {self.table_name};
        """

    def test_digest_all(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:

            query = self.generate_create_table()
            session_pool.execute_with_retries(query)

            query = self.generate_insert()
            session_pool.execute_with_retries(query)

        query = self.q_digest()
        """
        UDFs are compiled once on the node that initially receives the request.
        The compiled UDF is then propagated to all other nodes. Executing the query a single time only verifies
        compatibility in one direction—either from old to new or from new to old. Performing multiple retries
        increases the likelihood that the UDF will be compiled on both the old and new versions, thereby improving coverage of compatibility testing.

        Additionally, a session pool always sends requests to the same node. To ensure distribution across nodes, the session pool is recreated for each SELECT request.
        """
        for _ in range(10):
            with ydb.QuerySessionPool(self.driver) as session_pool:
                result = session_pool.execute_with_retries(query)
                assert len(result[0].rows) > 0
