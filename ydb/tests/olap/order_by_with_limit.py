import logging
import os
import yatest.common
import ydb
import random
import string
import datetime

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestOrderBy(object):
    test_name = "order_by"
    n = 200

    @classmethod
    def setup_class(cls):
        random.seed(0xBEDA)
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_columnshard_bool": True
            },
            column_shard_config={
                "compaction_enabled": False,
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE",
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    def write_data(self, table: str):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Uint64)

        # [2,  4,  6,  8, 10, ...]
        # [3,  6,  9, 12, 15, ...]
        # [4,  8, 12, 16, 20, ...]
        # [5, 10, 15, 20, 25, ...]
        # ...
        data = [[{"id": j, "value": j} for j in range(1, self.n) if j % i == 0] for i in range(2, self.n)]
        random.shuffle(data)

        for row in data:
            self.ydb_client.bulk_upsert(
                table,
                column_types,
                row,
            )

    def test_random(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/test_random"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64 NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        self.write_data(table_path)

        for i in range(100):
            limit = random.randint(1, self.n)
            offset = random.randint(1, self.n)
            is_desc = random.randint(0, 1)
            is_le = random.randint(0, 1)
            order = ["asc", "desc"]
            condition = [">", "<"]
            data = list(range(2, self.n))
            if is_le:
                answer = [i for i in data if i < offset]
            else:
                answer = [i for i in data if i > offset]
            if is_desc:
                answer = answer[::-1]
            answer = answer[:limit]

            result_sets = self.ydb_client.query(
                f"""
                select id from `{table_path}`
                where value {condition[is_le]} {offset}
                order by id {order[is_desc]} limit {limit}
                """
            )

            keys = [row['id'] for result_set in result_sets for row in result_set.rows]
            if keys != answer:
                print(keys)
                print(answer)
                print(f"""
                    select id from `{table_path}`
                    where value {condition[is_le]} {offset}
                    order by id {order[is_desc]} limit {limit}
                    """)
            assert keys == answer, keys

    def random_char(self):
        characters = string.ascii_letters + string.digits
        return random.choice(characters)

    def random_string(self, size: int):
        result_string = ''.join(self.random_char() for i in range(size))
        return result_string

    def gen_portion(self, start_idx: int, portion_size: int, start: str):
        return [{"id": i, "value": start + self.random_string(1000)} for i in range(start_idx, start_idx + portion_size)]

    def test_fetch_race(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/test_fetch_race"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY(id, value),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Utf8)

        big_portion1 = self.gen_portion(1, 10000, "3")
        small_portion = self.gen_portion(1, 1, "2")
        big_portion2 = self.gen_portion(1, 10000, "1")

        self.ydb_client.bulk_upsert(table_path, column_types, big_portion1)
        self.ydb_client.bulk_upsert(table_path, column_types, small_portion)
        self.ydb_client.bulk_upsert(table_path, column_types, big_portion2)

        result_sets = self.ydb_client.query(
            f"""
            select id, value from `{table_path}`
            order by id, value limit 1000
            """
        )

        keys = [row['id'] for result_set in result_sets for row in result_set.rows]

        assert len(keys) == 1000
        assert max(keys) == 500

    def test_filtered_portion(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/test_filtered_portion"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value bool NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Bool)

        portion1 = [{"id" : 1, "value" : True}]
        portion2 = [{"id" : 2, "value" : False}, {"id" : 3, "value" : False}]
        portion3 = [{"id" : 2, "value" : True}]
        portion4 = [{"id" : 2, "value" : True}, {"id" : 3, "value" : True}]

        self.ydb_client.bulk_upsert(table_path, column_types, portion1)
        self.ydb_client.bulk_upsert(table_path, column_types, portion2)
        self.ydb_client.bulk_upsert(table_path, column_types, portion3)
        self.ydb_client.bulk_upsert(table_path, column_types, portion4)

        result_sets = self.ydb_client.query(
            f"""
            select id, value from `{table_path}`
            order by id limit 10
            """
        )

        result = [(row['id'], row['value']) for result_set in result_sets for row in result_set.rows]

        assert len(result) == 3
        assert result == [(1, True), (2, True), (3, True)]

    def test_stress_sorting(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/test_stress_sorting"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                time Timestamp NOT NULL,
                uniq Utf8 NOT NULL,
                class Utf8 NOT NULL,
                value Utf8,
                PRIMARY KEY(time, class, uniq),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("time", ydb.PrimitiveType.Timestamp)
        column_types.add_column("class", ydb.PrimitiveType.Utf8)
        column_types.add_column("uniq", ydb.PrimitiveType.Utf8)
        column_types.add_column("value", ydb.OptionalType(ydb.PrimitiveType.Utf8))

        portions = [[{"time" : datetime.datetime.fromtimestamp(1234567890),
                      "class" : self.random_char(),
                      "uniq" : self.random_string(20),
                      "value" : self.random_char()
                      }] for _ in range(10000)]

        for portion in portions:
            self.ydb_client.bulk_upsert(table_path, column_types, portion)

        result_sets = self.ydb_client.query(
            f"""
            select * from `{table_path}`
            order by time, class, uniq limit 1000
            """
        )

        assert len(result_sets[0].rows) == 1000

    def test_reproduce(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/test_reproduce"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64 NOT NULL,
                PRIMARY KEY(id, value),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Uint64)

        portions = [
            [(10, 4)],
            [(9, 0), (10, 5)],
            [(9, 0), (10, 5)],
            [(10, 3)]
        ]

        for portion in portions:
            self.ydb_client.bulk_upsert(table_path, column_types, map(lambda tup: {"id": tup[0], "value": tup[1]}, portion))

        result_sets = self.ydb_client.query(
            f"""
            select id, value from `{table_path}`
            order by id limit 100
            """
        )

        assert len(result_sets[0].rows) == 4

    def test_corner(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/test_corner"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                pk4 Uint64 NOT NULL,
                pk2 Uint64 NOT NULL,
                pk1 Uint64 NOT NULL,
                order Uint64 NOT NULL,
                pk3 Uint64 NOT NULL,
                PRIMARY KEY(pk1, pk2, pk3, pk4),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("pk1", ydb.PrimitiveType.Uint64)
        column_types.add_column("pk2", ydb.PrimitiveType.Uint64)
        column_types.add_column("pk3", ydb.PrimitiveType.Uint64)
        column_types.add_column("pk4", ydb.PrimitiveType.Uint64)
        column_types.add_column("order", ydb.PrimitiveType.Uint64)

        # order should be (pk1, pk2, portion_id) because
        # pk1 requested
        # pk2 fetched
        # pk3 not fetched
        # pk4 fetched, but not used because pk3 not used

        portions = [
            # the second is smaller, because of pk2
            [(1, 3, 0, 0, 0)],
            [(1, 2, 0, 0, 1)],
            # the first is smaller, because of portion id. pk3 ignored
            [(2, 3, 5, 0, 2)],
            [(2, 3, 4, 0, 3)],
            # the first is smaller, because of portion id. pk4 ignored
            [(4, 5, 0, 7, 4)],
            [(4, 5, 0, 6, 5)],
            # smallest pk1, should be first
            [(0, 0, 0, 0, 6)],
        ]

        for portion in portions:
            self.ydb_client.bulk_upsert(table_path, column_types, map(
                lambda tup: {
                    "pk1": tup[0],
                    "pk2": tup[1],
                    "pk3": tup[2],
                    "pk4": tup[3],
                    "order": tup[4]
                }, portion))

        result_sets = self.ydb_client.query(
            f"""
            select order, pk2, pk4 from `{table_path}`
            order by pk1 limit 1
            """
        )
        order = list(map(lambda x: x["order"], result_sets[0].rows))
        assert order == [6]

        result_sets = self.ydb_client.query(
            f"""
            select order, pk2, pk4 from `{table_path}`
            order by pk1 limit 2
            """
        )
        order = list(map(lambda x: x["order"], result_sets[0].rows))
        assert order == [6, 1]

        result_sets = self.ydb_client.query(
            f"""
            select order, pk2, pk4 from `{table_path}`
            order by pk1 limit 3
            """
        )
        order = list(map(lambda x: x["order"], result_sets[0].rows))
        assert order == [6, 1, 0]

        result_sets = self.ydb_client.query(
            f"""
            select order, pk2, pk4 from `{table_path}`
            order by pk1 limit 4
            """
        )
        order = list(map(lambda x: x["order"], result_sets[0].rows))
        # should be
        # assert order == [6, 1, 0, 2]
        assert order == [6, 1, 0, 3]

        result_sets = self.ydb_client.query(
            f"""
            select order, pk2, pk4 from `{table_path}`
            order by pk1 limit 5
            """
        )
        order = list(map(lambda x: x["order"], result_sets[0].rows))
        # should be
        # assert order == [6, 1, 0, 2, 3]
        assert order == [6, 1, 0, 3, 2]

        result_sets = self.ydb_client.query(
            f"""
            select order, pk2, pk4 from `{table_path}`
            order by pk1 limit 6
            """
        )
        order = list(map(lambda x: x["order"], result_sets[0].rows))
        # should be
        # assert order == [6, 1, 0, 2, 3, 4]
        assert order == [6, 1, 0, 3, 2, 5]

        result_sets = self.ydb_client.query(
            f"""
            select order, pk2, pk4 from `{table_path}`
            order by pk1 limit 100
            """
        )
        order = list(map(lambda x: x["order"], result_sets[0].rows))
        # should be
        # assert order == [6, 1, 0, 2, 3, 4, 5]
        assert order == [6, 1, 0, 3, 2, 5, 4]

    def test_reproduce_with_prefix(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/test_reproduce_with_prefix"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64 NOT NULL,
                PRIMARY KEY(id, value),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Uint64)

        portions = [
            [(1, 0)],
            [(2, 0)],
            [(3, 0)],
            [(4, 0)],
            [(5, 0)],
            [(6, 0)],
            [(7, 0)],
            [(8, 0)],
            [(10, 4)],
            [(9, 0), (10, 5)],
            [(9, 0), (10, 5)],
            [(10, 3)]
        ]

        for portion in portions:
            self.ydb_client.bulk_upsert(table_path, column_types, map(lambda tup: {"id": tup[0], "value": tup[1]}, portion))

        result_sets = self.ydb_client.query(
            f"""
            select id from `{table_path}`
            order by id limit 100
            """
        )

        assert len(result_sets[0].rows) == 12
