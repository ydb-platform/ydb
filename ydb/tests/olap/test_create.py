from decimal import Decimal
import logging
import os
import pytest
import random
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestCreate(object):
    test_name = "create"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": True,
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE",
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"

    def get_table_path(self):
        # avoid using same table in parallel tests
        return f"{self.test_dir}/table{random.randrange(99999)}"

    def test_create_all_opt_ints(self):
        table_path = self.get_table_path() + "_1"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                v0 Int8,
                v1 Int16,
                v2 Int32,
                v3 Int64,
                v4 Uint8,
                v5 Uint16,
                v6 Uint32,
                v7 Uint64,
                PRIMARY KEY(id),
            )
            WITH (STORE = COLUMN);
            """
        )
        self.ydb_client.query(f"INSERT INTO `{table_path}` (id) VALUES (0);")
        self.ydb_client.query(
            f"""
            INSERT INTO `{table_path}`
            (id, v0, v1, v2, v3, v4, v5, v6, v7)
            VALUES
            (1, -2, -3, -4, -5, 6, 7, 8, 9);
            """
        )
        result_sets = self.ydb_client.query(f"SELECT * FROM `{table_path}`")

        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)
        assert len(rows) == 2
        for key in rows[0]:
            if key == 'id':
                assert rows[0][key] == 0
            else:
                assert rows[0][key] is None
        assert rows[1] == {'id': 1, 'v0': -2, 'v1': -3, 'v2': -4, 'v3': -5, 'v4': 6, 'v5': 7, 'v6': 8, 'v7': 9}

    def test_create_all_req_ints(self):
        table_path = self.get_table_path() + "_2"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                v0 Int8   NOT NULL,
                v1 Int16  NOT NULL,
                v2 Int32  NOT NULL,
                v3 Int64  NOT NULL,
                v4 Uint8  NOT NULL,
                v5 Uint16 NOT NULL,
                v6 Uint32 NOT NULL,
                v7 Uint64 NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (STORE = COLUMN);
            """
        )

        try:
            self.ydb_client.query(f"INSERT INTO `{table_path}` (id) VALUES (0);")
            assert False, "Should Fail"
        except ydb.issues.BadRequest as ex:
            assert "Missing not null column in input" in ex.message

        self.ydb_client.query(
            f"""
            INSERT INTO `{table_path}`
            (id, v0, v1, v2, v3, v4, v5, v6, v7)
            VALUES
            (1, -2, -3, -4, -5, 6, 7, 8, 9);
            """
        )
        result_sets = self.ydb_client.query(f"SELECT * FROM `{table_path}`;")

        rows = result_sets[0].rows
        assert len(rows) == 1
        assert rows[0] == {'id': 1, 'v0': -2, 'v1': -3, 'v2': -4, 'v3': -5, 'v4': 6, 'v5': 7, 'v6': 8, 'v7': 9}

    def test_create_decimals(self):
        table_path = self.get_table_path() + "_3"
        # create query
        ql = [f"CREATE TABLE `{table_path}` (id Uint64 NOT NULL, "]
        for p in range(1, 36):  # precision up to 35
            for s in range(p + 1):  # scale from 0 to p
                ql.append(f"d_{p}_{s}_o Decimal({p}, {s}), ")
                ql.append(f"d_{p}_{s} Decimal({p}, {s}) NOT NULL, ")
        ql.append("PRIMARY KEY(id)) WITH (STORE = COLUMN);")
        self.ydb_client.query(''.join(ql))

        # insert query
        ql = [f"INSERT INTO `{table_path}` (id, "]
        for p in range(1, 36):  # precision up to 35
            for s in range(p + 1):  # scale from 0 to p
                ql.append(f"d_{p}_{s}_o, d_{p}_{s}, ")
        ql[-1] = ql[-1][:-2]
        ql.append(") VALUES (9, ")
        for p in range(1, 36):  # precision up to 35
            for s in range(p + 1):  # scale from 0 to p
                if s == p:  # all digits are for scale
                    ql.append(f"Decimal('0.5',{p},{s}), Decimal('0.6',{p},{s}), ")
                elif s == 0:  # no digits for scale
                    ql.append(f"Decimal('7.0',{p},{s}), Decimal('8.0',{p},{s}), ")
                else:
                    ql.append(f"Decimal('1.2',{p},{s}), Decimal('3.4',{p},{s}), ")
        ql[-1] = ql[-1][:-2]
        ql.append(");")
        self.ydb_client.query(''.join(ql))
        ql = []

        result_sets = self.ydb_client.query(f"SELECT * FROM `{table_path}`;")

        assert len(result_sets[0].rows) == 1
        row = result_sets[0].rows[0]

        for p in range(1, 3):  # precision up to 35
            for s in range(p + 1):  # scale from 0 to p
                desc = f"p{p}, s{s}"
                if s == p:
                    assert row[f"d_{p}_{s}_o"] == Decimal('0.5'), desc
                    assert row[f"d_{p}_{s}"] == Decimal('0.6'), desc
                elif s == 0:
                    assert row[f"d_{p}_{s}_o"] == Decimal('7'), desc
                    assert row[f"d_{p}_{s}"] == Decimal('8'), desc
                else:
                    assert row[f"d_{p}_{s}_o"] == Decimal('1.2'), desc
                    assert row[f"d_{p}_{s}"] == Decimal('3.4'), desc

    def test_create_real_req(self):
        table_path = self.get_table_path() + "_4"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64   NOT NULL,
                v0 Float    NOT NULL,
                v1 Double   NOT NULL,
                PRIMARY KEY(id),
            )
            WITH (STORE = COLUMN);
            """
        )

        try:
            self.ydb_client.query(f"INSERT INTO `{table_path}` (id) VALUES (0);")
            assert False, "Should Fail"
        except ydb.issues.BadRequest as ex:
            assert "Missing not null column in input" in ex.message

        self.ydb_client.query(
            f"""
            INSERT INTO `{table_path}`
            (id, v0, v1)
            VALUES
            (1, 1.2f, 2.3);
            """
        )
        result_sets = self.ydb_client.query(f"SELECT * FROM `{table_path}`;")

        rows = result_sets[0].rows
        assert len(rows) == 1
        assert rows[0]['id'] == 1
        assert rows[0]['v0'] == pytest.approx(1.2)
        assert rows[0]['v1'] == pytest.approx(2.3)

    def test_create_real_opt(self):
        table_path = self.get_table_path() + "_5"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64   NOT NULL,
                v0 Float,
                v1 Double,
                PRIMARY KEY(id),
            )
            WITH (STORE = COLUMN);
            """
        )

        self.ydb_client.query(f"INSERT INTO `{table_path}` (id) VALUES (0);")

        self.ydb_client.query(
            f"""
            INSERT INTO `{table_path}`
            (id, v0, v1)
            VALUES
            (1, 6.7f, 8.9);
            """
        )

        result_sets = self.ydb_client.query(f"SELECT * FROM `{table_path}`;")
        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)

        assert len(rows) == 2
        assert rows[0]['id'] == 0
        assert rows[0]['v0'] is None
        assert rows[0]['v1'] is None
        assert rows[1]['id'] == 1
        assert rows[1]['v0'] == pytest.approx(6.7)
        assert rows[1]['v1'] == pytest.approx(8.9)

    def test_create_strings_opt(self):
        table_path = self.get_table_path() + "_6"
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                v0 String,
                v1 Utf8,
                PRIMARY KEY(id),
            )
            WITH (STORE = COLUMN);
            """
        )

        self.ydb_client.query(f"INSERT INTO `{table_path}` (id) VALUES (0);")

        self.ydb_client.query(
            f"""
            INSERT INTO `{table_path}`
            (id, v0, v1)
            VALUES
            (1, 'abc', 'xyz');
            """
        )
        result_sets = self.ydb_client.query(f"SELECT * FROM `{table_path}` ORDER BY id;")

        rows = []
        for result_set in result_sets:
            rows.extend(result_set.rows)
        assert len(rows) == 2
        assert rows[0]['id'] == 0
        assert rows[0]['v0'] is None
        assert rows[0]['v1'] is None
        assert rows[1]['id'] == 1
        assert rows[1]['v0'] == b'abc'
        assert rows[1]['v1'] == 'xyz'

    def test_dy_number_not_supported(self):
        table_path = self.get_table_path() + "_7"
        try:
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Uint64 NOT NULL,
                    v0 DyNumber,
                    PRIMARY KEY(id),
                )
                WITH (STORE = COLUMN);
                """
            )
            assert False, "Should Fail"
        except ydb.issues.SchemeError as ex:
            assert "Type \\'DyNumber\\' specified for column \\'v0\\' is not supported" in ex.message
