import pytest

from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
)
from ydb.tests.oss.ydb_sdk_import import ydb


class CompactWorkload:
    TABLE_NAME = "compact_compat_table"
    ROW_COUNT = 2000
    UPSERT_BATCH = 200
    PRE_COMPACT_UPSERT_ROWS = 128
    PAYLOAD_FILLER_LEN = 1024

    def __init__(self, fixture):
        self.fixture = fixture
        self._pre_compact_seq = 0

    def _payload(self, row_id: int) -> str:
        return ("p" * self.PAYLOAD_FILLER_LEN) + f"{row_id:010d}"

    def _list_type_for_upsert_rows(self):
        row_type = ydb.StructType()
        row_type.add_member("id", ydb.PrimitiveType.Uint64)
        row_type.add_member("sk", ydb.PrimitiveType.Uint64)
        row_type.add_member("v", ydb.PrimitiveType.String)
        return ydb.ListType(row_type)

    def _upsert_batch_query(self):
        return f"""
            DECLARE $data AS List<Struct<id: Uint64, sk: Uint64, v: String>>;
            UPSERT INTO `{self.TABLE_NAME}` (id, sk, v)
            SELECT id, sk, v FROM AS_TABLE($data);
        """

    def create_table(self):
        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{self.TABLE_NAME}` (
                    id Uint64 NOT NULL,
                    sk Uint64 NOT NULL,
                    v String NOT NULL,
                    PRIMARY KEY (id),
                    INDEX idx_sk GLOBAL ON (sk) COVER (v),
                    INDEX idx_v GLOBAL ON (v)
                ) WITH (
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1
                );
                """
            )

    def fill(self):
        list_type = self._list_type_for_upsert_rows()

        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            for start in range(0, self.ROW_COUNT, self.UPSERT_BATCH):
                end = min(start + self.UPSERT_BATCH, self.ROW_COUNT)
                rows = [
                    {
                        "id": i,
                        "sk": i % 97,
                        "v": self._payload(i).encode("utf-8"),
                    }
                    for i in range(start, end)
                ]
                session_pool.execute_with_retries(
                    self._upsert_batch_query(),
                    {"$data": (rows, list_type)},
                )

    def upsert_before_compact(self):
        """Rewrite a prefix of rows so COMPACT has fresh mutations to apply."""
        seq = self._pre_compact_seq
        self._pre_compact_seq += 1
        ch = chr(ord("A") + (seq % 26))
        list_type = self._list_type_for_upsert_rows()
        rows = [
            {
                "id": i,
                "sk": i % 97,
                "v": (ch * self.PAYLOAD_FILLER_LEN + f"{i:010d}").encode("utf-8"),
            }
            for i in range(self.PRE_COMPACT_UPSERT_ROWS)
        ]
        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            session_pool.execute_with_retries(
                self._upsert_batch_query(),
                {"$data": (rows, list_type)},
            )

    def compact(self):
        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            # TODO: add CASCADE = true and PARALLEL
            session_pool.execute_with_retries(f"ALTER TABLE `{self.TABLE_NAME}` COMPACT;")

    def assert_row_count(self, expected):
        table = self.TABLE_NAME
        count_queries = (
            ("base table", f"SELECT COUNT(*) AS cnt FROM `{table}`"),
            ("idx_sk", f"SELECT COUNT(*) AS cnt FROM `{table}` VIEW idx_sk"),
            ("idx_v", f"SELECT COUNT(*) AS cnt FROM `{table}` VIEW idx_v"),
        )
        with ydb.QuerySessionPool(self.fixture.driver) as session_pool:
            for label, query in count_queries:
                result_sets = session_pool.execute_with_retries(query + ";")
                cnt = result_sets[0].rows[0]["cnt"]
                assert cnt == expected, f"{label}: COUNT(*) is {cnt}, expected {expected}"

            if expected > 0:
                sample_ids = {0, expected - 1} if expected > 1 else {0}
                for row_id in sample_ids:
                    base_rs = session_pool.execute_with_retries(
                        f"SELECT id, sk, v FROM `{table}` WHERE id = {row_id};"
                    )
                    assert len(base_rs[0].rows) == 1, f"missing base row id={row_id}"
                    want = base_rs[0].rows[0]

                    sk_rs = session_pool.execute_with_retries(
                        f"SELECT id, sk, v FROM `{table}` VIEW idx_sk WHERE id = {row_id};"
                    )
                    assert len(sk_rs[0].rows) == 1, f"idx_sk: missing row id={row_id}"
                    got_sk = sk_rs[0].rows[0]
                    assert got_sk["id"] == want["id"] and got_sk["sk"] == want["sk"] and got_sk["v"] == want["v"]

                    v_rs = session_pool.execute_with_retries(
                        f"SELECT id, v FROM `{table}` VIEW idx_v WHERE id = {row_id};"
                    )
                    assert len(v_rs[0].rows) == 1, f"idx_v: missing row id={row_id}"
                    got_v = v_rs[0].rows[0]
                    assert got_v["id"] == want["id"] and got_v["v"] == want["v"]


def _compact_setup_cluster(fixture):
    return fixture.setup_cluster(
        extra_feature_flags={
            "enable_forced_compactions": True,
        },
    )


class TestCompactMixedCluster(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 1):
            pytest.skip(
                "ALTER TABLE COMPACT and enable_forced_compactions are not supported in < 26.1"
            )
        yield from _compact_setup_cluster(self)

    def test_compact_mixed_cluster(self):
        workload = CompactWorkload(self)
        workload.create_table()
        workload.fill()
        workload.assert_row_count(CompactWorkload.ROW_COUNT)
        workload.upsert_before_compact()
        workload.compact()
        workload.assert_row_count(CompactWorkload.ROW_COUNT)


class TestCompactRestartToAnotherVersion(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 1):
            pytest.skip(
                "ALTER TABLE COMPACT and enable_forced_compactions are not supported in < 26.1"
            )
        yield from _compact_setup_cluster(self)

    def test_compact_survives_binary_restart(self):
        workload = CompactWorkload(self)
        workload.create_table()
        workload.fill()
        workload.assert_row_count(CompactWorkload.ROW_COUNT)
        workload.upsert_before_compact()
        workload.compact()

        self.change_cluster_version()

        workload.assert_row_count(CompactWorkload.ROW_COUNT)
        workload.upsert_before_compact()
        workload.compact()
        workload.assert_row_count(CompactWorkload.ROW_COUNT)


class TestCompactRolling(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 1):
            pytest.skip(
                "ALTER TABLE COMPACT and enable_forced_compactions are not supported in < 26.1"
            )
        yield from _compact_setup_cluster(self)

    def test_compact_rolling(self):
        workload = CompactWorkload(self)
        workload.create_table()
        workload.fill()
        workload.assert_row_count(CompactWorkload.ROW_COUNT)
        workload.upsert_before_compact()
        workload.compact()
        workload.assert_row_count(CompactWorkload.ROW_COUNT)

        for _ in self.roll():
            workload.assert_row_count(CompactWorkload.ROW_COUNT)
            workload.upsert_before_compact()
            workload.compact()
            workload.assert_row_count(CompactWorkload.ROW_COUNT)
