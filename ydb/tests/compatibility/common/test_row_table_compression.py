import pytest

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class RowTableCompression:
    def query(self, sql):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(sql)

    def assert_zstd_rejected(self, sql):
        with pytest.raises((ydb.BadRequest, ydb.GenericError, ydb.SchemeError)) as error:
            self.query(sql)
        message = str(error.value).lower()
        assert "compression" in message or "codec" in message or "zstd" in message

    def create_table(self):
        self.query("""
            CREATE TABLE row_table (
                id Uint64 NOT NULL,
                value String FAMILY archive,
                PRIMARY KEY (id),
                FAMILY default (COMPRESSION = "lz4"),
                FAMILY archive (COMPRESSION = "lz4")
            );
        """)

    def check_zstd_disabled(self):
        self.assert_zstd_rejected("""
            CREATE TABLE zstd_table (
                id Uint64 NOT NULL,
                PRIMARY KEY (id),
                FAMILY default (COMPRESSION = "zstd")
            );
        """)
        self.assert_zstd_rejected("""
            ALTER TABLE row_table ALTER FAMILY archive SET COMPRESSION "zstd";
        """)
        self.assert_zstd_rejected("""
            ALTER TABLE row_table ADD FAMILY cold (COMPRESSION = "zstd");
        """)

    def write_and_check(self, count):
        key = count - 1
        self.query(f'UPSERT INTO row_table (id, value) VALUES ({key}ul, "value-{key}");')
        rows = self.query("SELECT id, value FROM row_table ORDER BY id;")[0].rows
        assert [(row.id, row.value) for row in rows] == [
            (i, f"value-{i}".encode()) for i in range(count)
        ]


class TestRowTableCompressionMixed(MixedClusterFixture, RowTableCompression):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        # Keep ZSTD disabled until every node has been upgraded.
        yield from self.setup_cluster()

    def test_zstd_disabled_in_mixed_cluster(self):
        self.create_table()
        self.check_zstd_disabled()
        self.write_and_check(1)


class TestRowTableCompressionUpgrade(RestartToAnotherVersionFixture, RowTableCompression):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.versions[0] >= self.versions[1]:
            pytest.skip("Only check forward compatibility")
        yield from self.setup_cluster()

    def test_zstd_disabled_during_upgrade(self):
        self.create_table()
        self.check_zstd_disabled()
        self.write_and_check(1)

        self.change_cluster_version()

        self.check_zstd_disabled()
        self.write_and_check(2)
