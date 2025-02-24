import os
import subprocess
import sys

import ydb
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.test_meta import link_test_case

ROWS_CHUNK_SIZE = 3000000
ROWS_CHUNKS_COUNT = 100000


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            column_shard_config={},
            static_pdisk_size=10 * 1024 * 1024,
            dynamic_pdisk_size=5 * 1024 * 1024
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def make_session(self):
        driver = ydb.Driver(endpoint=f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', database='/Root')
        session = ydb.QuerySessionPool(driver)
        driver.wait(5, fail_fast=True)
        return session

    def create_test_table(self, session, table):
        return session.execute_with_retries(f"""
                CREATE TABLE {table} (
                    k Int32 NOT NULL,
                    v Uint64,
                    PRIMARY KEY (k)
                ) WITH (STORE = COLUMN)
            """)

    def upsert_test_chunk(self, session, table, chunk_id, retries=10):
        return session.execute_with_retries(f"""
                $n = {ROWS_CHUNK_SIZE};
                $values_list = ListReplicate(42ul, $n);
                $rows_list = ListFoldMap($values_list, {chunk_id * ROWS_CHUNK_SIZE}, ($val, $i) ->  ((<|k:$i, v:$val|>, $i + 1)));

                UPSERT INTO {table}
                SELECT * FROM AS_TABLE($rows_list);
            """, None, ydb.retries.RetrySettings(max_retries=retries))

    def upsert_until_overload(self, session, table):
        try:
            for i in range(ROWS_CHUNKS_COUNT):
                res = self.upsert_test_chunk(session, table, i, retries=0)
                print(f"upsert #{i} ok, result:", res, file=sys.stderr)
        except ydb.issues.Overloaded:
            print('upsert: got overload issue', file=sys.stderr)

    @link_test_case("#13529")
    def test(self):
        """As per https://github.com/ydb-platform/ydb/issues/13529"""
        session = self.make_session()

        # Overflow the database
        self.create_test_table(session, 'huge')
        self.upsert_until_overload(session, 'huge')

        # Cleanup
        session.execute_with_retries("""DROP TABLE huge""")

        # Check database health after cleanup
        self.create_test_table(session, 'small')
        self.upsert_test_chunk(session, 'small', 0)

    def delete_test_chunk(self, session, table, chunk_id, retries=10):
        session.execute_with_retries(f"""
            DELETE FROM {table}
            WHERE {chunk_id * ROWS_CHUNK_SIZE} <= k AND k <= {chunk_id * ROWS_CHUNK_SIZE + ROWS_CHUNK_SIZE}
        """, None, ydb.retries.RetrySettings(max_retries=retries))

    def delete_until_overload(self, session, table):
        for i in range(ROWS_CHUNKS_COUNT):
            try:
                self.delete_test_chunk(session, table, i, retries=0)
                print(f"delete #{i} ok", file=sys.stderr)
            except ydb.issues.Overloaded:
                print('delete: got overload issue', file=sys.stderr)
                return i

    def ydbcli_db_schema_exec(self, node, operation_proto):
        endpoint = f"{node.host}:{node.port}"
        args = [
            node.binary_path,
            f"--server=grpc://{endpoint}",
            "db",
            "schema",
            "exec",
            operation_proto,
        ]
        command = subprocess.run(args, capture_output=True)
        assert command.returncode == 0, command.stderr.decode("utf-8")

    def alter_database_quotas(self, node, database_path, database_quotas):
        alter_proto = """ModifyScheme {
            OperationType: ESchemeOpAlterSubDomain
            WorkingDir: "%s"
            SubDomain {
                Name: "%s"
                DatabaseQuotas {
                    %s
                }
            }
        }""" % (
            os.path.dirname(database_path),
            os.path.basename(database_path),
            database_quotas,
        )

        self.ydbcli_db_schema_exec(node, alter_proto)

    def test_delete(self):
        """As per https://github.com/ydb-platform/ydb/issues/13653"""
        session = self.make_session()

        # Set soft and hard quotas to 6GB
        self.alter_database_quotas(self.cluster.nodes[1], '/Root', """
            data_size_hard_quota: 6000000000
            data_size_soft_quota: 6000000000
        """)

        # Overflow the database
        self.create_test_table(session, 'huge')
        self.upsert_until_overload(session, 'huge')

        # Check that deletion works at least first time
        # self.delete_test_chunk(session, 'huge', 0)
        # ^ uncomment after fixing https://github.com/ydb-platform/ydb/issues/13808

        # Check that deletions will lead to overflow at some moment
        i = self.delete_until_overload(session, 'huge')

        # Try to wait until deletion works again (after compaction)
        self.delete_test_chunk(session, 'huge', i)
