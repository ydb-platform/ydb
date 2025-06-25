import os
import subprocess
import sys
import logging
import time
import pytest

import ydb
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.test_meta import link_test_case

ROWS_CHUNK_SIZE = 1000000
ROWS_CHUNKS_COUNT = 500

logger = logging.getLogger(__name__)


class TestYdbWorkload(object):
    @classmethod
    def setup_method(self):
        self.cluster = KiKiMR(KikimrConfigGenerator(
            column_shard_config={
                "alter_object_enabled": True,
            },
            static_pdisk_size=10 * 1024 * 1024,
            dynamic_pdisk_size=5 * 1024 * 1024
        ))
        self.cluster.start()

        self.driver = ydb.Driver(endpoint=f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', database='/Root')
        self.session = ydb.QuerySessionPool(self.driver)
        self.driver.wait(5, fail_fast=True)

    @classmethod
    def teardown_method(self):
        self.session.stop()
        self.driver.stop()
        self.cluster.stop()

    def make_session(self):
        driver = ydb.Driver(endpoint=f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', database=self.database_name)
        session = ydb.QuerySessionPool(driver)
        driver.wait(5, fail_fast=True)
        return session

    def create_test_table(self, session, table):
        return session.execute_with_retries(f"""
                CREATE TABLE `{table}` (
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

                UPSERT INTO `{table}`
                SELECT * FROM AS_TABLE($rows_list);
            """, None, ydb.retries.RetrySettings(max_retries=retries))

    def try_upsert_test_chunk(self, session, table, chunk_id) -> bool:
        try:
            self.upsert_test_chunk(session, table, chunk_id, retries=0)
            return True
        except ydb.issues.Overloaded:
            return False
        except ydb.issues.Unavailable:
            return False

    def upsert_until_overload(self, do_upsert, timeout_seconds=0):
        deadline = time.time() + timeout_seconds
        try:
            for i in range(ROWS_CHUNKS_COUNT):
                res = do_upsert(i)
                print(f"upsert #{i} ok, result:", res, file=sys.stderr)
                described = self.cluster.client.describe('/Root', '')
                print('Quota exceeded {}'.format(described.PathDescription.DomainDescription.DomainState.DiskQuotaExceeded), file=sys.stderr)
                if timeout_seconds:
                    assert time.time() <= deadline, "deadline exceeded"
            assert False, "overload not reached"
        except ydb.issues.Overloaded:
            print('upsert: got overload issue', file=sys.stderr)
        except ydb.issues.Unavailable:
            print('upsert: got overload issue', file=sys.stderr)

    @link_test_case("#13529")
    def test(self):
        """As per https://github.com/ydb-platform/ydb/issues/13529"""
        self.database_name = os.path.join('/Root', 'test')
        self.cluster.create_database(
            self.database_name,
            storage_pool_units_count={
                'hdd': 1
            },
        )
        self.cluster.register_and_start_slots(self.database_name, count=1)
        self.cluster.wait_tenant_up(self.database_name)
        self.alter_database_quotas(self.cluster.nodes[1], self.database_name, """
            data_size_hard_quota: 40000000
            data_size_soft_quota: 40000000
        """)
        session = self.make_session()

        # Overflow the database
        self.create_test_table(session, 'huge')
        self.upsert_until_overload(lambda i: self.upsert_test_chunk(session, 'huge', i, retries=0))

    def delete_test_chunk(self, session, table, chunk_id, retries=10):
        session.execute_with_retries(f"""
            DELETE FROM `{table}`
            WHERE {chunk_id * ROWS_CHUNK_SIZE} <= k AND k <= {chunk_id * ROWS_CHUNK_SIZE + ROWS_CHUNK_SIZE}
        """, None, ydb.retries.RetrySettings(max_retries=retries))

    def delete_until_overload(self, session, table):
        for i in range(ROWS_CHUNKS_COUNT):
            try:
                self.delete_test_chunk(session, table, i, retries=0)
                print(f"delete #{i} ok", file=sys.stderr)
            except ydb.issues.Unavailable:
                print('delete: got unavailable issue', file=sys.stderr)
                return i
            except ydb.issues.Overloaded:
                print('delete: got overload issue', file=sys.stderr)
                return i
        return ROWS_CHUNKS_COUNT

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

    @link_test_case("#13653")
    def test_delete(self):
        """As per https://github.com/ydb-platform/ydb/issues/13653"""
        self.database_name = os.path.join('/Root', 'test')
        print('Database name {}'.format(self.database_name), file=sys.stderr)
        self.cluster.create_database(
            self.database_name,
            storage_pool_units_count={
                'hdd': 1
            },
        )
        self.cluster.register_and_start_slots(self.database_name, count=1)
        self.cluster.wait_tenant_up(self.database_name)

        # Set soft and hard quotas to 40 Mb
        self.alter_database_quotas(self.cluster.nodes[1], self.database_name, """
            data_size_hard_quota: 40000000
            data_size_soft_quota: 40000000
        """)

        session = self.make_session()

        # Overflow the database
        table_path = os.path.join(self.database_name, 'huge')
        self.create_test_table(session, table_path)
        self.upsert_until_overload(lambda i: self.upsert_test_chunk(session, 'huge', i, retries=0))

        # Check that deletion works at least first time
        self.delete_test_chunk(session, table_path, 0)

        # Check that deletions will lead to overflow at some moment
        i = self.delete_until_overload(session, table_path)

        # Check that all DELETE statements are completed
        assert i == ROWS_CHUNKS_COUNT

        # Writes enabling after data deletion will be checked in separate PR

    def wait_for(self, condition_func, timeout_seconds) -> bool:
        t0 = time.time()
        while time.time() - t0 < timeout_seconds:
            if condition_func():
                return True
            time.sleep(1)
        return False

    @link_test_case("#13652")
    def test_duplicates(self):
        self.database_name = os.path.join('/Root', 'test')
        print('Database name {}'.format(self.database_name), file=sys.stderr)
        self.cluster.create_database(
            self.database_name,
            storage_pool_units_count={
                'hdd': 1
            },
        )
        self.cluster.register_and_start_slots(self.database_name, count=1)
        self.cluster.wait_tenant_up(self.database_name)

        # Set soft and hard quotas to 40 Mb
        self.alter_database_quotas(self.cluster.nodes[1], self.database_name, """
            data_size_hard_quota: 40000000
            data_size_soft_quota: 40000000
        """)

        session = self.make_session()
        table_path = os.path.join(self.database_name, 'huge')
        self.create_test_table(session, table_path)

        # Delay compaction
        session.execute_with_retries(
            f"""
            ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {{"levels" : [{{"class_name" : "Zero", "portions_live_duration" : "200s", "expected_blobs_size" : 1000000000000, "portions_count_available" : 2}},
                                {{"class_name" : "Zero"}}]}}`);
            """
        )

        # Overflow the database
        self.upsert_until_overload(lambda i: self.upsert_test_chunk(session, table_path, 0, retries=0), timeout_seconds=200)

        assert self.wait_for(lambda: self.try_upsert_test_chunk(session, table_path, 1), 300), "can't write after overload by duplicates"

    @link_test_case("#14696")
    @pytest.mark.skip(reason="https://github.com/ydb-platform/ydb/issues/19629")
    def test_delete_after_overloaded(self):
        self.database_name = os.path.join('/Root', 'test')

        self.cluster.create_database(
            self.database_name,
            storage_pool_units_count={
                'hdd': 1
            },
        )

        self.cluster.register_and_start_slots(self.database_name, count=1)
        self.cluster.wait_tenant_up(self.database_name)

        self.alter_database_quotas(self.cluster.nodes[1], self.database_name, """
            data_size_hard_quota: 40000000
            data_size_soft_quota: 40000000
        """)

        session = self.make_session()
        table_path = os.path.join(self.database_name, 'huge')

        self.create_test_table(session, table_path)

        session.execute_with_retries(
            f"""
            ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                {{"levels" : [{{"class_name" : "OneLayer", "portions_live_duration" : "200s", "expected_blobs_size" : 1000000000000, "portions_count_available" : 2}},
                                {{"class_name" : "OneLayer"}}]}}`);
            """
        )

        self.upsert_until_overload(lambda i: self.upsert_test_chunk(session, table_path, i, retries=0))

        for i in range(100):
            while True:
                try:
                    self.delete_test_chunk(session, table_path, i, retries=10)
                    break
                except (ydb.issues.Overloaded, ydb.issues.Unavailable):
                    time.sleep(1)

        def can_write():
            try:
                self.upsert_test_chunk(session, table_path, 0, retries=0)
                return True
            except (ydb.issues.Overloaded, ydb.issues.Unavailable):
                return False

        assert self.wait_for(can_write, 300)
