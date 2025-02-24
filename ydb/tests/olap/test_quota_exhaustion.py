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
            column_shard_config={"disabled_on_scheme_shard": False},
            static_pdisk_size=10 * 1024 * 1024,
            dynamic_pdisk_size=5 * 1024 * 1024
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @link_test_case("#13529")
    def test(self):
        driver = ydb.Driver(endpoint=f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', database='/Root')
        session = ydb.QuerySessionPool(driver)
        driver.wait(5, fail_fast=True)

        def create_table(table):
            return session.execute_with_retries(f"""
                CREATE TABLE {table} (
                    k Int32 NOT NULL,
                    v Uint64,
                    PRIMARY KEY (k)
                ) WITH (STORE = COLUMN)
            """)

        def upsert_chunk(table, chunk_id, retries=10):
            return session.execute_with_retries(f"""
                $n = {ROWS_CHUNK_SIZE};
                $values_list = ListReplicate(42ul, $n);
                $rows_list = ListFoldMap($values_list, {chunk_id * ROWS_CHUNK_SIZE}, ($val, $i) ->  ((<|k:$i, v:$val|>, $i + 1)));

                UPSERT INTO {table}
                SELECT * FROM AS_TABLE($rows_list);
            """, None, ydb.retries.RetrySettings(max_retries=retries))

        create_table('huge')

        try:
            for i in range(ROWS_CHUNKS_COUNT):
                res = upsert_chunk('huge', i, retries=0)
                print(f"query #{i} ok, result:", res, file=sys.stderr)
        except ydb.issues.Overloaded:
            print('got overload issue', file=sys.stderr)

        session.execute_with_retries("""DROP TABLE huge""")

        # Check database health after cleanup
        create_table('small')
        upsert_chunk('small', 0)
