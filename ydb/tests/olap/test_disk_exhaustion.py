import sys

import ydb
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


class YdbClient:
    def __init__(self, endpoint, database):
        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.database = database
        self.session_pool = ydb.QuerySessionPool(self.driver)

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement):
        return self.session_pool.execute_with_retries(statement, None, ydb.retries.RetrySettings(max_retries=1))


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

    def test(self):
        client = YdbClient(f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', '/Root')

        try:
            client.query("DROP TABLE huge")
        except ydb.issues.SchemeError:
            pass

        def create_table(table):
            return client.query(f"""
                CREATE TABLE {table} (
                    k Int32 NOT NULL,
                    v Uint64,
                    PRIMARY KEY (k)
                ) WITH (STORE = COLUMN)
            """)

        def upsert_chunk(table, chunk_id):
            return client.query(f"""
                $n = {ROWS_CHUNK_SIZE};
                $values_list = ListReplicate(42ul, $n);
                $rows_list = ListFoldMap($values_list, {chunk_id * ROWS_CHUNK_SIZE}, ($val, $i) -> ((<|k:$i, v:$val|>, $i + 1)));

                UPSERT INTO {table}
                SELECT * FROM AS_TABLE($rows_list);
            """)

        create_table('huge')

        try:
            for i in range(ROWS_CHUNKS_COUNT):
                res = upsert_chunk('huge', i)
                print(f"query #{i} ok, result:", res, file=sys.stderr)
        except ydb.issues.Overloaded:
            print('got overload issue', file=sys.stderr)

        client.query("""DROP TABLE huge""")

        # Check database health after cleanup
        create_table('small')
        upsert_chunk('small', 0)
