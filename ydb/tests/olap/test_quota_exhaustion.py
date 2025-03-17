import sys
import datetime
import logging

import ydb
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

ROWS_CHUNK_SIZE = 3000000
ROWS_CHUNKS_COUNT = 100000

logger = logging.getLogger(__name__)


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            column_shard_config={},
            static_pdisk_size=10 * 1024 * 1024,
            dynamic_pdisk_size=5 * 1024 * 1024
        ))
        cls.cluster.start()

        cls.driver = ydb.Driver(endpoint=f'grpc://localhost:{cls.cluster.nodes[1].grpc_port}', database='/Root')
        cls.session = ydb.QuerySessionPool(cls.driver)
        cls.driver.wait(5, fail_fast=True)

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def create_table(self, table):
        return self.session.execute_with_retries(f"""
            CREATE TABLE {table} (
                k Int32 NOT NULL,
                v Uint64,
                PRIMARY KEY (k)
            ) WITH (STORE = COLUMN)
        """)

    def upsert_chunk(self, table, chunk_id, retries=10):
        return self.session.execute_with_retries(f"""
            $n = {ROWS_CHUNK_SIZE};
            $values_list = ListReplicate(42ul, $n);
            $rows_list = ListFoldMap($values_list, {chunk_id * ROWS_CHUNK_SIZE}, ($val, $i) ->  ((<|k:$i, v:$val|>, $i + 1)));

            UPSERT INTO {table}
            SELECT * FROM AS_TABLE($rows_list);
        """, None, ydb.retries.RetrySettings(max_retries=retries))

    def test(self):
        """As per https://github.com/ydb-platform/ydb/issues/13529"""
        self.create_table('huge')

        try:
            for i in range(ROWS_CHUNKS_COUNT):
                res = self.upsert_chunk('huge', i, retries=0)
                print(f"query #{i} ok, result:", res, file=sys.stderr)
        except ydb.issues.Overloaded:
            print('got overload issue', file=sys.stderr)

        self.session.execute_with_retries("""DROP TABLE huge""")

        # Check database health after cleanup
        self.create_table('small')
        self.upsert_chunk('small', 0)

    def test_duplicates(self):
        """https://github.com/ydb-platform/ydb/issues/13652"""
        self.create_table('table')

        duration_limit = datetime.timedelta(seconds=30)
        deadline = datetime.datetime.now() + duration_limit
        last_stats_report = datetime.datetime.now()
        while True:
            try:
                res = self.upsert_chunk('table', 0, retries=0)
                logger.debug(f"query ok, result: {res}")
            except ydb.issues.Overloaded:
                break

            # Report stats
            if datetime.datetime.now() - last_stats_report > datetime.timedelta(seconds=5):
                last_stats_report = datetime.datetime.now()
                res = self.session.execute_with_retries('SELECT SUM(ColumnBlobBytes) AS bytes FROM `table/.sys/primary_index_portion_stats`')[0].rows[0]['bytes']
                logger.info(f"stats: blobBytes={res}")

            # Check timeout
            assert datetime.datetime.now() <= deadline, f"couldn't reach storage limit in {duration_limit}"

        duration_limit = datetime.timedelta(seconds=10)
        deadline = datetime.datetime.now() + duration_limit
        while True:
            try:
                res = self.upsert_chunk('table', 1, retries=0)
                logger.debug(f"query ok, result: {res}")
                break
            except ydb.issues.Overloaded:
                logger.debug(f"query overloaded")

            # Check timeout
            assert datetime.datetime.now() <= deadline, f"database is still overloaded after {duration_limit}"
