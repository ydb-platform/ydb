import sys
import time

import ydb
from threading import Thread
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels

ROWS_CHUNK_SIZE = 100000
ROWS_CHUNKS_COUNT = 2


class TestZipBomb(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            column_shard_config={},
            additional_log_configs={'MEMORY_CONTROLLER': LogLevels.INFO, "TX_COLUMNSHARD": LogLevels.DEBUG},
            extra_feature_flags={'enable_write_portions_on_insert': True},
            static_pdisk_size=10 * 1024 * 1024,
            dynamic_pdisk_size=5 * 1024 * 1024
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def make_session(self):
        driver = ydb.Driver(endpoint=f'grpc://localhost:{self.cluster.nodes[1].grpc_port}', database=self.database_name)
        session = ydb.QuerySessionPool(driver)
        driver.wait(5, fail_fast=True)
        return session

    def create_test_str_table(self, session, table):
        return session.execute_with_retries(f"""
                CREATE TABLE `{table}` (
                    k Int32 NOT NULL,
                    v1 String,
                    v2 String,
                    v3 String,
                    v4 String,
                    v5 String,
                    PRIMARY KEY (k)
                ) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=1)
            """)

    def upsert_test_str_chunk(self, session, table, chunk_id, retries=10):
        long_string = 'x' * 5000
        return session.execute_with_retries(f"""
                $n = {ROWS_CHUNK_SIZE};
                $values_list = ListReplicate(42ul, $n);
                $value = '{long_string}';
                $rows_list = ListFoldMap($values_list, {chunk_id * ROWS_CHUNK_SIZE}, ($val, $i)->((<|k:$i, v1:$value||'1', v2:$value||'2', v3:$value||'3', v4:$value||'4', v5:$value||'5'|>, $i + 1)));

                UPSERT INTO `{table}`
                SELECT * FROM AS_TABLE($rows_list);
            """, None, ydb.retries.RetrySettings(max_retries=retries))

    def upsert_str(self, session, table):
        for i in range(ROWS_CHUNKS_COUNT):
            res = self.upsert_test_str_chunk(session, table, i, retries=0)
            print(f"upsert #{i} ok, result:", res, file=sys.stderr)

    def select(self, table, session):
        result = session.execute_with_retries("""
            SELECT
                MAX(v1),
                MAX(v2),
                MAX(v3),
                MAX(v4),
                MAX(v5)
            FROM huge
        """)
        print(result[0].rows, file=sys.stderr)

    def get_rss(self, pid):
        with open(f"/proc/{pid}/status", "r") as f:
            for line in f:
                if line.startswith("RssAnon:"):
                    return int(line.split()[1])

    def watch_rss(self, pid, rss):
        maxrss = 0
        try:
            while rss[1] == 0:
                rss_kb = self.get_rss(pid)
                if rss_kb > maxrss:
                    maxrss = rss_kb
                time.sleep(1)
        except FileNotFoundError:
            return
        rss[0] = maxrss

    def test(self):
        """As per https://github.com/ydb-platform/ydb/issues/13529"""
        pid = self.cluster.nodes[1].pid
        maxrss = [0, 0]
        watch_thread = Thread(target=self.watch_rss, args=[pid, maxrss])
        watch_thread.start()
        print('Pid {}'.format(pid), file=sys.stderr)
        self.database_name = '/Root'
        session = self.make_session()

        # Overflow the database
        self.create_test_str_table(session, 'huge')
        self.upsert_str(session, 'huge')
        rss = self.get_rss(pid)
        print('Rss after upsert {}'.format(rss), file=sys.stderr)
        threads = []
        for c in range(20):
            thread = Thread(target=self.select, args=['huge', session])
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        maxrss[1] = 1
        watch_thread.join()
        print('Max rss {}', format(maxrss[0]), file=sys.stderr)
        assert maxrss[0] < 12 * 1024 * 1024, "Too high memory consumption"
