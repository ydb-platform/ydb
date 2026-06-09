import logging
import random
import threading
import time

import pytest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------
#
# The test checks snapshot-isolation correctness under concurrent writes and
# during a rolling cluster upgrade/downgrade.
#
# Two source tables are maintained in parallel — one row-oriented
# (datashard_table) and one column-oriented (column_table) — each holding
# N_ROWS=10 000 rows with the schema (key, int_val, str_val):
#
#   key      : 0 .. 9_999  (unique row identifier)
#   int_val  : key % GROUP_SIZE   (cycles 0-9 within every group of 10 rows)
#   str_val  : zero-padded string mirror of int_val, e.g. "00003"
#
# Rows are divided into N_GROUPS=1000 consecutive groups of GROUP_SIZE=10
# rows each.  Within every group, int_val takes each value in 0..9 exactly
# once, so the group sum is always FIXED_GROUP_SUM = 0+1+…+9 = 45.
# Updater threads continuously shuffle int_val values within a randomly
# chosen group (preserving the group sum).
#
# ---------------------------------------------------------------------------
# Result tables  (datashard_results / column_results)
# ---------------------------------------------------------------------------
#
# Each result table holds exactly one row per filter type (filter_type is the
# primary key).  A row records the aggregate computed by the most recently
# committed aggregator transaction for that filter type:
#
#   filter_type    – one of "pk_range", "int_range", "str_range"
#   filter_lo      – lower bound used in the source-table query
#   filter_hi      – upper bound used in the source-table query
#   row_count      – COUNT(*) returned by that query
#   int_sum        – SUM(int_val) returned by that query
#   count_distinct – COUNT(DISTINCT str_val) returned by that query
#
# Each aggregator thread runs a snapshot-isolation read-write transaction:
#   1. READ aggregate from the source table with the chosen bounds.
#   2. Sleep a random short interval (to widen the window for conflicts).
#   3. UPSERT the result row — overwriting whichever bounds/values were there
#      before.
#
# Because filter_type is the only PK column, all concurrent aggregators of
# the same type write to the same single row, which generates write conflicts
# that the SDK retries automatically.
#
# ---------------------------------------------------------------------------
# Invariants verified by check_results()
# ---------------------------------------------------------------------------
#
#   pk_range  : the range [filter_lo, filter_hi] is always group-aligned, so
#               row_count == filter_hi - filter_lo + 1,
#               int_sum   == (row_count / GROUP_SIZE) * FIXED_GROUP_SUM, and
#               count_distinct == GROUP_SIZE (all 10 int_val values present).
#
#   int_range / str_range : a filter on int_val (or its string mirror) in
#               [filter_lo, filter_hi] matches exactly one row per group, so
#               row_count      == (filter_hi - filter_lo + 1) * N_GROUPS,
#               int_sum        == avg(filter_lo..filter_hi) * N_GROUPS * range_len,
#               count_distinct == filter_hi - filter_lo + 1.
#
# Any violation indicates that a transaction read an inconsistent snapshot
# (mixing data from different logical points in time).
# ---------------------------------------------------------------------------

N_ROWS = 10_000
GROUP_SIZE = 10
N_GROUPS = N_ROWS // GROUP_SIZE  # 1000
FIXED_GROUP_SUM = GROUP_SIZE * (GROUP_SIZE - 1) // 2  # 45 = 0+1+...+9

class TestSnapshotIsolation(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            table_service_config={
                "enable_snapshot_isolation_rw": True,
            },
        )

    # -------------------------------------------------------------------------
    # Table management
    # -------------------------------------------------------------------------

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def setup_tables(self):
        # 13 approximately uniformly partitioned shards in PK range [0, 10000)
        self._execute("""
            CREATE TABLE `datashard_table` (
                key     Int32 NOT NULL,
                int_val Int32,
                str_val String,
                PRIMARY KEY (key),
                INDEX int_val_index GLOBAL ON (int_val)
            ) WITH (
                PARTITION_AT_KEYS = (770, 1540, 2310, 3080, 3850, 4620, 5390, 6160, 6930, 7700, 8470, 9240)
            )
        """)
        self._execute("""
            CREATE TABLE `column_table` (
                key     Int32 NOT NULL,
                int_val Int32,
                str_val String,
                PRIMARY KEY (key)
            ) WITH (
                STORE = COLUMN,
                PARTITION_COUNT = 10
            )
        """)
        for name in ('datashard_results', 'column_results'):
            query = f"""
                CREATE TABLE `{name}` (
                    filter_type        String NOT NULL,
                    filter_lo          Int32,
                    filter_hi          Int32,
                    row_count          Int32,
                    int_sum            Int64,
                    count_distinct     Int32,
                    PRIMARY KEY (filter_type)
                )
            """
            if name == 'column_results':
                query += """
                WITH (
                    STORE = COLUMN,
                    PARTITION_COUNT = 1
                )"""
            self._execute(query)

    def populate_tables(self):
        # Use ListMap/AS_TABLE to generate 10k rows in a single YQL query.
        # int_val = key % GROUP_SIZE, so every group of GROUP_SIZE consecutive rows
        # sums to FIXED_GROUP_SUM = 0+1+...+(GROUP_SIZE-1) = 45.
        # str_val = "0000" || digit, a zero-padded 5-char mirror of int_val so that
        # lexicographic and numeric ordering agree for values 0-9.
        query = """
            $data = ListMap(ListFromRange(0, {n_rows}), ($x) -> {{
                RETURN AsStruct(
                    CAST($x AS Int32) AS key,
                    CAST($x % 10 AS Int32) AS int_val,
                    "0000" || CAST($x % 10 AS String) AS str_val
                );
            }});
            UPSERT INTO `{table}` SELECT * FROM AS_TABLE($data)
        """
        self._execute(query.format(table='datashard_table', n_rows=N_ROWS))
        self._execute(query.format(table='column_table', n_rows=N_ROWS))

    # -------------------------------------------------------------------------
    # Worker threads
    # -------------------------------------------------------------------------

    def _updater(self, table_name, stop_event, exec_counter, lock):
        """
        Repeatedly picks a random group and redistributes its int_val values
        while preserving the group sum invariant (SUM == FIXED_GROUP_SUM).
        """

        driver = self.create_driver()
        with ydb.QuerySessionPool(driver) as pool:
            while not stop_event.is_set():
                try:
                    group = random.randint(0, N_GROUPS - 1)
                    lo = group * GROUP_SIZE
                    # Shuffle [0..GROUP_SIZE-1] — any permutation also sums to FIXED_GROUP_SUM.
                    new_vals = list(range(GROUP_SIZE))
                    random.shuffle(new_vals)

                    values_str = ", ".join(
                        f"({lo + i}, {new_vals[i]}, \"{new_vals[i]:05d}\")"
                        for i in range(GROUP_SIZE)
                    )
                    pool.execute_with_retries(
                        f"UPSERT INTO `{table_name}` (key, int_val, str_val) VALUES {values_str}"
                    )

                    with lock:
                        exec_counter[0] += 1
                except Exception as e:
                    logger.warning("Updater [%s] error: %s", table_name, e)

    def _pk_aggregator(self, table_name, result_table, stop_event, exec_counter, lock):
        """
        Reads a group-aligned PK range under snapshot isolation, computes the
        aggregate, and writes it to the result table (generating write conflicts
        when multiple threads write with the same filter_type)
        """

        driver = self.create_driver()
        with ydb.QuerySessionPool(driver) as pool:
            while not stop_event.is_set():
                try:
                    lo_group = random.randint(0, N_GROUPS - 2)
                    hi_group = random.randint(lo_group + 1, N_GROUPS - 1)
                    lo = lo_group * GROUP_SIZE
                    hi = hi_group * GROUP_SIZE - 1

                    def callee(tx, lo=lo, hi=hi):
                        with tx.execute(
                            f"SELECT count(*) as rc, sum(int_val) as sum, count(distinct str_val) as cd "
                            f"FROM `{table_name}` WHERE key BETWEEN $lo AND $hi",
                            parameters={
                                '$lo': lo,
                                '$hi': hi,
                            }) as results:
                            res = list(results)[0].rows[0]

                        time.sleep(random.uniform(0, 0.1))

                        with tx.execute(
                            f"UPSERT INTO `{result_table}` "
                            "(filter_type, filter_lo, filter_hi, row_count, int_sum, count_distinct) "
                            f"VALUES (\"pk_range\", $lo, $hi, $row_count, $int_sum, $cd)",
                            parameters={
                                '$lo': (lo, ydb.PrimitiveType.Int32),
                                '$hi': (hi, ydb.PrimitiveType.Int32),
                                '$row_count': (res.rc, ydb.PrimitiveType.Int32),
                                '$int_sum': (res.sum, ydb.PrimitiveType.Int64),
                                '$cd': (res.cd, ydb.PrimitiveType.Int32),
                            },
                            commit_tx=True):
                            pass

                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())

                    with lock:
                        exec_counter[0] += 1
                except Exception as e:
                    logger.warning("PK aggregator [%s] error: %s", table_name, e)

    def _int_range_aggregator(self, table_name, result_table, stop_event, exec_counter, lock):
        """
        Reads `table_name` with an int value filter under snapshot isolation. For the datashard
        table uses a secondary index.
        """

        driver = self.create_driver()
        with ydb.QuerySessionPool(driver) as pool:
            while not stop_event.is_set():
                try:
                    lo = random.randint(0, GROUP_SIZE - 2)
                    hi = random.randint(lo + 1, GROUP_SIZE - 1)

                    def callee(tx, lo=lo, hi=hi):
                        view_clause = "VIEW int_val_index" if table_name == "datashard_table" else ""
                        agg_query = f"""
                        SELECT count(*) as rc, sum(int_val) as sum, count(distinct str_val) as cd
                        FROM `{table_name}` {view_clause}
                        WHERE int_val BETWEEN $lo AND $hi
                        """

                        with tx.execute(
                            agg_query,
                            parameters={
                                '$lo': lo,
                                '$hi': hi,
                            }) as results:
                            res = list(results)[0].rows[0]

                        time.sleep(random.uniform(0, 0.1))

                        with tx.execute(
                            f"UPSERT INTO `{result_table}` "
                            "(filter_type, filter_lo, filter_hi, row_count, int_sum, count_distinct) "
                            "VALUES (\"int_range\", $lo, $hi, $row_count, $int_sum, $cd)",
                            parameters={
                                '$lo': (lo, ydb.PrimitiveType.Int32),
                                '$hi': (hi, ydb.PrimitiveType.Int32),
                                '$row_count': (res.rc, ydb.PrimitiveType.Int32),
                                '$int_sum': (res.sum, ydb.PrimitiveType.Int64),
                                '$cd': (res.cd, ydb.PrimitiveType.Int32),
                            },
                            commit_tx=True):
                            pass

                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())

                    with lock:
                        exec_counter[0] += 1
                except Exception as e:
                    logger.warning("Int range aggregator [%s] error: %s", table_name, e)

    def _str_range_aggregator(self, table_name, result_table, stop_event, exec_counter, lock):
        """
        Reads `table_name` with a string-range filter under snapshot isolation.
        """

        driver = self.create_driver()
        with ydb.QuerySessionPool(driver) as pool:
            while not stop_event.is_set():
                try:
                    lo = random.randint(0, GROUP_SIZE - 2)
                    hi = random.randint(lo + 1, GROUP_SIZE - 1)
                    str_lo = f"{lo:05d}"
                    str_hi = f"{hi:05d}"

                    def callee(tx, lo=lo, hi=hi, str_lo=str_lo, str_hi=str_hi):
                        with tx.execute(
                            f"SELECT count(*) as rc, sum(int_val) as sum, count(distinct str_val) as cd "
                            f"FROM `{table_name}` "
                            f"WHERE str_val BETWEEN $str_lo AND $str_hi",
                            parameters={
                                '$str_lo': str_lo,
                                '$str_hi': str_hi,
                            }) as results:
                            res = list(results)[0].rows[0]

                        time.sleep(random.uniform(0, 0.1))

                        with tx.execute(
                            f"UPSERT INTO `{result_table}` "
                            "(filter_type, filter_lo, filter_hi, row_count, int_sum, count_distinct) "
                            "VALUES (\"str_range\", $lo, $hi, $row_count, $int_sum, $cd)",
                            parameters={
                                '$lo': (lo, ydb.PrimitiveType.Int32),
                                '$hi': (hi, ydb.PrimitiveType.Int32),
                                '$row_count': (res.rc, ydb.PrimitiveType.Int32),
                                '$int_sum': (res.sum, ydb.PrimitiveType.Int64),
                                '$cd': (res.cd, ydb.PrimitiveType.Int32),
                            },
                            commit_tx=True):
                            pass

                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())

                    with lock:
                        exec_counter[0] += 1
                except Exception as e:
                    logger.warning("String range aggregator [%s] error: %s", table_name, e)

    # -------------------------------------------------------------------------
    # Checker (runs in the main thread)
    # -------------------------------------------------------------------------

    def check_results(self, result_table):
        rows = []
        for rs in self._execute(f"SELECT * FROM `{result_table}`"):
            rows += rs.rows

        assert len(rows) == 3 # 3 filter types

        for row in rows:
            logger.info(f"Result row from {result_table}: {row}")
            filter_type = row.filter_type
            if isinstance(filter_type, bytes):
                filter_type = filter_type.decode()

            if filter_type == 'pk_range':
                assert row.row_count == (row.filter_hi - row.filter_lo + 1)
                expected_sum = (row.row_count // GROUP_SIZE) * FIXED_GROUP_SUM
                assert row.int_sum == expected_sum
                assert row.count_distinct == GROUP_SIZE

            elif filter_type in ('int_range', 'str_range'):
                range_len = row.filter_hi - row.filter_lo + 1
                assert row.row_count == range_len * N_GROUPS
                assert row.int_sum == (range_len * (row.filter_hi + row.filter_lo) / 2) * N_GROUPS
                assert row.count_distinct == range_len

            else:
                assert False, f"unknown filter type {filter_type}"

    # -------------------------------------------------------------------------
    # Progress tracking
    # -------------------------------------------------------------------------

    def _wait_for_progress(self, exec_counters, lock, min_per_counter, timeout=300):
        baseline = {}
        with lock:
            for name, c in exec_counters.items():
                baseline[name] = c[0]
        deadline = time.time() + timeout
        while time.time() < deadline:
            with lock:
                logger.debug(f"worker exec counters: {exec_counters}")
                done = all(
                    exec_counters[name][0] - baseline[name] >= min_per_counter
                    for name in exec_counters
                )
            if done:
                return
            time.sleep(1)
        raise TimeoutError(
            f"Threads did not make {min_per_counter} iterations each within {timeout}s"
        )

    # -------------------------------------------------------------------------
    # Test entry point
    # -------------------------------------------------------------------------

    def test_basic(self):
        self.setup_tables()
        self.populate_tables()

        stop_event = threading.Event()
        lock = threading.Lock()
        exec_counters = {}
        threads = []

        def start(name, target, *args):
            exec_counters[name] = [0]
            t = threading.Thread(
                target=target,
                args=(*args, stop_event, exec_counters[name], lock),
                daemon=True,
            )
            threads.append(t)
            t.start()

        # --- Updaters (1 per table) ---
        start('ds_upd',  self._updater, 'datashard_table')
        start('col_upd', self._updater, 'column_table')

        # --- Aggregators ---
        for i in range(2):
            start(f'ds_pk_{i}', self._pk_aggregator,
                'datashard_table', 'datashard_results')
            start(f'col_pk_{i}', self._pk_aggregator,
                'column_table', 'column_results')

        for i in range(2):
            start(f'ds_int_{i}', self._int_range_aggregator,
                'datashard_table', 'datashard_results')
        start(f'col_int', self._int_range_aggregator,
            'column_table', 'column_results')

        start(f'ds_str', self._str_range_aggregator,
            'datashard_table', 'datashard_results')
        start(f'col_str', self._str_range_aggregator,
            'column_table', 'column_results')

        # --- Rolling upgrade and checks ---
        try:
            for _ in self.roll():
                self._wait_for_progress(exec_counters, lock, min_per_counter=5)
                self.check_results('datashard_results')
                self.check_results('column_results')
        finally:
            stop_event.set()
            for t in threads:
                t.join(timeout=60)
