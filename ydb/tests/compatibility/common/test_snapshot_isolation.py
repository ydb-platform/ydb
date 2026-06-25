import logging
import random
import threading
import time
from enum import Enum
from dataclasses import dataclass

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
# The test is split into two cases - one for row tables and one for column tables.
# In each test case a source table and a result table are maintained.
# The source table holds N_ROWS=10 000 rows with the schema (key, int_val, str_val):
#
#   key      : 0 .. 9_999  (unique row identifier)
#   int_val  : key % GROUP_SIZE   (cycles 0-9 within every group of 10 rows)
#   str_val  : zero-padded string mirror of int_val, e.g. "00003"
#
# Rows are divided into N_GROUPS=1000 consecutive groups of GROUP_SIZE=10
# rows each.  Within every group, int_val takes each value in 0..9 exactly
# once, so the group sum is always FIXED_GROUP_SUM = 0+1+…+9 = 45.
# The updater thread continuously shuffles int_val values within a randomly
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
#   4. Sleep a random short interval.
#   5. READ the same aggregate again.
#
# Because filter_type is the only PK column, all concurrent aggregators of
# the same type write to the same single row, which generates write conflicts
# that the SDK retries automatically.
#
# Additionally, each aggregate query computes product_sum - SUM(key * int_val).
# This field is not saved to the result table, but is used to check snapshot stability
# between two read queries in steps 1 and 5 - if they use different snapshots,
# product_sum would not match due to concurrent value shuffles.
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


class TableType(Enum):
    ROW = 1
    COLUMN = 2


class TestSnapshotIsolation(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            table_service_config={
                "enable_snapshot_isolation_rw": True,
            },
            column_shard_config={
                "enable_cursor_v1": True,
            },
        )

    # -------------------------------------------------------------------------
    # Table management
    # -------------------------------------------------------------------------

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def setup_row_tables(self):
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
            CREATE TABLE `datashard_results` (
                filter_type        String NOT NULL,
                filter_lo          Int32,
                filter_hi          Int32,
                row_count          Int32,
                int_sum            Int64,
                count_distinct     Int32,
                PRIMARY KEY (filter_type)
            )
        """)

    def setup_column_tables(self):
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
        self._execute("""
            CREATE TABLE `column_results` (
                filter_type        String NOT NULL,
                filter_lo          Int32,
                filter_hi          Int32,
                row_count          Int32,
                int_sum            Int64,
                count_distinct     Int32,
                PRIMARY KEY (filter_type)
            ) WITH (
                STORE = COLUMN,
                PARTITION_COUNT = 1
            )
        """)

    def populate_table(self, table):
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
        self._execute(query.format(table=table, n_rows=N_ROWS))

    # -------------------------------------------------------------------------
    # Worker threads
    # -------------------------------------------------------------------------

    @dataclass
    class ExecState:
        successes: int = 0
        error: Exception = None

    RETRIABLE_ERRORS = (
        ydb.Unavailable,
        ydb.Aborted,
        ydb.Undetermined,
        ydb.ConnectionLost,
        ydb.NotFound,
        ydb.BadSession,
        ydb.Overloaded,
        ydb.SessionExpired,
        ydb.Cancelled,
    )

    def _updater(self, table_name, stop_event, exec_state, lock):
        """
        Repeatedly picks a random group and redistributes its int_val values
        while preserving the group sum invariant (SUM == FIXED_GROUP_SUM).
        """

        with self.create_driver() as driver, ydb.QuerySessionPool(driver) as pool:
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
                        exec_state.successes += 1
                except self.RETRIABLE_ERRORS as e:
                    logger.warning("Updater [%s] retriable error: %s", table_name, e)
                except Exception as e:
                    logger.error("Updater [%s] error: %s", table_name, e)
                    with lock:
                        exec_state.error = e
                    break

    def _pk_aggregator(self, table_name, results_table, stop_event, exec_state, lock):
        """
        Reads a group-aligned PK range under snapshot isolation, computes the
        aggregate, and writes it to the result table (generating write conflicts
        when multiple threads write with the same filter_type)
        """

        with self.create_driver() as driver, ydb.QuerySessionPool(driver) as pool:
            while not stop_event.is_set():
                try:
                    lo_group = random.randint(0, N_GROUPS - 2)
                    hi_group = random.randint(lo_group + 1, N_GROUPS - 1)
                    lo = lo_group * GROUP_SIZE
                    hi = hi_group * GROUP_SIZE - 1

                    def callee(tx, lo=lo, hi=hi):
                        agg_query = f"""
                            SELECT
                                count(*) as rc,
                                sum(int_val) as sum,
                                count(distinct str_val) as cd,
                                sum(key * int_val) as product_sum
                            FROM `{table_name}` WHERE key BETWEEN $lo AND $hi"""
                        with tx.execute(
                            agg_query,
                            parameters={
                                '$lo': lo,
                                '$hi': hi,
                            },
                        ) as results:
                            res = list(results)[0].rows[0]

                        time.sleep(random.uniform(0, 0.1))

                        with tx.execute(
                            f"UPSERT INTO `{results_table}` "
                            "(filter_type, filter_lo, filter_hi, row_count, int_sum, count_distinct) "
                            f"VALUES (\"pk_range\", $lo, $hi, $row_count, $int_sum, $cd)",
                            parameters={
                                '$lo': (lo, ydb.PrimitiveType.Int32),
                                '$hi': (hi, ydb.PrimitiveType.Int32),
                                '$row_count': (res.rc, ydb.PrimitiveType.Int32),
                                '$int_sum': (res.sum, ydb.PrimitiveType.Int64),
                                '$cd': (res.cd, ydb.PrimitiveType.Int32),
                            },
                        ):
                            pass

                        time.sleep(random.uniform(0, 0.1))

                        # check snapshot stability
                        with tx.execute(
                            agg_query,
                            parameters={
                                '$lo': lo,
                                '$hi': hi,
                            },
                        ) as results:
                            res2 = list(results)[0].rows[0]
                        assert res2.product_sum == res.product_sum

                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())

                    with lock:
                        exec_state.successes += 1
                except self.RETRIABLE_ERRORS as e:
                    logger.warning("PK aggregator [%s] retriable error: %s", table_name, e)
                except Exception as e:
                    logger.error("PK aggregator [%s] error: %s", table_name, e)
                    with lock:
                        exec_state.error = e
                    break

    def _int_range_aggregator(self, table_name, results_table, stop_event, exec_state, lock):
        """
        Reads `table_name` with an int value filter under snapshot isolation. Queries over
        the datashard table will use the secondary index.
        """

        with self.create_driver() as driver, ydb.QuerySessionPool(driver) as pool:
            while not stop_event.is_set():
                try:
                    lo = random.randint(0, GROUP_SIZE - 2)
                    hi = random.randint(lo + 1, GROUP_SIZE - 1)

                    def callee(tx, lo=lo, hi=hi):
                        view_clause = "VIEW int_val_index" if table_name == "datashard_table" else ""
                        agg_query = f"""
                        SELECT
                            count(*) as rc,
                            sum(int_val) as sum,
                            count(distinct str_val) as cd,
                            sum(key * int_val) as product_sum
                        FROM `{table_name}` {view_clause}
                        WHERE int_val BETWEEN $lo AND $hi
                        """

                        with tx.execute(
                            agg_query,
                            parameters={
                                '$lo': lo,
                                '$hi': hi,
                            },
                        ) as results:
                            res = list(results)[0].rows[0]

                        time.sleep(random.uniform(0, 0.1))

                        with tx.execute(
                            f"UPSERT INTO `{results_table}` "
                            "(filter_type, filter_lo, filter_hi, row_count, int_sum, count_distinct) "
                            "VALUES (\"int_range\", $lo, $hi, $row_count, $int_sum, $cd)",
                            parameters={
                                '$lo': (lo, ydb.PrimitiveType.Int32),
                                '$hi': (hi, ydb.PrimitiveType.Int32),
                                '$row_count': (res.rc, ydb.PrimitiveType.Int32),
                                '$int_sum': (res.sum, ydb.PrimitiveType.Int64),
                                '$cd': (res.cd, ydb.PrimitiveType.Int32),
                            },
                        ):
                            pass

                        time.sleep(random.uniform(0, 0.1))

                        # check snapshot stability
                        with tx.execute(
                            agg_query,
                            parameters={
                                '$lo': lo,
                                '$hi': hi,
                            },
                        ) as results:
                            res2 = list(results)[0].rows[0]
                        assert res2.product_sum == res.product_sum

                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())

                    with lock:
                        exec_state.successes += 1
                except self.RETRIABLE_ERRORS as e:
                    logger.warning("Int range aggregator [%s] retriable error: %s", table_name, e)
                except Exception as e:
                    logger.error("Int range aggregator [%s] error: %s", table_name, e)
                    with lock:
                        exec_state.error = e
                    break

    def _str_range_aggregator(self, table_name, results_table, stop_event, exec_state, lock):
        """
        Reads `table_name` with a string-range filter under snapshot isolation.
        """

        with self.create_driver() as driver, ydb.QuerySessionPool(driver) as pool:
            while not stop_event.is_set():
                try:
                    lo = random.randint(0, GROUP_SIZE - 2)
                    hi = random.randint(lo + 1, GROUP_SIZE - 1)
                    str_lo = f"{lo:05d}"
                    str_hi = f"{hi:05d}"

                    def callee(tx, lo=lo, hi=hi, str_lo=str_lo, str_hi=str_hi):
                        agg_query = f"""
                        SELECT
                            count(*) as rc,
                            sum(int_val) as sum,
                            count(distinct str_val) as cd,
                            sum(key * int_val) as product_sum
                        FROM `{table_name}`
                        WHERE str_val BETWEEN $str_lo AND $str_hi"""

                        with tx.execute(
                            agg_query,
                            parameters={
                                '$str_lo': str_lo,
                                '$str_hi': str_hi,
                            },
                        ) as results:
                            res = list(results)[0].rows[0]

                        time.sleep(random.uniform(0, 0.1))

                        with tx.execute(
                            f"UPSERT INTO `{results_table}` "
                            "(filter_type, filter_lo, filter_hi, row_count, int_sum, count_distinct) "
                            "VALUES (\"str_range\", $lo, $hi, $row_count, $int_sum, $cd)",
                            parameters={
                                '$lo': (lo, ydb.PrimitiveType.Int32),
                                '$hi': (hi, ydb.PrimitiveType.Int32),
                                '$row_count': (res.rc, ydb.PrimitiveType.Int32),
                                '$int_sum': (res.sum, ydb.PrimitiveType.Int64),
                                '$cd': (res.cd, ydb.PrimitiveType.Int32),
                            },
                        ):
                            pass

                        time.sleep(random.uniform(0, 0.1))

                        # check snapshot stability
                        with tx.execute(
                            agg_query,
                            parameters={
                                '$str_lo': str_lo,
                                '$str_hi': str_hi,
                            },
                        ) as results:
                            res2 = list(results)[0].rows[0]
                        assert res2.product_sum == res.product_sum

                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())

                    with lock:
                        exec_state.successes += 1
                except self.RETRIABLE_ERRORS as e:
                    logger.warning("String range aggregator [%s] retriable error: %s", table_name, e)
                except Exception as e:
                    logger.error("String range aggregator [%s] error: %s", table_name, e)
                    with lock:
                        exec_state.error = e
                    break

    # -------------------------------------------------------------------------
    # Checker (runs in the main thread)
    # -------------------------------------------------------------------------

    def check_results(self, results_table):
        rows = []
        for rs in self._execute(f"SELECT * FROM `{results_table}`"):
            rows += rs.rows

        filter_types = set()
        for row in rows:
            logger.info(f"Result row from {results_table}: {row}")
            filter_type = row.filter_type
            if isinstance(filter_type, bytes):
                filter_type = filter_type.decode()
            filter_types.add(filter_type)

            if filter_type == 'pk_range':
                assert row.row_count == (row.filter_hi - row.filter_lo + 1)
                expected_sum = (row.row_count // GROUP_SIZE) * FIXED_GROUP_SUM
                assert row.int_sum == expected_sum
                assert row.count_distinct == GROUP_SIZE

            elif filter_type in ('int_range', 'str_range'):
                range_len = row.filter_hi - row.filter_lo + 1
                assert row.row_count == range_len * N_GROUPS
                assert row.int_sum == (range_len * (row.filter_hi + row.filter_lo) // 2) * N_GROUPS
                assert row.count_distinct == range_len

            else:
                assert False, f"unknown filter type {filter_type}"

        assert len(filter_types) == 3

    # -------------------------------------------------------------------------
    # Progress tracking
    # -------------------------------------------------------------------------

    def _wait_for_progress(self, exec_states, lock, min_per_counter, timeout=300):
        def get_success_counters():
            ret = {}
            with lock:
                for name, st in exec_states.items():
                    if st.error is not None:
                        raise st.error
                    ret[name] = st.successes
            return ret

        baseline = get_success_counters()

        deadline = time.time() + timeout
        while time.time() < deadline:
            current = get_success_counters()
            logger.debug(f"worker success counters: {current}")
            done = all(
                current[name] - baseline[name] >= min_per_counter
                for name in current
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

    def run_basic(self, table_type):
        if table_type == TableType.ROW:
            data_table = "datashard_table"
            results_table = "datashard_results"
            self.setup_row_tables()
        elif table_type == TableType.COLUMN:
            data_table = "column_table"
            results_table = "column_results"
            self.setup_column_tables()
        else:
            assert False, f"unknown table type: {table_type}"

        self.populate_table(data_table)

        stop_event = threading.Event()
        lock = threading.Lock()
        exec_states = {}
        threads = []

        def start(name, target, *args):
            exec_states[name] = self.ExecState()
            t = threading.Thread(
                target=target,
                args=(*args, stop_event, exec_states[name], lock),
                daemon=True,
            )
            threads.append(t)
            t.start()

        # --- Updater (1 per table) ---
        start('updater',  self._updater, data_table)

        # --- Aggregators ---
        for i in range(2):
            start(f'pk_agg_{i}', self._pk_aggregator,
                  data_table, results_table)

        for i in range(2):
            start(f'int_agg_{i}', self._int_range_aggregator,
                  data_table, results_table)

        start('str_agg', self._str_range_aggregator,
              data_table, results_table)

        # --- Rolling upgrade and checks ---
        try:
            for _ in self.roll():
                self._wait_for_progress(exec_states, lock, min_per_counter=5)
                self.check_results(results_table)
        finally:
            stop_event.set()
            for t in threads:
                t.join(timeout=60)

    def test_row_tables(self):
        self.run_basic(TableType.ROW)

    def test_column_tables(self):
        self.run_basic(TableType.COLUMN)
