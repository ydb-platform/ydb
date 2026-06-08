import logging
import random
import threading
import time

import pytest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

N_ROWS = 10_000
GROUP_SIZE = 10
N_GROUPS = N_ROWS // GROUP_SIZE  # 1000
FIXED_GROUP_SUM = GROUP_SIZE * (GROUP_SIZE - 1) // 2  # 45 = 0+1+...+9

# Pooled result-table slots: multiple threads writing to the same slot generate write conflicts.
# datashard_results slot layout:  0..N_PK_SLOTS-1   = pk_range
#                                 N_PK_SLOTS..      = sec_idx  (1st slot)
#                                 N_PK_SLOTS+N_SEC_SLOTS.. = str_range (1st slot)
# column_results slot layout:     0..N_PK_SLOTS-1   = pk_range
#                                 N_PK_SLOTS..      = str_range (1st slot)
N_PK_SLOTS = 2
N_SEC_SLOTS = 1
N_STR_SLOTS = 1


class TestSnapshotIsolation(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            table_service_config={
                "enable_snapshot_isolation_rw": True,
            },
            column_shard_config={
                "disabled_on_scheme_shard": False,
            },
        )

    # -------------------------------------------------------------------------
    # Table management
    # -------------------------------------------------------------------------

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def setup_tables(self):
        self._execute("""
            CREATE TABLE `datashard_table` (
                key     Int32 NOT NULL,
                int_val Int32,
                str_val String,
                PRIMARY KEY (key),
                INDEX int_val_index GLOBAL ON (int_val)
            ) WITH (
                PARTITION_AT_KEYS = (770, 1540, 2310, 3080, 3850, 4620, 5390, 6160, 6930, 7700, 8470, 9240),
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 13
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
            self._execute(f"""
                CREATE TABLE `{name}` (
                    agg_id             Int32 NOT NULL,
                    filter_type        String,
                    filter_lo          Int32,
                    filter_hi          Int32,
                    row_count          Int32,
                    int_sum            Int64,
                    out_of_range_count Int32,
                    PRIMARY KEY (agg_id)
                )
            """)

    def populate_tables(self):
        # Use ListMap/AS_TABLE to generate 10k rows in a single YQL query.
        # int_val = key % GROUP_SIZE, so every group of GROUP_SIZE consecutive rows
        # sums to FIXED_GROUP_SUM = 0+1+...+(GROUP_SIZE-1) = 45.
        # str_val = "0000" || digit, a zero-padded 5-char mirror of int_val so that
        # lexicographic and numeric ordering agree for values 0-9.
        query = """
            $data = ListMap(ListFromRange(0, 10000), ($x) -> {
                RETURN AsStruct(
                    UNWRAP(CAST($x AS Int32)) AS key,
                    CAST($x % 10 AS Int32) AS int_val,
                    "0000" || COALESCE(CAST($x % 10 AS String), "0") AS str_val
                );
            });
            UPSERT INTO `{table}` SELECT * FROM AS_TABLE($data)
        """
        self._execute(query.format(table='datashard_table'))

        # Column tables use bulk_upsert (one atomic batch per group of GROUP_SIZE rows
        # maintains the group sum invariant).
        rows = [
            {"key": k, "int_val": k % GROUP_SIZE, "str_val": f"{k % GROUP_SIZE:05d}".encode()}
            for k in range(N_ROWS)
        ]
        col_types = ydb.BulkUpsertColumns()
        col_types.add_column("key", ydb.PrimitiveType.Int32)
        col_types.add_column("int_val", ydb.PrimitiveType.Int32)
        col_types.add_column("str_val", ydb.PrimitiveType.String)
        self.driver.table_client.bulk_upsert(
            f"{self.database_path}/column_table", rows, col_types
        )

    # -------------------------------------------------------------------------
    # Worker threads
    # -------------------------------------------------------------------------

    def _updater(self, table_name, use_bulk_upsert, stop_event, counter, lock):
        """Repeatedly picks a random group and redistributes its int_val values
        while preserving the group sum invariant (SUM == FIXED_GROUP_SUM)."""
        while not stop_event.is_set():
            try:
                group = random.randint(0, N_GROUPS - 1)
                lo = group * GROUP_SIZE
                # Shuffle [0..GROUP_SIZE-1] — any permutation also sums to FIXED_GROUP_SUM.
                new_vals = list(range(GROUP_SIZE))
                random.shuffle(new_vals)

                if use_bulk_upsert:
                    rows = [
                        {
                            "key": lo + i,
                            "int_val": new_vals[i],
                            "str_val": f"{new_vals[i]:05d}".encode(),
                        }
                        for i in range(GROUP_SIZE)
                    ]
                    ct = ydb.BulkUpsertColumns()
                    ct.add_column("key", ydb.PrimitiveType.Int32)
                    ct.add_column("int_val", ydb.PrimitiveType.Int32)
                    ct.add_column("str_val", ydb.PrimitiveType.String)
                    self.driver.table_client.bulk_upsert(
                        f"{self.database_path}/{table_name}", rows, ct
                    )
                else:
                    values_str = ", ".join(
                        f"({lo + i}, {new_vals[i]}, \"{new_vals[i]:05d}\")"
                        for i in range(GROUP_SIZE)
                    )
                    self._execute(
                        f"UPSERT INTO `{table_name}` (key, int_val, str_val) VALUES {values_str}"
                    )

                with lock:
                    counter[0] += 1
            except Exception as e:
                logger.warning("Updater [%s] error: %s", table_name, e)

    def _pk_aggregator(self, table_name, result_table, agg_id, stop_event, counter, lock):
        """Reads a group-aligned PK range under snapshot isolation, computes the
        aggregate, and writes it to the result table (generating write conflicts
        when multiple threads share the same agg_id slot)."""
        while not stop_event.is_set():
            try:
                lo_group = random.randint(0, N_GROUPS - 51)
                n_groups = random.randint(1, 50)
                lo = lo_group * GROUP_SIZE
                hi = lo + n_groups * GROUP_SIZE - 1

                def callee(tx, lo=lo, hi=hi):
                    rows = list(tx.execute(
                        f"SELECT int_val FROM `{table_name}` WHERE key BETWEEN {lo} AND {hi}"
                    )[0].rows)
                    row_count = len(rows)
                    int_sum = sum(r.int_val or 0 for r in rows)
                    tx.execute(
                        f"UPSERT INTO `{result_table}` "
                        "(agg_id, filter_type, filter_lo, filter_hi, row_count, int_sum, out_of_range_count) "
                        f"VALUES ({agg_id}, \"pk_range\", {lo}, {hi}, {row_count}, {int_sum}, 0)"
                    )

                with ydb.QuerySessionPool(self.driver) as pool:
                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())
                with lock:
                    counter[0] += 1
            except Exception as e:
                logger.warning("PK aggregator [%s] error: %s", table_name, e)

    def _sec_idx_aggregator(self, result_table, agg_id, stop_event, counter, lock):
        """Reads datashard_table via its secondary index under snapshot isolation.
        Invariant: every returned row must have int_val inside the filter range.
        A snapshot violation would allow the index shard to return stale PKs whose
        data rows have already been updated to a different int_val."""
        while not stop_event.is_set():
            try:
                lo = random.randint(0, GROUP_SIZE - 2)
                hi = random.randint(lo + 1, GROUP_SIZE - 1)

                def callee(tx, lo=lo, hi=hi):
                    rows = list(tx.execute(
                        "SELECT int_val FROM `datashard_table` VIEW int_val_index "
                        f"WHERE int_val BETWEEN {lo} AND {hi}"
                    )[0].rows)
                    row_count = len(rows)
                    int_sum = sum(r.int_val or 0 for r in rows)
                    out_of_range = sum(
                        1 for r in rows if (r.int_val or 0) < lo or (r.int_val or 0) > hi
                    )
                    tx.execute(
                        f"UPSERT INTO `{result_table}` "
                        "(agg_id, filter_type, filter_lo, filter_hi, row_count, int_sum, out_of_range_count) "
                        f"VALUES ({agg_id}, \"sec_idx\", {lo}, {hi}, {row_count}, {int_sum}, {out_of_range})"
                    )

                with ydb.QuerySessionPool(self.driver) as pool:
                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())
                with lock:
                    counter[0] += 1
            except Exception as e:
                logger.warning("Secondary index aggregator error: %s", e)

    def _str_aggregator(self, table_name, result_table, agg_id, stop_event, counter, lock):
        """Reads via a string-range filter under snapshot isolation.
        Same out-of-range invariant as the secondary index aggregator: str_val and
        int_val are always updated together, so a snapshot-consistent read must not
        return any row with int_val outside the int equivalent of [str_lo, str_hi]."""
        while not stop_event.is_set():
            try:
                lo = random.randint(0, GROUP_SIZE - 2)
                hi = random.randint(lo + 1, GROUP_SIZE - 1)
                str_lo = f"{lo:05d}"
                str_hi = f"{hi:05d}"

                def callee(tx, lo=lo, hi=hi, str_lo=str_lo, str_hi=str_hi):
                    rows = list(tx.execute(
                        f"SELECT int_val FROM `{table_name}` "
                        f"WHERE str_val BETWEEN \"{str_lo}\" AND \"{str_hi}\""
                    )[0].rows)
                    row_count = len(rows)
                    int_sum = sum(r.int_val or 0 for r in rows)
                    out_of_range = sum(
                        1 for r in rows if (r.int_val or 0) < lo or (r.int_val or 0) > hi
                    )
                    tx.execute(
                        f"UPSERT INTO `{result_table}` "
                        "(agg_id, filter_type, filter_lo, filter_hi, row_count, int_sum, out_of_range_count) "
                        f"VALUES ({agg_id}, \"str_range\", {lo}, {hi}, {row_count}, {int_sum}, {out_of_range})"
                    )

                with ydb.QuerySessionPool(self.driver) as pool:
                    pool.retry_tx_sync(callee, ydb.QuerySnapshotReadWrite())
                with lock:
                    counter[0] += 1
            except Exception as e:
                logger.warning("String aggregator [%s] error: %s", table_name, e)

    # -------------------------------------------------------------------------
    # Checker (runs in the main thread)
    # -------------------------------------------------------------------------

    def check_results(self, result_table):
        result_sets = self._execute(f"SELECT * FROM `{result_table}`")
        if not result_sets or not result_sets[0].rows:
            return
        for row in result_sets[0].rows:
            filter_type = row.filter_type
            if isinstance(filter_type, bytes):
                filter_type = filter_type.decode()
            if filter_type == 'pk_range':
                if row.row_count is None or row.int_sum is None:
                    continue
                expected = (row.row_count // GROUP_SIZE) * FIXED_GROUP_SUM
                assert row.int_sum == expected, (
                    f"[{result_table}] Group sum violation: "
                    f"filter=[{row.filter_lo},{row.filter_hi}], "
                    f"row_count={row.row_count}, int_sum={row.int_sum}, "
                    f"expected={expected}"
                )
            elif filter_type in ('sec_idx', 'str_range'):
                if row.out_of_range_count is None:
                    continue
                assert row.out_of_range_count == 0, (
                    f"[{result_table}] Out-of-range violation: "
                    f"filter_type={filter_type}, filter=[{row.filter_lo},{row.filter_hi}], "
                    f"out_of_range_count={row.out_of_range_count}"
                )

    # -------------------------------------------------------------------------
    # Progress tracking
    # -------------------------------------------------------------------------

    def _wait_for_progress(self, counters, lock, min_per_counter=5, timeout=300):
        baseline = {}
        with lock:
            for name, c in counters.items():
                baseline[name] = c[0]
        deadline = time.time() + timeout
        while time.time() < deadline:
            with lock:
                done = all(
                    counters[name][0] - baseline[name] >= min_per_counter
                    for name in counters
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
        counters = {}
        threads = []

        def start(name, target, *args):
            counters[name] = [0]
            t = threading.Thread(
                target=target,
                args=(*args, stop_event, counters[name], lock),
                daemon=True,
            )
            threads.append(t)
            t.start()

        # --- Updaters (1 per table) ---
        start('ds_upd',  self._updater, 'datashard_table', False)
        start('col_upd', self._updater, 'column_table',    True)

        # --- Datashard PK-range aggregators (2 threads, N_PK_SLOTS slots) ---
        for i in range(2):
            start(f'ds_pk_{i}', self._pk_aggregator,
                  'datashard_table', 'datashard_results', i % N_PK_SLOTS)

        # --- Secondary-index aggregators (2 threads share 1 slot → conflicts) ---
        for i in range(2):
            start(f'ds_sec_{i}', self._sec_idx_aggregator,
                  'datashard_results', N_PK_SLOTS)

        # --- Datashard string-range aggregator ---
        start('ds_str', self._str_aggregator,
              'datashard_table', 'datashard_results', N_PK_SLOTS + N_SEC_SLOTS)

        # --- Column PK-range aggregators (2 threads, N_PK_SLOTS slots) ---
        for i in range(2):
            start(f'col_pk_{i}', self._pk_aggregator,
                  'column_table', 'column_results', i % N_PK_SLOTS)

        # --- Column string-range aggregator ---
        start('col_str', self._str_aggregator,
              'column_table', 'column_results', N_PK_SLOTS)

        try:
            for _ in self.roll():
                self._wait_for_progress(counters, lock, min_per_counter=5)
                self.check_results('datashard_results')
                self.check_results('column_results')
        finally:
            stop_event.set()
            for t in threads:
                t.join(timeout=60)
