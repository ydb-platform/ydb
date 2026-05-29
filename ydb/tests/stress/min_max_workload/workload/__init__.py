# -*- coding: utf-8 -*-
import logging
import threading
import time
import random
import ydb

logger = logging.getLogger(__name__)

# Cycling helper values for types with limited range
_INT8_MOD = 100       # cycling 0..99
_UINT8_MOD = 200      # cycling 0..199 (fits Uint8 0..255)
_INT16_MOD = 30_000   # cycling 0..29999
_UINT16_MOD = 60_000  # cycling 0..59999
_DATE_BASE = 10957    # days since epoch for 2000-01-01
_DATE_MOD = 10_000    # cycling through ~27 years
_DATETIME_BASE = 1_000_000_000   # 2001-09-09 in seconds
_DATETIME_MOD = 54_000_000       # cycling ~1.7 years in minutes
_TIMESTAMP_BASE = 1_696_200_000_000_000  # 2023-10-02 in microseconds
_TIMESTAMP_STEP = 1_000           # 1 ms per row

# COLUMNS: (col_name, ydb_type, yql_value_expr, python_val_fn, sql_literal_fn)
#
# yql_value_expr: YQL expression using $x (Int64 id) to produce the column value
# python_val_fn(i): Python int i → column value used to build SQL filter literals
# sql_literal_fn(v): column value → SQL literal string for use in WHERE clause
#
# Values are correlated with id: monotonically increasing for large-range types,
# sawtooth (mod) for small-range types (Int8/Uint8/Int16/Uint16/Date/Datetime).
COLUMNS = [
    (
        "c_int64", "Int64",
        "$x",
        lambda i: i,
        lambda v: f"CAST({v} AS Int64)",
    ),
    (
        "c_uint8", "Uint8",
        f"CAST($x % {_UINT8_MOD} AS Uint8)",
        lambda i: i % _UINT8_MOD,
        lambda v: f"CAST({v} AS Uint8)",
    ),
    (
        "c_uint16", "Uint16",
        f"CAST($x % {_UINT16_MOD} AS Uint16)",
        lambda i: i % _UINT16_MOD,
        lambda v: f"CAST({v} AS Uint16)",
    ),
    (
        "c_uint32", "Uint32",
        "CAST($x AS Uint32)",
        lambda i: i,
        lambda v: f"CAST({v} AS Uint32)",
    ),
    (
        "c_uint64", "Uint64",
        "CAST($x AS Uint64)",
        lambda i: i,
        lambda v: f"CAST({v} AS Uint64)",
    ),
    (
        "c_float", "Float",
        "CAST($x AS Float)",
        lambda i: float(i),
        lambda v: f"CAST({v} AS Float)",
    ),
    (
        "c_double", "Double",
        "CAST($x AS Double) + 0.5",
        lambda i: float(i) + 0.5,
        lambda v: f"CAST({v} AS Double)",
    ),
    (
        "c_bool", "Bool",
        "$x % 2 == 0",
        lambda i: i % 2 == 0,
        lambda v: "True" if v else "False",
    ),
    (
        "c_string", "String",
        '"str_" || CAST($x AS String)',
        lambda i: f"str_{i}",
        lambda v: f"'{v}'",
    ),
    (
        "c_utf8", "Utf8",
        'Utf8("utf8_") || CAST($x AS Utf8)',
        lambda i: f"utf8_{i}",
        lambda v: f"'{v}'",
    ),
    (
        "c_date", "Date",
        f"CAST({_DATE_BASE} + $x % {_DATE_MOD} AS Date)",
        lambda i: _DATE_BASE + i % _DATE_MOD,
        lambda v: f"CAST({v} AS Date)",
    ),
    (
        "c_datetime", "Datetime",
        f"CAST({_DATETIME_BASE} + ($x % {_DATETIME_MOD}) * 60 AS Datetime)",
        lambda i: _DATETIME_BASE + (i % _DATETIME_MOD) * 60,
        lambda v: f"CAST({v} AS Datetime)",
    ),
    (
        "c_timestamp", "Timestamp",
        f"CAST({_TIMESTAMP_BASE} + $x * {_TIMESTAMP_STEP} AS Timestamp)",
        lambda i: _TIMESTAMP_BASE + i * _TIMESTAMP_STEP,
        lambda v: f"CAST({v} AS Timestamp)",
    ),
]

# Predicate types cycled across columns in the big AND filter
_PREDICATES = ["=", ">", ">=", "<", "<=", "BETWEEN"]


def _build_predicate(col_idx, target_id, window):
    """Return a SQL predicate string for the column at col_idx.

    window: half-width of the id range used for range predicates.
    """
    name, typ, _, val_fn, lit_fn = COLUMNS[col_idx]

    # Bool only supports equality
    if typ == "Bool":
        return f"{name} = {lit_fn(val_fn(target_id))}"

    pred = _PREDICATES[col_idx % len(_PREDICATES)]
    lo_id = max(0, target_id - window)
    hi_id = target_id + window
    v_mid = val_fn(target_id)
    v_lo = val_fn(lo_id)
    v_hi = val_fn(hi_id)

    if pred == "=":
        return f"{name} = {lit_fn(v_mid)}"
    elif pred == ">":
        return f"{name} > {lit_fn(v_lo)}"
    elif pred == ">=":
        return f"{name} >= {lit_fn(v_lo)}"
    elif pred == "<":
        return f"{name} < {lit_fn(v_hi)}"
    elif pred == "<=":
        return f"{name} <= {lit_fn(v_hi)}"
    else:  # BETWEEN
        return f"{name} BETWEEN {lit_fn(v_lo)} AND {lit_fn(v_hi)}"


class MinMaxWorkload:
    def __init__(self, endpoint, database, path, duration, batch_size, insert_threads, query_threads):
        self.database = database
        self.table_path = f"{database}/{path}/table"
        self.dir_path = f"{database}/{path}"
        self.duration = duration
        self.batch_size = batch_size
        self.insert_threads = insert_threads
        self.query_threads = query_threads
        self.driver = ydb.Driver(endpoint=endpoint, database=database)
        self.pool = ydb.QuerySessionPool(self.driver, size=insert_threads + query_threads + 2)
        self._next_id = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._rows_inserted = 0
        self._queries_run = 0

    def __enter__(self):
        self.driver.wait(timeout=30)
        self._setup()
        return self

    def __exit__(self, *_args):
        self.pool.stop()
        self.driver.stop()

    def _setup(self):
        try:
            self.driver.scheme_client.make_directory(self.dir_path)
        except Exception:
            pass  # directory may already exist

        col_defs = ",\n        ".join(f"{name} {typ}" for name, typ, *_ in COLUMNS)
        self.pool.execute_with_retries(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_path}` (
                id Int64 NOT NULL,
                {col_defs},
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN)
        """)
        logger.info("Table created with sql:")
        logger.info(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_path}` (
                id Int64 NOT NULL,
                {col_defs},
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH (STORE = COLUMN)
        """)

        for name, *_ in COLUMNS:
            try:
                self.pool.execute_with_retries(f"""
                    ALTER TABLE `{self.table_path}` ADD INDEX `idx_{name}_minmax` LOCAL USING min_max ON(`{name}`);
                """)
                logger.info(f"Added min_max index for {name}")
            except Exception as e:
                logger.error(f"Could not add min_max index for {name}: {e}")
                raise

    def _insert_loop(self):
        col_exprs = ",\n                    ".join(
            f"{expr} AS {name}" for name, _, expr, *_ in COLUMNS
        )
        step = max(1, self.batch_size // 3)

        while not self._stop.is_set():
            with self._lock:
                start = self._next_id
                self._next_id += step
            query = f"""
                $data = ListMap(ListFromRange({start}L, {start + self.batch_size}L), ($x) -> {{
                    RETURN AsStruct(
                        $x AS id,
                        {col_exprs}
                    );
                }});
                UPSERT INTO `{self.table_path}` SELECT * FROM AS_TABLE($data);
            """
            try:
                self.pool.execute_with_retries(query)
                with self._lock:
                    self._rows_inserted += self.batch_size
            except Exception as e:
                logger.error(f"Insert error (start={start}): {e}")
                raise

    def _query_loop(self):
        while not self._stop.is_set():
            with self._lock:
                max_id = self._next_id
            time.sleep(1)
            if max_id < self.batch_size * 3:
                time.sleep(2)
                continue

            target_id = random.randint(self.batch_size, max_id - self.batch_size)
            window = max(1, self.batch_size // 2)

            predicates = [
                _build_predicate(i, target_id, window)
                for i in range(len(COLUMNS))
            ]
            where_clause = "\n        AND ".join(predicates)

            query = f"""
                SELECT COUNT(*) AS cnt FROM `{self.table_path}`
                WHERE
                    {where_clause}
            """
            try:
                result = self.pool.execute_with_retries(query)
                cnt = result[0].rows[0]["cnt"]
                with self._lock:
                    self._queries_run += 1
                logger.info(f"query({query})\n target_id={target_id} max_id={max_id} count={cnt}")
            except Exception as e:
                logger.error(f"Query({query})\n error (target_id={target_id}): {e}")
                raise

    def run(self):
        threads = []
        for _ in range(self.insert_threads):
            t = threading.Thread(target=self._insert_loop, daemon=True)
            t.start()
            threads.append(t)
        for _ in range(self.query_threads):
            t = threading.Thread(target=self._query_loop, daemon=True)
            t.start()
            threads.append(t)

        started_at = time.time()
        while time.time() - started_at < self.duration:
            elapsed = int(time.time() - started_at)
            with self._lock:
                rows = self._rows_inserted
                queries = self._queries_run
            approx_gb = rows * len(COLUMNS) * 8 // (1024 ** 3)
            logger.info(
                f"elapsed={elapsed}s rows={rows:,} ~{approx_gb}GB queries={queries}"
            )
            time.sleep(30)

        self._stop.set()
        for t in threads:
            t.join(timeout=60)
