"""Shared row-table workload for snapshot retention E2E and compatibility tests.

Only independent writes and fresh reads may use SDK retries. A HeldSnapshot owns
one session and transaction: repeating its initial query would choose a new
snapshot and hide the loss of the old one.
"""

import logging
import queue
import random
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)

HOLD_SECONDS = 12
KEY_COUNT = 20


def session_owner_node_id(session_id):
    """Return the runtime owner node encoded in a Table Service session id."""
    return int(parse_qs(urlparse(session_id).query)["node_id"][0])


def expected_rows(generation):
    """Every generation changes values, deletes rows, and introduces a new key."""
    keys = list(range(8))
    keys.extend(key for key in range(8, 16) if key % 2 == generation % 2)
    keys.append(16 + generation % 4)
    return [(key, generation, f"generation-{generation}/key-{key}/" + "x" * 256) for key in sorted(keys)]


def _path(path):
    if not path or "`" in path or "\x00" in path:
        raise ValueError(f"Invalid table path: {path!r}")
    return path


def _settings():
    return ydb.BaseRequestSettings().with_timeout(50).with_operation_timeout(45)


def _scheme_settings():
    # A cold MIRROR_3_DC cluster may spend tens of seconds allocating and
    # recovering tablets. Keep this separate from the checked read budget.
    return ydb.BaseRequestSettings().with_timeout(180).with_operation_timeout(120)


def _read_queries(table_path, kind):
    table_path = _path(table_path)
    if kind == "point":
        # Separate equality queries exercise point lookups; an IN expression
        # over consecutive keys can be coalesced into a single range scan.
        return [
            f"SELECT key, generation, payload FROM `{table_path}` WHERE key = {key}u;"
            for key in range(KEY_COUNT)
        ]
    elif kind == "range":
        predicate = f"WHERE key >= 0u AND key < {KEY_COUNT}u"
        view = ""
    elif kind == "index":
        predicate = "WHERE generation >= 0u"
        view = " VIEW by_generation"
    else:
        raise ValueError(f"Unknown read kind: {kind}")
    return [f"SELECT key, generation, payload FROM `{table_path}`{view} {predicate} ORDER BY key;"]


def _rows(result_sets):
    rows = []
    for row in result_sets[0].rows:
        payload = row["payload"]
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        rows.append((int(row["key"]), int(row["generation"]), payload))
    return rows


class HeldSnapshot:
    """An explicit snapshot transaction whose first query only visits anchor."""

    def __init__(self, workload, tx_mode=None):
        self.workload = workload
        self._closed = False
        self.session = workload.driver.table_client.session().create(settings=_settings())
        self.tx = self.session.transaction(
            ydb.SnapshotReadOnly() if tx_mode is None else tx_mode,
            allow_split_transactions=False,
        )
        try:
            result = self.tx.execute(
                f"SELECT generation FROM `{workload.anchor_path}` WHERE key = 0u;",
                settings=_settings(),
            )
            if len(result[0].rows) != 1:
                raise AssertionError("Snapshot anchor is missing")
            self.generation = int(result[0].rows[0]["generation"])
            self.tx_id = self.tx.tx_id
            self.session_id = self.tx.session_id
            if not self.tx_id:
                raise AssertionError("Snapshot query did not leave an open transaction")
        except Exception:
            self._delete_session_after_error()
            raise

    @property
    def owner_node_id(self):
        return session_owner_node_id(self.session_id)

    def _delete_session_after_error(self):
        try:
            self.session.delete(settings=_settings())
        except ydb.Error:
            logger.warning("Failed to delete snapshot session during error cleanup", exc_info=True)

    def execute(self, query):
        """Execute in the original transaction without retrying or committing."""
        if self._closed:
            raise RuntimeError("Snapshot transaction is already closed")
        if self.tx.tx_id != self.tx_id:
            raise AssertionError("Snapshot transaction changed before the retained statement")
        result = self.tx.execute(query, settings=_settings())
        if self.tx.tx_id != self.tx_id:
            raise AssertionError("Snapshot transaction changed during the retained statement")
        return result

    def read(self, kind="range", table_path=None):
        rows = []
        for query in _read_queries(table_path or self.workload.target_path, kind):
            rows.extend(_rows(self.execute(query)))
        return rows

    def assert_matches(self, kind="range", table_path=None):
        actual = self.read(kind, table_path)
        expected = expected_rows(self.generation)
        if actual != expected:
            raise AssertionError(
                f"Retained snapshot {self.tx_id} generation {self.generation}, {kind} read: "
                f"expected {expected!r}, got {actual!r}"
            )
        return actual

    def close(self, mode="rollback"):
        if mode not in ("rollback", "commit", "session"):
            raise ValueError(f"Unknown snapshot close mode: {mode}")
        if self._closed:
            return
        self._closed = True
        if mode == "session":
            self.session.delete(settings=_settings())
            return
        try:
            getattr(self.tx, mode)(settings=_settings())
        except Exception:
            self._delete_session_after_error()
            raise
        else:
            self.session.delete(settings=_settings())

    def abandon(self):
        """Forget a transaction after its owner crashed, without sending a release."""
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
        elif not self._closed:
            self._closed = True
            self._delete_session_after_error()


@dataclass(frozen=True)
class WorkloadStats:
    completed_reads: int
    writes: int
    completed_by_reader: tuple


class _BackgroundWriter:
    def __init__(self, workload, retry_uncertain):
        self.workload = workload
        self.retry_uncertain = retry_uncertain
        self.stop = threading.Event()
        self._error = None
        with workload._lock:
            self.initial_generation = workload._last_generation
            self._initial_writes = workload._writes

    @property
    def generation(self):
        with self.workload._lock:
            return self.workload._last_generation

    @property
    def writes(self):
        with self.workload._lock:
            return self.workload._writes - self._initial_writes

    def check(self):
        with self.workload._lock:
            error = self._error
        if error is not None:
            raise error

    def wait(self, timeout):
        stopped = self.stop.wait(timeout)
        self.check()
        return stopped

    def run(self):
        generation = self.initial_generation + 1
        try:
            while not self.stop.is_set():
                self.workload.write_generation(generation, retry_uncertain=self.retry_uncertain)
                generation += 1
                self.stop.wait(0.2)
        except Exception as error:
            # A stopped writer cannot recover through a monitoring retry.
            failure = RuntimeError("Background snapshot retention writer failed")
            failure.__cause__ = error
            with self.workload._lock:
                self._error = failure
            self.stop.set()


class SnapshotRetentionWorkload:
    def __init__(self, driver, database, prefix="snapshot_retention"):
        self.driver = driver
        self.database = database.rstrip("/")
        prefix = prefix.strip("/")
        if not prefix:
            raise ValueError("Table prefix must not be empty")
        self.anchor_path = _path(f"{self.database}/{prefix}_anchor")
        self.target_path = _path(f"{self.database}/{prefix}_target")
        self.index_path = self.target_path + "/by_generation/indexImplTable"
        self._pool = ydb.SessionPool(driver, size=8)
        self._lock = threading.Lock()
        self._last_generation = None
        self._writes = 0

    def close(self):
        self._pool.stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _retry(self, operation, retry_uncertain=False):
        # Only independent operations enter SDK retries, never a HeldSnapshot.
        return self._pool.retry_operation_sync(
            operation, retry_settings=ydb.RetrySettings(max_retries=5, idempotent=retry_uncertain)
        )

    def create_tables(self):
        tables = (
            (self.anchor_path, ""),
            (self.target_path, "INDEX by_generation GLOBAL ON (generation) COVER (payload),"),
        )
        for path, index in tables:
            query = f"""
                CREATE TABLE `{path}` (
                    key Uint64 NOT NULL,
                    generation Uint64,
                    payload Utf8,
                    {index}
                    PRIMARY KEY (key)
                ) WITH (
                    AUTO_PARTITIONING_BY_SIZE = DISABLED,
                    AUTO_PARTITIONING_BY_LOAD = DISABLED
                );
            """
            self._retry(lambda session, query=query: session.execute_scheme(query, settings=_scheme_settings()))

    def execute_scheme(self, query):
        return self._retry(lambda session: session.execute_scheme(query, settings=_scheme_settings()))

    def copy_table(self, source, destination):
        return self.driver.table_client.copy_table(source, destination, settings=_scheme_settings())

    def drop_table(self, path):
        return self.driver.table_client.drop_table(path, settings=_scheme_settings())

    def execute(self, query, retry_uncertain=False):
        return self._retry(lambda session: session.transaction().execute(
            query, commit_tx=True, settings=_settings()
        ), retry_uncertain=retry_uncertain)

    def write_generation(self, generation, retry_uncertain=False):
        if not isinstance(generation, int) or generation < 0:
            raise ValueError("Generation must be a nonnegative integer")
        rows = expected_rows(generation)
        values = ", ".join(f'({key}u, {gen}u, "{payload}")' for key, gen, payload in rows)
        keys = ", ".join(f"{key}u" for key, _, _ in rows)
        # A and B are changed in one distributed transaction. Thus reading A's
        # generation establishes an exact, independent oracle for an unread B.
        # Deletions and upserts touch disjoint key sets within the query.
        query = "\n".join(
            f"DELETE FROM `{path}` WHERE key NOT IN ({keys}); "
            f"UPSERT INTO `{path}` (key, generation, payload) VALUES {values};"
            for path in (self.anchor_path, self.target_path)
        )
        # Replacing a fixed generation is idempotent. Fault-mode writers
        # may opt into SDK retries for an uncertain commit acknowledgement;
        # checked old reads and ordinary callers keep their existing policy.
        self.execute(query, retry_uncertain=retry_uncertain)
        with self._lock:
            self._last_generation = generation
            self._writes += 1

    def pulse(self, generation):
        # Equal rows may skip index maintenance. Commit real changes, then
        # restore the deterministic current generation in a separate commit.
        self.write_generation(generation + 1)
        self.write_generation(generation)

    def begin_snapshot(self, tx_mode=None):
        return HeldSnapshot(self, tx_mode=tx_mode)

    def read_current(self, kind="range", table_path=None):
        queries = _read_queries(table_path or self.target_path, kind)

        def read(session):
            # Point reads must share one fresh snapshot, even if a writer
            # commits a new generation between individual RPCs. A retry reruns
            # this whole independent read and discards any partial results.
            with session.transaction(ydb.SnapshotReadOnly(), allow_split_transactions=False) as tx:
                rows = []
                for index, query in enumerate(queries):
                    rows.extend(_rows(tx.execute(
                        query, commit_tx=index == len(queries) - 1, settings=_settings()
                    )))
                return rows

        return self._retry(read)

    def assert_current(self, generation, kind="range", table_path=None):
        actual = self.read_current(kind, table_path)
        expected = expected_rows(generation)
        if actual != expected:
            raise AssertionError(f"Fresh {kind} read: expected generation {generation}: {expected!r}, got {actual!r}")
        return actual

    @contextmanager
    def background_writer(self, retry_uncertain=False):
        """Keep writes running, exposing progress and failures to fault scenarios."""
        with self._lock:
            initial_generation = self._last_generation
        if initial_generation is None:
            self.write_generation(0)
        writer = _BackgroundWriter(self, retry_uncertain)
        thread = threading.Thread(target=writer.run, name="snapshot-retention-writer")
        thread.start()
        body_error = None
        try:
            yield writer
        except BaseException as error:
            body_error = error
        finally:
            writer.stop.set()
            thread.join()
        try:
            writer.check()
        except Exception as writer_error:
            if body_error is not None and body_error is not writer_error:
                raise body_error from writer_error
            raise
        if body_error is not None:
            raise body_error

    def run(self, duration, hold_seconds=HOLD_SECONDS, readers=4, seed=1, before_read=None):
        """Run writes and aged reads; propagate every reader failure.

        A cluster-aware runner can pass before_read(held) to wait for a measured
        LowWatermark beyond the held version before its first target access.
        The standalone remote workload uses the configured hold duration.
        """
        if duration <= 0 or hold_seconds <= 0 or readers <= 0:
            raise ValueError("Duration, hold duration, and reader count must be positive")
        errors = queue.Queue()
        completed = [0] * readers

        with self.background_writer() as writer:
            deadline = time.monotonic() + duration

            def reader(index):
                rng = random.Random(seed + index)
                try:
                    while not writer.stop.is_set() and (
                        completed[index] == 0 or time.monotonic() + hold_seconds < deadline
                    ):
                        with self.begin_snapshot() as held:
                            if writer.wait(hold_seconds * rng.uniform(1.0, 1.25)):
                                break
                            if before_read is not None:
                                before_read(held)
                            held.assert_matches(rng.choice(("point", "range", "index")))
                            if writer.generation <= held.generation:
                                raise AssertionError("Writer made no progress while the snapshot was held")
                            completed[index] += 1
                            held.close(rng.choice(("rollback", "commit", "session")))
                except Exception as error:
                    errors.put(error)
                    writer.stop.set()

            reader_threads = [
                threading.Thread(target=reader, args=(index,), name=f"snapshot-retention-reader-{index}")
                for index in range(readers)
            ]
            started_threads = []
            try:
                for thread in reader_threads:
                    thread.start()
                    started_threads.append(thread)
                for thread in started_threads:
                    thread.join()
            finally:
                writer.stop.set()
                for thread in started_threads:
                    thread.join()
            if not errors.empty():
                raise errors.get()
        if any(count == 0 for count in completed):
            raise AssertionError(f"Not all readers completed a retained read: {completed!r}")
        stats = WorkloadStats(sum(completed), writer.writes, tuple(completed))
        self.assert_current(writer.generation)
        logger.info("Snapshot retention workload completed: %s", stats)
        return stats
