import logging
import os
import random
import threading
import time

import ydb

from ydb.tests.stress.common.common import WorkloadBase


logger = logging.getLogger("CombinedIndexesWorkload")


class WorkloadCombinedIndexes(WorkloadBase):
    """Deterministic cross-feature workload for unique, compact fulltext and hybrid search."""

    DEFAULT_SEED = 20260820
    CORE_KEYS = {"doc-a", "doc-b", "doc-c", "doc-d"}
    MARKER_KEYS = {"doc-a", "doc-c"}

    def __init__(self, client, prefix, stop, seed=None):
        super().__init__(client, prefix, "combined_indexes", stop)
        self.seed = seed if seed is not None else int(
            os.getenv("YDB_COMBINED_INDEX_SEED", str(self.DEFAULT_SEED)), 0
        )
        self._rng = random.Random(self.seed)
        self._stats_lock = threading.Lock()
        self._iterations = 0
        self._unique_conflicts = 0
        self._rebuilds = 0
        self._row_ids = None

    @staticmethod
    def _embedding(x, y):
        return (
            "Untag(Knn::ToBinaryStringUint8("
            f"Cast([{x}, {y}] AS List<Uint8>)), \"Uint8Vector\")"
        )

    def _create_tables(self, table, unique_table):
        logger.info(
            "combined-index seed=%d hybrid_table=%s unique_table=%s replay='YDB_COMBINED_INDEX_SEED=%d'",
            self.seed, table, unique_table, self.seed,
        )
        self.client.query(
            f"""
            CREATE TABLE `{table}` (
                pk Utf8,
                text Utf8,
                embedding String,
                payload Uint64,
                PRIMARY KEY (pk)
            );

            CREATE TABLE `{unique_table}` (
                pk Uint64,
                unique_tag Utf8,
                payload Utf8,
                version Uint64,
                PRIMARY KEY (pk),
                INDEX unique_tag_idx GLOBAL UNIQUE SYNC ON (unique_tag)
            );
            """,
            True,
        )
        self.client.query(
            f"""
            UPSERT INTO `{table}` (pk, text, embedding, payload) VALUES
                ("doc-a", "marker marker marker alpha", {self._embedding(240, 15)}, 1),
                ("doc-b", "plain beta", {self._embedding(250, 10)}, 2),
                ("doc-c", "marker gamma", {self._embedding(10, 250)}, 3),
                ("doc-d", "plain delta", {self._embedding(200, 60)}, 4),
                ("ephemeral-0", "noncandidate-0", NULL, 0),
                ("ephemeral-1", "noncandidate-0", NULL, 0);

            UPSERT INTO `{unique_table}` (pk, unique_tag, payload, version) VALUES
                (1, "unique-a", "winner", 1),
                (2, "unique-b", "stable", 1);
            """,
            False,
        )
        # The kmeans build needs a non-empty training set; fulltext build also backfills these rows and
        # auto-provisions __ydb_row_id because the primary key is Utf8. Keep ephemeral rows pre-created:
        # a Query Service transaction that DELETEs a missing row and UPSERTs a new row after row-id
        # provisioning historically exposed kqp_write_actor::HandleGenSequence task bookkeeping. That
        # lifecycle now has a focused C++ regression test; UPDATE below keeps this mixed nightly workload
        # deterministic while exercising DML generations and row-id stability.
        self._create_fulltext_index(table)
        self._create_vector_index(table)

    def _create_fulltext_index(self, table):
        self.client.query(
            f"""
            ALTER TABLE `{table}` ADD INDEX ft_idx
                GLOBAL USING fulltext_relevance
                ON (text)
                WITH (tokenizer=standard, use_filter_lowercase=true);
            """,
            True,
        )

    def _create_vector_index(self, table):
        self.client.query(
            f"""
            ALTER TABLE `{table}` ADD INDEX vec_idx
                GLOBAL USING vector_kmeans_tree
                ON (embedding)
                WITH (distance=cosine, vector_type=uint8, vector_dimension=2, levels=2, clusters=2);
            """,
            True,
        )

    def _hybrid_keys(self, table):
        result = self.client.query(
            f"""
            PRAGMA ydb.KMeansTreeSearchTopSize = "4";
            $target = Untag(Knn::ToBinaryStringUint8(
                Cast([250, 10] AS List<Uint8>)), "Uint8Vector");

            SELECT pk FROM `{table}`
            ORDER BY HybridRank(
                FullTextScore(text, "marker"),
                Knn::CosineDistance(embedding, $target),
                (4, 4) AS Limits)
            LIMIT 4;
            """,
            False,
        )
        if len(result) != 1:
            raise AssertionError(f"HybridRank returned {len(result)} result sets")
        return [row["pk"] for row in result[0].rows]

    def _wait_hybrid_ready(self, table):
        deadline = time.time() + 120
        last_error = None
        while time.time() < deadline and not self.is_stop_requested():
            try:
                self._check_hybrid(table)
                return
            except Exception as error:
                last_error = error
                message = str(error).lower()
                if "not ready" not in message and "no ready" not in message and "no global indexes" not in message:
                    raise
                time.sleep(1)
        if self.is_stop_requested():
            return False
        raise AssertionError(f"hybrid indexes did not become ready: {last_error}")

    def _check_hybrid(self, table):
        keys = self._hybrid_keys(table)
        if len(keys) != 4 or set(keys) != self.CORE_KEYS:
            raise AssertionError(f"HybridRank candidate union mismatch: {keys}")
        if len(set(keys)) != len(keys):
            raise AssertionError(f"HybridRank returned duplicate candidates: {keys}")
        if set(keys[:2]) != self.MARKER_KEYS:
            raise AssertionError(f"fulltext marker documents must lead fused order: {keys}")

    def _check_row_ids(self, table):
        result = self.client.query(
            f"SELECT pk, __ydb_row_id AS row_id FROM `{table}` ORDER BY pk;",
            False,
        )
        rows = result[0].rows
        row_ids = {row["pk"]: row["row_id"] for row in rows}
        if any(row_id is None for row_id in row_ids.values()):
            raise AssertionError(f"NULL __ydb_row_id found: {rows}")
        if len(row_ids) != len(set(row_ids.values())):
            raise AssertionError(f"duplicate __ydb_row_id found: {rows}")
        if self._row_ids is None:
            self._row_ids = row_ids
        elif row_ids != self._row_ids:
            raise AssertionError(f"__ydb_row_id changed across UPDATE/rebuild: before={self._row_ids}, after={row_ids}")

    def _check_unique_conflict_atomicity(self, table):
        logger.info("combined-index phase=unique-conflict table=%s", table)
        try:
            self.client.query(
                f"""
                UPSERT INTO `{table}` (pk, unique_tag, payload, version) VALUES
                    (1, "unique-a", "atomicity-corruption", 1000),
                    (99, "unique-b", "must-not-exist", 1001);
                """,
                False,
                log_error=False,
            )
        except ydb.issues.PreconditionFailed:
            pass
        else:
            raise AssertionError("duplicate unique_tag UPSERT unexpectedly succeeded")

        result = self.client.query(
            f"""
            SELECT pk, payload, version FROM `{table}`
            WHERE pk IN (1, 99) ORDER BY pk;
            """,
            False,
        )
        rows = result[0].rows
        if len(rows) != 1 or rows[0]["pk"] != 1:
            raise AssertionError(f"unique conflict left a partial main-table row: {rows}")
        if rows[0]["payload"] != "winner" or rows[0]["version"] != 1:
            raise AssertionError(f"unique conflict partially updated winner row: {rows}")
        with self._stats_lock:
            self._unique_conflicts += 1

    def _dml_cycle(self, table, iteration):
        current = f"ephemeral-{iteration % 2}"
        logger.info("combined-index phase=row-id-update table=%s pk=%s iteration=%d", table, current, iteration)
        self.client.query(
            f"""
            UPDATE `{table}`
            SET text = "noncandidate-{iteration % 3}", payload = {iteration}
            WHERE pk = "{current}";
            """,
            False,
        )

    def _rebuild_one_index(self, table):
        index = self._rng.choice(("fulltext", "vector"))
        if index == "fulltext":
            self.client.query(f"ALTER TABLE `{table}` DROP INDEX ft_idx;", True)
            self._create_fulltext_index(table)
        else:
            self.client.query(f"ALTER TABLE `{table}` DROP INDEX vec_idx;", True)
            self._create_vector_index(table)
        self._wait_hybrid_ready(table)
        with self._stats_lock:
            self._rebuilds += 1

    def _loop(self):
        table = self.get_table_path("docs")
        unique_table = self.get_table_path("unique_docs")
        self._create_tables(table, unique_table)
        self._wait_hybrid_ready(table)

        iteration = 0
        while not self.is_stop_requested():
            self._dml_cycle(table, iteration)
            self._check_unique_conflict_atomicity(unique_table)
            self._check_row_ids(table)
            self._check_hybrid(table)
            iteration += 1
            with self._stats_lock:
                self._iterations = iteration
            if iteration % 25 == 0:
                self._rebuild_one_index(table)

        self.client.drop_table(table)
        self.client.drop_table(unique_table)

    def get_stat(self):
        with self._stats_lock:
            return (
                f"seed={self.seed}, iterations={self._iterations}, "
                f"unique_conflicts={self._unique_conflicts}, rebuilds={self._rebuilds}"
            )

    def get_workload_thread_funcs(self):
        return [self._loop]
