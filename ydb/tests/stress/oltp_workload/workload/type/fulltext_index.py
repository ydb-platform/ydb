import logging
import os
import random
import time
import zlib
from itertools import cycle, product

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.library.fixtures import fulltext

logger = logging.getLogger("FulltextIndexWorkload")


class WorkloadFulltextIndex(WorkloadBase):
    def __init__(self, client, prefix, stop, seed=None):
        super().__init__(client, prefix, "fulltext_index", stop)
        self.table_name_prefix = "table"
        self.index_name_prefix = "fulltext_idx"
        self.row_count = 50
        self.limit = 10
        self.query_count = 10
        # Number of distinct prefix (user_id) values in the prefixed tables.
        # Every prefixed query matches only one of these groups, so the number of
        # groups is kept small to leave enough rows in each of them - otherwise
        # random word queries return nothing too often.
        self.user_count = 2
        # Stable across processes (unlike Python's hash()) and logged for exact reproduction. An explicit
        # runner seed takes precedence, then the component-specific replay override, then the old prefix
        # derived default used by PR tests.
        default_seed = zlib.crc32(str(prefix).encode("utf-8")) & 0xFFFFFFFF
        self.base_seed = seed if seed is not None else int(
            os.getenv("YDB_FULLTEXT_INDEX_SEED", str(default_seed)), 0
        )
        logger.info(
            "Fulltext base seed=%d replay='YDB_FULLTEXT_INDEX_SEED=%d'",
            self.base_seed, self.base_seed,
        )
        self.marker_expectations = {}
        self.row_id_snapshots = {}

    def _pk_type(self, string_pk):
        return "String" if string_pk else "Uint64"

    def _pk_literal(self, key, string_pk):
        # Zero-padded strings keep lexical order equal to numeric order for range deletes.
        if string_pk:
            return f'"{key:010d}"'
        return str(key)

    def _random_words(self, rng, length):
        pos = rng.randint(0, len(fulltext.seed_words) - length)
        return fulltext.seed_words[pos:pos + length]

    def _random_text(self, rng, iters=3, minlen=3, maxlen=17):
        words = []
        for _ in range(iters):
            words.extend(self._random_words(rng, rng.randint(minlen, maxlen)))
        return ' '.join(words)

    def _create_table(self, table_path, utf8, with_prefix=False, string_pk=False):
        logger.info(f"Create table {table_path}")
        if utf8:
            texttype = "Utf8"
        else:
            texttype = "String"
        pktype = self._pk_type(string_pk)
        if with_prefix:
            create_table_sql = f"""
                CREATE TABLE `{table_path}` (
                    pk {pktype},
                    user_id Uint64,
                    text {texttype},
                    PRIMARY KEY (pk)
                );
            """
        else:
            create_table_sql = f"""
                CREATE TABLE `{table_path}` (
                    pk {pktype},
                    text {texttype},
                    PRIMARY KEY (pk)
                );
            """
        self.client.query(create_table_sql, True)

    def _drop_table(self, table_path):
        logger.info(f"Drop table {table_path}")
        drop_table_sql = f"""
            DROP TABLE `{table_path}`;
        """
        self.client.query(drop_table_sql, True)

    def _drop_index(self, index_name, table_path):
        logger.info(f"Drop index {index_name}")
        drop_index_sql = f"""
            ALTER TABLE `{table_path}`
            DROP INDEX `{index_name}`;
        """
        self.client.query(drop_index_sql, True)

    def _create_index(
        self, index_name, table_path, index_type, tokenizer='standard', with_prefix=False
    ):
        logger.info(f"""Creating index index_type={index_type}, tokenizer={tokenizer}, with_prefix={with_prefix}""")
        if with_prefix:
            create_index_sql = f"""
                ALTER TABLE `{table_path}`
                ADD INDEX `{index_name}` GLOBAL USING {index_type}
                ON (user_id, text)
                WITH (
                    tokenizer={tokenizer},
                    use_filter_lowercase=true,
                    use_filter_snowball=true,
                    language="english"
                );
            """
        else:
            create_index_sql = f"""
                ALTER TABLE `{table_path}`
                ADD INDEX `{index_name}` GLOBAL USING {index_type}
                ON (text)
                WITH (
                    tokenizer={tokenizer},
                    use_filter_lowercase=true,
                    use_filter_snowball=true,
                    language="english"
                );
            """
        logger.info(create_index_sql)
        self.client.query(create_index_sql, True)

    def _upsert_values(
        self, table_path, use_upsert, min_key, max_key, rng,
        with_prefix=False, string_pk=False
    ):
        logger.info("Upsert values")
        values = []

        for key in range(min_key, max_key):
            text = self._random_text(rng)
            pk = self._pk_literal(key, string_pk)
            if with_prefix:
                user_id = (key % self.user_count) + 1
                values.append(f'({pk}, {user_id}, "{text}")')
            else:
                values.append(f'({pk}, "{text}")')

        if use_upsert:
            insert = "UPSERT"
        else:
            insert = "INSERT"
        if with_prefix:
            upsert_sql = f"""
                {insert} INTO `{table_path}` (pk, user_id, text)
                VALUES {",".join(values)};
            """
        else:
            upsert_sql = f"""
                {insert} INTO `{table_path}` (pk, text)
                VALUES {",".join(values)};
            """
        self.client.query(upsert_sql, False)

    def _delete_rows(self, table_path, min_key, max_key, string_pk=False):
        logger.info("Delete rows")
        min_pk = self._pk_literal(min_key, string_pk)
        max_pk = self._pk_literal(max_key, string_pk)
        delete_sql = f"""
            DELETE FROM `{table_path}` WHERE pk >= {min_pk} AND pk < {max_pk};
        """
        self.client.query(delete_sql, False)

    def _normalize_pk(self, value):
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    def _insert_marker_rows(self, table_path, iteration, with_prefix=False, string_pk=False):
        # Marker text is independent of random corpus text and consists of tokenizer-safe lowercase
        # alphanumerics. It is therefore stable for standard/whitespace tokenizers and Snowball.
        marker = f"oraclemarker{iteration:08d}"
        rows = []
        expected = {}
        users = range(1, self.user_count + 1) if with_prefix else [None]
        for offset, user_id in enumerate(users):
            key = 1_000_000 + iteration * 10 + offset
            pk = self._pk_literal(key, string_pk)
            expected.setdefault(user_id, set()).add(self._normalize_pk(f"{key:010d}" if string_pk else key))
            if with_prefix:
                rows.append(f'({pk}, {user_id}, "{marker} stable")')
            else:
                rows.append(f'({pk}, "{marker} stable")')
        columns = "pk, user_id, text" if with_prefix else "pk, text"
        self.client.query(f"""
            UPSERT INTO `{table_path}` ({columns}) VALUES {','.join(rows)};
        """, False)
        self.marker_expectations.setdefault(table_path, {})[marker] = expected
        return marker

    def _select_pk_set(self, query):
        result = self.client.query(query, False)
        if len(result) != 1:
            raise Exception(f"Expected one result set, got {len(result)}")
        return {self._normalize_pk(row["pk"]) for row in result[0].rows}

    def _assert_primary_marker(self, table_path, marker, with_prefix=False):
        for user_id, expected in self.marker_expectations[table_path][marker].items():
            prefix = f"user_id = {user_id} AND" if with_prefix else ""
            actual = self._select_pk_set(f"""
                SELECT pk FROM `{table_path}`
                WHERE {prefix} text = "{marker} stable";
            """)
            if actual != expected:
                raise Exception(
                    f"Primary marker mismatch table={table_path} marker={marker} "
                    f"user_id={user_id}: expected={expected}, actual={actual}"
                )

    def _assert_index_oracle(
        self, index_name, table_path, marker, with_prefix=False, relevance=False
    ):
        self._assert_primary_marker(table_path, marker, with_prefix=with_prefix)
        for user_id, expected in self.marker_expectations[table_path][marker].items():
            prefix = f"user_id = {user_id} AND" if with_prefix else ""
            predicate = (
                f'FulltextScore(text, "{marker}") > 0'
                if relevance else f'FulltextMatch(text, "{marker}")'
            )
            actual = self._select_pk_set(f"""
                SELECT pk FROM `{table_path}` VIEW `{index_name}`
                WHERE {prefix} {predicate};
            """)
            if actual != expected:
                raise Exception(
                    f"Fulltext oracle mismatch table={table_path} index={index_name} "
                    f"marker={marker} user_id={user_id}: expected={expected}, actual={actual}"
                )

    def _assert_row_id_invariants(self, table_path, string_pk):
        if not string_pk:
            return
        result = self.client.query(f"""
            SELECT pk, __ydb_row_id FROM `{table_path}` ORDER BY pk;
        """, False)
        rows = result[0].rows
        current = {self._normalize_pk(row["pk"]): row["__ydb_row_id"] for row in rows}
        if len(current.values()) != len(set(current.values())):
            raise Exception(f"Duplicate __ydb_row_id detected in {table_path}: {current}")
        previous = self.row_id_snapshots.get(table_path, {})
        for pk in current.keys() & previous.keys():
            if current[pk] != previous[pk]:
                raise Exception(
                    f"Unstable __ydb_row_id table={table_path} pk={pk}: "
                    f"previous={previous[pk]}, current={current[pk]}"
                )
        self.row_id_snapshots[table_path] = current

    def _assert_phase_invariants(
        self, phase, index_name, table_path, marker, index_type, with_prefix, string_pk
    ):
        logger.info(
            "Checking fulltext invariants phase=%s table=%s index=%s marker=%s",
            phase, table_path, index_name, marker,
        )
        self._assert_index_oracle(
            index_name=index_name,
            table_path=table_path,
            marker=marker,
            with_prefix=with_prefix,
            relevance=index_type == "fulltext_relevance",
        )
        self._assert_row_id_invariants(table_path, string_pk)

    def _select_contains(self, index_name, table_path, rng, with_prefix=False):
        query = ' '.join(self._random_words(rng, 3))
        if with_prefix:
            user_id = rng.randint(1, self.user_count)
            select_sql = f"""
                SELECT `pk`, `text`
                FROM `{table_path}`
                VIEW `{index_name}`
                WHERE user_id = {user_id} AND FulltextMatch(`text`, "{query}")
                LIMIT {self.limit};
            """
        else:
            select_sql = f"""
                SELECT `pk`, `text`
                FROM `{table_path}`
                VIEW `{index_name}`
                WHERE FulltextMatch(`text`, "{query}")
                LIMIT {self.limit};
            """
        res = self.client.query(select_sql, False)
        if len(res) == 0:
            raise Exception("Query returned no resultsets")
        n = len(res[0].rows)
        logger.info(f"Selected {n} rows using contains")
        return n

    def _select_relevance(self, index_name, table_path, rng, with_prefix=False):
        query = ' '.join(self._random_words(rng, 3))
        if with_prefix:
            user_id = rng.randint(1, self.user_count)
            select_sql = f"""
                SELECT `pk`, `text`, FulltextScore(`text`, "{query}") as `rel`
                FROM `{table_path}`
                VIEW `{index_name}`
                WHERE user_id = {user_id} AND FulltextScore(`text`, "{query}") > 0
                ORDER BY `rel`
                LIMIT {self.limit};
            """
        else:
            select_sql = f"""
                SELECT `pk`, `text`, FulltextScore(`text`, "{query}") as `rel`
                FROM `{table_path}`
                VIEW `{index_name}`
                WHERE FulltextScore(`text`, "{query}") > 0
                ORDER BY `rel`
                LIMIT {self.limit};
            """
        res = self.client.query(select_sql, False)
        if len(res) == 0:
            raise Exception("Query returned no resultsets")
        n = len(res[0].rows)
        logger.info(f"Selected {n} rows using relevance")
        prev = -100
        for row in res[0].rows:
            rel = row['rel']
            if rel < prev:
                raise Exception(f"Relevance not in order, prev: {prev}, rel: {rel}")
            prev = rel
        return n

    def _wait_index_ready(
        self, index_name, table_path, marker, index_type, with_prefix=False, string_pk=False
    ):
        start_time = time.time()
        while time.time() - start_time < 60:
            try:
                self._assert_phase_invariants(
                    phase="index-ready",
                    index_name=index_name,
                    table_path=table_path,
                    marker=marker,
                    index_type=index_type,
                    with_prefix=with_prefix,
                    string_pk=string_pk,
                )
            except Exception as ex:
                if "No global indexes for table" in str(ex) or "Required global index not found" in str(ex):
                    time.sleep(1)
                    continue
                raise ex
            logger.info(f"Index {index_name} is ready")
            return
        raise Exception("Error getting index status")

    def _check_loop(
        self, table_path, index_type, iteration, rng, tokenizer='standard', utf8=False,
        with_prefix=False, string_pk=False
    ):
        if utf8:
            texttype = "Utf8"
        else:
            texttype = "String"
        prefix_suffix = "_prefixed" if with_prefix else ""
        pk_suffix = "_string_pk" if string_pk else ""
        index_name = f"{self.index_name_prefix}_{texttype}_{index_type}_{tokenizer}{prefix_suffix}{pk_suffix}"
        marker = self._insert_marker_rows(
            table_path=table_path,
            iteration=iteration,
            with_prefix=with_prefix,
            string_pk=string_pk,
        )
        self._create_index(
            table_path=table_path,
            index_name=index_name,
            index_type=index_type,
            tokenizer=tokenizer,
            with_prefix=with_prefix,
        )
        self._wait_index_ready(
            table_path=table_path,
            index_name=index_name,
            marker=marker,
            index_type=index_type,
            with_prefix=with_prefix,
            string_pk=string_pk,
        )
        self._assert_phase_invariants(
            "after-add", index_name, table_path, marker, index_type, with_prefix, string_pk
        )
        n = 0
        for i in range(0, self.query_count):
            # select from index with FulltextMatch
            n += self._select_contains(
                index_name=index_name,
                table_path=table_path,
                rng=rng,
                with_prefix=with_prefix,
            )
        logger.info("Random contains smoke queries selected %d rows", n)
        if index_type == 'fulltext_relevance':
            n = 0
            for i in range(0, self.query_count):
                # select from index with FulltextScore
                n += self._select_relevance(
                    index_name=index_name,
                    table_path=table_path,
                    rng=rng,
                    with_prefix=with_prefix,
                )
            logger.info("Random relevance smoke queries selected %d rows", n)
        # insert into index
        self._upsert_values(
            table_path=table_path,
            use_upsert=False,
            min_key=self.row_count+1,
            max_key=self.row_count+3,
            rng=rng,
            with_prefix=with_prefix,
            string_pk=string_pk,
        )
        self._assert_phase_invariants(
            "after-insert", index_name, table_path, marker, index_type, with_prefix, string_pk
        )
        # update the index using upsert
        self._upsert_values(
            table_path=table_path,
            use_upsert=True,
            min_key=self.row_count-3,
            max_key=self.row_count+2,
            rng=rng,
            with_prefix=with_prefix,
            string_pk=string_pk,
        )
        self._assert_phase_invariants(
            "after-upsert", index_name, table_path, marker, index_type, with_prefix, string_pk
        )
        # delete from index
        self._delete_rows(
            table_path=table_path,
            min_key=self.row_count-3,
            max_key=self.row_count+3,
            string_pk=string_pk,
        )
        self._assert_phase_invariants(
            "after-delete", index_name, table_path, marker, index_type, with_prefix, string_pk
        )
        # sometimes replace the index
        if rng.randint(0, 1) == 0:
            replacement = index_name+'Rename'
            self._create_index(
                index_name=replacement,
                table_path=table_path,
                index_type=index_type,
                tokenizer=tokenizer,
                with_prefix=with_prefix,
            )
            self._wait_index_ready(
                replacement, table_path, marker, index_type,
                with_prefix=with_prefix, string_pk=string_pk,
            )
            self._assert_phase_invariants(
                "before-replace", replacement, table_path, marker,
                index_type, with_prefix, string_pk,
            )
            self.client.replace_index(table_path, replacement, index_name)
            self._assert_phase_invariants(
                "after-replace", index_name, table_path, marker,
                index_type, with_prefix, string_pk,
            )
        self._drop_index(index_name, table_path)
        self._assert_primary_marker(table_path, marker, with_prefix=with_prefix)
        self._assert_row_id_invariants(table_path, string_pk)
        logger.info('check was completed successfully')

    def _loop(self):
        # Tables cover text type × prefix × PK type (Uint64 legacy doc_id vs String -> __ydb_row_id).
        table_specs = [
            (False, False, False),  # String text, no prefix, Uint64 PK
            (True, False, False),   # Utf8 text, no prefix, Uint64 PK
            (False, True, False),   # String text, prefixed, Uint64 PK
            (True, True, False),    # Utf8 text, prefixed, Uint64 PK
            (False, False, True),   # String text, no prefix, String PK
            (True, False, True),    # Utf8 text, no prefix, String PK
            (False, True, True),    # String text, prefixed, String PK
            (True, True, True),     # Utf8 text, prefixed, String PK
        ]
        tables = []
        for utf8, with_prefix, string_pk in table_specs:
            text_suffix = "utf8" if utf8 else "text"
            prefix_suffix = "_prefixed" if with_prefix else ""
            pk_suffix = "_string_pk" if string_pk else ""
            table_path = self.get_table_path(
                f"{self.table_name_prefix}_{text_suffix}{prefix_suffix}{pk_suffix}"
            )
            self._create_table(
                table_path, utf8, with_prefix=with_prefix, string_pk=string_pk
            )
            tables.append(table_path)

        utf8_opts = [0, 1]
        index_type_opts = ['fulltext_plain', 'fulltext_relevance']
        tokenizer_opts = ['standard', 'whitespace']
        prefix_opts = [False, True]
        string_pk_opts = [False, True]
        opts = [
            opt for opt in product(
                utf8_opts, index_type_opts, tokenizer_opts, prefix_opts, string_pk_opts
            )
            # Prefixed relevance is not a supported index layout.
            if not (opt[1] == 'fulltext_relevance' and opt[3])
        ]
        option_rng = random.Random(self.base_seed)
        option_rng.shuffle(opts)
        opt_iter = cycle(opts)

        iteration = 0
        while not self.is_stop_requested():
            [utf8, index_type, tokenizer, with_prefix, string_pk] = next(opt_iter)
            seed = (self.base_seed + iteration) & 0xFFFFFFFF
            rng = random.Random(seed)
            logger.info(
                "Fulltext stress iteration=%d seed=%d option=(utf8=%s,index_type=%s,"
                "tokenizer=%s,prefix=%s,string_pk=%s)",
                iteration, seed, utf8, index_type, tokenizer, with_prefix, string_pk,
            )
            try:
                # Same layout as table_specs: utf8 + 2*prefix + 4*string_pk
                table_idx = utf8 + (2 if with_prefix else 0) + (4 if string_pk else 0)
                self._upsert_values(
                    table_path=tables[table_idx],
                    use_upsert=True,
                    min_key=0,
                    max_key=self.row_count,
                    rng=rng,
                    with_prefix=with_prefix,
                    string_pk=string_pk,
                )
                self._check_loop(
                    table_path=tables[table_idx],
                    index_type=index_type,
                    iteration=iteration,
                    rng=rng,
                    tokenizer=tokenizer,
                    utf8=utf8,
                    with_prefix=with_prefix,
                    string_pk=string_pk,
                )
            except Exception as ex:
                logger.info("ERROR iteration=%d seed=%d: %s", iteration, seed, ex)
                raise ex
            iteration += 1
        for t in tables:
            self._drop_table(t)

    def get_stat(self):
        return ""

    def get_workload_thread_funcs(self):
        return [self._loop]
