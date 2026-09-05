import pytest
import re
import ydb as ydbs

from ydb.tests.library.fixtures import fulltext
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestFulltextIndex(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 3):
            pytest.skip("Only available since 26-3")
        self.row_count = 50
        self.query_count = 5
        self.limit = 5
        self.good_queries = {}
        self.good_query_users = {}
        yield from self.setup_cluster(extra_feature_flags=[
            "enable_fulltext_index",
            "enable_fulltext_index_prefix",
            "enable_fulltext_index_row_id",
            "enable_compact_fulltext_index",
            "enable_add_unique_index",
        ], table_service_config={
            "enable_hybrid_search": True,
            # Compact online maintenance is implemented by the stream-index write path.
            "enable_index_stream_write": True,
        })

    def create_table(self, table_name, with_prefix=False):
        if with_prefix:
            query = f"""
                CREATE TABLE {table_name} (
                    key Uint64 NOT NULL,
                    user_id Uint64 NOT NULL,
                    text String NOT NULL,
                    PRIMARY KEY (key)
                )
                """
        else:
            query = f"""
                CREATE TABLE {table_name} (
                    key Uint64 NOT NULL,
                    text String NOT NULL,
                    PRIMARY KEY (key)
                )
                """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _write_data(self, table_name, with_prefix=False):
        good = []
        good_users = []
        values = []
        for key in range(self.row_count):
            text = fulltext.get_random_text()
            if with_prefix:
                user_id = (key % 10) + 1  # Distribute across 10 users
                values.append(f'({key}, {user_id}, "{text}")')
            else:
                values.append(f'({key}, "{text}")')
            words = text.split(' ')
            good_query = []
            for w in words:
                if len(w) >= 4 and re.fullmatch('\\w+', w):
                    good_query.append(w)
                    if len(good_query) >= 3:
                        break
            if len(good_query) >= 3:
                good.append(' '.join(good_query))
                if with_prefix:
                    good_users.append(user_id)
        self.good_queries[table_name] = good
        if with_prefix:
            self.good_query_users[table_name] = good_users
        if with_prefix:
            sql_upsert = f"""
                UPSERT INTO `{table_name}` (`key`, `user_id`, `text`)
                VALUES {",".join(values)};
                """
        else:
            sql_upsert = f"""
                UPSERT INTO `{table_name}` (`key`, `text`)
                VALUES {",".join(values)};
                """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(sql_upsert)

    def _create_index(self, table_name, index_name, index_type, tokenizer='standard', with_prefix=False):
        if with_prefix:
            create_index_sql = f"""
                ALTER TABLE `{table_name}`
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
                ALTER TABLE `{table_name}`
                ADD INDEX `{index_name}` GLOBAL USING {index_type}
                ON (text)
                WITH (
                    tokenizer={tokenizer},
                    use_filter_lowercase=true,
                    use_filter_snowball=true,
                    language="english"
                );
                """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create_index_sql)

    def wait_index_ready(self):
        def predicate():
            try:
                self.select_from_index_without_roll()
            except ydbs.issues.SchemeError as ex:
                if "Required global index not found, index name" in str(ex):
                    return False
                raise ex
            return True

        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Error getting index status"

    def _get_queries(self):
        queries = []
        for text_type in ['string', 'utf8']:
            for index_type in ['fulltext_plain', 'fulltext_relevance']:
                for tokenizer in ['standard', 'whitespace']:
                    queries.extend(self._get_queries_for(text_type, index_type, tokenizer, with_prefix=False))
                    # Prefixed relevance is deliberately unsupported. Keep compatibility coverage on
                    # the supported prefixed plain layout instead of accepting a broken DDL contract.
                    if index_type == 'fulltext_plain':
                        queries.extend(self._get_queries_for(text_type, index_type, tokenizer, with_prefix=True))
        return queries

    def _get_queries_for(self, text_type, index_type, tokenizer, with_prefix=False):
        table_suffix = "_prefixed" if with_prefix else ""
        table_name = f"table_{text_type}{table_suffix}"
        index_name = f"idx_{index_type}_{tokenizer}{table_suffix}"
        queries = []
        for i in range(self.query_count):
            query = self.good_queries[table_name][i]
            if with_prefix:
                # good_queries may skip source rows that do not contain three usable terms, so the
                # list position is not the original row key. Retain the actual row's prefix.
                user_id = self.good_query_users[table_name][i]
                queries.append([
                    True, f"""
                    SELECT `key`, `text`
                    FROM `{table_name}`
                    VIEW `{index_name}`
                    WHERE user_id = {user_id} AND FulltextMatch(`text`, "{query}")
                    LIMIT {self.limit};
                    """
                ])
                if index_type == 'fulltext_relevance':
                    queries.append([
                        True, f"""
                        SELECT `key`, `text`, FulltextScore(`text`, "{query}") as `rel`
                        FROM `{table_name}`
                        VIEW `{index_name}`
                        WHERE user_id = {user_id} AND FulltextScore(`text`, "{query}") > 0
                        ORDER BY `rel` DESC
                        LIMIT {self.limit};
                        """
                    ])
                # Insert, update, upsert, delete with prefix
                key = self.row_count+1
                queries.append([
                    False, f"""
                    INSERT INTO `{table_name}` (`key`, `user_id`, `text`)
                    VALUES ({key}, {user_id}, "{fulltext.get_random_text()}")
                    """
                ])
                queries.append([
                    False, f"""
                    UPDATE `{table_name}` SET `text`="{fulltext.get_random_text()}"
                    WHERE key={key}
                    """
                ])
                queries.append([
                    False, f"""
                    UPSERT INTO `{table_name}` (`key`, `user_id`, `text`)
                    VALUES ({key}, {user_id}, "{fulltext.get_random_text()}")
                    """
                ])
                queries.append([
                    False, f"""
                    DELETE FROM `{table_name}` WHERE key={key}
                    """
                ])
            else:
                queries.append([
                    True, f"""
                    SELECT `key`, `text`
                    FROM `{table_name}`
                    VIEW `{index_name}`
                    WHERE FulltextMatch(`text`, "{query}")
                    LIMIT {self.limit};
                    """
                ])
                if index_type == 'fulltext_relevance':
                    queries.append([
                        True, f"""
                        SELECT `key`, `text`, FulltextScore(`text`, "{query}") as `rel`
                        FROM `{table_name}`
                        VIEW `{index_name}`
                        WHERE FulltextScore(`text`, "{query}") > 0
                        ORDER BY `rel` DESC
                        LIMIT {self.limit};
                        """
                    ])
                # Insert, update, upsert, delete
                key = self.row_count+1
                queries.append([
                    False, f"""
                    INSERT INTO `{table_name}` (`key`, `text`)
                    VALUES ({key}, "{fulltext.get_random_text()}")
                    """
                ])
                queries.append([
                    False, f"""
                    UPDATE `{table_name}` SET `text`="{fulltext.get_random_text()}"
                    WHERE key={key}
                    """
                ])
                queries.append([
                    False, f"""
                    UPSERT INTO `{table_name}` (`key`, `text`)
                    VALUES ({key}, "{fulltext.get_random_text()}")
                    """
                ])
                queries.append([
                    False, f"""
                    DELETE FROM `{table_name}` WHERE key={key}
                    """
                ])
        return queries

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for [is_select, query] in queries:
                result_sets = session_pool.execute_with_retries(query)
                if is_select:
                    assert len(result_sets[0].rows) > 0, "Query returned an empty set"
                    rows = result_sets[0].rows
                    for row in rows:
                        assert 'rel' not in row or row['rel'] is not None, "relevance is None"

    def select_from_index(self):
        queries = self._get_queries()
        for _ in self.roll():
            self._do_queries(queries)

    def select_from_index_without_roll(self):
        queries = self._get_queries()
        self._do_queries(queries)

    def test_fulltext_index(self):
        for text_type in ['string', 'utf8']:
            # Test regular fulltext indexes
            table_name = f"table_{text_type}"
            self.create_table(table_name, with_prefix=False)
            self._write_data(table_name, with_prefix=False)
            for index_type in ['fulltext_plain', 'fulltext_relevance']:
                for tokenizer in ['standard', 'whitespace']:
                    index_name = f"idx_{index_type}_{tokenizer}"
                    self._create_index(
                        table_name=table_name,
                        index_name=index_name,
                        index_type=index_type,
                        tokenizer=tokenizer,
                        with_prefix=False,
                    )

            # Test prefixed (filtered) fulltext indexes
            table_name_prefixed = f"table_{text_type}_prefixed"
            self.create_table(table_name_prefixed, with_prefix=True)
            self._write_data(table_name_prefixed, with_prefix=True)
            for index_type in ['fulltext_plain']:
                for tokenizer in ['standard', 'whitespace']:
                    index_name = f"idx_{index_type}_{tokenizer}_prefixed"
                    self._create_index(
                        table_name=table_name_prefixed,
                        index_name=index_name,
                        index_type=index_type,
                        tokenizer=tokenizer,
                        with_prefix=True,
                    )
        self.wait_index_ready()
        self.select_from_index()

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            return session_pool.execute_with_retries(query)

    def _select_column(self, query, column):
        result_sets = self._execute(query)
        return [row[column] for row in result_sets[0].rows]

    def _create_exact_feature_tables(self):
        self._execute("""
            CREATE TABLE `compact_docs` (
                `key` Uint64 NOT NULL,
                `text` Utf8 NOT NULL,
                PRIMARY KEY (`key`)
            );
        """)
        self._execute("""
            UPSERT INTO `compact_docs` (`key`, `text`) VALUES
                (1, "anchor base"u),
                (2, "unrelated base"u);
        """)
        self._execute("""
            ALTER TABLE `compact_docs` ADD INDEX `plain_idx`
                GLOBAL USING fulltext_plain ON (`text`)
                WITH (tokenizer=standard, use_filter_lowercase=true);
        """)
        self._execute("""
            ALTER TABLE `compact_docs` ADD INDEX `relevance_idx`
                GLOBAL USING fulltext_relevance ON (`text`)
                WITH (tokenizer=standard, use_filter_lowercase=true);
        """)

        # A String PK cannot be used as the fulltext doc id directly. With the exact feature flags,
        # the ALTER build provisions __ydb_row_id, its sequence and unique index.
        self._execute("""
            CREATE TABLE `rowid_docs` (
                `pk` String NOT NULL,
                `text` Utf8 NOT NULL,
                PRIMARY KEY (`pk`)
            );
        """)
        self._execute("""
            UPSERT INTO `rowid_docs` (`pk`, `text`) VALUES
                ("base", "rowanchor base"u),
                ("other", "unrelated base"u);
        """)
        self._execute("""
            ALTER TABLE `rowid_docs` ADD INDEX `rowid_idx`
                GLOBAL USING fulltext_plain ON (`text`)
                WITH (tokenizer=standard, use_filter_lowercase=true);
        """)

        self._execute("""
            CREATE TABLE `prefix_docs` (
                `key` Uint64 NOT NULL,
                `tenant` Uint64 NOT NULL,
                `text` Utf8 NOT NULL,
                PRIMARY KEY (`key`)
            );
        """)
        self._execute("""
            UPSERT INTO `prefix_docs` (`key`, `tenant`, `text`) VALUES
                (1, 1, "prefixanchor one"u),
                (2, 2, "prefixanchor two"u);
        """)
        self._execute("""
            ALTER TABLE `prefix_docs` ADD INDEX `prefix_idx`
                GLOBAL USING fulltext_plain ON (`tenant`, `text`)
                WITH (tokenizer=standard, use_filter_lowercase=true);
        """)

    def _wait_exact_indexes(self):
        def predicate():
            try:
                self._select_column("""
                    SELECT `key` FROM `compact_docs` VIEW `plain_idx`
                    WHERE FulltextMatch(`text`, "anchor");
                """, "key")
                self._select_column("""
                    SELECT `pk` FROM `rowid_docs` VIEW `rowid_idx`
                    WHERE FulltextMatch(`text`, "rowanchor");
                """, "pk")
                self._select_column("""
                    SELECT `key` FROM `prefix_docs` VIEW `prefix_idx`
                    WHERE `tenant` = 1 AND FulltextMatch(`text`, "prefixanchor");
                """, "key")
            except ydbs.issues.SchemeError as ex:
                if "Required global index not found, index name" in str(ex):
                    return False
                raise
            return True

        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Exact-config indexes are not ready"

    def _exercise_new_ddl_at_roll_step(self, step):
        """SchemeShard remains the deterministic DDL authority while binaries are mixed."""
        table = f"roll_ddl_{step}"
        self._execute(f"""
            CREATE TABLE `{table}` (
                `pk` String NOT NULL,
                `text` Utf8 NOT NULL,
                PRIMARY KEY (`pk`)
            );
        """)
        self._execute(f"""
            UPSERT INTO `{table}` (`pk`, `text`) VALUES
                ("base", "ddlanchor base"u);
        """)
        self._execute(f"""
            ALTER TABLE `{table}` ADD INDEX `idx`
                GLOBAL USING fulltext_plain ON (`text`)
                WITH (tokenizer=standard, use_filter_lowercase=true);
        """)

        def ready():
            try:
                keys = self._select_column(f"""
                    SELECT `pk` FROM `{table}` VIEW `idx`
                    WHERE FulltextMatch(`text`, "ddlanchor") ORDER BY `pk`;
                """, "pk")
                return keys == [b"base"]
            except ydbs.issues.SchemeError as error:
                if "not ready to use" in str(error) or "Required global index not found" in str(error):
                    return False
                raise

        assert wait_for(ready, timeout_seconds=100, step_seconds=1), f"{table}.idx is not ready"
        self._execute(f"""
            UPSERT INTO `{table}` (`pk`, `text`) VALUES
                ("mixed", "ddlanchor mixed"u);
        """)
        keys = self._select_column(f"""
            SELECT `pk` FROM `{table}` VIEW `idx`
            WHERE FulltextMatch(`text`, "ddlanchor") ORDER BY `pk`;
        """, "pk")
        assert keys == [b"base", b"mixed"]
        rows = self._execute(f"SELECT `__ydb_row_id` FROM `{table}`;")[0].rows
        row_ids = [row["__ydb_row_id"] for row in rows]
        assert len(row_ids) == len(set(row_ids))
        self._execute(f"DROP TABLE `{table}`;")

    def _restart_with_exact_feature_gates(self, enabled):
        flags = self.config.yaml_config.setdefault("feature_flags", {})
        for name in (
            "enable_fulltext_index_prefix",
            "enable_fulltext_index_row_id",
            "enable_compact_fulltext_index",
        ):
            flags[name] = enabled
        self.stop_driver()
        self.cluster.update_configurator_and_restart(self.config)
        self.driver = self.create_driver()

    def _exercise_feature_gate_authority(self):
        # Physical indexes created while the flags were enabled remain authoritative: their read/write
        # path is schema-driven, not reinterpreted from the current creation gates.
        self._restart_with_exact_feature_gates(False)
        self._execute("""
            UPSERT INTO `rowid_docs` (`pk`, `text`) VALUES
                ("flag-off", "rowanchor flag off"u);
            UPSERT INTO `prefix_docs` (`key`, `tenant`, `text`) VALUES
                (9999, 1, "prefixanchor flag off"u);
        """)
        assert b"flag-off" in self._select_column("""
            SELECT `pk` FROM `rowid_docs` VIEW `rowid_idx`
            WHERE FulltextMatch(`text`, "rowanchor") ORDER BY `pk`;
        """, "pk")
        assert 9999 in self._select_column("""
            SELECT `key` FROM `prefix_docs` VIEW `prefix_idx`
            WHERE `tenant` = 1 AND FulltextMatch(`text`, "prefixanchor") ORDER BY `key`;
        """, "key")
        assert 1 in self._select_column("""
            SELECT `key` FROM `compact_docs` VIEW `relevance_idx`
            WHERE FulltextScore(`text`, "anchor") > 0 ORDER BY `key`;
        """, "key")

        try:
            self._execute("""
                ALTER TABLE `prefix_docs` ADD INDEX `disabled_prefix_idx`
                    GLOBAL USING fulltext_plain ON (`tenant`, `text`)
                    WITH (tokenizer=standard, use_filter_lowercase=true);
            """)
        except Exception as error:
            assert "Prefixed fulltext/json index support is disabled" in str(error)
        else:
            raise AssertionError("new prefixed fulltext DDL unexpectedly succeeded with its gate disabled")

        self._restart_with_exact_feature_gates(True)
        self._execute("""
            ALTER TABLE `prefix_docs` ADD INDEX `reenabled_prefix_idx`
                GLOBAL USING fulltext_plain ON (`tenant`, `text`)
                WITH (tokenizer=standard, use_filter_lowercase=true);
        """)

        def ready():
            try:
                keys = self._select_column("""
                    SELECT `key` FROM `prefix_docs` VIEW `reenabled_prefix_idx`
                    WHERE `tenant` = 1 AND FulltextMatch(`text`, "prefixanchor");
                """, "key")
                return 9999 in keys
            except ydbs.issues.SchemeError as error:
                if "not ready to use" in str(error) or "Required global index not found" in str(error):
                    return False
                raise

        assert wait_for(ready, timeout_seconds=100, step_seconds=1), "re-enabled prefix index is not ready"
        self._execute("ALTER TABLE `prefix_docs` DROP INDEX `reenabled_prefix_idx`;")
        self._exercise_new_ddl_at_roll_step(10000)

    def _exercise_exact_feature_config(self, step):
        compact_key = 1000 + step
        scratch_key = 2000 + step
        rowid_pk = f"roll-{step:03d}"
        rowid_scratch = f"scratch-{step:03d}"
        prefix_key = 3000 + step
        prefix_scratch = 4000 + step

        # Compact plain + relevance: all write forms run while nodes are at this roll step.
        self._execute(f"""
            INSERT INTO `compact_docs` (`key`, `text`)
                VALUES ({compact_key}, "anchor inserted {step}"u);
            UPDATE `compact_docs` SET `text` = "anchor updated {step}"u
                WHERE `key` = {compact_key};
            UPSERT INTO `compact_docs` (`key`, `text`)
                VALUES ({compact_key}, "anchor final {step}"u);
            INSERT INTO `compact_docs` (`key`, `text`)
                VALUES ({scratch_key}, "anchor scratch {step}"u);
            DELETE FROM `compact_docs` WHERE `key` = {scratch_key};
        """)

        self._execute(f"""
            INSERT INTO `rowid_docs` (`pk`, `text`)
                VALUES ("{rowid_pk}", "rowanchor inserted {step}"u);
            UPDATE `rowid_docs` SET `text` = "rowanchor updated {step}"u
                WHERE `pk` = "{rowid_pk}";
            UPSERT INTO `rowid_docs` (`pk`, `text`)
                VALUES ("{rowid_pk}", "rowanchor final {step}"u);
            INSERT INTO `rowid_docs` (`pk`, `text`)
                VALUES ("{rowid_scratch}", "rowanchor scratch {step}"u);
            DELETE FROM `rowid_docs` WHERE `pk` = "{rowid_scratch}";
        """)

        self._execute(f"""
            INSERT INTO `prefix_docs` (`key`, `tenant`, `text`)
                VALUES ({prefix_key}, 1, "prefixanchor inserted {step}"u);
            UPDATE `prefix_docs` SET `text` = "prefixanchor updated {step}"u
                WHERE `key` = {prefix_key};
            UPSERT INTO `prefix_docs` (`key`, `tenant`, `text`)
                VALUES ({prefix_key}, 1, "prefixanchor final {step}"u);
            INSERT INTO `prefix_docs` (`key`, `tenant`, `text`)
                VALUES ({prefix_scratch}, 2, "prefixanchor scratch {step}"u);
            DELETE FROM `prefix_docs` WHERE `key` = {prefix_scratch};
        """)

        expected_compact = [1] + [1000 + i for i in range(step + 1)]
        plain_keys = self._select_column("""
            SELECT `key` FROM `compact_docs` VIEW `plain_idx`
            WHERE FulltextMatch(`text`, "anchor") ORDER BY `key`;
        """, "key")
        assert plain_keys == expected_compact

        relevance_keys = self._select_column("""
            SELECT `key` FROM `compact_docs` VIEW `relevance_idx`
            WHERE FulltextScore(`text`, "anchor") > 0 ORDER BY `key`;
        """, "key")
        assert relevance_keys == expected_compact

        # The Python SDK exposes YDB String as bytes (Utf8 columns above are str).
        expected_rowid = [b"base"] + [f"roll-{i:03d}".encode() for i in range(step + 1)]
        rowid_keys = self._select_column("""
            SELECT `pk` FROM `rowid_docs` VIEW `rowid_idx`
            WHERE FulltextMatch(`text`, "rowanchor") ORDER BY `pk`;
        """, "pk")
        assert rowid_keys == expected_rowid

        rowid_rows = self._execute("""
            SELECT `pk`, `__ydb_row_id` FROM `rowid_docs` ORDER BY `pk`;
        """)[0].rows
        current_row_ids = {row["pk"]: row["__ydb_row_id"] for row in rowid_rows}
        assert len(current_row_ids.values()) == len(set(current_row_ids.values()))
        for pk, row_id in getattr(self, "exact_row_ids", {}).items():
            assert current_row_ids[pk] == row_id
        self.exact_row_ids = current_row_ids

        expected_prefix = [1] + [3000 + i for i in range(step + 1)]
        prefix_keys = self._select_column("""
            SELECT `key` FROM `prefix_docs` VIEW `prefix_idx`
            WHERE `tenant` = 1 AND FulltextMatch(`text`, "prefixanchor") ORDER BY `key`;
        """, "key")
        assert prefix_keys == expected_prefix
        tenant_two = self._select_column("""
            SELECT `key` FROM `prefix_docs` VIEW `prefix_idx`
            WHERE `tenant` = 2 AND FulltextMatch(`text`, "prefixanchor") ORDER BY `key`;
        """, "key")
        assert tenant_two == [2]

        # New compact+row-id DDL is deliberately issued after each node transition, not only before
        # rolling begins. This covers both homogeneous endpoints and every mixed-binary boundary.
        self._exercise_new_ddl_at_roll_step(step)

    def test_fulltext_exact_feature_config(self):
        self._create_exact_feature_tables()
        self._wait_exact_indexes()
        for step, _ in enumerate(self.roll()):
            self._exercise_exact_feature_config(step)
        self._exercise_feature_gate_authority()
