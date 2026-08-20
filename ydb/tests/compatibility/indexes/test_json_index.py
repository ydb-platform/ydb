import logging
import pytest
import ydb as ydbs

from ydb.tests.library.fixtures import json
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestJsonIndex(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 3):
            pytest.skip("Only available since 26-3")

        self.row_count = 50
        self.query_count = 5
        self.limit = 5
        yield from self.setup_cluster(extra_feature_flags=["enable_json_index"])

    def create_table(self, table_name, json_type):
        logger.info(f"Creating table: {table_name}")

        query = f"""
            CREATE TABLE {table_name} (
                `pk` Uint64 NOT NULL,
                `json` {json_type} NOT NULL,
                PRIMARY KEY (`pk`)
            )
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _write_data(self, table_name, json_type):
        logger.info(f"Writing {self.row_count} rows to table: {table_name}")
        values = []
        for key in range(self.row_count):
            json_value = json.get_random_json()
            values.append(f'({key}, {json_type}(\'{json_value}\'))')

        sql_upsert = f"""
            UPSERT INTO `{table_name}` (`pk`, `json`)
            VALUES {",".join(values)};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(sql_upsert)

    def _create_index(self, table_name, index_name):
        logger.info(f"Creating index: {index_name} on table: {table_name}")
        create_index_sql = f"""
            ALTER TABLE `{table_name}`
            ADD INDEX `{index_name}` GLOBAL USING json
            ON (`json`);
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create_index_sql)

    def wait_index_ready(self, mode):
        logger.info("Waiting for index to be ready")

        def predicate():
            try:
                self.select_from_index_without_roll(mode=mode)
            except ydbs.issues.SchemeError as ex:
                if "Required global index not found, index name" in str(ex):
                    logger.debug("Index not yet ready, retrying...")
                    return False
                raise ex
            return True

        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Error getting index status"
        logger.info("Index is ready")

    def _get_queries(self, mode):
        queries = []
        for json_type in ['Json', 'JsonDocument']:
            table_name = f"table_{json_type.lower()}"
            index_name = f"idx_{json_type.lower()}"
            queries.extend(self._get_queries_for(table_name, index_name, json_type, mode))
        return queries

    def _get_queries_for(self, table_name, index_name, json_type, mode):
        queries = []
        for query_idx in range(self.query_count):
            predicate = json.get_random_predicate(mode=mode)
            logger.debug(f"Query {query_idx + 1}/{self.query_count} for {table_name} ({mode}): predicate=`{predicate}`")

            queries.append(f"""
                SELECT `pk`, `json`
                FROM `{table_name}`
                VIEW `{index_name}`
                WHERE {predicate}
                LIMIT {self.limit};
            """)

            key = self.row_count + 1

            queries.append(f"""
                INSERT INTO `{table_name}` (`pk`, `json`)
                VALUES ({key}, {json_type}('{json.get_random_json()}'))
            """)

            queries.append(f"""
                UPDATE `{table_name}` SET `json` = {json_type}('{json.get_random_json()}')
                WHERE pk = {key}
            """)

            queries.append(f"""
                UPSERT INTO `{table_name}` (`pk`, `json`)
                VALUES ({key}, {json_type}('{json.get_random_json()}'))
            """)

            queries.append(f"""
                DELETE FROM `{table_name}` WHERE pk = {key}
            """)
        return queries

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                session_pool.execute_with_retries(query)

    def _assert_legacy_indexes_match_primary(self, mode):
        """Keep an old-layout index honest while nodes are on mixed binaries."""
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for json_type in ['Json', 'JsonDocument']:
                table_name = f"table_{json_type.lower()}"
                index_name = f"idx_{json_type.lower()}"
                predicate = json.get_random_predicate(mode=mode)

                def select(view):
                    result = session_pool.execute_with_retries(f"""
                        SELECT `pk`
                        FROM `{table_name}` {view}
                        WHERE {predicate}
                        ORDER BY `pk`;
                    """)
                    return [row['pk'] for row in result[0].rows]

                assert select(f"VIEW `{index_name}`") == select("VIEW PRIMARY KEY")

    def select_from_index(self, mode):
        logger.info("Starting select_from_index with rolling upgrades")

        for roll_idx, _ in enumerate(self.roll(), 1):
            self._assert_legacy_indexes_match_primary(mode)
            queries = self._get_queries(mode=mode)
            logger.info(f"Generated {len(queries)} queries for roll step {roll_idx}")
            self._do_queries(queries)

        logger.info("Completed select_from_index with all rolling upgrades")

    def select_from_index_without_roll(self, mode):
        queries = self._get_queries(mode=mode)
        self._do_queries(queries)

    @pytest.mark.parametrize("mode", ["strict", "lax"])
    def test_json_index(self, mode):
        for json_type in ['Json', 'JsonDocument']:
            table_name = f"table_{json_type.lower()}"
            self.create_table(table_name, json_type)
            self._write_data(table_name, json_type)

            index_name = f"idx_{json_type.lower()}"
            self._create_index(
                table_name=table_name,
                index_name=index_name,
            )

        self.wait_index_ready(mode=mode)
        self.select_from_index(mode=mode)
        logger.info("Completed successfully")


class TestCompactJsonIndexWithRowId(RollingUpgradeAndDowngradeFixture):
    """Exact compact/row-id/auto-select configuration across rolling binaries."""

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        # All features used below first shipped together with JSON indexes.
        if min(self.versions) < (26, 3):
            pytest.skip("Compact JSON row-id and auto-select are available since 26-3")

        yield from self.setup_cluster(
            extra_feature_flags=[
                "enable_json_index",
                "enable_json_index_auto_select",
                "enable_compact_fulltext_index",
                "enable_fulltext_index_row_id",
                "enable_add_unique_index",
            ],
            table_service_config={"enable_index_stream_write": True},
        )

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            return session_pool.execute_with_retries(query)

    def _create_tables_and_indexes(self):
        self._execute("""
            CREATE TABLE `compact_json_string_pk` (
                `pk` Utf8 NOT NULL,
                `json` JsonDocument,
                PRIMARY KEY (`pk`)
            );
        """)
        self._execute("""
            CREATE TABLE `compact_json_composite_pk` (
                `tenant` Uint64 NOT NULL,
                `pk` Utf8 NOT NULL,
                `json` Json,
                PRIMARY KEY (`tenant`, `pk`)
            );
        """)
        self._execute("""
            UPSERT INTO `compact_json_string_pk` (`pk`, `json`) VALUES
                ("alpha"u, JsonDocument('{"group":"match","value":1}')),
                ("beta"u,  JsonDocument('{"group":"other","value":2}')),
                ("gamma"u, JsonDocument('{"group":"match","value":3}'));
        """)
        self._execute("""
            UPSERT INTO `compact_json_composite_pk` (`tenant`, `pk`, `json`) VALUES
                (1, "alpha"u, Json('{"group":"match","value":1}')),
                (1, "beta"u,  Json('{"group":"other","value":2}')),
                (2, "alpha"u, Json('{"group":"match","value":3}'));
        """)
        self._execute("""
            ALTER TABLE `compact_json_string_pk`
            ADD INDEX `json_idx` GLOBAL USING json ON (`json`);
        """)
        self._execute("""
            ALTER TABLE `compact_json_composite_pk`
            ADD INDEX `json_idx` GLOBAL USING json ON (`json`);
        """)

    @staticmethod
    def _key_rows(result_sets, key_columns):
        return [tuple(row[column] for column in key_columns) for row in result_sets[0].rows]

    def _select_keys(self, table_name, key_columns, view):
        columns = ", ".join(f"`{column}`" for column in key_columns)
        order = ", ".join(f"`{column}`" for column in key_columns)
        result = self._execute(f"""
            SELECT {columns}
            FROM `{table_name}` {view}
            WHERE JSON_VALUE(`json`, '$.group' RETURNING Utf8) == "match"u
            ORDER BY {order};
        """)
        return self._key_rows(result, key_columns)

    def _assert_index_and_auto_select_match_primary(self, table_name, key_columns):
        primary = self._select_keys(table_name, key_columns, "VIEW PRIMARY KEY")
        explicit = self._select_keys(table_name, key_columns, "VIEW `json_idx`")
        automatic = self._select_keys(table_name, key_columns, "")
        assert explicit == primary
        assert automatic == primary

    def _wait_indexes_ready(self):
        def predicate():
            try:
                self._assert_index_and_auto_select_match_primary(
                    "compact_json_string_pk", ("pk",))
                self._assert_index_and_auto_select_match_primary(
                    "compact_json_composite_pk", ("tenant", "pk"))
            except ydbs.issues.SchemeError as error:
                message = str(error)
                if "not ready to use" in message or "Required global index not found" in message:
                    return False
                raise
            return True

        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "JSON indexes did not become ready"

    def _exercise_string_pk_dml(self, roll_idx):
        key = f"roll-{roll_idx}"
        self._execute(f"""
            UPSERT INTO `compact_json_string_pk` (`pk`, `json`) VALUES
                ("{key}"u, JsonDocument('{{"group":"match","roll":{roll_idx}}}'));
        """)
        self._assert_index_and_auto_select_match_primary("compact_json_string_pk", ("pk",))
        self._execute(f"""
            UPDATE `compact_json_string_pk`
            SET `json` = JsonDocument('{{"group":"other","roll":{roll_idx}}}')
            WHERE `pk` = "{key}"u;
        """)
        self._assert_index_and_auto_select_match_primary("compact_json_string_pk", ("pk",))
        self._execute(f"""
            UPSERT INTO `compact_json_string_pk` (`pk`, `json`) VALUES
                ("{key}"u, JsonDocument('{{"group":"match","roll":{roll_idx}}}'));
            DELETE FROM `compact_json_string_pk` WHERE `pk` = "{key}"u;
        """)
        self._assert_index_and_auto_select_match_primary("compact_json_string_pk", ("pk",))

    def _exercise_composite_pk_dml(self, roll_idx):
        key = f"roll-{roll_idx}"
        self._execute(f"""
            UPSERT INTO `compact_json_composite_pk` (`tenant`, `pk`, `json`) VALUES
                (99, "{key}"u, Json('{{"group":"match","roll":{roll_idx}}}'));
        """)
        self._assert_index_and_auto_select_match_primary(
            "compact_json_composite_pk", ("tenant", "pk"))
        self._execute(f"""
            UPDATE `compact_json_composite_pk`
            SET `json` = Json('{{"group":"other","roll":{roll_idx}}}')
            WHERE `tenant` = 99 AND `pk` = "{key}"u;
        """)
        self._assert_index_and_auto_select_match_primary(
            "compact_json_composite_pk", ("tenant", "pk"))
        self._execute(f"""
            UPSERT INTO `compact_json_composite_pk` (`tenant`, `pk`, `json`) VALUES
                (99, "{key}"u, Json('{{"group":"match","roll":{roll_idx}}}'));
            DELETE FROM `compact_json_composite_pk`
            WHERE `tenant` = 99 AND `pk` = "{key}"u;
        """)
        self._assert_index_and_auto_select_match_primary(
            "compact_json_composite_pk", ("tenant", "pk"))

    def test_compact_json_row_id_across_rolls(self):
        # Both indexes are created by the initial (old) binary before the first
        # mixed-version step. TestJsonIndex above retains the legacy-layout case.
        self._create_tables_and_indexes()
        self._wait_indexes_ready()

        for roll_idx, _ in enumerate(self.roll(), 1):
            self._assert_index_and_auto_select_match_primary(
                "compact_json_string_pk", ("pk",))
            self._assert_index_and_auto_select_match_primary(
                "compact_json_composite_pk", ("tenant", "pk"))
            self._exercise_string_pk_dml(roll_idx)
            self._exercise_composite_pk_dml(roll_idx)
