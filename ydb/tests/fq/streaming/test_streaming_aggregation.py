import json
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal

import pytest
import ydb

from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.library.common.wait_for import wait_for

IN_MEMORY = "PRAGMA ydb.UseInMemoryStreamingAggregation = 'true';"
NO_VALIDATION = "PRAGMA ydb.OptValidateStreamingConstraints = 'false';"
MULTIPLE_AGGREGATES = "SELECT key, subkey, SUM(value) AS value, MIN(value) AS other FROM $input GROUP BY key, subkey"
STATE_ERROR = "requires an output state table"
FINALIZER_ERROR = "require an identity finalizer or a finalizer equal to serialization"


@dataclass(frozen=True)
class ValidationCase:
    name: str
    select: str = "SELECT * FROM $agg"
    primary_key: str = "key, subkey"
    aggregation: str = "SELECT key, subkey, SUM(value) AS value FROM $input GROUP BY key, subkey"
    columns: str = "value Int64, other Int64"
    prelude: str = ""
    input_filter: str = ""
    input_limit: int | None = None
    extra_write: str = ""
    error: str = ""
    error_type: type = ydb.issues.GenericError
    state_table: str | None = "first"
    lookup: bool = False


VALIDATION_CASES = [
    ValidationCase("composite_keys_and_all_aggregates", aggregation=MULTIPLE_AGGREGATES),
    ValidationCase(
        "renames_and_optionality",
        select="SELECT subkey AS key, key AS subkey, Just(value) AS value FROM $agg",
        primary_key="subkey, key",
    ),
    ValidationCase("filter_before", input_filter="WHERE value > 0"),
    ValidationCase(
        "row_value_filter",
        select="SELECT key, subkey, CAST(ListLength(ListFilter(AsList(value, -value), ($v) -> ($v > 0))) AS Int64) AS value FROM $agg",
        prelude=IN_MEMORY,
        state_table=None,
    ),
    ValidationCase(
        "reordered_primary_key",
        select="SELECT key, subkey, value + 1 AS value FROM $agg",
        primary_key="subkey, key",
        prelude=IN_MEMORY,
        state_table=None,
    ),
    ValidationCase(
        "second_state_table",
        select="SELECT key, subkey, value + 1 AS value FROM $agg",
        extra_write="UPSERT INTO `{second}` SELECT * FROM $agg;",
        state_table="second",
    ),
    ValidationCase("two_eligible_tables", extra_write="UPSERT INTO `{second}` SELECT * FROM $agg;"),
    ValidationCase(
        "modified_second_consumer",
        extra_write="UPSERT INTO `{second}` SELECT key, subkey, value + 1 AS value FROM $agg;",
        prelude=IN_MEMORY,
        state_table=None,
    ),
    ValidationCase(
        "raw_second_consumer",
        extra_write="UPSERT INTO `{second}` SELECT * FROM $input WHERE value > 0;",
        prelude=IN_MEMORY,
        state_table=None,
    ),
    ValidationCase(
        "left_lookup",
        select="SELECT a.key AS key, a.subkey AS subkey, a.value AS value, r.value AS other "
        "FROM $agg AS a LEFT JOIN /*+ streamlookup(TTL 1) */ ANY `{lookup}` AS r "
        "ON a.key = r.key AND a.subkey = r.subkey",
        lookup=True,
    ),
    ValidationCase(
        "left_lookup_modified_value",
        select="SELECT a.key AS key, a.subkey AS subkey, a.value + Coalesce(r.value, 0l) AS value "
        "FROM $agg AS a LEFT JOIN /*+ streamlookup(TTL 1) */ ANY `{lookup}` AS r "
        "ON a.key = r.key AND a.subkey = r.subkey",
        lookup=True,
        prelude=IN_MEMORY,
        state_table=None,
    ),
    ValidationCase(
        "filter_after",
        select="SELECT * FROM $agg WHERE value > 0",
        error="Filtering over streaming aggregation results is not supported",
    ),
    ValidationCase(
        "limit_after",
        select="SELECT * FROM $agg LIMIT 1",
        prelude="PRAGMA ydb.DisableCheckpoints = 'true';",
        error="LIMIT operator is not supported over streaming aggregation results",
    ),
    ValidationCase(
        "dropped_key",
        select="SELECT key, value FROM $agg",
        primary_key="key",
        error="Please consume all aggregation keys and write into table",
    ),
    ValidationCase("primary_key_subset", primary_key="key", error="must exactly match the primary key"),
    ValidationCase(
        "primary_key_superset", primary_key="key, subkey, value", error="must exactly match the primary key"
    ),
    ValidationCase(
        "filtered_second_consumer",
        extra_write="UPSERT INTO `{second}` SELECT * FROM $agg WHERE value > 0;",
        error="Flattening streaming aggregation results is not supported",
    ),
    ValidationCase(
        "union_with_raw_input",
        select="SELECT * FROM $agg UNION ALL SELECT * FROM $input",
        error="Union of streaming aggregation results with another data is not supported",
    ),
    ValidationCase(
        "split_aggregates",
        select="SELECT key, subkey, value FROM $agg",
        aggregation=MULTIPLE_AGGREGATES,
        extra_write="UPSERT INTO `{second}` SELECT key, subkey, other FROM $agg;",
        error=STATE_ERROR,
    ),
    ValidationCase(
        "modified_one_of_two_aggregates",
        select="SELECT key, subkey, value, other + 1 AS other FROM $agg",
        aggregation=MULTIPLE_AGGREGATES,
        error=STATE_ERROR,
    ),
    ValidationCase(
        "conflicting_write",
        extra_write="UPSERT INTO `{first}` SELECT 'x' AS key, 'y' AS subkey, value FROM $input;",
        error="queries with intermediate writes",
        error_type=ydb.issues.Unsupported,
    ),
    ValidationCase(
        "inner_lookup",
        select="SELECT a.key AS key, a.subkey AS subkey, a.value AS value FROM $agg AS a INNER JOIN /*+ streamlookup(TTL 1) */ ANY `{lookup}` AS r ON a.key = r.key AND a.subkey = r.subkey",
        lookup=True,
        error="Streamlookup supports only LEFT JOIN",
    ),
]


class TestStreamingAggregation(StreamingTestBase):
    @contextmanager
    def running_query(self, kikimr, query_name, body, wait_checkpoint=True):
        kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{query_name}` AS DO BEGIN {body} END DO;")
        try:
            ast = self.query_ast(kikimr, query_name)
            if wait_checkpoint:
                self.wait_completed_checkpoints(kikimr, query_name)
            yield ast
        finally:
            kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")

    def query_ast(self, kikimr, query_name):
        path = f"{kikimr.get_database_name()}/{query_name}"
        ast = ""

        def ready():
            nonlocal ast
            rows = kikimr.ydb_client.query(f"SELECT Ast FROM `.sys/streaming_queries` WHERE Path = '{path}';")[0].rows
            ast = rows[0]["Ast"] if rows else ""
            return bool(ast)

        assert wait_for(ready, timeout_seconds=120, step_seconds=0.2), f"No AST for {path}"
        assert "KqpStreamingAggregation" in ast, ast
        return ast

    def check_rows(self, kikimr, query, expected):
        actual = None

        def matches():
            nonlocal actual
            result = kikimr.ydb_client.query(query)[0]
            actual = [tuple(v.decode() if isinstance(v, bytes) else v for v in row[:]) for row in result.rows]
            return actual == expected

        assert wait_for(
            matches, timeout_seconds=120, step_seconds=0.2
        ), f"{query}\nExpected: {expected}\nActual: {actual}"

    def setup_validation(self, kikimr, entity_name, case):
        source, endpoint = self.get_input_name(kikimr, "aggregation", False, entity_name)
        names = {name: entity_name(name) for name in ("first", "second", "lookup", "query")}
        for name in ("first", "second"):
            kikimr.ydb_client.query(
                f"CREATE TABLE `{names[name]}` (key String, subkey String, {case.columns}, PRIMARY KEY ({case.primary_key}));"
            )
        if case.lookup:
            kikimr.ydb_client.query(
                f"CREATE TABLE `{names['lookup']}` (key String, subkey String, value Int64, PRIMARY KEY (key, subkey));"
            )
            kikimr.ydb_client.query(f"UPSERT INTO `{names['lookup']}` (key, subkey, value) VALUES ('a', 'b', 11);")
        body = f"""
            {case.prelude}
            $input = SELECT * FROM {source} WITH (
                FORMAT = 'json_each_row', SCHEMA (key String NOT NULL, subkey String NOT NULL, value Int64 NOT NULL)
            ) {case.input_filter} {f'LIMIT {case.input_limit}' if case.input_limit is not None else ''};
            $agg = {case.aggregation};
            UPSERT INTO `{names['first']}` {case.select.format(**names)};
            {case.extra_write.format(**names)}
        """
        return names, endpoint, body

    @pytest.mark.parametrize(
        "kikimr,enabled",
        [({"enable_streaming_aggregation": enabled}, enabled) for enabled in (False, True)],
        indirect=["kikimr"],
        ids=["disabled", "enabled"],
    )
    @pytest.mark.parametrize("keyed", [False, True], ids=["keyless", "keyed"])
    def test_finite_input_is_not_streaming(self, kikimr, enabled, keyed):
        query = f"""
            PRAGMA EmitAggApply;
            PRAGMA ydb.StreamingAggregationStateTablePath = '/Root/nonexistent';
            $input = AsList(AsStruct('a' AS key, 2l AS value), AsStruct('a' AS key, 3l AS value));
            SELECT Unwrap(CAST(SUM(value) AS String)) AS Data FROM AS_TABLE($input) {'GROUP BY key' if keyed else ''};
        """
        with kikimr.ydb_client.session_pool.checkout() as session:
            results = list(session.execute(query, stats_mode=ydb.QueryStatsMode.FULL))
            assert [row["Data"] for result in results if result is not None for row in result.rows] == [b"5"]
            ast = session.last_query_stats.query_ast
            assert ast and "KqpStreamingAggregation" not in ast, (enabled, ast)

    @pytest.mark.parametrize("local_topics", [False, True], ids=["external_topic", "local_topic"])
    def test_finite_streaming_topic_uses_in_memory_by_default(self, kikimr, entity_name, local_topics):
        source, endpoint = self.get_input_name(kikimr, "finite_aggregation", local_topics, entity_name)
        table = entity_name("result")
        kikimr.ydb_client.query(f"CREATE TABLE `{table}` (key String, value Int64, PRIMARY KEY (key));")
        kikimr.ydb_client.query(f"UPSERT INTO `{table}` (key, value) VALUES ('a', 100), ('b', 200);")
        query = f"""
            $input = SELECT * FROM {source} WITH (
                STREAMING = 'TRUE', FORMAT = 'json_each_row', SCHEMA (key String NOT NULL, value Int64 NOT NULL)
            ) LIMIT 4;
            UPSERT INTO `{table}` SELECT key, SUM(value) AS value FROM $input GROUP BY key;
        """
        with kikimr.ydb_client.session_pool.checkout() as session:
            session.explain(query)
            ast = session.last_query_stats.query_ast
            assert ast and "KqpStreamingAggregation" in ast, ast
            assert "output_state_table" not in ast, ast
            assert "state_table_path" not in ast, ast

        future = kikimr.ydb_client.query_async(query, timeout=120)
        time.sleep(1)
        self.write_stream(
            [json.dumps(dict(key=key, value=value)) for key, value in [("a", 2), ("b", 10), ("a", 3), ("b", -3), ("a", 1000)]],
            endpoint=endpoint,
        )
        future.result(timeout=120)
        self.check_rows(kikimr, f"SELECT key, value FROM `{table}` ORDER BY key;", [("a", 5), ("b", 7)])

    @pytest.mark.parametrize(
        "kikimr,enabled",
        [({"enable_streaming_aggregation": enabled}, enabled) for enabled in (False, True)],
        indirect=["kikimr"],
        ids=["disabled", "enabled"],
    )
    @pytest.mark.parametrize("keyed", [False, True], ids=["keyless", "keyed"])
    def test_feature_flag_controls_rewrite(self, kikimr, entity_name, enabled, keyed):
        source, output, endpoint = self.get_io_names(kikimr, "rewrite", False, entity_name)
        query = entity_name("query")
        body = f"""
            PRAGMA EmitAggApply;
            {IN_MEMORY}
            {NO_VALIDATION if enabled else ''}
            INSERT INTO {output} SELECT Unwrap(CAST(SUM(value) AS String)) AS Data
            FROM {source} WITH (FORMAT = 'json_each_row', SCHEMA (key String NOT NULL, value Int64 NOT NULL))
            {'GROUP BY key' if keyed else ''};
        """
        if not enabled:
            with pytest.raises(
                ydb.issues.GenericError, match="Aggregation of streaming input without windows is not supported"
            ):
                kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{query}` AS DO BEGIN {body} END DO;")
            return
        with self.running_query(kikimr, query, body):
            self.write_stream([json.dumps(dict(key="a", value=v)) for v in (2, 3)], endpoint=endpoint)
            assert self.read_stream(2, endpoint=endpoint) == ["2", "5"]

    @pytest.mark.parametrize(
        "kikimr,advanced",
        [({"enable_streaming_aggregation_advanced": enabled}, enabled) for enabled in (False, True)],
        indirect=["kikimr"],
        ids=["disabled", "enabled"],
    )
    @pytest.mark.parametrize("use_state_table", [False, True], ids=["memory", "explicit_state"])
    def test_explicit_state_table_requires_advanced_flag(self, kikimr, entity_name, advanced, use_state_table):
        source, output, endpoint = self.get_io_names(kikimr, "advanced_aggregation", False, entity_name)
        query, table = entity_name("query"), entity_name("state")
        state_path = f"{kikimr.get_database_name()}/{table}" if use_state_table else ""
        if use_state_table:
            kikimr.ydb_client.query(f"CREATE TABLE `{table}` (key String NOT NULL, state String, PRIMARY KEY (key));")
        # No in-memory pragma: an explicit state table must work independently of that setting.
        body = f"""
            PRAGMA ydb.DisableCheckpoints = 'true';
            PRAGMA ydb.MaxTasksPerStage = '1';
            PRAGMA ydb.StreamingAggregationStateTablePath = '{state_path}';
            {NO_VALIDATION}
            INSERT INTO {output} SELECT Unwrap(key || ':' || CAST(SUM(value) AS String)) AS Data
            FROM {source} WITH (FORMAT = 'json_each_row', SCHEMA (key String NOT NULL, value Int64 NOT NULL)) GROUP BY key;
        """
        if use_state_table and not advanced:
            with pytest.raises(
                ydb.issues.GenericError,
                match="Streaming aggregation with a state table requires EnableStreamingAggregationAdvanced",
            ):
                kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{query}` AS DO BEGIN {body} END DO;")
            return
        with self.running_query(kikimr, query, body, wait_checkpoint=False):
            self.write_stream(
                [json.dumps(dict(key=k, value=v)) for k, v in [("a", 1), ("b", 2), ("a", 3)]], endpoint=endpoint
            )
            assert self.read_stream(3, endpoint=endpoint) == ["a:1", "b:2", "a:4"]
            if use_state_table:
                rows = kikimr.ydb_client.query(f"SELECT state FROM `{table}`;")[0].rows
                assert rows and all(row["state"] is not None for row in rows)

    @pytest.mark.parametrize("case", VALIDATION_CASES, ids=lambda case: case.name)
    def test_validation(self, kikimr, entity_name, case):
        names, endpoint, body = self.setup_validation(kikimr, entity_name, case)
        if case.error:
            with pytest.raises(case.error_type, match=case.error):
                kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{names['query']}` AS DO BEGIN {body} END DO;")
            return

        with self.running_query(kikimr, names["query"], body) as ast:
            assert ("output_state_table" in ast) == (case.state_table is not None), ast
            if case.state_table:
                # Table selection is not visible in the final rows when both sinks are eligible.
                # The AST printer can factor the path atom into a numbered let binding.
                atoms = dict(re.findall(r"""\(let (\$\d+) '"([^"\n]+)"\)""", ast))
                refs = re.findall(r""""output_state_table"\s+'\((\$\d+|'"[^"\n]+")""", ast)
                paths = [atoms[ref] if ref.startswith("$") else ref[2:-1] for ref in refs]
                assert paths == [f"{kikimr.get_database_name()}/{names[case.state_table]}"], ast
            if case.lookup:
                assert "DqCnStreamLookup" in ast, ast
            for batch, values, minima in [
                ([("a", "b", 2), ("a", "b", 3), ("c", "d", 7)], [5, 7], [2, 7]),
                ([("a", "b", 4), ("c", "d", -2)], [9, 5], [2, -2]),
            ]:
                self.write_stream([json.dumps(dict(key=k, subkey=s, value=v)) for k, s, v in batch], endpoint=endpoint)
                expected = []
                for i, (key, subkey) in enumerate([("a", "b"), ("c", "d")]):
                    value = values[i]
                    other = minima[i] if case.aggregation == MULTIPLE_AGGREGATES else None
                    if case.name == "renames_and_optionality":
                        key, subkey = subkey, key
                    if case.name == "filter_before" and i == 1:
                        value = 7
                    if case.name in ("reordered_primary_key", "second_state_table"):
                        value += 1
                    if case.name == "row_value_filter":
                        value = 1
                    if case.name == "left_lookup":
                        other = 11 if i == 0 else None
                    if case.name == "left_lookup_modified_value" and i == 0:
                        value += 11
                    expected.append((key, subkey, value, other))
                self.check_rows(
                    kikimr, f"SELECT key, subkey, value, other FROM `{names['first']}` ORDER BY key, subkey;", expected
                )
                if case.extra_write:
                    second_values = [v + 1 for v in values] if case.name == "modified_second_consumer" else values
                    if case.name == "raw_second_consumer":
                        second_values = [batch[-2][2], 7]
                    self.check_rows(
                        kikimr,
                        f"SELECT key, subkey, value FROM `{names['second']}` ORDER BY key, subkey;",
                        [("a", "b", second_values[0]), ("c", "d", second_values[1])],
                    )

    @pytest.mark.parametrize("pragma", [None, False, True], ids=["default", "false", "true"])
    @pytest.mark.parametrize("validation", [True, False], ids=["validate", "no_validation"])
    @pytest.mark.parametrize("checkpoints", [True, False], ids=["checkpoints", "no_checkpoints"])
    def test_in_memory_pragma(self, kikimr, entity_name, pragma, validation, checkpoints):
        prelude = "" if pragma is None else f"PRAGMA ydb.UseInMemoryStreamingAggregation = '{str(pragma).lower()}';"
        if not validation:
            prelude += NO_VALIDATION
        if not checkpoints:
            prelude += "PRAGMA ydb.DisableCheckpoints = 'true';"
        case = ValidationCase(
            "pragma",
            select="SELECT key, subkey, value + 1 AS value FROM $agg",
            prelude=prelude,
            # Without checkpoints, finish the input to flush the table sink's small writes.
            input_limit=None if checkpoints else 2,
            error=STATE_ERROR if checkpoints and validation and pragma is not True else "",
            state_table=None,
        )
        names, endpoint, body = self.setup_validation(kikimr, entity_name, case)
        if case.error:
            with pytest.raises(ydb.issues.GenericError, match=STATE_ERROR):
                kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{names['query']}` AS DO BEGIN {body} END DO;")
            return
        kikimr.ydb_client.query(f"UPSERT INTO `{names['first']}` (key, subkey, value) VALUES ('a', 'b', 100);")
        with self.running_query(kikimr, names["query"], body, wait_checkpoint=checkpoints) as ast:
            assert "output_state_table" not in ast
            assert "state_table_path" not in ast
            batches = [([2], 3), ([3], 6)] if checkpoints else [([2, 3], 6)]
            for values, expected in batches:
                self.write_stream([json.dumps(dict(key="a", subkey="b", value=value)) for value in values], endpoint=endpoint)
                self.check_rows(kikimr, f"SELECT value FROM `{names['first']}`;", [(expected,)])

    @pytest.mark.parametrize("modify_first", [False, True])
    def test_filtered_consumer_with_validation_disabled(self, kikimr, entity_name, modify_first):
        case = ValidationCase(
            "filtered",
            prelude=NO_VALIDATION,
            select="SELECT key, subkey, value + 1 AS value FROM $agg" if modify_first else "SELECT * FROM $agg",
            extra_write="UPSERT INTO `{second}` SELECT * FROM $agg WHERE value > 0;",
            state_table=None,
        )
        names, endpoint, body = self.setup_validation(kikimr, entity_name, case)
        with self.running_query(kikimr, names["query"], body) as ast:
            assert "output_state_table" not in ast
            for value, total in [(2, 2), (3, 5)]:
                self.write_stream([json.dumps(dict(key="a", subkey="b", value=value))], endpoint=endpoint)
                for table, expected in [("first", total + int(modify_first)), ("second", total)]:
                    self.check_rows(kikimr, f"SELECT value FROM `{names[table]}`;", [(expected,)])

    @pytest.mark.parametrize("kind", ["average", "matching_udaf", "mismatched_udaf"])
    def test_finalizers(self, kikimr, entity_name, kind):
        prelude = (
            ""
            if kind == "average"
            else f"""
            $init = ($item) -> ($item);
            $update = ($state, $item) -> ($state + $item);
            $finish = ($state) -> ($state + 1l);
            $save = ($saved) -> ($saved + {1 if kind == 'matching_udaf' else 2}l);
            $load = ($saved) -> ($saved - 1l);
            $merge = ($left, $right) -> ($left + $right);
            $factory = AggregationFactory('UDAF', $init, $update, $merge, $finish, $save, $load);
        """
        )
        aggregate = "AVG(value)" if kind == "average" else "AGGREGATE_BY(value, $factory)"
        case = ValidationCase(
            kind,
            columns="value Double" if kind == "average" else "value Int64",
            prelude=prelude,
            aggregation=f"SELECT key, subkey, {aggregate} AS value FROM $input GROUP BY key, subkey",
            error="" if kind == "matching_udaf" else FINALIZER_ERROR,
        )
        names, endpoint, body = self.setup_validation(kikimr, entity_name, case)
        if case.error:
            with pytest.raises(ydb.issues.GenericError, match=FINALIZER_ERROR):
                kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{names['query']}` AS DO BEGIN {body} END DO;")
            return
        with self.running_query(kikimr, names["query"], body):
            for value, total in [(2, 3), (3, 6)]:
                self.write_stream([json.dumps(dict(key="a", subkey="b", value=value))], endpoint=endpoint)
                self.check_rows(kikimr, f"SELECT value FROM `{names['first']}`;", [(total,)])

    @pytest.mark.parametrize("left", [False, True], ids=["inner", "left"])
    def test_map_join_rejected(self, kikimr, entity_name, left):
        case = ValidationCase(
            "map_join",
            prelude=IN_MEMORY + "PRAGMA ydb.HashJoinMode = 'map';",
            select=f"""SELECT a.key AS key, a.subkey AS subkey, a.value + Coalesce(r.value, 0l) AS value
                                  FROM $agg AS a {'LEFT' if left else 'INNER'} JOIN ANY
                                  AS_TABLE(AsList(AsStruct('a' AS key, 'b' AS subkey, 1l AS value),
                                                  AsStruct('c' AS key, 'd' AS subkey, 2l AS value))) AS r
                                  ON a.key = r.key AND a.subkey = r.subkey""",
            error="distinct constraint for aggregation key was lost" if left else "LEFT ANY",
        )
        self.test_validation(kikimr, entity_name, case)

    @pytest.mark.parametrize("mode", ["UPSERT", "REPLACE", "INSERT"])
    def test_sink_modes(self, kikimr, entity_name, mode):
        # Streaming-query DDL rejects these modes before the physical aggregation validator.
        # Keep the provider unit matrix to exercise that validator's distinct contract.
        case = ValidationCase(
            "sink_mode",
            error=(
                f"Only UPSERT writing mode is supported for YDB writes inside streaming queries, got mode: {'INSERT_ABORT' if mode == 'INSERT' else mode}"
                if mode != "UPSERT"
                else ""
            ),
        )
        names, endpoint, body = self.setup_validation(kikimr, entity_name, case)
        body = body.replace(f"UPSERT INTO `{names['first']}`", f"{mode} INTO `{names['first']}`")
        if case.error:
            with pytest.raises(ydb.issues.GenericError, match=case.error):
                kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{names['query']}` AS DO BEGIN {body} END DO;")
            return
        with self.running_query(kikimr, names["query"], body):
            for value, total in [(2, 2), (3, 5)]:
                self.write_stream([json.dumps(dict(key="a", subkey="b", value=value))], endpoint=endpoint)
                self.check_rows(kikimr, f"SELECT value FROM `{names['first']}`;", [(total,)])

    @pytest.mark.parametrize("destination", ["result", "topic"])
    def test_non_table_output_rejected(self, kikimr, entity_name, destination):
        source, output, _ = self.get_io_names(kikimr, "aggregation_output", False, entity_name)
        select = f"SELECT key, SUM(value) AS value FROM {source} WITH (STREAMING = 'TRUE', FORMAT = 'json_each_row', SCHEMA (key String NOT NULL, value Int64 NOT NULL)) GROUP BY key"
        sql = (
            select
            if destination == "result"
            else f"CREATE STREAMING QUERY `{entity_name('query')}` AS DO BEGIN INSERT INTO {output} SELECT key AS Data FROM ({select}); END DO;"
        )
        with pytest.raises(ydb.issues.GenericError, match="Streaming aggregation output must be written to a table"):
            kikimr.ydb_client.query(sql)

    # The read actor's TRACE cell formatter does not support parameterized Decimal types.
    # Remove this workaround after https://github.com/ydb-platform/ydb/issues/54964 is fixed.
    # Keep DEBUG for KQP_COMPUTE and preserve the default TRACE level for other components.
    @pytest.mark.parametrize("kikimr", [{"log_levels": {"KQP_COMPUTE": 7}}], indirect=True)
    @pytest.mark.parametrize("nullable", [False, True])
    def test_standard_aggregates_use_defaults(self, kikimr, entity_name, nullable):
        source, endpoint = self.get_input_name(kikimr, "standard_aggregation", False, entity_name)
        table, query = entity_name("result"), entity_name("query")
        fields = {
            "count_all": ("Uint64", "COUNT(*)"),
            "count_value": ("Uint64", "COUNT(value)"),
            "count_if": ("Uint64", "COUNT_IF(flag)"),
            "sum_value": ("Int64", "SUM(value)"),
            "sum_if": ("Int64", "SUM_IF(value, flag)"),
            "min_value": ("Int32", "MIN(value)"),
            "max_value": ("Int32", "MAX(value)"),
            "some_value": ("Int32", "SOME(value)"),
            "bool_and": ("Bool", "BOOL_AND(flag)"),
            "bool_or": ("Bool", "BOOL_OR(flag)"),
            "bool_xor": ("Bool", "BOOL_XOR(flag)"),
            "bit_and": ("Uint32", "BIT_AND(CAST(value AS Uint32))"),
            "bit_or": ("Uint32", "BIT_OR(CAST(value AS Uint32))"),
            "bit_xor": ("Uint32", "BIT_XOR(CAST(value AS Uint32))"),
            "sum_unsigned": ("Uint64", "SUM(CAST(value AS Uint32))"),
            "sum_real": ("Double", "SUM(CAST(value AS Double) / 2.0)"),
            "sum_decimal": ("Decimal(35, 2)", "SUM(CAST(value AS Decimal(22, 2)))"),
        }
        kikimr.ydb_client.query(
            f"CREATE TABLE `{table}` (key String NOT NULL, {', '.join(f'{n} {t}' for n, (t, _) in fields.items())}, PRIMARY KEY (key));"
        )
        body = f"""UPSERT INTO `{table}` SELECT key, {', '.join(f'{expr} AS {n}' for n, (_, expr) in fields.items())}
                   FROM {source} WITH (FORMAT = 'json_each_row', SCHEMA (
                       key String NOT NULL, value Int32 {'' if nullable else 'NOT NULL'}, flag Bool {'' if nullable else 'NOT NULL'}
                   )) GROUP BY key;"""
        with self.running_query(kikimr, query, body) as ast:
            assert "output_state_table" in ast and "state_table_path" not in ast
            initial = [("a", None, None), ("c", None, None)] if nullable else []
            initial += [("a", 6, True), ("a", 3, False), ("b", 4, True)]
            expected = [
                (
                    "a",
                    3 if nullable else 2,
                    2,
                    1,
                    9,
                    6,
                    3,
                    6,
                    False,
                    True,
                    None if nullable else True,
                    2,
                    7,
                    5,
                    9,
                    4.5,
                    Decimal(9),
                ),
                ("b", 1, 1, 1, 4, 4, 4, 4, True, True, True, 4, 4, 4, 4, 2.0, Decimal(4)),
            ]
            if nullable:
                expected.append(("c", 1, 0, 0, *([None] * 13)))
            columns = [n for n in fields if n != "some_value"]
            for batch, wanted, some in [
                (initial, expected, {"a": [6, 3], "b": [4], "c": []}),
                (
                    [("a", 5, True), ("b", 2, False)] + ([("c", 7, True)] if nullable else []),
                    [
                        (
                            "a",
                            4 if nullable else 3,
                            3,
                            2,
                            14,
                            11,
                            3,
                            6,
                            False,
                            True,
                            None if nullable else False,
                            0,
                            7,
                            0,
                            14,
                            7.0,
                            Decimal(14),
                        ),
                        ("b", 2, 2, 1, 6, 4, 2, 4, False, True, True, 0, 6, 6, 6, 3.0, Decimal(6)),
                    ]
                    + ([("c", 2, 1, 1, 7, 7, 7, 7, None, True, None, 7, 7, 7, 7, 3.5, Decimal(7))] if nullable else []),
                    {"a": [6, 3, 5], "b": [4, 2], "c": [7]},
                ),
            ]:
                self.write_stream([json.dumps(dict(key=k, value=v, flag=f)) for k, v, f in batch], endpoint=endpoint)
                self.check_rows(kikimr, f"SELECT key, {', '.join(columns)} FROM `{table}` ORDER BY key;", wanted)
                for row in kikimr.ydb_client.query(f"SELECT key, some_value FROM `{table}`;")[0].rows:
                    choices = some[row["key"].decode()]
                    assert row["some_value"] in choices if choices else row["some_value"] is None
                # Ensure the next batch exercises merging with the durable table baseline.
                self.wait_completed_checkpoints(kikimr, query)

    @pytest.mark.parametrize("local_topic", [False, True], ids=["external_topic", "local_topic"])
    @pytest.mark.parametrize("manual_restart", [False, True], ids=["node_restart", "manual_restart"])
    def test_checkpoint_batches_and_idle_recovery(self, kikimr, entity_name, local_topic, manual_restart):
        source, endpoint = self.get_input_name(kikimr, "batches", local_topic, entity_name)
        table, query = entity_name("result"), entity_name("query")
        kikimr.ydb_client.query(
            f"CREATE TABLE `{table}` (key String NOT NULL, count Uint64, total Int64, PRIMARY KEY (key));"
        )
        body = f"""
            UPSERT INTO `{table}` SELECT key, COUNT(*) AS count, SUM(value) AS total
            FROM {source} WITH (FORMAT = 'json_each_row', SCHEMA (key String NOT NULL, value Int64 NOT NULL))
            GROUP BY key;
        """
        keys = [f"key{i:03}" for i in range(64)]
        result = f"SELECT key, count, total FROM `{table}` ORDER BY key;"
        with self.running_query(kikimr, query, body) as ast:
            assert "output_state_table" in ast
            self.write_stream(
                [json.dumps(dict(key=key, value=value)) for value in [1, 2, 3] for key in keys], endpoint=endpoint
            )
            self.check_rows(kikimr, result, [(key, 3, 6) for key in keys])
            self.wait_completed_checkpoints(kikimr, query)
            if manual_restart:
                kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{query}` SET (RUN = FALSE);")
                self.check_rows(
                    kikimr,
                    f"SELECT Status FROM `.sys/streaming_queries` WHERE Path = '{kikimr.get_database_name()}/{query}';",
                    [("STOPPED",)],
                )
                kikimr.ydb_client.query(f"ALTER STREAMING QUERY `{query}` SET (RUN = TRUE);")
            else:
                self.restart_streaming_node(kikimr)
            # Completed-checkpoint restoration must progress without another input row.
            self.wait_completed_checkpoints(kikimr, query)
            self.check_rows(kikimr, result, [(key, 3, 6) for key in keys])
            self.write_stream([json.dumps(dict(key=key, value=5)) for key in keys], endpoint=endpoint)
            self.check_rows(kikimr, result, [(key, 4, 11) for key in keys])

    @pytest.mark.parametrize("kikimr", [{"enable_streaming_query_state_recompute": True}], indirect=True)
    def test_alter_text_preserves_output_state(self, kikimr, entity_name):
        names, endpoint, body = self.setup_validation(kikimr, entity_name, ValidationCase("alter"))
        query, table = names["query"], names["first"]
        body = """
            PRAGMA ydb.MaxTasksPerStage = '1';
            PRAGMA ydb.OverridePlanner = @@ [
                {"tx": 0, "stage": 0, "tasks": 1},
                {"tx": 0, "stage": 1, "tasks": 1}
            ] @@;
        """ + body
        rows = f"SELECT key, subkey, value FROM `{table}` ORDER BY key, subkey;"
        with self.running_query(kikimr, query, body):
            self.write_stream([json.dumps(dict(key="a", subkey="b", value=v)) for v in (2, 3)], endpoint=endpoint)
            self.check_rows(kikimr, rows, [("a", "b", 5)])
            self.wait_completed_checkpoints(kikimr, query)
            updated = body.replace("GROUP BY key, subkey", "WHERE value > 0 GROUP BY key, subkey")
            kikimr.ydb_client.query(
                f"ALTER STREAMING QUERY `{query}` SET (FORCE = FALSE) AS DO BEGIN {updated} END DO;"
            )
            self.write_stream([json.dumps(dict(key="a", subkey="b", value=v)) for v in (4, -100)], endpoint=endpoint)
            self.check_rows(kikimr, rows, [("a", "b", 9)])
            self.wait_completed_checkpoints(kikimr, query)
            self.restart_streaming_node(kikimr)
            self.wait_completed_checkpoints(kikimr, query)
            self.write_stream([json.dumps(dict(key="a", subkey="b", value=1))], endpoint=endpoint)
            self.check_rows(kikimr, rows, [("a", "b", 10)])

    @pytest.mark.parametrize("valid_load", [False, True], ids=["invalid_load", "checkpoint_recovery"])
    def test_udaf_serialization_use_defaults(self, kikimr, entity_name, valid_load):
        source, endpoint = self.get_input_name(kikimr, "udaf", False, entity_name)
        table, query = entity_name("result"), entity_name("query")
        kikimr.ydb_client.query(
            f"CREATE TABLE `{table}` (key String NOT NULL, raw_count Int64, serialized_value String, PRIMARY KEY (key));"
        )
        load = "Unwrap(CAST($saved AS Int64))" if valid_load else "$saved"
        body = f"""
            $init = ($item) -> ($item);
            $update = ($state, $item) -> ($state + $item);
            $save = ($state) -> (CAST($state AS String));
            $load = ($saved) -> ({load});
            $merge = ($left, $right) -> ($left + $right);
            $raw = AggregationFactory('UDAF', ($item) -> (1l), ($state, $item) -> ($state + 1l), $merge, ($state) -> ($state), $save, $load);
            $serialized = AggregationFactory('UDAF', $init, $update, $merge, ($state) -> (CAST($state AS String)), $save, $load);
            UPSERT INTO `{table}` SELECT key, AGGREGATE_BY(value, $raw) AS raw_count, AGGREGATE_BY(value, $serialized) AS serialized_value
            FROM {source} WITH (FORMAT = 'json_each_row', SCHEMA (key String NOT NULL, value Int64 NOT NULL)) GROUP BY key;
        """
        if not valid_load:
            with pytest.raises(ydb.issues.GenericError, match="Mismatch state type after load"):
                kikimr.ydb_client.query(f"CREATE STREAMING QUERY `{query}` AS DO BEGIN {body} END DO;")
            return
        with self.running_query(kikimr, query, body) as ast:
            assert "output_state_table" in ast
            self.write_stream([json.dumps(dict(key="a", value=v)) for v in [6, 3]], endpoint=endpoint)
            self.check_rows(kikimr, f"SELECT raw_count, serialized_value FROM `{table}`;", [(2, "9")])
            self.wait_completed_checkpoints(kikimr, query)
            self.restart_streaming_node(kikimr)
            self.wait_completed_checkpoints(kikimr, query)
            self.write_stream([json.dumps(dict(key="a", value=5))], endpoint=endpoint)
            self.check_rows(
                kikimr,
                f"SELECT raw_count, serialized_value, Unwrap(CAST(serialized_value AS Int64)) + 7l AS next_state FROM `{table}`;",
                [(3, "14", 21)],
            )
