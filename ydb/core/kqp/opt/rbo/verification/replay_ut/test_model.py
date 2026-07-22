import base64
import copy
import hashlib
import io
import json
import subprocess
import tempfile
import unittest

from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import parse_snapshot
from ydb.core.kqp.opt.rbo.verification.inspector.plan import snapshot_digest
from ydb.core.kqp.opt.rbo.verification.replay import cli as replay_cli
from ydb.core.kqp.opt.rbo.verification.replay.case import (
    load_json,
    prepare_case,
    table_path,
)
from ydb.core.kqp.opt.rbo.verification.replay.materialize import (
    render_import,
    rewrite_read_only_query,
    target_bundle,
)
from ydb.core.kqp.opt.rbo.verification.replay.model import InconclusiveReplay, ReplayError
from ydb.core.kqp.opt.rbo.verification.replay.observation import (
    compare_results,
    optimizer_mode,
    parse_result,
)
from ydb.core.kqp.opt.rbo.verification.replay.runner import Target, run_replay


def identity(path="/Root/source/table", cluster="Я"):
    fields = {
        "cluster": cluster,
        "path": path,
        "path_id": "1:2",
        "sys_view": "",
        "version": "7",
    }
    return "".join(
        f"{name}:{len(value.encode('utf-8'))}:{value};"
        for name, value in fields.items()
    )


TABLE = identity()


def schema(columns=None, key=("id",)):
    return {
        "tables": [{
            "name": TABLE,
            "columns": columns or [
                {"name": "id", "type": "Uint64", "nullable": False},
                {"name": "value", "type": "Int64", "nullable": True},
            ],
            "unique_keys": [{"columns": list(key), "nulls_distinct": False}],
        }]
    }


def snapshot(staged, ordered=False, columns=None, key=("id",), storage="column"):
    table_columns = columns or schema()["tables"][0]["columns"]
    mappings = [
        {"source": column["name"], "output": column["name"]}
        for column in table_columns
    ]
    nodes = [{
        "id": "scan",
        "op": "scan",
        "table": TABLE,
        "columns": mappings,
        "predicate": None,
        "pushed_limit": None,
    }]
    root = "scan"
    if ordered:
        nodes.append({
            "id": "sort",
            "op": "sort",
            "input": "scan",
            "order": [{"column": "id", "ascending": True, "nulls_first": False}],
            "limit": None,
            "phase": "undefined",
        })
        root = "sort"
    graph = None
    if staged:
        graph = {
            "root_stage": "source",
            "stages": [{
                "id": "source",
                "nodes": [node["id"] for node in nodes],
                "inputs": [],
                "outputs": [{"index": 0, "node": root}],
                "source_storage": storage,
            }],
            "edges": [],
            "assumptions": [],
        }
    return parse_snapshot({
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": schema(table_columns, key),
        "plan": {
            "nodes": nodes,
            "root": root,
            "output": [column["name"] for column in table_columns],
        },
        "stage_graph": graph,
    })


def trace(rows=None, ordered=False, outcomes=1, columns=None):
    table_columns = columns or schema()["tables"][0]["columns"]
    witness_rows = rows if rows is not None else [{"id": 1, "value": 2}]
    rendered_rows = [
        {
            "slot": index,
            "present": True,
            "values": [
                {"column": column["name"], "type": column["type"], "value": row[column["name"]]}
                for column in table_columns
            ],
        }
        for index, row in enumerate(witness_rows)
    ]
    family = {
        "columns": [
            {"name": column["name"], "type": column["type"], "nullable": column["nullable"]}
            for column in table_columns
        ],
        "disabled_outcome_count": 0,
        "outcomes": [
            {
                "index": index,
                "decisions": [],
                "sequence": ordered,
                "order": [] if ordered else None,
                "rows": copy.deepcopy(rendered_rows),
            }
            for index in range(outcomes)
        ],
    }
    after_family = copy.deepcopy(family)
    for outcome in after_family["outcomes"]:
        outcome["rows"][0]["values"][0]["value"] += 1
    return {
        "format": "ydb-rbo-concrete-trace",
        "version": 1,
        "status": "COUNTEREXAMPLE",
        "row_bound": 2,
        "task_bound": 2,
        "witness": {TABLE: witness_rows},
        "mismatches": [
            {
                "source": side,
                "outcome": index,
                "decisions": [],
                "matching_outcomes": [],
            }
            for side in ("before", "after")
            for index in range(outcomes)
        ],
        "trace": {
            "comparison": {
                "semantics": "sequence" if ordered else "bag",
                "before": copy.deepcopy(family),
                "after": after_family,
            }
        },
    }


def prepared(before, after, value, query):
    value["inputs"] = {
        "before_semantic_sha256": snapshot_digest(before),
        "after_semantic_sha256": snapshot_digest(after),
        "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
    }
    return prepare_case(before, after, value, query)


class IdentityTest(unittest.TestCase):
    def test_lengths_are_utf8_bytes(self):
        self.assertEqual(table_path(identity("/Root/таблица")), "/Root/таблица")

    def test_noncanonical_or_system_view_identity_is_rejected(self):
        with self.assertRaisesRegex(ReplayError, "non-canonical"):
            table_path(TABLE.replace("cluster:2", "cluster:02"))
        with self.assertRaisesRegex(ReplayError, "system-view"):
            table_path(TABLE.replace("sys_view:0:;", "sys_view:1:x;"))


class CaseTest(unittest.TestCase):
    def test_prepares_exact_path_rewrite_and_column_ddl(self):
        before = snapshot(False)
        after = snapshot(True)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        case = prepared(before, after, trace(), query)
        bundle = target_bundle(case, "/Root/replay", "_rbo_replay_" + "a" * 32)
        self.assertEqual(bundle.prefix, "/Root/replay/_rbo_replay_" + "a" * 32)
        self.assertIn("`/Root/replay/_rbo_replay_", bundle.query)
        self.assertIn("PARTITION BY HASH (`id`)", bundle.ddls[0])
        self.assertIn("PARTITION_COUNT = 2", bundle.ddls[0])

    def test_query_must_contain_exact_quoted_source_path(self):
        before = snapshot(False)
        after = snapshot(True)
        case = prepared(before, after, trace(), "SELECT 1;")
        with self.assertRaisesRegex(ReplayError, "no executable"):
            target_bundle(case, "/Root/replay", "_rbo_replay_" + "0" * 32)

    def test_trace_is_bound_to_snapshots_and_query(self):
        before = snapshot(False)
        after = snapshot(True)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        value = trace()
        value["inputs"] = {
            "before_semantic_sha256": "0" * 64,
            "after_semantic_sha256": snapshot_digest(after),
            "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
        }
        with self.assertRaisesRegex(ReplayError, "exact snapshots and query"):
            prepare_case(before, after, value, query)

    def test_trace_semantics_must_match_initial_order(self):
        before = snapshot(False, ordered=True)
        after = snapshot(True, ordered=True)
        query = f"SELECT * FROM `{table_path(TABLE)}` ORDER BY id;"
        with self.assertRaisesRegex(ReplayError, "semantics disagree"):
            prepared(before, after, trace(ordered=False), query)

    def test_distinct_enabled_results_are_inconclusive(self):
        value = trace(outcomes=2)
        value["trace"]["comparison"]["after"]["outcomes"][1]["rows"][0]["values"][1]["value"] = 9
        before = snapshot(False)
        after = snapshot(True)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        with self.assertRaises(InconclusiveReplay):
            prepared(before, after, value, query)

    def test_equal_trace_results_and_incomplete_mismatches_are_rejected(self):
        before = snapshot(False)
        after = snapshot(True)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        equal = trace()
        equal["trace"]["comparison"]["after"] = copy.deepcopy(
            equal["trace"]["comparison"]["before"]
        )
        with self.assertRaisesRegex(ReplayError, "root results are equal"):
            prepared(before, after, equal, query)

        incomplete = trace(outcomes=2)
        incomplete["mismatches"].pop()
        with self.assertRaisesRegex(ReplayError, "cover every enabled"):
            prepared(before, after, incomplete, query)

    def test_out_of_range_and_duplicate_primary_keys_are_rejected(self):
        before = snapshot(False)
        after = snapshot(True)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        with self.assertRaisesRegex(ReplayError, "outside Uint64"):
            prepared(before, after, trace(rows=[{"id": -1, "value": 0}]), query)
        duplicate = trace(rows=[{"id": 1, "value": 0}, {"id": 1, "value": 1}])
        with self.assertRaisesRegex(ReplayError, "duplicate primary key"):
            prepared(before, after, duplicate, query)

    def test_row_replay_requires_uniformly_partitionable_key(self):
        columns = [
            {"name": "id", "type": "Int64", "nullable": False},
            {"name": "value", "type": "Int64", "nullable": True},
        ]
        before = snapshot(False, columns=columns)
        after = snapshot(True, columns=columns, storage="row")
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        case = prepared(before, after, trace(columns=columns), query)
        with self.assertRaisesRegex(ReplayError, "Uint32/Uint64"):
            target_bundle(case, "/Root/replay", "_rbo_replay_" + "b" * 32)


class ValueRenderingTest(unittest.TestCase):
    def test_import_preserves_string_utf8_date_and_exact_decimal(self):
        columns = [
            {"name": "id", "type": "Uint64", "nullable": False},
            {"name": "raw", "type": "String", "nullable": False},
            {"name": "text", "type": "Utf8", "nullable": False},
            {"name": "day", "type": "Date", "nullable": False},
            {"name": "amount", "type": "Decimal(5,2)", "nullable": False},
        ]
        raw = "a\0é"
        text = "e\u0301"
        rows = [{"id": 1, "raw": raw, "text": text, "day": 0, "amount": -7}]
        before = snapshot(False, columns=columns)
        after = snapshot(True, columns=columns)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        case = prepared(before, after, trace(rows=rows, columns=columns), query)
        data = json.loads(render_import(case.tables[0]))
        self.assertEqual(data["raw"], base64.b64encode(raw.encode()).decode())
        self.assertEqual(data["text"], text)
        self.assertEqual(data["day"], "1970-01-01")
        self.assertEqual(data["amount"], "-0.07")

    def test_import_preserves_decimal_special_values(self):
        columns = [
            {"name": "id", "type": "Uint64", "nullable": False},
            {"name": "amount", "type": "Decimal(5,2)", "nullable": False},
        ]
        before = snapshot(False, columns=columns)
        after = snapshot(True, columns=columns)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        for value, expected in (
            (-decimal.INF, "-inf"),
            (decimal.INF, "inf"),
            (decimal.NAN, "nan"),
        ):
            with self.subTest(value=value):
                case = prepared(
                    before,
                    after,
                    trace(rows=[{"id": 1, "amount": value}], columns=columns),
                    query,
                )
                rendered = json.loads(render_import(case.tables[0]))
                self.assertEqual(rendered["amount"], expected)

    def test_replay_rejects_decimal_error_codes(self):
        columns = [
            {"name": "id", "type": "Uint64", "nullable": False},
            {"name": "amount", "type": "Decimal(5,2)", "nullable": False},
        ]
        before = snapshot(False, columns=columns)
        after = snapshot(True, columns=columns)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        for value in (-decimal.INF - 1, decimal.NAN + 1):
            with self.subTest(value=value), self.assertRaisesRegex(
                ReplayError,
                "outside Decimal",
            ):
                prepared(
                    before,
                    after,
                    trace(rows=[{"id": 1, "amount": value}], columns=columns),
                    query,
                )


class ResultTest(unittest.TestCase):
    def test_bag_comparison_preserves_multiplicity(self):
        equal, difference = compare_results([{"x": 1}, {"x": 1}], [{"x": 1}], False)
        self.assertFalse(equal)
        self.assertEqual(difference["baseline_only"][0]["multiplicity"], 1)

    def test_sequence_comparison_reports_first_difference(self):
        equal, difference = compare_results([{"x": 1}], [{"x": 2}], True)
        self.assertFalse(equal)
        self.assertEqual(difference, {"first_mismatch": 0})

    def test_result_parser_rejects_floats_and_wrong_columns(self):
        with self.assertRaisesRegex(ReplayError, "floating"):
            parse_result('[{"x":1.5}]', ("x",))
        with self.assertRaisesRegex(ReplayError, "columns"):
            parse_result('[{"y":1}]', ("x",))


class OptimizerModeTest(unittest.TestCase):
    def test_distinguishes_current_legacy_and_new_stats_shapes(self):
        legacy = {"SimplifiedPlan": {"OptimizerStats": {
            "JoinsCount": 1,
            "EquiJoinsCount": 1,
            "CBOTreesTotal": 1,
            "CBOTreesOptimized": 1,
        }}}
        new = {"SimplifiedPlan": {"OptimizerStats": {
            "CBOTreesTotal": 1,
            "CBOTreesOptimized": 1,
        }}}
        self.assertEqual(optimizer_mode(legacy)[0], "LEGACY_RBO")
        self.assertEqual(optimizer_mode(new)[0], "NEW_RBO")
        self.assertEqual(optimizer_mode({"SimplifiedPlan": {}})[0], "LEGACY_RBO")

    def test_partial_cbo_is_rejected(self):
        with self.assertRaisesRegex(ReplayError, "every CBO tree"):
            optimizer_mode({"SimplifiedPlan": {"OptimizerStats": {
                "CBOTreesTotal": 2,
                "CBOTreesOptimized": 1,
            }}})


class JsonTest(unittest.TestCase):
    def test_strict_loader_rejects_duplicate_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            path.write_text('{"x":1,"x":2}', encoding="utf-8")
            with self.assertRaisesRegex(ReplayError, "duplicate"):
                load_json(path)

    def test_strict_loader_translates_decoder_recursion_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            path.write_text("[" * 10000 + "0" + "]" * 10000, encoding="utf-8")
            with self.assertRaisesRegex(ReplayError, "invalid JSON"):
                load_json(path)


class QueryRewriteTest(unittest.TestCase):
    def test_rewrites_only_code_identifiers(self):
        source = table_path(TABLE)
        query = (
            f"-- `{source}` in a comment\n"
            f"$text = \"`{source}` in a string\";\n"
            f"PRAGMA YqlSelect = 'force'; SELECT * FROM `{source}`;"
        )
        target = "/Root/replay/t000"
        rewritten = rewrite_read_only_query(query, {source: target})
        self.assertEqual(rewritten.count(f"`{target}`"), 1)
        self.assertEqual(rewritten.count(f"`{source}`"), 2)

    def test_comment_only_path_and_writes_are_rejected(self):
        source = table_path(TABLE)
        with self.assertRaisesRegex(ReplayError, "no executable"):
            rewrite_read_only_query(f"-- `{source}`\nSELECT 1;", {source: "/Root/x"})
        with self.assertRaisesRegex(ReplayError, "non-read-only"):
            rewrite_read_only_query(
                f"UPSERT INTO `{source}` (id) VALUES (1); SELECT 1;",
                {source: "/Root/x"},
            )

    def test_unmapped_absolute_path_and_unknown_pragma_are_rejected(self):
        source = table_path(TABLE)
        with self.assertRaisesRegex(ReplayError, "unmapped absolute"):
            rewrite_read_only_query(
                f"SELECT * FROM `{source}` JOIN `/Root/other`;", {source: "/Root/x"}
            )
        with self.assertRaisesRegex(ReplayError, "unsupported PRAGMA"):
            rewrite_read_only_query(
                f"PRAGMA TablePathPrefix='/Root'; SELECT * FROM `{source}`;",
                {source: "/Root/x"},
            )

    def test_bare_carriage_return_ends_comment_and_second_statement_is_rejected(self):
        source = table_path(TABLE)
        with self.assertRaisesRegex(ReplayError, "non-read-only keyword"):
            rewrite_read_only_query(
                f"SELECT * FROM `{source}`; -- comment\rANALYZE table;",
                {source: "/Root/x"},
            )
        with self.assertRaisesRegex(ReplayError, "exactly one"):
            rewrite_read_only_query(
                f"SELECT * FROM `{source}`; SELECT 2;",
                {source: "/Root/x"},
            )


class FakeYdb:
    def __init__(self, same=False, candidate_fallback=False):
        self.commands = []
        self.same = same
        self.candidate_fallback = candidate_fallback

    def __call__(self, command, **settings):
        self.commands.append((command, settings))
        endpoint = command[command.index("--endpoint") + 1]
        if "--explain" in command:
            stats = (
                {
                    "JoinsCount": 1,
                    "EquiJoinsCount": 1,
                    "CBOTreesTotal": 1,
                    "CBOTreesOptimized": 1,
                }
                if endpoint == "grpc://baseline" or self.candidate_fallback
                else {"CBOTreesTotal": 1, "CBOTreesOptimized": 1}
            )
            output = json.dumps({"SimplifiedPlan": {"OptimizerStats": stats}})
        elif "json-base64-array" in command:
            value = 2 if self.same or endpoint == "grpc://baseline" else 3
            output = json.dumps([{"id": 1, "value": value}])
        else:
            output = ""
        return subprocess.CompletedProcess(command, 0, output, "")


class RunnerTest(unittest.TestCase):
    def case(self):
        before = snapshot(False)
        after = snapshot(True)
        query = f"SELECT * FROM `{table_path(TABLE)}`;"
        return prepared(before, after, trace(), query)

    def test_confirms_difference_after_bulk_import_and_mode_preflight(self):
        fake = FakeYdb()
        result = run_replay(
            self.case(),
            Target("baseline", "grpc://baseline", "/Root/base"),
            Target("candidate", "grpc://candidate", "/Root/new"),
            "/bin/ydb",
            30,
            invoke=fake,
            namespace="_rbo_replay_" + "c" * 32,
        )
        self.assertEqual(result["status"], "REAL_RESULT_DIVERGENCE")
        self.assertEqual(result["baseline"]["optimizer"], "LEGACY_RBO")
        self.assertEqual(result["candidate"]["optimizer"], "NEW_RBO")
        imports = [item for item in fake.commands if "import" in item[0]]
        self.assertEqual(len(imports), 2)
        self.assertTrue(all("file" in command for command, _ in imports))
        self.assertTrue(all(settings["input"] for _, settings in imports))
        self.assertTrue(all("--no-discovery" in command for command, _ in fake.commands))

    def test_equal_real_results_are_not_reproduced(self):
        result = run_replay(
            self.case(),
            Target("baseline", "grpc://baseline", "/Root/base"),
            Target("candidate", "grpc://candidate", "/Root/new"),
            "/bin/ydb",
            30,
            invoke=FakeYdb(same=True),
            namespace="_rbo_replay_" + "d" * 32,
        )
        self.assertEqual(result["status"], "NOT_REPRODUCED")

    def test_same_target_is_rejected_before_invocation(self):
        fake = FakeYdb()
        with self.assertRaisesRegex(ValueError, "different YDB targets"):
            run_replay(
                self.case(),
                Target("baseline", "grpc://same", "/Root/db"),
                Target("candidate", "grpc://same", "/Root/db"),
                "/bin/ydb",
                30,
                invoke=fake,
            )
        self.assertEqual(fake.commands, [])

    def test_mode_failure_reports_both_retained_namespaces(self):
        namespace = "_rbo_replay_" + "e" * 32
        with self.assertRaisesRegex(
            ValueError,
            rf"/Root/base/{namespace}, /Root/new/{namespace}",
        ):
            run_replay(
                self.case(),
                Target("baseline", "grpc://baseline", "/Root/base"),
                Target("candidate", "grpc://candidate", "/Root/new"),
                "/bin/ydb",
                30,
                invoke=FakeYdb(candidate_fallback=True),
                namespace=namespace,
            )

    def test_subprocess_failure_reports_both_retained_namespaces(self):
        namespace = "_rbo_replay_" + "f" * 32

        def fail_after_baseline_creation(command, **settings):
            endpoint = command[command.index("--endpoint") + 1]
            if endpoint == "grpc://candidate" and "mkdir" in command:
                raise OSError("cannot spawn")
            return subprocess.CompletedProcess(command, 0, "", "")

        with self.assertRaisesRegex(
            ValueError,
            rf"cannot spawn.*?/Root/base/{namespace}, /Root/new/{namespace}",
        ):
            run_replay(
                self.case(),
                Target("baseline", "grpc://baseline", "/Root/base"),
                Target("candidate", "grpc://candidate", "/Root/new"),
                "/bin/ydb",
                30,
                invoke=fail_after_baseline_creation,
                namespace=namespace,
            )

    def test_unexpected_failure_reports_both_retained_namespaces(self):
        namespace = "_rbo_replay_" + "9" * 32

        def fail_after_baseline_creation(command, **settings):
            endpoint = command[command.index("--endpoint") + 1]
            if endpoint == "grpc://candidate" and "mkdir" in command:
                raise RuntimeError("unexpected failure")
            return subprocess.CompletedProcess(command, 0, "", "")

        with self.assertRaisesRegex(
            ValueError,
            rf"RuntimeError: unexpected failure.*?/Root/base/{namespace}, /Root/new/{namespace}",
        ):
            run_replay(
                self.case(),
                Target("baseline", "grpc://baseline", "/Root/base"),
                Target("candidate", "grpc://candidate", "/Root/new"),
                "/bin/ydb",
                30,
                invoke=fail_after_baseline_creation,
                namespace=namespace,
            )


class ReplayCliExitTest(unittest.TestCase):
    def arguments(self, root):
        query = root / "query.yql"
        query.write_text("SELECT 1;", encoding="utf-8")
        return [
            str(root / "initial.json"),
            str(root / "final.json"),
            str(root / "trace.json"),
            str(query),
            "--ydb",
            "/bin/ydb",
            "--baseline-endpoint",
            "grpc://baseline",
            "--baseline-database",
            "/Root/base",
            "--candidate-endpoint",
            "grpc://candidate",
            "--candidate-database",
            "/Root/new",
        ]

    def run_cli(self, root, result=None, error=None):
        output = io.StringIO()
        errors = io.StringIO()
        with (
            mock.patch.object(
                replay_cli, "load_snapshot", side_effect=[object(), object()]
            ),
            mock.patch.object(replay_cli, "load_json", return_value=object()),
            mock.patch.object(replay_cli, "prepare_case", return_value=object()),
            mock.patch.object(
                replay_cli, "run_replay", return_value=result, side_effect=error
            ),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            exit_code = replay_cli.main(self.arguments(root))
        return exit_code, output.getvalue(), errors.getvalue()

    def test_real_divergence_is_a_failing_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            exit_code, output, errors = self.run_cli(
                Path(temporary), {"status": "REAL_RESULT_DIVERGENCE"}
            )
        self.assertEqual(exit_code, 1)
        self.assertIn('"status": "REAL_RESULT_DIVERGENCE"', output)
        self.assertEqual(errors, "")

    def test_not_reproduced_is_a_successful_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            exit_code, output, errors = self.run_cli(
                Path(temporary), {"status": "NOT_REPRODUCED"}
            )
        self.assertEqual(exit_code, 0)
        self.assertIn('"status": "NOT_REPRODUCED"', output)
        self.assertEqual(errors, "")

    def test_inconclusive_and_setup_errors_are_distinct_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            exit_code, output, errors = self.run_cli(
                Path(temporary), error=InconclusiveReplay("multiple outcomes")
            )
        self.assertEqual(exit_code, 2)
        self.assertIn('"status": "INCONCLUSIVE_NONDETERMINISM"', output)
        self.assertEqual(errors, "")

        with tempfile.TemporaryDirectory() as temporary:
            exit_code, output, errors = self.run_cli(
                Path(temporary), error=ReplayError("invalid replay")
            )
        self.assertEqual(exit_code, 2)
        self.assertEqual(output, "")
        self.assertIn('"status": "SETUP_ERROR"', errors)


if __name__ == "__main__":
    unittest.main()
