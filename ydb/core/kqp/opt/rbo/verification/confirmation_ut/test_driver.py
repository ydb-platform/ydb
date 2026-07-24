import hashlib
import io
import json
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.confirmation import cli
from ydb.core.kqp.opt.rbo.verification.confirmation.driver import confirm
from ydb.core.kqp.opt.rbo.verification.confirmation.model import (
    Config,
    ConfirmationError,
)


def _sha(value):
    return hashlib.sha256(value).hexdigest()


class Fixture:
    def __init__(self, root, statuses=("COUNTEREXAMPLE",)):
        self.root = root
        self.query = b"SELECT 1;\r\n"
        self.witnesses = {}
        self.verdict_bytes = {}
        rows = []
        for query_id, status in enumerate(statuses, 1):
            row = {
                "query_id": query_id,
                "suite": "TPCDS_YQL",
                "source": f"q{query_id}.yql",
                "status": status,
            }
            if status == "COUNTEREXAMPLE":
                stem = f"candidate_{query_id}"
                initial = f"{stem}.initial.json"
                final = f"{stem}.final.json"
                query = f"{stem}.query.yql"
                initial_bytes = json.dumps({"side": "initial", "q": query_id}).encode()
                final_bytes = json.dumps({"side": "final", "q": query_id}).encode()
                (root / initial).write_bytes(initial_bytes)
                (root / final).write_bytes(final_bytes)
                (root / query).write_bytes(self.query)
                witness = {"A": [{"x": query_id}]}
                self.witnesses[query_id] = witness
                raw_verdict = {
                    "status": "COUNTEREXAMPLE",
                    "row_bound": 2,
                    "task_bound": 2,
                    "witness": witness,
                }
                verdict_bytes = (json.dumps(raw_verdict) + "\n").encode()
                verdict = f"{stem}.verdict.json"
                (root / verdict).write_bytes(verdict_bytes)
                self.verdict_bytes[query_id] = verdict_bytes
                row.update({
                    "verdict": {
                        "status": "COUNTEREXAMPLE",
                        "row_bound": 2,
                        "task_bound": 2,
                    },
                    "artifacts": {
                        "initial_snapshot": initial,
                        "initial_snapshot_sha256": _sha(initial_bytes),
                        "final_snapshot": final,
                        "final_snapshot_sha256": _sha(final_bytes),
                        "query": query,
                        "query_sha256": _sha(self.query),
                        "verifier_verdict": verdict,
                        "verifier_verdict_sha256": _sha(verdict_bytes),
                    },
                })
            rows.append(row)
        summary = {}
        for status in statuses:
            summary[status] = summary.get(status, 0) + 1
        self.report_value = {
            "format": "ydb-rbo-benchmark-coverage",
            "version": 4,
            "suite": "TPCDS_YQL",
            "row_bound": 2,
            "task_bound": 2,
            "solver_present": any(status == "COUNTEREXAMPLE" for status in statuses),
            "summary": summary,
            "queries": rows,
        }
        self.report = root / "coverage.json"
        self.write_report()

    def write_report(self):
        self.report.write_text(json.dumps(self.report_value), encoding="utf-8")

    def use_version_five(self, prepare_statuses=None):
        if prepare_statuses is None:
            prepare_statuses = ("SUCCEEDED",) * len(self.report_value["queries"])
        if len(prepare_statuses) != len(self.report_value["queries"]):
            raise AssertionError("one prepare status is required for every report row")
        prepare_summary = {}
        for row, prepare_status in zip(
            self.report_value["queries"],
            prepare_statuses,
            strict=True,
        ):
            row["prepare_status"] = prepare_status
            row["prepare_reason"] = (
                "host preparation outcome is unavailable"
                if prepare_status == "UNKNOWN"
                else "host preparation failed after snapshot capture"
                if prepare_status == "FAILED"
                else ""
            )
            prepare_summary[prepare_status] = prepare_summary.get(prepare_status, 0) + 1
        self.report_value["version"] = 5
        self.report_value["prepare_summary"] = prepare_summary
        self.write_report()

    def set_witness(self, query_id, witness):
        row = next(
            item for item in self.report_value["queries"]
            if item["query_id"] == query_id
        )
        verdict = {
            "status": "COUNTEREXAMPLE",
            "row_bound": 2,
            "task_bound": 2,
            "witness": witness,
        }
        content = (json.dumps(verdict) + "\n").encode()
        (self.root / row["artifacts"]["verifier_verdict"]).write_bytes(content)
        row["artifacts"]["verifier_verdict_sha256"] = _sha(content)
        self.witnesses[query_id] = witness
        self.verdict_bytes[query_id] = content
        self.write_report()

    def config(self):
        return Config(
            report=self.report,
            inspector=Path("/tools/inspect"),
            solver=Path("/tools/z3"),
            replay=Path("/tools/replay"),
            ydb=Path("/tools/ydb"),
            artifacts=self.root / "confirmation",
            baseline_endpoint="grpc://baseline",
            baseline_database="/Root/baseline",
            candidate_endpoint="grpc://candidate",
            candidate_database="/Root/candidate",
        )


class FakeCommands:
    def __init__(
        self,
        witnesses,
        replay_status="NOT_REPRODUCED",
        mutate_trace=None,
        mutate_replay=None,
    ):
        self.witnesses = witnesses
        self.replay_status = replay_status
        self.mutate_trace = mutate_trace
        self.mutate_replay = mutate_replay
        self.calls = []

    def __call__(self, arguments, timeout):
        arguments = list(arguments)
        self.calls.append((arguments, timeout))
        query_id = int(Path(arguments[2]).parent.name[1:])
        if arguments[0] == "/tools/inspect":
            def result_family():
                return {
                    "columns": [{
                        "name": "x",
                        "type": "Int64",
                        "nullable": False,
                    }],
                    "disabled_outcome_count": 0,
                    "outcomes": [{}],
                }

            trace = {
                "format": "ydb-rbo-concrete-trace",
                "version": 1,
                "status": "COUNTEREXAMPLE",
                "row_bound": 2,
                "task_bound": 2,
                "inputs": {
                    "before_semantic_sha256": "a" * 64,
                    "after_semantic_sha256": "b" * 64,
                    "query_sha256": _sha(b"SELECT 1;\r\n"),
                },
                "witness": self.witnesses[query_id],
                "mismatches": [{}],
                "trace": {
                    "before": {},
                    "after": {},
                    "comparison": {
                        "semantics": "bag",
                        "before": result_family(),
                        "after": result_family(),
                    },
                },
            }
            if self.mutate_trace is not None:
                self.mutate_trace(trace)
            return subprocess.CompletedProcess(arguments, 1, json.dumps(trace), "")
        if arguments[0] != "/tools/replay":
            raise AssertionError(arguments)
        if self.replay_status == "SETUP_ERROR":
            return subprocess.CompletedProcess(
                arguments,
                2,
                "",
                json.dumps({"status": "SETUP_ERROR", "reason": "cannot connect"}),
            )
        exit_code = 1 if self.replay_status == "REAL_RESULT_DIVERGENCE" else 0
        baseline_rows = [{"x": 1}]
        candidate_rows = (
            [{"x": 2}]
            if self.replay_status == "REAL_RESULT_DIVERGENCE"
            else [{"x": 1}]
        )
        replay = {
            "format": "ydb-rbo-real-replay",
            "version": 1,
            "status": self.replay_status,
            "comparison": "bag",
            "row_bound": 2,
            "symbolic_string_cells": 0,
            "trace_plan_reproduced": False,
            "namespaces_retained": True,
            "baseline": {
                "database": "/Root/baseline",
                "namespace": "/Root/baseline/_rbo_replay_" + "a" * 32,
                "optimizer": "LEGACY_RBO",
                "optimizer_stats": None,
                "rows": baseline_rows,
            },
            "candidate": {
                "database": "/Root/candidate",
                "namespace": "/Root/candidate/_rbo_replay_" + "a" * 32,
                "optimizer": "NEW_RBO",
                "optimizer_stats": {
                    "CBOTreesTotal": 0,
                    "CBOTreesOptimized": 0,
                },
                "rows": candidate_rows,
            },
            "difference": (
                {
                    "baseline_only": [{
                        "row": repr(("object", (("x", ("int", 1)),))),
                        "multiplicity": 1,
                    }],
                    "candidate_only": [{
                        "row": repr(("object", (("x", ("int", 2)),))),
                        "multiplicity": 1,
                    }],
                }
                if self.replay_status == "REAL_RESULT_DIVERGENCE"
                else {}
            ),
        }
        if self.mutate_replay is not None:
            self.mutate_replay(replay)
        return subprocess.CompletedProcess(arguments, exit_code, json.dumps(replay), "")


class ConfirmationTest(unittest.TestCase):
    def test_version_four_report_remains_supported(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary), ("VERIFIED_BOUNDED",))
            result = confirm(fixture.config(), run=self.fail)
            self.assertEqual(result["status"], "NO_COUNTEREXAMPLES")

    def test_version_five_failed_prepare_keeps_counterexample_logic(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            fixture.use_version_five(("FAILED",))
            commands = FakeCommands(fixture.witnesses)
            result = confirm(fixture.config(), run=commands)
            self.assertEqual(result["status"], "ALL_NOT_REPRODUCED")
            self.assertEqual(len(commands.calls), 2)
            self.assertEqual(result["candidates"][0]["query_id"], 1)

    def test_version_five_allows_not_run_only_for_query_zero(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary), ("HARNESS_ERROR",))
            fixture.report_value["queries"][0]["query_id"] = 0
            fixture.use_version_five(("NOT_RUN",))
            result = confirm(fixture.config(), run=self.fail)
            self.assertEqual(result["status"], "NO_COUNTEREXAMPLES")

        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary), ("HARNESS_ERROR",))
            fixture.use_version_five(("NOT_RUN",))
            with self.assertRaisesRegex(ConfirmationError, "invalid prepare_status"):
                confirm(fixture.config(), run=self.fail)
            self.assertFalse(fixture.config().artifacts.exists())

    def test_version_five_allows_unknown_preparation_for_query_harness_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary), ("HARNESS_ERROR",))
            fixture.use_version_five(("UNKNOWN",))
            result = confirm(fixture.config(), run=self.fail)
            self.assertEqual(result["status"], "NO_COUNTEREXAMPLES")

    def test_version_five_prepare_contract_fails_closed(self):
        mutations = {
            "missing_status": (
                lambda report: report["queries"][0].pop("prepare_status"),
                "invalid prepare_status",
            ),
            "unknown_status": (
                lambda report: report["queries"][0].update(
                    prepare_status="BROKEN"
                ),
                "invalid prepare_status",
            ),
            "non_string_status": (
                lambda report: report["queries"][0].update(prepare_status=[]),
                "invalid prepare_status",
            ),
            "missing_reason": (
                lambda report: report["queries"][0].pop("prepare_reason"),
                "invalid prepare_reason",
            ),
            "non_string_reason": (
                lambda report: report["queries"][0].update(prepare_reason=None),
                "invalid prepare_reason",
            ),
            "empty_failed_reason": (
                lambda report: report["queries"][0].update(prepare_reason=""),
                "invalid prepare_reason",
            ),
            "nonempty_succeeded_reason": (
                lambda report: report["queries"][0].update(
                    prepare_status="SUCCEEDED",
                    prepare_reason="unexpected",
                    prepare_summary={"SUCCEEDED": 1},
                ),
                "invalid prepare_reason",
            ),
            "missing_summary": (
                lambda report: report.pop("prepare_summary"),
                "prepare_summary is invalid",
            ),
            "non_object_summary": (
                lambda report: report.update(prepare_summary=[]),
                "prepare_summary is invalid",
            ),
            "unknown_summary_status": (
                lambda report: report.update(prepare_summary={"BROKEN": 1}),
                "prepare_summary is invalid",
            ),
            "boolean_summary_count": (
                lambda report: report.update(prepare_summary={"FAILED": True}),
                "prepare_summary is invalid",
            ),
            "mismatched_summary": (
                lambda report: report.update(prepare_summary={"SUCCEEDED": 1}),
                "prepare_summary does not match",
            ),
        }
        for name, (mutate, reason) in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                fixture = Fixture(Path(temporary), ("VERIFIED_BOUNDED",))
                fixture.use_version_five(("FAILED",))
                mutate(fixture.report_value)
                fixture.write_report()
                with self.assertRaisesRegex(ConfirmationError, reason):
                    confirm(fixture.config(), run=self.fail)
                self.assertFalse(fixture.config().artifacts.exists())

    def test_no_candidates_is_success_and_runs_no_children(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary), ("VERIFIED_BOUNDED",))

            def unexpected(*_):
                self.fail("no child command should run")

            result = confirm(fixture.config(), run=unexpected)
            self.assertEqual(result["status"], "NO_COUNTEREXAMPLES")
            self.assertEqual(result["summary"], {"total": 0})
            self.assertTrue((fixture.config().artifacts / "result.json").is_file())

    def test_fixed_witness_runs_inspector_then_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            commands = FakeCommands(fixture.witnesses)
            result = confirm(fixture.config(), run=commands)
            self.assertEqual(result["status"], "ALL_NOT_REPRODUCED")
            self.assertEqual(result["summary"], {"NOT_REPRODUCED": 1, "total": 1})
            self.assertEqual([call[0][0] for call in commands.calls], [
                "/tools/inspect",
                "/tools/replay",
            ])
            inspector = commands.calls[0][0]
            self.assertIn("--verifier-verdict", inspector)
            self.assertEqual(
                json.loads(Path(inspector[-1]).read_text(encoding="utf-8"))["witness"],
                fixture.witnesses[1],
            )
            self.assertEqual(Path(inspector[-1]).read_bytes(), fixture.verdict_bytes[1])
            candidate = result["candidates"][0]
            self.assertEqual(candidate["classification"], "NOT_REPRODUCED")
            self.assertTrue(
                (fixture.config().artifacts / candidate["inspector"]["stdout"]).is_file()
            )

    def test_real_divergence_is_a_correctness_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            result = confirm(
                fixture.config(),
                run=FakeCommands(fixture.witnesses, "REAL_RESULT_DIVERGENCE"),
            )
            self.assertEqual(result["status"], "REAL_RESULT_DIVERGENCE")
            self.assertEqual(
                result["candidates"][0]["classification"],
                "REAL_RESULT_DIVERGENCE",
            )

    def test_raw_decimal_sized_witness_integer_is_preserved_exactly(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            exact = 10**35 + 1
            fixture.set_witness(1, {"A": [{"x": exact}]})
            commands = FakeCommands(fixture.witnesses)
            result = confirm(fixture.config(), run=commands)
            self.assertEqual(result["status"], "ALL_NOT_REPRODUCED")
            verifier = Path(commands.calls[0][0][-1])
            self.assertEqual(verifier.read_bytes(), fixture.verdict_bytes[1])
            self.assertEqual(json.loads(verifier.read_text())["witness"]["A"][0]["x"], exact)

    def test_every_candidate_is_processed_in_query_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary), ("COUNTEREXAMPLE", "COUNTEREXAMPLE"))
            fixture.report_value["queries"].reverse()
            fixture.write_report()
            commands = FakeCommands(fixture.witnesses)
            result = confirm(fixture.config(), run=commands)
            self.assertEqual(
                [candidate["query_id"] for candidate in result["candidates"]],
                [1, 2],
            )
            self.assertEqual(len(commands.calls), 4)

    def test_changed_inspector_witness_is_unresolved_and_skips_replay(self):
        for changed in (99, True):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temporary:
                fixture = Fixture(Path(temporary))

                def change_witness(trace):
                    trace["witness"] = {"A": [{"x": changed}]}

                commands = FakeCommands(fixture.witnesses, mutate_trace=change_witness)
                result = confirm(fixture.config(), run=commands)
                self.assertEqual(result["status"], "UNRESOLVED")
                self.assertEqual(len(commands.calls), 1)
                self.assertIn("saved verifier witness", result["candidates"][0]["reason"])

    def test_changed_trace_result_schema_is_unresolved_and_skips_replay(self):
        def change_schema(trace):
            trace["trace"]["comparison"]["after"]["columns"][0]["name"] = "y"

        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            commands = FakeCommands(fixture.witnesses, mutate_trace=change_schema)
            result = confirm(fixture.config(), run=commands)
            self.assertEqual(result["status"], "UNRESOLVED")
            self.assertEqual(len(commands.calls), 1)
            self.assertIn("schemas differ", result["candidates"][0]["reason"])

    def test_replay_setup_failure_is_unresolved(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            result = confirm(
                fixture.config(),
                run=FakeCommands(fixture.witnesses, "SETUP_ERROR"),
            )
            self.assertEqual(result["status"], "UNRESOLVED")
            self.assertEqual(result["candidates"][0]["replay"]["status"], "SETUP_ERROR")
            self.assertEqual(result["candidates"][0]["unresolved_phase"], "replay")

    def test_truncated_or_semantically_inconsistent_replay_is_unresolved(self):
        mutations = {
            "missing_baseline": lambda value: value.pop("baseline"),
            "wrong_candidate_mode": lambda value: value["candidate"].update(
                optimizer="LEGACY_RBO"
            ),
            "wrong_database": lambda value: value["baseline"].update(
                database="/Root/other"
            ),
            "namespaces_not_retained": lambda value: value.update(
                namespaces_retained=False
            ),
            "invalid_namespace_suffix": lambda value: value["baseline"].update(
                namespace="/Root/baseline/_rbo_replay_not-a-uuid"
            ),
            "different_namespace_suffix": lambda value: value["candidate"].update(
                namespace="/Root/candidate/_rbo_replay_" + "b" * 32
            ),
            "missing_result_columns": lambda value: (
                value["baseline"].update(rows=[{}]),
                value["candidate"].update(rows=[{}]),
            ),
            "status_disagrees_with_rows": lambda value: value["candidate"].update(
                rows=[{"x": 2}]
            ),
            "difference_disagrees_with_rows": lambda value: value.update(
                difference={"unexpected": True}
            ),
            "incomplete_candidate_stats": lambda value: value["candidate"].update(
                optimizer_stats={"CBOTreesTotal": 1}
            ),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                fixture = Fixture(Path(temporary))
                result = confirm(
                    fixture.config(),
                    run=FakeCommands(fixture.witnesses, mutate_replay=mutation),
                )
                self.assertEqual(result["status"], "UNRESOLVED")
                self.assertEqual(result["candidates"][0]["unresolved_phase"], "replay")

    def test_sequence_comparison_distinguishes_bool_from_integer(self):
        def sequence_trace(value):
            value["trace"]["comparison"]["semantics"] = "sequence"

        def bool_and_integer(value):
            value["comparison"] = "sequence"
            value["baseline"]["rows"] = [{"x": True}]
            value["candidate"]["rows"] = [{"x": 1}]

        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            result = confirm(
                fixture.config(),
                run=FakeCommands(
                    fixture.witnesses,
                    mutate_trace=sequence_trace,
                    mutate_replay=bool_and_integer,
                ),
            )
            self.assertEqual(result["status"], "UNRESOLVED")
            self.assertIn("retained result rows", result["candidates"][0]["reason"])

    def test_tampered_or_escaping_artifact_is_unresolved_without_children(self):
        for mutation in ("digest", "path"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                fixture = Fixture(Path(temporary))
                artifacts = fixture.report_value["queries"][0]["artifacts"]
                if mutation == "digest":
                    artifacts["query_sha256"] = "0" * 64
                else:
                    artifacts["query"] = "../outside.yql"
                fixture.write_report()
                commands = FakeCommands(fixture.witnesses)
                result = confirm(fixture.config(), run=commands)
                self.assertEqual(result["status"], "UNRESOLVED")
                self.assertEqual(commands.calls, [])
                self.assertEqual(result["candidates"][0]["unresolved_phase"], "input")

    def test_report_summary_duplicates_and_existing_output_fail_before_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary), ("COUNTEREXAMPLE", "COUNTEREXAMPLE"))
            fixture.report_value["queries"][1]["query_id"] = 1
            fixture.write_report()
            with self.assertRaisesRegex(ConfirmationError, "repeats query_id"):
                confirm(fixture.config(), run=FakeCommands(fixture.witnesses))
            self.assertFalse(fixture.config().artifacts.exists())

        with tempfile.TemporaryDirectory() as temporary:
            fixture = Fixture(Path(temporary))
            fixture.config().artifacts.mkdir()
            with self.assertRaisesRegex(ConfirmationError, "already exists"):
                confirm(fixture.config(), run=FakeCommands(fixture.witnesses))


class CliTest(unittest.TestCase):
    def arguments(self, root):
        return [
            str(root / "coverage.json"),
            "--inspector", "/tools/inspect",
            "--solver", "/tools/z3",
            "--replay", "/tools/replay",
            "--ydb", "/tools/ydb",
            "--artifacts", str(root / "result"),
            "--baseline-endpoint", "grpc://baseline",
            "--baseline-database", "/Root/baseline",
            "--candidate-endpoint", "grpc://candidate",
            "--candidate-database", "/Root/candidate",
        ]

    def test_exit_codes_preserve_classification(self):
        for status, expected in (
            ("NO_COUNTEREXAMPLES", 0),
            ("ALL_NOT_REPRODUCED", 0),
            ("REAL_RESULT_DIVERGENCE", 1),
            ("UNRESOLVED", 2),
        ):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary:
                output = io.StringIO()
                with mock.patch.object(cli, "confirm", return_value={"status": status}), redirect_stdout(output):
                    exit_code = cli.main(self.arguments(Path(temporary)))
                self.assertEqual(exit_code, expected)
                self.assertIn(status, output.getvalue())

    def test_structured_configuration_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            errors = io.StringIO()
            with mock.patch.object(
                cli,
                "confirm",
                side_effect=ConfirmationError("bad report"),
            ), redirect_stderr(errors):
                exit_code = cli.main(self.arguments(Path(temporary)))
        self.assertEqual(exit_code, 2)
        self.assertIn('"status": "CONFIRMATION_ERROR"', errors.getvalue())


if __name__ == "__main__":
    unittest.main()
