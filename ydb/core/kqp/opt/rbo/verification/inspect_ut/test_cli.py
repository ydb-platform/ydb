import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.inspector import cli
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import SchemaMismatch


class CliTest(unittest.TestCase):
    def test_missing_snapshot_is_a_structured_io_error(self):
        errors = io.StringIO()
        with redirect_stderr(errors):
            exit_code = cli.main(["plan", "definitely-missing.snapshot.json"])
        self.assertEqual(exit_code, 2)
        self.assertIn('"status": "IO_ERROR"', errors.getvalue())

    def test_solver_verdict_exit_codes_are_stable(self):
        for status, expected in (
            ("VERIFIED_BOUNDED", 0),
            ("COUNTEREXAMPLE", 1),
            ("UNKNOWN", 2),
            ("WITNESS_NOT_REPRODUCED", 2),
        ):
            with self.subTest(status=status):
                prepared = mock.Mock()
                prepared.solve.return_value = {"status": status}
                output = io.StringIO()
                with (
                    mock.patch.object(cli, "load_snapshot", side_effect=[object(), object()]),
                    mock.patch.object(cli, "prepare", return_value=prepared),
                    redirect_stdout(output),
                ):
                    exit_code = cli.main(
                        ["witness", "before.json", "after.json", "--solver", "z3"]
                    )
                self.assertEqual(exit_code, expected)
                self.assertIn(f'"status": "{status}"', output.getvalue())

    def test_schema_mismatch_is_a_correctness_exit(self):
        errors = io.StringIO()
        with (
            mock.patch.object(cli, "load_snapshot", side_effect=[object(), object()]),
            mock.patch.object(cli, "prepare", side_effect=SchemaMismatch("changed root")),
            redirect_stderr(errors),
        ):
            exit_code = cli.main(
                ["witness", "before.json", "after.json", "--solver", "z3"]
            )
        self.assertEqual(exit_code, 1)
        self.assertIn('"status": "SCHEMA_MISMATCH"', errors.getvalue())

    def test_query_file_digest_is_added_to_replayable_trace(self):
        prepared = mock.Mock()
        prepared.solve.return_value = {"status": "COUNTEREXAMPLE", "inputs": {}}
        with tempfile.TemporaryDirectory() as directory:
            query = Path(directory) / "query.yql"
            query.write_bytes(b"SELECT 1;\r\n")
            output = io.StringIO()
            with (
                mock.patch.object(cli, "load_snapshot", side_effect=[object(), object()]),
                mock.patch.object(cli, "prepare", return_value=prepared),
                redirect_stdout(output),
            ):
                exit_code = cli.main([
                    "witness",
                    "before.json",
                    "after.json",
                    "--query",
                    str(query),
                    "--solver",
                    "z3",
                ])
        self.assertEqual(exit_code, 1)
        result = json.loads(output.getvalue())
        self.assertEqual(
            result["inputs"]["query_sha256"], hashlib.sha256(b"SELECT 1;\r\n").hexdigest()
        )

    def test_saved_verifier_verdict_is_passed_as_a_fixed_database(self):
        prepared = mock.Mock()
        prepared.solve.return_value = {"status": "COUNTEREXAMPLE"}
        witness = {"A": [{"x": 7}]}
        with tempfile.TemporaryDirectory() as directory:
            verdict = Path(directory) / "verdict.json"
            verdict.write_text(
                json.dumps({
                    "status": "COUNTEREXAMPLE",
                    "row_bound": 2,
                    "task_bound": 2,
                    "witness": witness,
                }),
                encoding="utf-8",
            )
            with (
                mock.patch.object(cli, "load_snapshot", side_effect=[object(), object()]),
                mock.patch.object(cli, "prepare", return_value=prepared) as prepare,
                redirect_stdout(io.StringIO()),
            ):
                exit_code = cli.main([
                    "witness",
                    "before.json",
                    "after.json",
                    "--solver",
                    "z3",
                    "--verifier-verdict",
                    str(verdict),
                ])
        self.assertEqual(exit_code, 1)
        self.assertEqual(prepare.call_args.kwargs["fixed_witness"], witness)

    def test_unextracted_or_wrong_bound_verifier_witness_fails_closed(self):
        cases = (
            ({"status": "COUNTEREXAMPLE", "row_bound": 2, "task_bound": 2}, "no extracted witness"),
            ({
                "status": "COUNTEREXAMPLE",
                "row_bound": 1,
                "task_bound": 2,
                "witness": {},
            }, "row bound"),
        )
        for verdict_value, reason in cases:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                verdict = Path(directory) / "verdict.json"
                verdict.write_text(json.dumps(verdict_value), encoding="utf-8")
                errors = io.StringIO()
                with (
                    mock.patch.object(cli, "load_snapshot", side_effect=[object(), object()]),
                    redirect_stderr(errors),
                ):
                    exit_code = cli.main([
                        "witness",
                        "before.json",
                        "after.json",
                        "--solver",
                        "z3",
                        "--verifier-verdict",
                        str(verdict),
                    ])
                self.assertEqual(exit_code, 2)
                self.assertIn(reason, errors.getvalue())
                self.assertIn('"status": "INVALID_WITNESS"', errors.getvalue())

    def test_transformation_prefix_verdict_cannot_be_laundered_into_normal_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            verdict = Path(directory) / "verdict.json"
            verdict.write_text(
                json.dumps({
                    "status": "COUNTEREXAMPLE",
                    "row_bound": 2,
                    "task_bound": 2,
                    "witness": {},
                    "comparison_scope": "OPTIMIZER_TRANSFORMATION_PREFIX",
                }),
                encoding="utf-8",
            )
            errors = io.StringIO()
            with (
                mock.patch.object(cli, "load_snapshot", side_effect=[object(), object()]),
                redirect_stderr(errors),
            ):
                exit_code = cli.main([
                    "witness",
                    "before.json",
                    "after.json",
                    "--solver",
                    "z3",
                    "--verifier-verdict",
                    str(verdict),
                ])
        self.assertEqual(exit_code, 2)
        self.assertIn('"status": "INVALID_WITNESS"', errors.getvalue())
        self.assertIn("transformation-prefix", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
