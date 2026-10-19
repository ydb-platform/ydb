import io
import json
import signal
import tempfile
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import cli


@unittest.skipUnless(hasattr(signal, "setitimer"), "requires POSIX timers")
class ConstructionDeadlineTest(unittest.TestCase):
    def test_timeout_during_loading_building_or_rendering_is_unknown(self):
        def slow(*args, **kwargs):
            time.sleep(0.25)
            return mock.sentinel.unfinished

        for phase in ("load", "build", "render"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                artifact = Path(directory) / "formula.smt2"
                artifact.write_text("previous artifact")
                problem = SimpleNamespace(observation_kind=None, formula=mock.Mock(return_value="formula"))
                if phase == "render":
                    problem.formula.side_effect = slow
                output = io.StringIO()
                previous = signal.getsignal(signal.SIGALRM)
                with (
                    mock.patch.object(cli, "load_snapshot", side_effect=slow if phase == "load" else None),
                    mock.patch.object(cli, "build_problem", return_value=problem,
                                      side_effect=slow if phase == "build" else None),
                    mock.patch.object(cli, "solve") as solve,
                    redirect_stdout(output),
                ):
                    code = cli.main(["before", "after", "--solver", "solver", "--emit-smt", str(artifact),
                                     "--construction-timeout-ms", "20"])
                self.assertEqual(code, 2)
                verdict = json.loads(output.getvalue())
                self.assertEqual(verdict["status"], "UNKNOWN")
                self.assertEqual(verdict["phase"], "construction")
                self.assertEqual((verdict["row_bound"], verdict["task_bound"]), (2, 2))
                self.assertEqual(verdict["construction_timeout_ms"], 20)
                solve.assert_not_called()
                self.assertEqual(artifact.read_text(), "previous artifact")
                self.assertEqual(signal.getsignal(signal.SIGALRM), previous)
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0, 0))

    def test_construction_timer_is_removed_before_solving(self):
        previous = signal.getsignal(signal.SIGALRM)

        def solve(*args):
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0, 0))
            self.assertEqual(signal.getsignal(signal.SIGALRM), previous)
            return SimpleNamespace(status="VERIFIED_BOUNDED", to_json=lambda: {"status": "VERIFIED_BOUNDED"})

        with (
            mock.patch.object(cli, "load_snapshot"),
            mock.patch.object(cli, "build_problem", return_value=SimpleNamespace(observation_kind=None)),
            mock.patch.object(cli, "solve", side_effect=solve),
            redirect_stdout(io.StringIO()) as output,
        ):
            self.assertEqual(cli.main(["before", "after", "--solver", "solver",
                                       "--construction-timeout-ms", "500"]), 0)
        self.assertEqual(json.loads(output.getvalue())["status"], "VERIFIED_BOUNDED")

    def test_existing_timer_is_not_replaced(self):
        with (
            mock.patch.object(signal, "getitimer", return_value=(1, 0)),
            mock.patch.object(signal, "setitimer") as timer,
            mock.patch.object(signal, "signal") as handler,
            redirect_stderr(io.StringIO()) as output,
        ):
            self.assertEqual(cli.main(["before", "after", "--solver", "solver",
                                       "--construction-timeout-ms", "20"]), 2)
        self.assertEqual(json.loads(output.getvalue())["status"], "INVALID_ARGUMENT")
        timer.assert_not_called()
        handler.assert_not_called()

    def test_exception_restores_the_original_signal_handler(self):
        previous = signal.getsignal(signal.SIGALRM)
        with self.assertRaisesRegex(RuntimeError, "construction failed"):
            with cli._construction_deadline(500):
                raise RuntimeError("construction failed")
        self.assertEqual(signal.getsignal(signal.SIGALRM), previous)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0, 0))

    def test_bundle_timeout_preserves_comparison_scope(self):
        with (
            mock.patch.object(cli, "load_bundle"),
            mock.patch.object(cli, "build_bundle_problem", side_effect=cli._ConstructionTimeout("expired")),
            mock.patch.object(cli, "solve") as solve,
            redirect_stdout(io.StringIO()) as output,
        ):
            self.assertEqual(cli.main(["--bundle", "bundle", "--solver", "solver",
                                       "--construction-timeout-ms", "20"]), 2)
        verdict = json.loads(output.getvalue())
        self.assertEqual(verdict["status"], "UNKNOWN")
        self.assertEqual(verdict["comparison_scope"], "BUFFERED_RESULT_BUNDLE")
        solve.assert_not_called()

    def test_invalid_budget_is_rejected(self):
        for value in ("0", "-1"):
            with redirect_stderr(io.StringIO()) as output:
                self.assertEqual(cli.main(["before", "after", "--solver", "solver",
                                           "--construction-timeout-ms", value]), 2)
            self.assertEqual(json.loads(output.getvalue())["status"], "INVALID_ARGUMENT")


if __name__ == "__main__":
    unittest.main()
