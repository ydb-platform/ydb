from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from ydb.core.kqp.opt.rbo.verification.tools.bisect import localize
from ydb.core.kqp.opt.rbo.verification.tools.protocol import (
    PROTOCOL,
    Config,
    LocalizationError,
)


class Harness:
    def __init__(
        self,
        applications,
        prefix_verdicts,
        final_verdict,
        *,
        export_gaps=(),
        final_export_unsupported=False,
        mutate_initial_at=None,
        mutate_sequence_at=None,
    ):
        self.applications = applications
        self.prefix_verdicts = prefix_verdicts
        self.final_verdict = final_verdict
        self.export_gaps = set(export_gaps)
        self.final_export_unsupported = final_export_unsupported
        self.mutate_initial_at = mutate_initial_at
        self.mutate_sequence_at = mutate_sequence_at
        self.capture_ordinals = []
        self.verifier_diagnostics = []

    def __call__(self, arguments, timeout):
        del timeout
        if arguments[0] == "capture":
            return self.capture(arguments)
        return self.verify(arguments)

    def capture(self, arguments):
        ordinal = int(arguments[arguments.index("--rbo-rule-prefix-ordinal") + 1])
        output = Path(arguments[arguments.index("--rbo-rule-prefix-output") + 1])
        self.capture_ordinals.append(ordinal)
        initial = "different" if ordinal == self.mutate_initial_at else "stable"
        (output / "initial.json").write_text(initial, encoding="utf-8")
        prefix = self.applications[:ordinal]
        if ordinal == self.mutate_sequence_at:
            prefix = [*prefix]
            prefix[0] = {**prefix[0], "rule": "changed"}
        manifest = {
            "protocol": PROTOCOL,
            "requested_ordinal": ordinal,
            "initial_snapshot": "initial.json",
            "applications": prefix,
        }
        if ordinal <= len(self.applications):
            if ordinal in self.export_gaps:
                manifest.update(
                    status="PREFIX_UNSUPPORTED",
                    unsupported_reason=f"cannot export prefix {ordinal}",
                )
            else:
                (output / "prefix.json").write_text(
                    f"prefix {ordinal}", encoding="utf-8"
                )
                manifest.update(status="PREFIX_CAPTURED", prefix_snapshot="prefix.json")
        elif self.final_export_unsupported:
            manifest.update(
                status="FINAL_UNSUPPORTED",
                unsupported_reason="cannot export final plan",
            )
        else:
            (output / "final.json").write_text("final", encoding="utf-8")
            manifest.update(status="OPTIMIZER_COMPLETE", final_snapshot="final.json")
        (output / "capture.json").write_text(json.dumps(manifest), encoding="utf-8")
        return subprocess.CompletedProcess(arguments, 0, "capture output", "")

    def verify(self, arguments):
        diagnostic = "--diagnostic-rule-prefix" in arguments
        self.verifier_diagnostics.append(diagnostic)
        Path(arguments[arguments.index("--emit-smt") + 1]).write_text(
            "(check-sat)\n", encoding="utf-8"
        )
        if diagnostic:
            ordinal = int(Path(arguments[2]).read_text(encoding="utf-8").split()[1])
            status = self.prefix_verdicts[ordinal - 1]
        else:
            status = self.final_verdict
        verdict = {"status": status, "row_bound": 2, "task_bound": 2}
        if status in {"UNSUPPORTED", "UNKNOWN"}:
            verdict["reason"] = f"diagnostic {status.lower()}"
        if diagnostic:
            verdict["comparison_scope"] = "RULE_APPLICATION_PREFIX"
        exit_code = {
            "VERIFIED_BOUNDED": 0,
            "COUNTEREXAMPLE": 1,
            "SCHEMA_MISMATCH": 1,
            "UNKNOWN": 2,
            "UNSUPPORTED": 2,
        }[status]
        output = json.dumps(verdict)
        to_stderr = status == "UNSUPPORTED"
        return subprocess.CompletedProcess(
            arguments,
            exit_code,
            "" if to_stderr else output,
            output if to_stderr else "",
        )


class SequentialLocalizationTest(unittest.TestCase):
    def config(self, root):
        return Config(
            ("capture", "--query", "q.sql"),
            Path("verify"),
            Path("z3"),
            root,
            max_applications=10,
        )

    def applications(self, count=2):
        names = ["PushFilter", "JoinAssociativity", "InlineCbo", "BuildStages"]
        return [
            {"ordinal": ordinal, "stage": "rewrite", "rule": names[ordinal - 1]}
            for ordinal in range(1, count + 1)
        ]

    def test_verified_final_does_not_inspect_transient_prefixes(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.applications(),
                ["COUNTEREXAMPLE", "UNSUPPORTED"],
                "VERIFIED_BOUNDED",
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FINAL_VERIFIED_BOUNDED")
        self.assertEqual(result["applications_checked"], 0)
        self.assertEqual(harness.capture_ordinals, [11])
        self.assertEqual(harness.verifier_diagnostics, [False])

    def test_unsupported_final_stops_before_prefix_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(self.applications(), [], "UNSUPPORTED")
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FINAL_UNSUPPORTED")
        self.assertEqual(harness.capture_ordinals, [11])
        self.assertEqual(harness.verifier_diagnostics, [False])

    def test_final_export_failure_is_reported_without_a_verifier_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.applications(),
                [],
                "VERIFIED_BOUNDED",
                final_export_unsupported=True,
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FINAL_UNSUPPORTED")
        self.assertEqual(result["final_verifier"]["source"], "SNAPSHOT_EXPORT")
        self.assertEqual(harness.verifier_diagnostics, [])

    def test_reports_exact_failure_immediately_after_a_verified_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.applications(),
                ["VERIFIED_BOUNDED", "COUNTEREXAMPLE"],
                "COUNTEREXAMPLE",
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FIRST_FAILING_PREFIX")
        self.assertEqual(result["observed_failing_application"]["ordinal"], 2)
        self.assertEqual(result["last_verified_ordinal"], 1)
        self.assertEqual(harness.capture_ordinals, [11, 1, 2])
        self.assertEqual(harness.verifier_diagnostics, [False, True, True])

    def test_continues_over_export_and_verifier_gaps_and_reports_interval(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.applications(4),
                ["VERIFIED_BOUNDED", "VERIFIED_BOUNDED", "UNKNOWN", "SCHEMA_MISMATCH"],
                "COUNTEREXAMPLE",
                export_gaps={2},
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FAILING_PREFIX_INTERVAL")
        self.assertEqual(
            result["failing_interval"],
            {"first_possible_application": 2, "observed_failing_application": 4},
        )
        self.assertEqual([gap["ordinal"] for gap in result["prefix_gaps"]], [2, 3])
        self.assertEqual(harness.capture_ordinals, [11, 1, 2, 3, 4])

    def test_trailing_gap_prevents_attributing_failure_to_global_suffix(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.applications(),
                ["VERIFIED_BOUNDED", "UNKNOWN"],
                "SCHEMA_MISMATCH",
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FAILING_INTERVAL_TO_FINAL")
        self.assertEqual(result["last_verified_ordinal"], 1)
        self.assertEqual(result["failing_interval"]["first_possible_application"], 2)
        self.assertEqual(result["failing_interval"]["observed_failing_boundary"], "FINAL")

    def test_all_verified_prefixes_isolate_the_global_suffix(self):
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = Path(temporary) / "artifacts"
            harness = Harness(
                self.applications(),
                ["VERIFIED_BOUNDED", "VERIFIED_BOUNDED"],
                "SCHEMA_MISMATCH",
            )
            result = localize(self.config(artifacts), harness)
            persisted = json.loads((artifacts / "result.json").read_text(encoding="utf-8"))
            retained = [
                (artifacts / "completion" / name).is_file()
                for name in ("capture.stdout", "verifier.stderr", "obligation.smt2")
            ]

        self.assertEqual(result["status"], "GLOBAL_SUFFIX_FAILURE")
        self.assertEqual(result["localization_region"], "GLOBAL_SUFFIX_AFTER_RULE_APPLICATIONS")
        self.assertEqual(result["last_verified_ordinal"], 2)
        self.assertEqual(persisted, result)
        self.assertEqual(retained, [True, True, True])

    def test_rejects_changed_initial_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.applications(),
                ["VERIFIED_BOUNDED", "VERIFIED_BOUNDED"],
                "COUNTEREXAMPLE",
                mutate_initial_at=2,
            )
            with self.assertRaisesRegex(LocalizationError, "initial snapshot changed"):
                localize(self.config(Path(temporary) / "artifacts"), harness)

    def test_rejects_changed_rule_sequence(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.applications(),
                ["VERIFIED_BOUNDED", "VERIFIED_BOUNDED"],
                "COUNTEREXAMPLE",
                mutate_sequence_at=2,
            )
            with self.assertRaisesRegex(LocalizationError, "rule sequence changed"):
                localize(self.config(Path(temporary) / "artifacts"), harness)


if __name__ == "__main__":
    unittest.main()
