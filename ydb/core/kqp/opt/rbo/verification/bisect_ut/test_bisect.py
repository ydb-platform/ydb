from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from ydb.core.kqp.opt.rbo.verification.tools.bisect import localize
from ydb.core.kqp.opt.rbo.verification.tools.protocol import (
    PROTOCOL,
    Config,
    LocalizationError,
    digest,
)


class Harness:
    def __init__(
        self,
        events,
        prefix_verdicts,
        final_verdict,
        *,
        export_gaps=(),
        final_export_unsupported=False,
        mutate_initial_at=None,
        mutate_sequence_at=None,
        states=None,
        pair_verdicts=None,
        mutate_cached=False,
        verdict_overrides=None,
    ):
        self.events = events
        self.prefix_verdicts = prefix_verdicts
        self.final_verdict = final_verdict
        self.export_gaps = set(export_gaps)
        self.final_export_unsupported = final_export_unsupported
        self.mutate_initial_at = mutate_initial_at
        self.mutate_sequence_at = mutate_sequence_at
        self.capture_ordinals = []
        self.verifier_diagnostics = []
        self.verifier_pairs = []
        self.states = states
        self.pair_verdicts = pair_verdicts or {}
        self.mutate_cached = mutate_cached
        self.verdict_overrides = verdict_overrides or {}

    def __call__(self, arguments, timeout):
        del timeout
        if arguments[0] == "capture":
            return self.capture(arguments)
        return self.verify(arguments)

    def capture(self, arguments):
        ordinal = int(
            arguments[arguments.index("--rbo-transformation-prefix-ordinal") + 1]
        )
        output = Path(
            arguments[arguments.index("--rbo-transformation-prefix-output") + 1]
        )
        self.capture_ordinals.append(ordinal)
        if self.mutate_cached and ordinal == 2:
            (output.parent / "prefix-000001" / "prefix.json").write_text("prefix 2", encoding="utf-8")
        initial = "different" if ordinal == self.mutate_initial_at else "stable"
        (output / "initial.json").write_text(initial, encoding="utf-8")
        prefix = self.events[:ordinal]
        if ordinal == self.mutate_sequence_at:
            prefix = [*prefix]
            prefix[0] = {**prefix[0], "name": "changed"}
        manifest = {
            "protocol": PROTOCOL,
            "requested_ordinal": ordinal,
            "initial_snapshot": "initial.json",
            "events": prefix,
        }
        if ordinal <= len(self.events):
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
        pair = "--diagnostic-transformation-pair" in arguments
        diagnostic = pair or "--diagnostic-transformation-prefix" in arguments
        self.verifier_diagnostics.append(diagnostic)
        Path(arguments[arguments.index("--emit-smt") + 1]).write_text(
            "(check-sat)\n", encoding="utf-8"
        )
        if pair:
            anchor = Path(arguments[arguments.index("--diagnostic-observation-snapshot") + 1])
            if anchor.read_text(encoding="utf-8") != "stable":
                raise AssertionError("pair comparison lost its initial observation anchor")

            def ordinal(filename):
                text = Path(filename).read_text(encoding="utf-8")
                return 0 if text == "stable" else len(self.events) + 1 if text == "final" else int(text.split()[1])
            left, right = ordinal(arguments[1]), ordinal(arguments[2])
            self.verifier_pairs.append((left, right))
            status = self.pair_verdicts.get((left, right)) or (
                "VERIFIED_BOUNDED" if self.states[left] == self.states[right] else "COUNTEREXAMPLE"
            )
        elif diagnostic:
            ordinal = int(Path(arguments[2]).read_text(encoding="utf-8").split()[1])
            status = self.prefix_verdicts[ordinal - 1]
        else:
            status = self.final_verdict
        verdict = {"status": status, "row_bound": 2, "task_bound": 2}
        if status in {"UNSUPPORTED", "UNKNOWN"}:
            verdict["reason"] = f"diagnostic {status.lower()}"
        if diagnostic:
            verdict["comparison_scope"] = "OPTIMIZER_TRANSFORMATION_PAIR" if pair else "OPTIMIZER_TRANSFORMATION_PREFIX"
        if pair:
            verdict.update(observation_snapshot_sha256=digest(anchor), observation_kind="bag")
        verdict.update(self.verdict_overrides)
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
            max_events=10,
            strategy="sequential",
        )

    def events(self, count=2):
        names = ["PushFilter", "Constant folding", "InlineCbo", "Hash propagation"]
        kinds = [
            "RULE_APPLICATION",
            "ATOMIC_STAGE_COMMIT",
            "RULE_APPLICATION",
            "ATOMIC_STAGE_COMMIT",
        ]
        return [
            {
                "ordinal": ordinal,
                "kind": kinds[(ordinal - 1) % len(kinds)],
                "stage": "rewrite",
                "name": names[(ordinal - 1) % len(names)],
            }
            for ordinal in range(1, count + 1)
        ]

    def test_capture_failure_retains_diagnostic_tail_and_complete_artifacts(self):
        for stderr in ("", "initial snapshot is unsupported: Void count carrier",
                       "prefix log\n" * 1000 + "Cannot execute shared Limit with channel spilling disabled"):
            with self.subTest(stderr_length=len(stderr)), tempfile.TemporaryDirectory() as temporary:
                config = self.config(Path(temporary) / "artifacts")

                def failed_capture(arguments, timeout):
                    del timeout
                    return subprocess.CompletedProcess(arguments, 2, "capture output", stderr)

                with self.assertRaises(LocalizationError) as raised:
                    localize(config, failed_capture)
                message = str(raised.exception)
                self.assertIn("ordinal 11 with exit code 2", message)
                self.assertLess(len(message), 4300)
                if stderr:
                    self.assertIn(stderr.strip()[-4096:], message)
                else:
                    self.assertNotIn("stderr tail", message)
                self.assertEqual((config.artifacts / "completion/capture.stderr").read_text(), stderr)
                self.assertEqual((config.artifacts / "completion/capture.stdout").read_text(), "capture output")

    def test_verified_final_does_not_inspect_transient_prefixes(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.events(),
                ["COUNTEREXAMPLE", "UNSUPPORTED"],
                "VERIFIED_BOUNDED",
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FINAL_VERIFIED_BOUNDED")
        self.assertEqual(result["events_checked"], 0)
        self.assertEqual(harness.capture_ordinals, [11])
        self.assertEqual(harness.verifier_diagnostics, [False])

    def test_unsupported_final_stops_before_prefix_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(self.events(), [], "UNSUPPORTED")
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FINAL_UNSUPPORTED")
        self.assertEqual(harness.capture_ordinals, [11])
        self.assertEqual(harness.verifier_diagnostics, [False])

    def test_final_export_failure_is_reported_without_a_verifier_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.events(),
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
                self.events(),
                ["VERIFIED_BOUNDED", "COUNTEREXAMPLE"],
                "COUNTEREXAMPLE",
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FIRST_FAILING_PREFIX")
        self.assertEqual(result["observed_failing_event"]["ordinal"], 2)
        self.assertEqual(
            result["observed_failing_event"]["kind"], "ATOMIC_STAGE_COMMIT"
        )
        self.assertEqual(result["last_verified_ordinal"], 1)
        self.assertEqual(harness.capture_ordinals, [11, 1, 2])
        self.assertEqual(harness.verifier_diagnostics, [False, True, True])

    def test_continues_over_export_and_verifier_gaps_and_reports_interval(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.events(4),
                ["VERIFIED_BOUNDED", "VERIFIED_BOUNDED", "UNKNOWN", "SCHEMA_MISMATCH"],
                "COUNTEREXAMPLE",
                export_gaps={2},
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FAILING_PREFIX_INTERVAL")
        self.assertEqual(
            result["failing_interval"],
            {"first_possible_event": 2, "observed_failing_event": 4},
        )
        self.assertEqual([gap["ordinal"] for gap in result["prefix_gaps"]], [2, 3])
        self.assertEqual(harness.capture_ordinals, [11, 1, 2, 3, 4])

    def test_trailing_gap_prevents_attributing_failure_to_global_suffix(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.events(),
                ["VERIFIED_BOUNDED", "UNKNOWN"],
                "SCHEMA_MISMATCH",
            )
            result = localize(self.config(Path(temporary) / "artifacts"), harness)

        self.assertEqual(result["status"], "FAILING_INTERVAL_TO_FINAL")
        self.assertEqual(result["last_verified_ordinal"], 1)
        self.assertEqual(result["failing_interval"]["first_possible_event"], 2)
        self.assertEqual(result["failing_interval"]["observed_failing_boundary"], "FINAL")

    def test_all_verified_prefixes_isolate_the_global_suffix(self):
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = Path(temporary) / "artifacts"
            harness = Harness(
                self.events(),
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
        self.assertEqual(
            result["localization_region"], "GLOBAL_SUFFIX_AFTER_TRANSFORMATIONS"
        )
        self.assertEqual(result["last_verified_ordinal"], 2)
        self.assertEqual(persisted, result)
        self.assertEqual(retained, [True, True, True])

    def test_rejects_changed_initial_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.events(),
                ["VERIFIED_BOUNDED", "VERIFIED_BOUNDED"],
                "COUNTEREXAMPLE",
                mutate_initial_at=2,
            )
            with self.assertRaisesRegex(LocalizationError, "initial snapshot changed"):
                localize(self.config(Path(temporary) / "artifacts"), harness)

    def test_rejects_changed_transformation_sequence(self):
        with tempfile.TemporaryDirectory() as temporary:
            harness = Harness(
                self.events(),
                ["VERIFIED_BOUNDED", "VERIFIED_BOUNDED"],
                "COUNTEREXAMPLE",
                mutate_sequence_at=2,
            )
            with self.assertRaisesRegex(
                LocalizationError, "transformation sequence changed"
            ):
                localize(self.config(Path(temporary) / "artifacts"), harness)

    def test_rejects_malformed_event_kind(self):
        for malformed in ("UNKNOWN", []):
            with self.subTest(kind=malformed), tempfile.TemporaryDirectory() as temporary:
                events = self.events()
                events[0]["kind"] = malformed
                harness = Harness(events, [], "VERIFIED_BOUNDED")
                with self.assertRaisesRegex(LocalizationError, "event 1 has an invalid kind"):
                    localize(self.config(Path(temporary) / "artifacts"), harness)

    def test_midpoint_first_finds_multiple_changes_inside_equivalent_interval(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "artifacts"
            config = replace(self.config(root), strategy="divide-and-conquer")
            harness = Harness(self.events(4), [], "COUNTEREXAMPLE", states=[0, 1, 0, 2, 2, 2])
            result = localize(config, harness)
            self.assertEqual(json.loads((root / "result.json").read_text()), result)
            for owner in [*result["boundaries"], *result["comparisons"]]:
                for artifact in owner["artifacts"].values():
                    self.assertEqual(digest(root / artifact["path"]), artifact["sha256"])

        self.assertEqual(harness.capture_ordinals, [11, 2, 1, 3, 4])
        self.assertEqual(harness.verifier_pairs, [(0, 2), (2, 5), (0, 1), (1, 2), (2, 3), (3, 5), (3, 4), (4, 5)])
        self.assertEqual([finding["event"]["ordinal"] for finding in result["findings"]], [1, 2, 3])
        self.assertEqual(result["status"], "LOCALIZED_FAILURES")
        self.assertEqual(result["completeness"], "COMPLETE")
        self.assertFalse(result["gaps"])

    def test_adjacent_mode_finds_cancelled_changes_without_coarse_proofs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "artifacts"
            config = replace(self.config(root), strategy="adjacent", all_steps=True)
            harness = Harness(self.events(4), [], "VERIFIED_BOUNDED", states=[0, 1, 0, 2, 0, 0])
            result = localize(config, harness)
            for owner in [*result["boundaries"], *result["comparisons"]]:
                for artifact in owner["artifacts"].values():
                    self.assertEqual(digest(root / artifact["path"]), artifact["sha256"])
        self.assertEqual(harness.capture_ordinals, [11, 1, 2, 3, 4])
        self.assertEqual(harness.verifier_pairs, [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)])
        self.assertEqual([finding["event"]["ordinal"] for finding in result["findings"]], [1, 2, 3, 4])
        self.assertEqual(result["strategy"], "adjacent")
        self.assertEqual(result["events_checked"], 4)
        self.assertEqual(result["status"], "LOCALIZED_FAILURES")
        self.assertEqual(result["completeness"], "COMPLETE")

    def test_adjacent_mode_retains_gaps_and_detects_final_suffix(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = replace(self.config(Path(temporary) / "artifacts"), strategy="adjacent")
            harness = Harness(self.events(4), [], "COUNTEREXAMPLE", states=[0, 1, 2, 3, 3, 4],
                              export_gaps={2}, pair_verdicts={(3, 4): "UNKNOWN"})
            result = localize(config, harness)
        self.assertEqual(result["findings"][0]["event"]["ordinal"], 1)
        self.assertEqual(result["findings"][1], {"comparison": "4:5", "status": "COUNTEREXAMPLE",
                                                 "region": "GLOBAL_SUFFIX_AFTER_TRANSFORMATIONS"})
        self.assertEqual(result["completeness"], "GAPS")
        self.assertEqual([gap["comparison"] for gap in result["gaps"]], ["1:2", "2:3", "3:4"])
        self.assertEqual(harness.verifier_pairs, [(0, 1), (3, 4), (4, 5)])

    def test_adjacent_mode_keeps_snapshot_and_observation_guards(self):
        for mutation in ("initial", "sequence", "cached", "observation"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                config = replace(self.config(Path(temporary) / "artifacts"), strategy="adjacent", all_steps=True)
                harness = Harness(self.events(4), [], "VERIFIED_BOUNDED", states=[0] * 6,
                                  mutate_initial_at=1 if mutation == "initial" else None,
                                  mutate_sequence_at=1 if mutation == "sequence" else None,
                                  mutate_cached=mutation == "cached",
                                  verdict_overrides={"observation_snapshot_sha256": "0" * 64}
                                  if mutation == "observation" else None)
                with self.assertRaisesRegex(LocalizationError, "snapshot changed|sequence changed|observation"):
                    localize(config, harness)

    def test_adjacent_mode_handles_empty_traces_and_explicit_all_steps(self):
        for count in (0, 1, 8):
            for all_steps in (False, True):
                with self.subTest(count=count, all_steps=all_steps), tempfile.TemporaryDirectory() as temporary:
                    config = replace(self.config(Path(temporary) / "artifacts"), strategy="adjacent", all_steps=all_steps)
                    harness = Harness(self.events(count), [], "VERIFIED_BOUNDED", states=[0] * (count + 2))
                    result = localize(config, harness)
                    self.assertEqual(result["status"], "STEPS_VERIFIED_BOUNDED" if all_steps else "FINAL_VERIFIED_BOUNDED")
                    expected = count + 2 if all_steps and count else 1
                    self.assertEqual(len(harness.verifier_diagnostics), expected)

    def test_adjacent_audit_continues_when_final_snapshot_cannot_be_exported(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = replace(self.config(Path(temporary) / "artifacts"), strategy="adjacent", all_steps=True)
            harness = Harness(self.events(4), [], "UNSUPPORTED", states=[0, 0, 1, 1, 1, 1],
                              final_export_unsupported=True)
            result = localize(config, harness)
        self.assertEqual(harness.verifier_pairs, [(0, 1), (1, 2), (2, 3), (3, 4)])
        self.assertEqual(result["events_checked"], 4)
        self.assertEqual(result["findings"][0]["event"]["ordinal"], 2)
        self.assertEqual(result["completeness"], "GAPS")
        self.assertEqual([gap["comparison"] for gap in result["gaps"]], ["0:5", "4:5"])

    def test_complete_step_proofs_do_not_hide_a_contradictory_final_failure(self):
        for strategy in ("adjacent", "divide-and-conquer"):
            for status in ("COUNTEREXAMPLE", "SCHEMA_MISMATCH"):
                with self.subTest(strategy=strategy, status=status), tempfile.TemporaryDirectory() as temporary:
                    config = replace(self.config(Path(temporary) / "artifacts"), strategy=strategy)
                    harness = Harness(self.events(), [], status, states=[0, 0, 0, 0])
                    result = localize(config, harness)
                    self.assertEqual(result["status"], "LOCALIZATION_INCONSISTENT")
                    self.assertEqual(result["inconsistencies"][0]["comparison"], "0:3")
                    self.assertEqual(result["inconsistencies"][0]["verified_chain"], ["0:1", "1:2", "2:3"])
                    self.assertEqual(result["findings"], [])
                    self.assertEqual(result["completeness"], "COMPLETE")

    def test_interval_consistency_requires_every_adjacent_proof(self):
        for missing in (False, True):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as temporary:
                config = replace(self.config(Path(temporary) / "artifacts"),
                                 strategy="divide-and-conquer", all_steps=True)
                verdicts = {(1, 3): "COUNTEREXAMPLE"}
                if missing:
                    verdicts[1, 2] = "UNKNOWN"
                harness = Harness(self.events(), [], "VERIFIED_BOUNDED", states=[0, 0, 0, 0],
                                  pair_verdicts=verdicts)
                result = localize(config, harness)
                self.assertEqual(result["status"], "LOCALIZATION_INCOMPLETE" if missing else "LOCALIZATION_INCONSISTENT")
                self.assertEqual([item["comparison"] for item in result["inconsistencies"]],
                                 [] if missing else ["1:3"])

    def test_adjacent_cli_retains_explicit_audit_and_existing_default(self):
        from ydb.core.kqp.opt.rbo.verification.tools.cli import parser

        arguments = ["--verifier", "verify", "--solver", "z3", "--artifacts", "out"]
        options = parser().parse_args(arguments + ["--strategy", "adjacent", "--all-steps", "--", "capture"])
        self.assertEqual(options.strategy, "adjacent")
        self.assertTrue(options.all_steps)
        self.assertEqual(options.capture_command, ["--", "capture"])
        default = parser().parse_args(arguments + ["--", "capture"])
        self.assertEqual(default.strategy, "divide-and-conquer")
        self.assertFalse(default.all_steps)

    def test_all_steps_is_explicit_and_detects_cancelled_final_failure(self):
        for all_steps in (False, True):
            with self.subTest(all_steps=all_steps), tempfile.TemporaryDirectory() as temporary:
                config = replace(self.config(Path(temporary) / "artifacts"), strategy="divide-and-conquer", all_steps=all_steps)
                harness = Harness(self.events(), [], "VERIFIED_BOUNDED", states=[0, 1, 0, 0])
                result = localize(config, harness)
            if all_steps:
                self.assertEqual([finding["event"]["ordinal"] for finding in result["findings"]], [1, 2])
            else:
                self.assertEqual(result["status"], "FINAL_VERIFIED_BOUNDED")
                self.assertEqual(harness.capture_ordinals, [11])

    def test_exhaustive_gaps_do_not_blame_rules_or_skip_other_intervals(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = replace(self.config(Path(temporary) / "artifacts"), strategy="divide-and-conquer")
            harness = Harness(self.events(4), [], "COUNTEREXAMPLE", states=[0, 1, 2, 3, 3, 3],
                              export_gaps={2}, pair_verdicts={(3, 4): "UNKNOWN"})
            result = localize(config, harness)
        self.assertEqual([finding["event"]["ordinal"] for finding in result["findings"]], [1])
        self.assertEqual(result["completeness"], "GAPS")
        self.assertEqual([gap["comparison"] for gap in result["gaps"] if gap["adjacent"]], ["1:2", "2:3", "3:4"])
        self.assertIn((4, 5), harness.verifier_pairs)

    def test_exhaustive_capture_binding_and_final_suffix(self):
        for mutation in (None, "initial", "sequence", "cached"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                config = replace(self.config(Path(temporary) / "artifacts"), strategy="divide-and-conquer")
                harness = Harness(self.events(), [], "COUNTEREXAMPLE", states=[0, 0, 0, 1],
                                  mutate_initial_at=1 if mutation == "initial" else None,
                                  mutate_sequence_at=1 if mutation == "sequence" else None,
                                  mutate_cached=mutation == "cached")
                if mutation:
                    with self.assertRaisesRegex(LocalizationError, "snapshot changed|sequence changed"):
                        localize(config, harness)
                else:
                    result = localize(config, harness)
                    self.assertEqual(result["findings"], [{"comparison": "2:3", "status": "COUNTEREXAMPLE",
                                                           "region": "GLOBAL_SUFFIX_AFTER_TRANSFORMATIONS"}])

    def test_rejects_mismatched_or_invalid_verifier_bounds(self):
        for field, value in (("row_bound", 0), ("task_bound", 3), ("row_bound", None), ("task_bound", True)):
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as temporary:
                harness = Harness(self.events(), [], "VERIFIED_BOUNDED", verdict_overrides={field: value})
                with self.assertRaisesRegex(LocalizationError, field):
                    localize(self.config(Path(temporary) / "artifacts"), harness)

    def test_pair_requires_original_observation_receipt(self):
        for overrides in ({"observation_snapshot_sha256": "0" * 64}, {"observation_kind": None}):
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as temporary:
                config = replace(self.config(Path(temporary) / "artifacts"), strategy="divide-and-conquer")
                harness = Harness(self.events(), [], "COUNTEREXAMPLE", states=[0, 0, 0, 1],
                                  verdict_overrides=overrides)
                with self.assertRaisesRegex(LocalizationError, "observation"):
                    localize(config, harness)

    def test_comparison_deadline_includes_construction_and_solver_budgets(self):
        calls = []
        harness = Harness(self.events(), [], "VERIFIED_BOUNDED")

        def run(arguments, timeout):
            calls.append((arguments, timeout))
            return harness(arguments, timeout)
        with tempfile.TemporaryDirectory() as temporary:
            config = replace(self.config(Path(temporary) / "artifacts"),
                             timeout_ms=17_342, construction_timeout_ms=123_456)
            result = localize(config, run)
        self.assertEqual(result["status"], "FINAL_VERIFIED_BOUNDED")
        arguments, timeout = next(call for call in calls if call[0][0] == "verify")
        self.assertEqual(arguments[arguments.index("--construction-timeout-ms") + 1], "123456")
        self.assertEqual(arguments[arguments.index("--timeout-ms") + 1], "17342")
        self.assertEqual(timeout, 171)

    def test_construction_timeout_is_a_gap_before_observation_kind_is_known(self):
        class ConstructionTimeoutHarness(Harness):
            def verify(self, arguments):
                process = super().verify(arguments)
                if "--diagnostic-transformation-pair" not in arguments:
                    return process
                verdict = json.loads(process.stdout)
                verdict.update(status="UNKNOWN", phase="construction", reason="construction deadline exceeded",
                               construction_timeout_ms=120_000)
                verdict.pop("observation_kind")
                Path(arguments[arguments.index("--emit-smt") + 1]).unlink()
                return subprocess.CompletedProcess(arguments, 2, json.dumps(verdict), "")
        harness = ConstructionTimeoutHarness(self.events(4), [], "VERIFIED_BOUNDED", states=[0] * 6)
        with tempfile.TemporaryDirectory() as temporary:
            config = replace(self.config(Path(temporary) / "artifacts"),
                             strategy="divide-and-conquer", all_steps=True)
            result = localize(config, harness)
        self.assertEqual(result["status"], "LOCALIZATION_INCOMPLETE")
        self.assertEqual(result["completeness"], "GAPS")
        self.assertIsNone(result["observation_kind"])
        self.assertEqual(result["findings"], [])
        self.assertEqual(result["events_checked"], 4)
        self.assertEqual({(a, b) for a, b in harness.verifier_pairs if b == a + 1},
                         {(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)})

    def test_unknown_without_observation_still_requires_exact_construction_receipt(self):
        for phase, reported_budget in (("solver", 120_000), ("construction", 1),
                                       ("construction", None), ("construction", True)):
            class MissingReceiptHarness(Harness):
                def verify(self, arguments):
                    process = super().verify(arguments)
                    if "--diagnostic-transformation-pair" not in arguments:
                        return process
                    verdict = json.loads(process.stdout)
                    verdict.update(status="UNKNOWN", phase=phase, reason="timeout",
                                   construction_timeout_ms=reported_budget)
                    verdict.pop("observation_kind")
                    return subprocess.CompletedProcess(arguments, 2, json.dumps(verdict), "")
            harness = MissingReceiptHarness(self.events(), [], "VERIFIED_BOUNDED", states=[0] * 4)
            with self.subTest(phase=phase, budget=reported_budget), tempfile.TemporaryDirectory() as temporary:
                config = replace(self.config(Path(temporary) / "artifacts"),
                                 strategy="divide-and-conquer", all_steps=True)
                with self.assertRaisesRegex(LocalizationError, "observation|construction timeout"):
                    localize(config, harness)

    def test_rejects_invalid_construction_budgets(self):
        for budget in (0, -1, True, None):
            with self.subTest(budget=budget), tempfile.TemporaryDirectory() as temporary:
                config = replace(self.config(Path(temporary) / "artifacts"), construction_timeout_ms=budget)
                with self.assertRaisesRegex(LocalizationError, "construction timeout"):
                    localize(config, Harness(self.events(), [], "VERIFIED_BOUNDED"))


if __name__ == "__main__":
    unittest.main()
