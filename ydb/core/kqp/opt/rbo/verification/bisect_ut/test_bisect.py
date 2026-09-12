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
                "kind": kinds[ordinal - 1],
                "stage": "rewrite",
                "name": names[ordinal - 1],
            }
            for ordinal in range(1, count + 1)
        ]

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


if __name__ == "__main__":
    unittest.main()
