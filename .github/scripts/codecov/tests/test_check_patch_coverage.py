#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import check_patch_coverage as gate

BASE = "a" * 40
HEAD = "b" * 40
CLI = "ydb/public/lib/ydb_cli/main.cpp"
SDK = "ydb/public/sdk/cpp/main.cpp"
UPLOAD = "cli-123-1"
ROOT = "/runner/ydb"


def lcov(kinds, path=CLI):
    rows = ["TN:", f"SF:{ROOT}/{path}"]
    for line, kind in enumerate(kinds, 1):
        rows.append(f"DA:{line},{int(kind != 'M')}")
        if kind == "P":
            rows += [f"BRDA:{line},0,0,1", f"BRDA:{line},0,1,0"]
    return "\n".join([*rows, f"LF:{len(kinds)}", "end_of_record", ""])


def save_upload(directory, kinds, attempt=1, path=CLI):
    coverage = directory / f"upload-{attempt}.lcov"
    coverage.write_text(lcov(kinds, path), encoding="utf-8")
    receipt = directory / f"receipt-{attempt}.json"
    receipt.write_text(json.dumps({
        "version": 2, "head_sha": HEAD, "flag": "cli", "run_id": "123",
        "run_attempt": str(attempt), "upload_name": f"cli-123-{attempt}",
        "source_root": ROOT, "report_sha256": hashlib.sha256(coverage.read_bytes()).hexdigest(),
    }))
    return receipt, coverage


def report(kinds):
    return {"totals": {"lineCount": len(kinds)}, "coverage": [{"line": number, "coverage": kind} for number, kind in enumerate(kinds, 1)]}


def policy_config(target="80%", tolerance="0%"):
    return {"flag_management": {"individual_flags": [
        {"name": flag, "statuses": [{"type": "patch", "target": target, "threshold": tolerance}]}
        for flag in gate.SUITES
    ]}}


class PatchPolicyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "policy.json"

    def read(self, data, flag="cli"):
        self.path.write_text(json.dumps(data))
        return gate.load_patch_policy(self.path, flag)

    def test_numeric_percentages_and_tolerance_come_from_policy(self):
        for target in (85, 85.0, "85", "85%", "85.0%"):
            with self.subTest(target=target):
                policy = self.read(policy_config(target, "2.5%"))
                self.assertEqual(policy.minimum, Decimal("82.5"))
                self.assertIn("tolerance 2.5 percentage points", policy.description())
        self.assertEqual(self.read(policy_config("5%", "10%")).minimum, Decimal(0))

    def test_each_flag_uses_its_own_resolved_policy(self):
        config = policy_config()
        for index, item in enumerate(config["flag_management"]["individual_flags"]):
            item["statuses"][0]["target"] = 70 + index
        for index, flag in enumerate(gate.SUITES):
            self.assertEqual(self.read(config, flag).minimum, Decimal(70 + index))

    def test_unknown_or_ambiguous_policies_fail_without_default(self):
        config = policy_config()
        flag = next(item for item in config["flag_management"]["individual_flags"] if item["name"] == "cli")
        for data in (None, [], {}, {"flag_management": None},
                     {"flag_management": {"individual_flags": {}}},
                     {"flag_management": {"individual_flags": []}},
                     {"flag_management": {"individual_flags": [None]}},
                     {"flag_management": {"individual_flags": [flag, flag]}}):
            with self.subTest(data=data), self.assertRaises(gate.CoverageError):
                self.read(data)
        for statuses in (None, {}, [], [None], [{"type": "project", "target": 80}],
                         [flag["statuses"][0]] * 2):
            with self.subTest(statuses=statuses), self.assertRaises(gate.CoverageError):
                self.read({"flag_management": {"individual_flags": [{"name": "cli", "statuses": statuses}]}})

    def test_missing_or_invalid_percentages_fail_without_default(self):
        for field in ("target", "threshold"):
            for value in (None, True, False, [], {}, "auto", "NaN", "Infinity", "-1%", "101%", "80%%"):
                config = policy_config()
                for item in config["flag_management"]["individual_flags"]:
                    item["statuses"][0][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(gate.CoverageError):
                    self.read(config)
            config = policy_config()
            for item in config["flag_management"]["individual_flags"]:
                del item["statuses"][0][field]
            with self.subTest(field=field), self.assertRaises(gate.CoverageError):
                self.read(config)

    def test_missing_malformed_or_oversized_policy_fails(self):
        with self.assertRaises(gate.CoverageError):
            gate.load_patch_policy(self.path, "cli")
        self.path.write_text("not JSON")
        with self.assertRaises(gate.CoverageError):
            gate.load_patch_policy(self.path, "cli")
        with mock.patch.object(gate, "MAX_POLICY_BYTES", 10), self.assertRaises(gate.CoverageError):
            self.read(policy_config())

    def test_editing_one_yaml_target_updates_all_flags_and_the_gate(self):
        source = Path(__file__).resolve().parents[3] / "codecov.yml"
        yaml = source.read_text()
        self.assertEqual(yaml.count("target: 80%"), 1)
        for text, expected in ((yaml, Decimal(80)), (yaml.replace("target: 80%", "target: 90%"), Decimal(90))):
            parsed = subprocess.run(
                ["ruby", "-rjson", "-ryaml", "-e",
                 "puts JSON.generate(YAML.safe_load(STDIN.read, aliases: true))"],
                input=text, check=True, capture_output=True, text=True,
            )
            for flag in gate.SUITES:
                policy = self.read(json.loads(parsed.stdout), flag)
                self.assertEqual(policy.minimum, expected)
                self.assertEqual(gate.Totals(8, 2, 0).passes(policy.minimum), expected == 80)


class PatchTotalsTest(unittest.TestCase):
    def test_exactly_eighty_passes_and_partial_lines_do_not_count_as_hits(self):
        totals = gate.Totals()
        totals.add_file([[1, 10]], report("HHHHHHHHMP"))
        self.assertEqual((totals.hits, totals.misses, totals.partials), (8, 1, 1))
        self.assertTrue(totals.passes(Decimal(80)))
        self.assertFalse(totals.passes(Decimal("80.00001")))

    def test_below_eighty_does_not_pass_after_rounding(self):
        self.assertFalse(gate.Totals(7999999, 2000001, 0).passes(Decimal(80)))

    def test_only_changed_lines_are_counted(self):
        totals = gate.Totals()
        totals.add_file([[2, 3], [6, 6]], report("HHMPPMHH"))
        self.assertEqual((totals.hits, totals.misses, totals.partials), (1, 2, 0))

    def test_no_coverable_lines_is_not_applicable_not_one_hundred_percent(self):
        totals = gate.Totals()
        totals.add_file([[10, 20]], report("HHM"))
        self.assertTrue(totals.passes(Decimal(80)))
        self.assertIn("not applicable", totals.description("cli", Decimal(80)))
        self.assertNotIn("100", totals.description("cli", Decimal(80)))

    def test_absent_file_is_explicit_and_does_not_add_hits(self):
        totals = gate.Totals()
        self.assertFalse(totals.add_file([[1, 20]], {"totals": None, "coverage": []}))
        self.assertEqual(totals.lines, 0)

    def test_truncated_annotations_are_not_a_false_not_applicable(self):
        with self.assertRaises(gate.CoverageError):
            gate.Totals().add_file([[1, 20]], {"totals": {"lineCount": 4}, "coverage": []})

    def test_malformed_annotations_fail_closed(self):
        cases = [None, {}, {"coverage": None}, {"totals": None, "coverage": [{"line": 1, "coverage": "H"}]}]
        for annotation in ({"line": 0, "coverage": "H"}, {"line": True, "coverage": "H"}, {"line": 1, "coverage": None}, {"line": 1, "coverage": "unknown"}):
            cases.append({"totals": {"lineCount": 1}, "coverage": [annotation]})
        cases.append({"totals": {"lineCount": 2}, "coverage": [{"line": 1, "coverage": "H"}] * 2})
        for case in cases:
            with self.subTest(case=case), self.assertRaises(gate.CoverageError):
                gate.Totals().add_file([[1, 20]], case)

    def test_verified_pr_54168_fixture_uses_original_patch_not_cached_flag_comparison(self):
        # On this actual PR the cached per-flag comparison contained only two
        # lines. HEAD H/M/P annotations intersected with the original PR diff
        # match the bot comment exactly: 647 hits / 705 coverable changed lines.
        totals = gate.Totals()
        totals.add_file([[1, 705]], report("H" * 647 + "M" * 20 + "P" * 38))
        self.assertEqual(totals.lines, 705)
        self.assertIn("91.77305%", totals.description("cli", Decimal(80)))


class ManifestTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "patch.json"

    def read(self, files, **overrides):
        data = {"version": 1, "base_sha": BASE, "head_sha": HEAD, "files": files, **overrides}
        self.path.write_text(json.dumps(data), encoding="utf-8")
        return gate.load_changed_lines(self.path, BASE, HEAD, "cli")

    def test_only_requested_component_is_selected(self):
        self.assertEqual(self.read({CLI: [[2, 5]], SDK: [[1, 3]]}), {CLI: [[2, 5]]})

    def test_stale_base_and_head_are_rejected(self):
        for field in ("base_sha", "head_sha"):
            with self.subTest(field=field), self.assertRaises(gate.CoverageError):
                self.read({CLI: [[1, 2]]}, **{field: "c" * 40})

    def test_malformed_ranges_fail_closed(self):
        for ranges in ([], [[0, 2]], [[2, 1]], [[1, 2], [2, 3]], [[1, 2], [1, 1]], [[1, True]], [[1, 2**31]], "1-2"):
            with self.subTest(ranges=ranges), self.assertRaises(gate.CoverageError):
                self.read({CLI: ranges})

    def test_unowned_traversal_and_excluded_paths_are_rejected(self):
        for path in ("other/file.cpp", "ydb/public/lib/ydb_cli/../secret", "ydb/public/lib/ydb_cli/tests/test.cpp", "ydb/public/lib/ydb_cli/foo_ut.cpp"):
            with self.subTest(path=path), self.assertRaises(gate.CoverageError):
                self.read({path: [[1, 2]]})

    def test_quoted_unicode_and_newline_paths_are_lossless(self):
        path = 'ydb/public/lib/ydb_cli/a "имя"\n.cpp'
        self.assertEqual(self.read({path: [[1, 3]]}), {path: [[1, 3]]})

    def test_oversized_manifest_is_not_truncated(self):
        with mock.patch.object(gate, "MAX_FILES", 1), self.assertRaises(gate.CoverageError):
            self.read({CLI: [[1, 3]], SDK: [[1, 3]]})

    def test_manifest_byte_limit(self):
        with mock.patch.object(gate, "MAX_MANIFEST_BYTES", 10), self.assertRaises(gate.CoverageError):
            self.read({CLI: [[1, 3]]})


class CollectTest(unittest.TestCase):
    def test_added_replaced_deleted_and_binary_hunks(self):
        names = b"M\0" + CLI.encode() + b"\0A\0" + SDK.encode() + b"\0"
        patch = b"@@ -1,2 +1,0 @@\n-old\n-old2\n@@ -5 +4,2 @@\n-old\n+new\n+new2\n@@ -10,0 +11 @@\n+added\n"
        with mock.patch.object(gate, "git", side_effect=[names, patch, b"Binary files differ\n"]) as run:
            result = gate.collect(BASE, HEAD)
        self.assertEqual(result["files"], {CLI: [[4, 5], [11, 11]]})
        for call in run.call_args_list:
            self.assertIn("--no-ext-diff", call.args)
            self.assertIn("--no-textconv", call.args)

    def test_deleted_and_test_only_files_are_not_measured(self):
        with mock.patch.object(gate, "git", return_value=b"M\0ydb/public/lib/ydb_cli/tests/test.cpp\0") as run:
            self.assertEqual(gate.collect(BASE, HEAD)["files"], {})
            self.assertEqual(run.call_count, 1)

    def test_renamed_blob_is_isolated_from_replacement_at_old_path(self):
        new_path = CLI.replace("main", "renamed")
        names = b"R090\0" + CLI.encode() + b"\0" + new_path.encode() + b"\0A\0" + CLI.encode() + b"\0"
        with mock.patch.object(gate, "git", side_effect=[names, BASE.encode(), b"@@ -3 +3 @@\n-old\n+new\n", b"@@ -0,0 +1,2 @@\n+one\n+two\n"]) as run:
            result = gate.collect(BASE, HEAD)
        self.assertEqual(result["files"], {new_path: [[3, 3]], CLI: [[1, 2]]})
        rename_args = run.call_args_list[2].args
        self.assertIn(f"{BASE}:{CLI}", rename_args)
        self.assertIn(f"{HEAD}:{new_path}", rename_args)

    def test_real_git_renames_and_unusual_paths(self):
        # All commits are private disposable test fixtures, not the user's repo.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def run(*args):
                return subprocess.run(["git", "-C", directory, *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout

            run("init", "-q")
            run("config", "user.name", "Coverage helper test")
            run("config", "user.email", "coverage-test@example.invalid")
            run("config", "core.hooksPath", "/dev/null")
            folder = root / "ydb/public/lib/ydb_cli"
            folder.mkdir(parents=True)
            original = "".join(f"line {i}\n" for i in range(30))
            (folder / "old.cpp").write_text(original)
            (folder / "unchanged.cpp").write_text("another file\n" * 20)
            (folder / "deleted.cpp").write_text("delete me\n")
            run("add", ".")
            run("commit", "-qm", "base fixture")
            base = run("rev-parse", "HEAD").decode().strip()
            new_name = 'new "имя"\n.cpp'
            (folder / "old.cpp").rename(folder / new_name)
            (folder / new_name).write_text(original.replace("line 12\n", "changed line\n"))
            (folder / "unchanged.cpp").rename(folder / "renamed.cpp")
            (folder / "deleted.cpp").unlink()
            (folder / "binary.cpp").write_bytes(b"\0\0\1\2")
            run("add", ".")
            run("commit", "-qm", "head fixture")
            head = run("rev-parse", "HEAD").decode().strip()
            with mock.patch.object(gate, "git", side_effect=run):
                result = gate.collect(base, head)
            self.assertEqual(result["files"], {f"ydb/public/lib/ydb_cli/{new_name}": [[13, 13]]})


class UploadReceiptTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "receipt.json"

    def read(self, **overrides):
        data = {
            "version": 2, "head_sha": HEAD, "flag": "cli", "run_id": "123",
            "source_root": ROOT, "report_sha256": "c" * 64,
            "run_attempt": "1", "upload_name": UPLOAD, **overrides,
        }
        self.path.write_text(json.dumps(data), encoding="utf-8")
        return gate.load_upload_receipt(self.path, HEAD, "cli", 123)

    def test_receipt_binds_the_exact_successful_suite_attempt(self):
        self.assertEqual(self.read().name, UPLOAD)
        self.assertEqual(self.read(run_attempt="2", upload_name="cli-123-2").name, "cli-123-2")

    def test_older_successful_receipt_is_valid_when_only_other_jobs_rerun(self):
        # No comparison with GITHUB_RUN_ATTEMPT: successful coverage jobs need
        # not run again when GitHub reruns a failed patch checker or other suite.
        self.assertEqual(self.read(run_attempt="1").name, UPLOAD)

    def test_stale_head_wrong_flag_and_other_run_are_rejected(self):
        for override in ({"head_sha": BASE}, {"flag": "cpp_sdk"}, {"run_id": "124"}, {"run_id": 123}):
            with self.subTest(override=override), self.assertRaises(gate.CoverageError):
                self.read(**override)

    def test_invalid_attempt_name_and_version_are_rejected(self):
        for override in ({"run_attempt": "0"}, {"run_attempt": 1}, {"run_attempt": "2-extra"}, {"upload_name": "cli-123-2"}, {"version": True}, {"version": 1}):
            with self.subTest(override=override), self.assertRaises(gate.CoverageError):
                self.read(**override)

    def test_invalid_root_and_digest_are_rejected(self):
        for override in ({"source_root": "relative"}, {"source_root": "/"}, {"source_root": "/a/../b"}, {"source_root": "/a/"}, {"report_sha256": "c" * 63}, {"report_sha256": None}):
            with self.subTest(override=override), self.assertRaises(gate.CoverageError):
                self.read(**override)

    def test_missing_malformed_and_oversized_receipts_are_errors(self):
        with self.assertRaises(gate.CoverageError):
            gate.load_upload_receipt(self.path, HEAD, "cli", 123)
        self.path.write_text("not JSON")
        with self.assertRaises(gate.CoverageError):
            gate.load_upload_receipt(self.path, HEAD, "cli", 123)
        with mock.patch.object(gate, "MAX_RECEIPT_BYTES", 10), self.assertRaises(gate.CoverageError):
            self.read()


class LcovTest(unittest.TestCase):
    def read(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coverage.lcov"
            path.write_text(text, encoding="utf-8")
            receipt = gate.UploadReceipt(UPLOAD, ROOT, hashlib.sha256(path.read_bytes()).hexdigest())
            return gate.load_lcov(path, receipt, "cli")

    def test_branch_lines_keep_codecov_partial_semantics_not_just_da_hits(self):
        text = lcov("HHH").replace("LF:3", "BRDA:1,0,0,0\nBRDA:1,0,1,-\nBRDA:2,0,0,1\nBRDA:2,0,1,0\nBRDA:3,0,0,1\nBRDA:3,0,1,2\nLF:3")
        self.assertEqual(self.read(text)[CLI], report("MPH"))

    def test_mangled_cpp_function_start_keeps_da_as_in_codecov_parser(self):
        for extension, expected in (("cpp", "HHP"), ("h", "PPP")):
            path = CLI.replace(".cpp", f".{extension}")
            text = lcov("PPP", path).replace("LF:3", "FN:1,_Zfoo\nFN:2,_Gbar\nFN:3,ordinary_function\nLF:3")
            self.assertEqual(self.read(text)[path], report(expected))

    def test_branch_only_line_is_measured(self):
        text = lcov("H").replace("LF:1", "BRDA:2,0,0,0\nLF:1")
        self.assertEqual(self.read(text)[CLI], report("HM"))

    def test_relative_and_absolute_source_paths(self):
        for text in (lcov("H"), lcov("H").replace(ROOT + "/", "")):
            self.assertEqual(self.read(text), {CLI: report("H")})

    def test_llvm_summary_does_not_replace_actual_line_records(self):
        # Real LLVM exports have LF != count(DA), e.g. 148 versus 104 for
        # ydb_root.cpp. Codecov's parser classifies DA/BRDA and ignores LF.
        self.assertEqual(self.read(lcov("HMP").replace("LF:3", "LF:148"))[CLI], report("HMP"))

    def test_wrong_component_unknown_root_and_traversal_are_errors(self):
        for path in (SDK, f"../{CLI}", f"/different/{CLI}", CLI.replace("main", "../main")):
            with self.subTest(path=path), self.assertRaises(gate.CoverageError):
                self.read(lcov("H").replace(f"{ROOT}/{CLI}", path))

    def test_incomplete_duplicate_and_malformed_reports_fail_closed(self):
        cases = ["", lcov(""), lcov("H").replace("end_of_record", ""), lcov("H").replace("LF:1\n", ""), lcov("H") * 2]
        for replacement in ("DA:1,1\nDA:1,0", "DA:0,1", "DA:1,-1", "DA:1,nan", "BRDA:1,0,0,1\nBRDA:1,0,0,0", "FN:bad", "UNKNOWN:1", "DA:1,1\nLF:1"):
            cases.append(lcov("H").replace("DA:1,1", replacement))
        for text in cases:
            with self.subTest(text=text), self.assertRaises(gate.CoverageError):
                self.read(text)

    def test_wrong_digest_and_oversized_report_are_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt, coverage = save_upload(Path(directory), "H")
            pin = gate.load_upload_receipt(receipt, HEAD, "cli", 123)
            with mock.patch.object(gate, "MAX_LCOV_BYTES", 10), self.assertRaises(gate.CoverageError):
                gate.load_lcov(coverage, pin, "cli")
            coverage.write_text(lcov("M"))
            with self.assertRaisesRegex(gate.CoverageError, "digest"):
                gate.load_lcov(coverage, pin, "cli")


class CliTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = self.root / "patch.json"
        self.policy = self.root / "policy.json"
        self.policy.write_text(json.dumps(policy_config()))
        self.summary = self.root / "summary.md"
        self.manifest.write_text(json.dumps({
            "version": 1, "base_sha": BASE, "head_sha": HEAD, "files": {CLI: [[1, 10]]},
        }))

    def args(self, kinds, attempt=1, path=CLI):
        receipt, coverage = save_upload(self.root, kinds, attempt=attempt, path=path)
        return [
            "check", "--base-sha", BASE, "--head-sha", HEAD,
            "--flag", "cli", "--run-id", "123",
            "--upload-receipt", str(receipt), "--coverage-report", str(coverage),
            "--changed-lines-file", str(self.manifest),
            "--policy-file", str(self.policy),
        ]

    def invoke(self, args):
        # Reproduce a completely unavailable remote service. A local verdict
        # must still work, including N/A and genuine below-threshold failures.
        with mock.patch("socket.create_connection", side_effect=AssertionError("network is forbidden")) as network, mock.patch.dict(gate.os.environ, {"GITHUB_STEP_SUMMARY": str(self.summary)}), contextlib.redirect_stdout(io.StringIO()):
            result = gate.main(args)
        network.assert_not_called()
        return result

    def test_invalid_percentages_and_shas_are_rejected(self):
        for value in ("nan", "Infinity", "-1", "101", "not a number"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                gate.percentage(value)
        with self.assertRaises(argparse.ArgumentTypeError):
            gate.sha("main")

    def test_offline_gate_exit_codes_and_absent_files_summary(self):
        for kinds, expected in (("HHHHHHHHMP", 0), ("HHHHHHHMPP", 1)):
            with self.subTest(kinds=kinds):
                self.assertEqual(self.invoke(self.args(kinds)), expected)
        self.assertEqual(self.invoke(self.args("H", path=CLI.replace("main", "other"))), 0)
        summary = self.summary.read_text()
        self.assertIn("not applicable", summary)
        self.assertIn("not assumed covered", summary)
        self.assertIn("Calculated offline", summary)

    def test_policy_edit_alone_changes_cli_verdict_and_summary(self):
        args = self.args("HHHHHHHHMP")
        self.assertEqual(self.invoke(args), 0)
        self.policy.write_text(json.dumps(policy_config("81%")))
        self.assertEqual(self.invoke(args), 1)
        self.assertIn("minimum 81%", self.summary.read_text())
        self.policy.write_text(json.dumps(policy_config("85%", "5%")))
        self.assertEqual(self.invoke(args), 0)
        self.assertIn("target 85%, tolerance 5 percentage points; minimum 80%", self.summary.read_text())

    def test_missing_policy_is_error_even_with_no_measured_changes(self):
        args = self.args("H", path=CLI.replace("main", "other"))
        self.policy.unlink()
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.invoke(args), 2)
        self.assertIn("Measurement error", self.summary.read_text())

    def test_command_line_cannot_override_policy(self):
        args = self.args("M")
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as result:
            gate.main(args + ["--threshold", "0"])
        self.assertEqual(result.exception.code, 2)

    def test_missing_report_is_measurement_error_not_a_fake_pass(self):
        args = self.args("H")
        Path(args[args.index("--coverage-report") + 1]).unlink()
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.invoke(args), 2)
        self.assertIn("Measurement error", self.summary.read_text())
        self.assertIn("No coverage verdict was produced", self.summary.read_text())

    def test_malformed_patch_is_a_measurement_error_even_offline(self):
        args = self.args("H")
        self.manifest.write_text("not JSON")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.invoke(args), 2)
        self.assertIn("Measurement error", self.summary.read_text())

    def test_pr_56580_regression_remains_below_threshold_without_api_timeout(self):
        # That real run timed out waiting for Codecov, although the preserved
        # CLI LCOV already gave an unambiguous 3/10 (30%) patch verdict.
        self.assertEqual(self.invoke(self.args("HHH" + "M" * 7)), 1)
        self.assertIn("30.00000%", self.summary.read_text())
        self.assertIn("3/10 covered", self.summary.read_text())

    def test_two_uploads_same_head_and_flag_cannot_mask_current_misses(self):
        older = self.args("H" * 10, attempt=1)
        current = self.args("M" * 10, attempt=2)
        # A remote commit/flag view would merge these into 100%. Local receipts
        # must evaluate each attempt separately regardless of remote state.
        self.assertEqual(self.invoke(older), 0)
        self.assertEqual(self.invoke(current), 1)
        self.assertIn("0/10 covered", self.summary.read_text())
        current[current.index("--coverage-report") + 1] = older[older.index("--coverage-report") + 1]
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.invoke(current), 2)
        self.assertIn("digest", self.summary.read_text())

    def test_stale_head_cannot_use_another_attempts_local_report(self):
        args = self.args("H" * 10)
        args[args.index("--head-sha") + 1] = "c" * 40
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.invoke(args), 2)


if __name__ == "__main__":
    unittest.main()
