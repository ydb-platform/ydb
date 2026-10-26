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
import urllib.error
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


def upload(name=UPLOAD, state="MERGED", flags=None, errors=0):
    return {"name": name, "state": state, "flags": flags if flags is not None else ["cli"], "errors": {"totalCount": errors}}


def commit(uploads=None, flags=None, lines=10, head=HEAD, cursor=None):
    return {
        "commitid": head,
        "coverageAnalytics": {"flagNames": flags if flags is not None else ["cli"], "totals": {"lineCount": lines}},
        "uploads": {
            "edges": [{"node": item} for item in (uploads if uploads is not None else [upload()])],
            "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor},
        },
    }


def response(value):
    return {"data": {"owner": {"repository": {"__typename": "Repository", "commit": value}}}}


def report(kinds):
    return {"totals": {"lineCount": len(kinds)}, "coverage": [{"line": number, "coverage": kind} for number, kind in enumerate(kinds, 1)]}


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


class UploadReadinessTest(unittest.TestCase):
    def client(self, value):
        client = gate.Client("ydb-platform/ydb", HEAD)
        client.query = mock.Mock(return_value=value)
        return client

    def test_current_upload_merged_without_waiting_for_ci_notifications(self):
        self.assertEqual(self.client(commit()).upload_ready("cli", UPLOAD), UPLOAD)

    def test_wrong_name_does_not_reuse_old_report(self):
        with self.assertRaises(gate.NotReady):
            self.client(commit()).upload_ready("cli", "cli-456-1")

    def test_missing_flag_report_or_upload_waits(self):
        for value in (commit(flags=[]), commit(lines=0), commit(uploads=[]), commit(uploads=[upload(state="PROCESSED")]), commit(uploads=[upload(state="COMPLETE")])):
            with self.subTest(value=value), self.assertRaises(gate.NotReady):
                self.client(value).upload_ready("cli", UPLOAD)

    def test_error_or_wrong_flag_is_never_successful(self):
        for item in (upload(state="ERROR"), upload(errors=1), upload(flags=["cli", "cpp_sdk"])):
            with self.subTest(item=item), self.assertRaises(gate.CoverageError):
                self.client(commit(uploads=[item])).upload_ready("cli", UPLOAD)

    def test_latest_attempt_not_older_merged_upload_is_checked(self):
        client = self.client(commit(uploads=[upload(), upload(name="cli-123-2", state="UPLOADED")]))
        with self.assertRaises(gate.NotReady):
            client.upload_ready("cli", "cli-123-2")

    def test_invisible_new_attempt_cannot_reuse_old_merged_report(self):
        with self.assertRaises(gate.NotReady):
            self.client(commit()).upload_ready("cli", "cli-123-2")

    def test_other_run_and_similarly_prefixed_names_are_ignored(self):
        items = [upload(name="cli-1234-2"), upload(name="cli-123-2-extra"), upload(name="cli-123-0")]
        with self.assertRaises(gate.NotReady):
            self.client(commit(uploads=items)).upload_ready("cli", "cli-123-2")

    def test_only_the_receipt_attempt_matters(self):
        value = commit(uploads=[upload(name="cli-123-9", state="ERROR"), upload(name="cli-123-10")])
        self.assertEqual(self.client(value).upload_ready("cli", "cli-123-10"), "cli-123-10")

    def test_pagination_is_fully_read_before_accepting_the_receipt_upload(self):
        client = self.client(None)
        client.query.side_effect = [commit(cursor="next"), commit(uploads=[upload(name="cli-123-2")])]
        self.assertEqual(client.upload_ready("cli", "cli-123-2"), "cli-123-2")
        self.assertEqual(client.query.call_args_list[1].args[1], {"after": "next"})

    def test_repeated_pagination_cursor_is_error(self):
        with self.assertRaises(gate.CoverageError):
            self.client(commit(cursor="next")).upload_ready("cli", UPLOAD)


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


class ApiTest(unittest.TestCase):
    def test_wrong_sha_is_not_accepted(self):
        with self.assertRaises(gate.CoverageError):
            gate.commit_from_response(response(commit(head=BASE)), HEAD)

    def test_missing_commit_is_not_ready(self):
        with self.assertRaises(gate.NotReady):
            gate.commit_from_response(response(None), HEAD)

    def test_graphql_errors_reject_partial_data(self):
        value = response(commit())
        value["errors"] = [{"message": "some query failed"}]
        with mock.patch.object(gate.urllib.request, "urlopen") as open_url:
            open_url.return_value.__enter__.return_value.read.return_value = json.dumps(value).encode()
            with self.assertRaisesRegex(gate.NotReady, "some query failed"):
                gate.request("query", {}, 1)

    def test_http_and_network_failures_are_not_a_pass(self):
        for error in (urllib.error.URLError("network"), TimeoutError(), urllib.error.HTTPError(gate.API_URL, 429, "rate limit", {}, None), urllib.error.HTTPError(gate.API_URL, 500, "server", {}, None)):
            with self.subTest(error=error), mock.patch.object(gate.urllib.request, "urlopen", side_effect=error), self.assertRaises(gate.NotReady):
                gate.request("query", {}, 1)

    def test_permission_and_schema_errors_are_fatal(self):
        for status in (400, 401, 403, 404):
            with self.subTest(status=status), mock.patch.object(gate.urllib.request, "urlopen", side_effect=urllib.error.HTTPError(gate.API_URL, status, "error", {}, None)), self.assertRaises(gate.CoverageError):
                gate.request("query", {}, 1)

    def test_polling_has_one_bounded_budget(self):
        client = gate.Client("ydb-platform/ydb", HEAD)
        client.deadline = 20
        operation = mock.Mock(side_effect=gate.NotReady("processing"))
        with mock.patch.object(gate.time, "monotonic", side_effect=[5, 20]), mock.patch.object(gate.time, "sleep") as sleep, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(gate.CoverageError, "timed out"):
                client.wait(operation)
        sleep.assert_called_once_with(15)

    def test_temporary_error_can_recover(self):
        client = gate.Client("ydb-platform/ydb", HEAD)
        with mock.patch.object(gate.time, "sleep"), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(client.wait(mock.Mock(side_effect=[gate.NotReady("processing"), 42])), 42)

class CliTest(unittest.TestCase):
    def test_invalid_percentages_and_shas_are_rejected(self):
        for value in ("nan", "Infinity", "-1", "101", "not a number"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                gate.percentage(value)
        with self.assertRaises(argparse.ArgumentTypeError):
            gate.sha("main")

    def test_gate_exit_codes_and_absent_files_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "patch.json"
            summary = Path(directory) / "summary.md"
            manifest.write_text(json.dumps({"version": 1, "base_sha": BASE, "head_sha": HEAD, "files": {CLI: [[1, 10]]}}))
            for kinds, expected in (("HHHHHHHHMP", 0), ("HHHHHHHMPP", 1)):
                receipt, coverage = save_upload(Path(directory), kinds)
                args = ["check", "--repository", "ydb-platform/ydb", "--base-sha", BASE, "--head-sha", HEAD, "--flag", "cli", "--run-id", "123", "--upload-receipt", str(receipt), "--coverage-report", str(coverage), "--changed-lines-file", str(manifest)]
                with mock.patch.object(gate.Client, "upload_ready", return_value=UPLOAD), mock.patch.dict(gate.os.environ, {"GITHUB_STEP_SUMMARY": str(summary)}), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(gate.main(args), expected)
            save_upload(Path(directory), "H", path=CLI.replace("main", "other"))
            with mock.patch.object(gate.Client, "upload_ready", return_value=UPLOAD), mock.patch.dict(gate.os.environ, {"GITHUB_STEP_SUMMARY": str(summary)}), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(gate.main(args), 0)
            self.assertIn("not applicable", summary.read_text())
            self.assertIn("not assumed covered", summary.read_text())
            with mock.patch.object(gate.Client, "upload_ready", side_effect=gate.CoverageError("missing report")), mock.patch.dict(gate.os.environ, {"GITHUB_STEP_SUMMARY": str(summary)}), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(gate.main(args), 2)
            self.assertIn("Measurement error: missing report", summary.read_text())

    def test_two_uploads_same_head_and_flag_cannot_mask_current_misses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "patch.json"
            manifest.write_text(json.dumps({"version": 1, "base_sha": BASE, "head_sha": HEAD, "files": {CLI: [[1, 10]]}}))
            older = save_upload(root, "H" * 10, attempt=1)
            current = save_upload(root, "M" * 10, attempt=2)
            # The processed commit/flag report would merge these into 100%.
            # Expose both MERGED uploads, but no coverageFile query is allowed.
            def query(_client, text, extra=None):
                self.assertEqual(text, gate.UPLOAD_QUERY)
                return commit(uploads=[upload(), upload(name="cli-123-2")])

            for (receipt, coverage), expected, fraction in ((older, 0, "10/10"), (current, 1, "0/10")):
                args = ["check", "--repository", "ydb-platform/ydb", "--base-sha", BASE, "--head-sha", HEAD, "--flag", "cli", "--run-id", "123", "--upload-receipt", str(receipt), "--coverage-report", str(coverage), "--changed-lines-file", str(manifest)]
                output = io.StringIO()
                with mock.patch.object(gate.Client, "query", new=query), contextlib.redirect_stdout(output):
                    self.assertEqual(gate.main(args), expected)
                self.assertIn(fraction, output.getvalue())
            # A receipt from the new attempt cannot be paired with the old LCOV.
            args[args.index("--coverage-report") + 1] = str(older[1])
            with mock.patch.object(gate.Client, "query") as api, contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(gate.main(args), 2)
                api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
