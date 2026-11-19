import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import revalidate_muted_tests as revalidate


def test_result(name="Flaky.test", **changes):
    return dict(type="test", path="ydb/tests/example", name=name, status="FAILED", **changes)


def matcher(pattern="Flaky.*"):
    check = revalidate.YaMuteCheck()
    check.populate("ydb/tests/example", pattern)
    return check


class FakeGitHub:
    repo = "ydb-platform/ydb"

    def __init__(self):
        self.run = dict(id=123, run_attempt=1, head_sha="head", event="pull_request_target",
                        status="completed", conclusion="failure", path=".github/workflows/pr_check.yml",
                        html_url="https://github.com/ydb-platform/ydb/actions/runs/123")
        self.pr = dict(state="open", head={"sha": "head"}, base={"ref": "main"})
        self.statuses = {
            "checks_integrated": dict(state="failure", target_url=self.run["html_url"]),
            "build_relwithdebinfo": dict(state="success"),
            "test_relwithdebinfo": dict(state="failure"),
        }
        self.jobs = [dict(name="Build and test relwithdebinfo", conclusion="failure",
                          steps=[dict(name="Build and test", conclusion="failure")])]
        self.writes = []
        self.after_comment = lambda: None

    def request(self, path, data=None):
        if data is not None:
            self.writes.append((path, data))
            if path.endswith("/comments"):
                self.after_comment()
                return {"html_url": "https://github.com/ydb-platform/ydb/pull/42#issuecomment-1"}
            self.statuses[data["context"]] = data
            return data
        if path == "/pulls/42":
            return self.pr
        if path == "/actions/runs/123":
            return self.run
        if path == "/commits/head/status":
            return {"statuses": [dict(context=name, **value) for name, value in self.statuses.items()
                                 if "context" not in value] +
                    [value for value in self.statuses.values() if "context" in value]}
        raise AssertionError(path)

    def pages(self, path, key):
        assert path == "/actions/runs/123/attempts/1/jobs"
        return self.jobs


class RevalidationTests(unittest.TestCase):
    def setUp(self):
        self.gh = FakeGitHub()
        self.record = dict(run_id=123, run_attempt=1, pr=42, head_sha="head", base="main",
                           preset="relwithdebinfo", results=[test_result()])
        self.env = patch.dict(os.environ, AWS_ENDPOINT="https://storage.example", AWS_BUCKET="ci")
        self.env.start()
        self.addCleanup(self.env.stop)

    def run_revalidation(self, dry_run=False):
        with patch.object(revalidate.urllib.request, "urlopen", return_value=io.BytesIO(json.dumps(self.record).encode())), \
                patch.object(revalidate, "current_mutes", return_value=(matcher(), "mute-sha")), \
                patch("builtins.print"):
            revalidate.revalidate_run(self.gh, copy.deepcopy(self.gh.run), dry_run)

    def test_all_failures_must_match(self):
        self.record["results"].append(test_result("RealRegression.test"))
        self.run_revalidation()
        self.assertEqual(self.gh.writes, [])

    def test_success_has_explicit_comment_and_both_statuses(self):
        self.run_revalidation()
        self.assertEqual(len(self.gh.writes), 3)
        self.assertIn("Tests were not rerun", self.gh.writes[0][1]["body"])
        self.assertIn("mute-sha", self.gh.writes[0][1]["body"])
        self.assertIn("Flaky.test", self.gh.writes[0][1]["body"])
        self.assertEqual(self.gh.statuses["test_relwithdebinfo"]["state"], "success")
        self.assertEqual(self.gh.statuses["checks_integrated"]["state"], "success")
        self.run_revalidation()
        self.assertEqual(len(self.gh.writes), 3)

    def test_dry_run_does_not_write(self):
        self.run_revalidation(dry_run=True)
        self.assertEqual(self.gh.writes, [])

    def test_missing_record_does_not_write(self):
        error = revalidate.urllib.error.HTTPError("url", 404, "missing", {}, None)
        with patch.object(revalidate.urllib.request, "urlopen", side_effect=error):
            revalidate.revalidate_run(self.gh, self.gh.run, False)
        self.assertEqual(self.gh.writes, [])

    def test_stale_or_ineligible_run_does_not_write(self):
        changes = [
            lambda: self.gh.pr.update(state="closed"),
            lambda: self.gh.pr["head"].update(sha="new-head"),
            lambda: self.gh.pr["base"].update(ref="stable-other"),
            lambda: self.record.update(run_attempt=2),
            lambda: self.record.update(run_id=122),
            lambda: self.record.update(preset="release-asan"),
            lambda: self.gh.run.update(status="in_progress"),
            lambda: self.gh.run.update(conclusion="cancelled"),
            lambda: self.gh.statuses["checks_integrated"].update(state="pending"),
            lambda: self.gh.statuses["checks_integrated"].update(target_url="new-run"),
            lambda: self.gh.statuses["build_relwithdebinfo"].update(state="failure"),
            lambda: self.gh.statuses["test_relwithdebinfo"].update(state="pending"),
            lambda: self.gh.jobs[0]["steps"].append(dict(name="Post Checkout", conclusion="failure")),
            lambda: self.gh.jobs.append(dict(name="Setup", conclusion="failure", steps=[])),
            lambda: self.gh.jobs.clear(),
        ]
        for change in changes:
            with self.subTest(change=change):
                self.gh = FakeGitHub()
                self.record.update(run_attempt=1, run_id=123, preset="relwithdebinfo")
                change()
                self.run_revalidation()
                self.assertEqual(self.gh.writes, [])

    def test_new_run_started_while_commenting_keeps_statuses_unchanged(self):
        self.gh.after_comment = lambda: self.gh.statuses["checks_integrated"].update(state="pending")
        self.run_revalidation()
        self.assertEqual(len(self.gh.writes), 1)
        self.assertEqual(self.gh.statuses["test_relwithdebinfo"]["state"], "failure")

    def test_resume_after_test_status_was_published(self):
        self.gh.statuses["test_relwithdebinfo"] = dict(
            state="success", description=revalidate.REVALIDATED_DESCRIPTION, target_url="existing-comment")
        self.run_revalidation()
        self.assertEqual(len(self.gh.writes), 2)
        self.assertEqual(self.gh.statuses["checks_integrated"]["target_url"], "existing-comment")

    def test_malformed_or_non_test_failure_is_rejected(self):
        invalid = [{}, {"results": []}, {"results": [dict(test_result(), status="UNKNOWN")]},
                   {"results": [dict(test_result(), type="build")]},
                   {"results": [test_result(chunk=True)]},
                   {"results": [test_result(suite=True)]},
                   {"results": [dict(test_result(), path="")]},
                   {"results": [dict(test_result(), type="configure")]},
                   {"results": [dict(test_result(), status="NOT_LAUNCHED")]}]
        for report in invalid:
            with self.subTest(report=report), self.assertRaises((ValueError, KeyError)):
                revalidate.accepted_failures(report, matcher())

    def test_existing_muted_and_passed_chunks_are_not_failures(self):
        self.record["results"].extend([
            dict(test_result(chunk=True), status="PASSED"),
            dict(test_result(chunk=True), status="MUTE"),
        ])
        self.assertEqual(len(revalidate.accepted_failures(self.record, matcher())), 1)

    def test_matches_suite_and_subtest_using_existing_wildcards(self):
        report = {"results": [test_result("Flaky", subtest_name="test")]}
        self.assertEqual(len(revalidate.accepted_failures(report, matcher())), 1)
        report["results"][0]["path"] = "another/suite"
        self.assertEqual(revalidate.accepted_failures(report, matcher()), [])

    def test_record_binds_failures_to_pr_head_run_attempt_and_target(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory, "report.json")
            report.write_text(json.dumps({"results": [test_result(), dict(test_result(), status="PASSED")]}))
            event = Path(directory, "event.json")
            event.write_text(json.dumps({"pull_request": dict(self.gh.pr, number=42)}))
            with patch.dict(os.environ, GITHUB_EVENT_PATH=str(event), GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="2",
                            BUILD_PRESET="relwithdebinfo", PUBLIC_DIR=directory):
                revalidate.record_failures(report)
            record = json.loads(Path(directory, "mute_revalidation.json").read_text())
            self.assertEqual(record, dict(self.record, run_attempt=2))

    def test_apply_preserves_original_until_caller_publishes_explanation(self):
        with tempfile.TemporaryDirectory() as directory:
            original_cwd = os.getcwd()
            os.chdir(directory)
            try:
                path = Path(directory, "last_report.json")
                original = json.dumps({"results": [test_result()]})
                path.write_text(original)
                Path("statusrep.txt").write_text("failure")
                with patch.dict(os.environ, GITHUB_BASE_REF="main", BUILD_PRESET="relwithdebinfo",
                                PUBLIC_DIR_URL="https://storage.example/report"), \
                        patch.object(revalidate, "current_mutes", return_value=(matcher(), "mute-sha")):
                    revalidate.apply_current_mutes(self.gh, path)
                self.assertEqual(path.read_text(), original)
                self.assertEqual(Path("statusrep.txt").read_text(), "failure")
                self.assertEqual(json.loads(Path("revalidated_report.json").read_text())["results"][0]["status"], "MUTE")
                self.assertIn("Tests were not rerun", Path("mute_revalidation_comment.txt").read_text())
            finally:
                os.chdir(original_cwd)

    def test_fetch_mutes_uses_target_branch_and_preset_at_one_commit(self):
        from unittest.mock import Mock
        gh = Mock()
        gh.request.side_effect = [
            {"sha": "stable-sha"},
            json.dumps({"default_muted_ya_path": "default.txt", "muted_ya_paths": {"release-asan": "asan.txt"}}),
            "ydb/tests/example Flaky.*\n",
        ]
        check, sha = revalidate.current_mutes(gh, "stable-26-4", "release-asan")
        self.assertTrue(check("ydb/tests/example", "Flaky.test"))
        self.assertEqual(sha, "stable-sha")
        self.assertEqual(gh.request.call_args_list[0].args[0], "/commits/stable-26-4")
        self.assertEqual(gh.request.call_args_list[-1].args[0], "/contents/asan.txt?ref=stable-sha")


if __name__ == "__main__":
    unittest.main()
