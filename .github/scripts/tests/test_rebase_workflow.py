"""Regression tests for rebase event routing. Requires PyYAML, Node.js and git.

Run: python -m unittest discover -s .github/scripts/tests -p test_rebase_workflow.py
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


WORKFLOWS = Path(__file__).resolve().parents[2] / "workflows"
REBASE = yaml.safe_load((WORKFLOWS / "rebase.yml").read_text())
CHECKS = yaml.safe_load((WORKFLOWS / "pr_check.yml").read_text())
STEPS = REBASE["jobs"]["rebase"]["steps"]


def node(program, data, env=None):
    result = subprocess.run(
        ["node", "-e", program, json.dumps(data)], check=True,
        capture_output=True, text=True, env=env,
    )
    return json.loads(result.stdout)


def evaluate(expression, context):
    return node(r"""
      const {expression, context} = JSON.parse(process.argv[1]);
      const format = (s, ...args) => s.replace(/\{(\d+)\}/g, (_, n) => args[n]);
      const evaluate = s => Function(...Object.keys(context), 'format', 'fromJSON',
        'return (' + s + ')')(...Object.values(context), format, JSON.parse);
      const whole = expression.match(/^\s*\$\{\{([\s\S]*?)\}\}\s*$/);
      const result = whole ? evaluate(whole[1]) : expression.includes('${{')
        ? expression.replace(/\$\{\{([\s\S]*?)\}\}/g, (_, s) => evaluate(s))
        : evaluate(expression);
      console.log(JSON.stringify(result));
    """, {"expression": expression, "context": context})


def event(label, run_id=1):
    return {"github": {"event_name": "pull_request_target", "run_id": run_id,
                       "workflow": "PR-check", "event": {"action": "labeled",
                       "label": {"name": label}, "pull_request": {"number": 123, "state": "open"}}},
            "inputs": {}, "vars": {"CHECKS_SWITCH": '{"pr_check":true}'}}


class RebaseWorkflowTest(unittest.TestCase):
    def test_labels_route_to_one_workflow(self):
        for label, rebase, checks in [("rebase-and-check", True, False),
                                      ("rebase-user-branch", True, False),
                                      ("ok-to-test", False, True),
                                      ("documentation", False, False)]:
            with self.subTest(label=label):
                self.assertEqual(evaluate(REBASE["jobs"]["rebase"]["if"], event(label)), rebase)
                self.assertEqual(evaluate(CHECKS["jobs"]["check-running-allowed"]["if"], event(label)), checks)

    def restart_step(self):
        step = next((s for s in STEPS if s.get("id") == "restart_checks"), None)
        self.assertIsNotNone(step, "An unchanged head must explicitly restart checks")
        return step

    def test_unrelated_labels_cannot_cancel_rebase(self):
        group = REBASE["concurrency"]["group"]
        active = evaluate(group, event("rebase-and-check"))
        self.assertEqual(active, evaluate(group, event("rebase-user-branch")))
        for label in ("ok-to-test", "documentation"):
            with self.subTest(label=label):
                self.assertNotEqual(active, evaluate(group, event(label, 2)))

    def test_real_rebase_outputs(self):
        script = next(s["run"] for s in STEPS if s.get("id") == "rebase")
        for mode in ("unchanged", "changed", "conflict"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                def git(*args):
                    return subprocess.check_output(["git", "-C", tmp, *args], stderr=subprocess.STDOUT, text=True).strip()
                git("init", "-q")
                git("config", "user.name", "Test")
                git("config", "user.email", "test@example.invalid")
                (root / "base").write_text("base\n")
                git("add", ".")
                git("commit", "-qm", "base")
                git("branch", "upstream/main")
                git("checkout", "-qb", "topic")
                (root / "base" if mode == "conflict" else root / "topic").write_text("topic\n")
                git("add", ".")
                git("commit", "-qm", "topic")
                original = git("rev-parse", "HEAD")
                if mode != "unchanged":
                    git("checkout", "-q", "upstream/main")
                    (root / "base").write_text("upstream\n")
                    git("add", ".")
                    git("commit", "-qm", "advance base")
                    git("checkout", "-q", "topic")
                output = root / "outputs"
                subprocess.run(["bash", "-e", "-c", script], cwd=tmp, check=True,
                               capture_output=True, text=True,
                               env={**os.environ, "BASE_REF": "main", "GITHUB_OUTPUT": str(output)})
                values = dict(line.split("=", 1) for line in output.read_text().splitlines())
                self.assertEqual(values["rebase_status"], "failed" if mode == "conflict" else "success")
                if mode == "conflict":
                    self.assertEqual(git("rev-parse", "HEAD"), original)
                else:
                    self.assertEqual(values.get("head_changed"), str(mode == "changed").lower())

    def test_restart_only_after_successful_unchanged_push(self):
        step = self.restart_step()
        for status, changed, pushed, expected in [
            ("success", "false", "success", True),
            ("success", "true", "success", False),
            ("success", "false", "failure", False),
            ("failed", "false", "skipped", False),
        ]:
            context = {"steps": {"rebase": {"outputs": {"rebase_status": status, "head_changed": changed}},
                                 "push": {"outcome": pushed}}}
            self.assertEqual(evaluate(step["if"], context), expected)

    def test_label_restart_and_stale_head_guard(self):
        step = self.restart_step()
        self.assertEqual(step["with"]["github-token"], "${{ secrets.GH_PERSONAL_ACCESS_TOKEN }}")
        for scenario, expected, failed in [
            ({}, ["remove", "add"], False),
            ({"removeStatus": 404}, ["remove", "add"], False),
            ({"removeStatus": 403}, ["remove"], True),
            ({"head": "new-head"}, [], False),
            ({"state": "closed"}, [], False),
            ({"addStatus": 403}, ["remove", "add"], True),
        ]:
            with self.subTest(scenario=scenario):
                result = node("""
                  const {script, scenario} = JSON.parse(process.argv[1]);
                  const calls = [];
                  const github = {rest: {
                    pulls: {get: async () => ({data: {state: scenario.state || 'open', head: {sha: scenario.head || 'old-head'}}})},
                    issues: {
                      removeLabel: async x => {calls.push(['remove', x]); if (scenario.removeStatus) throw {status: scenario.removeStatus};},
                      addLabels: async x => {calls.push(['add', x]); if (scenario.addStatus) throw {status: scenario.addStatus};}
                    }
                  }};
                  const core = {info() {}, warning() {}};
                  const context = {repo: {owner: 'ydb-platform', repo: 'ydb'}};
                  const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
                  (async () => {
                    let failed = false;
                    try {await new AsyncFunction('github','context','core',script)(github,context,core);}
                    catch (_) {failed = true;}
                    console.log(JSON.stringify({calls, failed}));
                  })();
                """, {"script": step["with"]["script"], "scenario": scenario},
                    env={**os.environ, "PR_NUMBER": "123", "EXPECTED_HEAD": "old-head"})
                self.assertEqual([c[0] for c in result["calls"]], expected)
                self.assertEqual(result["failed"], failed)
                for name, params in result["calls"]:
                    self.assertEqual(params["issue_number"], 123)
                    self.assertEqual(params["name"] if name == "remove" else params["labels"],
                                     "ok-to-test" if name == "remove" else ["ok-to-test"])


if __name__ == "__main__":
    unittest.main()
