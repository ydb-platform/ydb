#!/usr/bin/env python3
"""Tests for scaffold.py. Run: python3.9 -m unittest discover -s <this dir>"""
import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scaffold  # noqa: E402

GITIGNORE = "*\n!*.*\n!*/\n.claude/\n"
DESCRIPTION = "Demo. Not for real use."


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def read(path):
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


class ScaffoldTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="scaffold-test-")
        write(os.path.join(self.root, ".gitignore"), GITIGNORE)
        write(os.path.join(self.root, "ydb", "agents", "GUIDE.md"), "# guide\n")
        if shutil.which("git"):
            subprocess.run(["git", "init", "-q", self.root], check=True)
        self.target = os.path.join(self.root, "ydb", "foo")
        os.makedirs(self.target)

    def tearDown(self):
        shutil.rmtree(self.root)

    def run_scaffold(self, *args, **kwargs):
        out = io.StringIO()
        err = io.StringIO()
        argv = list(args) + ["--root", self.root]
        if "--description" not in args and not kwargs.get("no_description"):
            argv += ["--description", DESCRIPTION]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = scaffold.main(argv)
            except SystemExit as exc:
                code = exc.code
        return code, out.getvalue() + err.getvalue()

    def parts(self):
        return {
            "skill": os.path.join(self.target, ".agents", "skills", "ydb-demo-skill", "SKILL.md"),
            "agents": os.path.join(self.target, "AGENTS.md"),
            "claude": os.path.join(self.target, "CLAUDE.md"),
        }

    def test_creates_all_parts_and_passes_check(self):
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 0, out)
        parts = self.parts()
        self.assertIn("name: ydb-demo-skill", read(parts["skill"]))
        self.assertIn("TODO: one or two rules", read(parts["agents"]))
        self.assertIn("Build and test commands: ydb/agents/GUIDE.md.", read(parts["agents"]))
        self.assertEqual(read(parts["claude"]), "@./AGENTS.md\n\nSkills, read the one that matches your task:\n- .agents/skills/ydb-demo-skill/SKILL.md: %s\n" % DESCRIPTION)
        self.assertFalse(os.path.lexists(os.path.join(self.target, ".claude")))
        self.assertIn("0 errors", out)
        self.assertNotIn("ERROR", out)

    def test_second_run_does_nothing(self):
        self.run_scaffold(self.target, "ydb-demo-skill")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 0)
        self.assertIn("nothing to do", out)

    def test_dry_run_writes_nothing(self):
        code, out = self.run_scaffold(self.target, "ydb-demo-skill", "--dry-run")
        self.assertEqual(code, 0)
        self.assertIn("would create", out)
        self.assertEqual(os.listdir(self.target), [])

    def test_conflicts_write_nothing(self):
        write(os.path.join(self.target, "CLAUDE.md"), "rules\n")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("exists without the line", out)
        self.assertFalse(os.path.exists(self.parts()["skill"]))
        os.remove(os.path.join(self.target, "CLAUDE.md"))
        os.mkdir(os.path.join(self.target, "AGENTS.md"))
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("is not a file", out)

    def test_claude_md_with_include_only_gets_a_manual_step(self):
        write(os.path.join(self.target, "CLAUDE.md"), "@./AGENTS.md\n")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertIn("MANUAL STEP", out)
        self.assertIn("- .agents/skills/ydb-demo-skill/SKILL.md: " + DESCRIPTION, out)
        self.assertEqual(read(os.path.join(self.target, "CLAUDE.md")), "@./AGENTS.md\n")

    def test_second_skill_is_added_to_a_new_claude_md_list(self):
        self.run_scaffold(self.target, "ydb-demo-skill")
        os.remove(os.path.join(self.target, "CLAUDE.md"))
        code, out = self.run_scaffold(self.target, "ydb-other-skill", "--description", "Other.")
        self.assertEqual(code, 0, out)
        text = read(os.path.join(self.target, "CLAUDE.md"))
        self.assertIn("- .agents/skills/ydb-demo-skill/SKILL.md: " + DESCRIPTION, text)
        self.assertIn("- .agents/skills/ydb-other-skill/SKILL.md: Other.", text)

    def test_existing_agents_md_is_not_edited(self):
        write(os.path.join(self.target, "AGENTS.md"), "# Foo\n")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 0, out)
        self.assertIn("exists, not touched", out)
        self.assertEqual(read(os.path.join(self.target, "AGENTS.md")), "# Foo\n")

    def test_root_dir_is_named_in_text(self):
        code, out = self.run_scaffold(self.root, "ydb-root-skill")
        self.assertEqual(code, 0, out)
        self.assertIn("apply to the repo root.", read(os.path.join(self.root, "AGENTS.md")))

    def test_missing_guide_drops_the_build_link(self):
        os.remove(os.path.join(self.root, "ydb", "agents", "GUIDE.md"))
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 0, out)
        self.assertIn("build and test link is left out", out)
        self.assertNotIn("GUIDE.md", read(self.parts()["agents"]))
        self.assertNotIn("GUIDE.md", read(self.parts()["skill"]))

    def test_description_is_a_valid_yaml_scalar(self):
        self.run_scaffold(self.target, "ydb-demo-skill", "--description", "line one\nline two \"q\" back\\slash")
        self.assertIn('description: "line one line two \\"q\\" back\\\\slash"', read(self.parts()["skill"]))

    def test_duplicate_name_is_conflict(self):
        if not shutil.which("git"):
            self.skipTest("git not installed")
        other = os.path.join(self.root, "ydb", "other")
        os.makedirs(other)
        self.run_scaffold(other, "ydb-demo-skill")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("already used by", out)

    def test_usage_errors_exit_2(self):
        self.assertEqual(self.run_scaffold(self.target, "Bad_Name")[0], 2)
        self.assertEqual(self.run_scaffold(self.target, "no-prefix")[0], 2)
        self.assertEqual(self.run_scaffold(self.target, "ydb-a--b")[0], 2)
        self.assertEqual(self.run_scaffold(os.path.join(self.root, "nope"), "ydb-x-skill")[0], 2)
        self.assertEqual(self.run_scaffold(self.target, "ydb-x-skill", no_description=True)[0], 2)
        outside = tempfile.mkdtemp()
        try:
            self.assertEqual(self.run_scaffold(outside, "ydb-x-skill")[0], 2)
        finally:
            shutil.rmtree(outside)


if __name__ == "__main__":
    unittest.main()
