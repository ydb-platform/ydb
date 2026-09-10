#!/usr/bin/env python3
"""Tests for scaffold.py. Run: python3.8 -m unittest discover -s <this dir>"""
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

    def run_scaffold(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = scaffold.main(list(args) + ["--root", self.root])
        return code, out.getvalue()

    def parts(self):
        return {
            "skill": os.path.join(self.target, ".agents", "skills", "ydb-demo-skill", "SKILL.md"),
            "agents": os.path.join(self.target, "AGENTS.md"),
            "claude": os.path.join(self.target, "CLAUDE.md"),
            "link": os.path.join(self.target, ".claude"),
        }

    def test_creates_all_parts_and_passes_check(self):
        code, out = self.run_scaffold(self.target, "ydb-demo-skill", "--description", "Demo. Not for real use.")
        self.assertEqual(code, 0, out)
        parts = self.parts()
        self.assertTrue(os.path.isfile(parts["skill"]))
        self.assertIn("name: ydb-demo-skill", read(parts["skill"]))
        self.assertIn("read .agents/skills/ydb-demo-skill/SKILL.md.", read(parts["agents"]))
        self.assertIn("Build and test commands: ../agents/GUIDE.md.", read(parts["agents"]))
        self.assertEqual(read(parts["claude"]), "@./AGENTS.md\n")
        self.assertEqual(os.readlink(parts["link"]), ".agents")
        self.assertIn("0 errors, 4 warnings", out)  # the four TODO lines of the template

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

    def test_real_claude_dir_is_conflict_and_nothing_is_written(self):
        os.mkdir(os.path.join(self.target, ".claude"))
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("conflicts, nothing written", out)
        self.assertFalse(os.path.exists(self.parts()["skill"]))

    def test_claude_md_with_other_content_is_conflict(self):
        write(os.path.join(self.target, "CLAUDE.md"), "rules\n")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("exists without the line", out)

    def test_claude_md_with_include_and_extra_text_is_accepted(self):
        write(os.path.join(self.target, "CLAUDE.md"), "@./AGENTS.md\n\nExtra.\n")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 0, out)
        self.assertIn("exists with the include line", out)

    def test_symlink_to_other_target_is_conflict(self):
        os.symlink("elsewhere", os.path.join(self.target, ".claude"))
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("not to .agents", out)

    def test_root_dir_is_named_in_text(self):
        os.mkdir(os.path.join(self.root, ".claude"))
        self.run_scaffold(self.root, "ydb-root-skill")
        self.assertIn("apply to the repo root.", read(os.path.join(self.root, "AGENTS.md")))

    def test_agents_md_that_is_a_directory_is_conflict(self):
        os.mkdir(os.path.join(self.target, "AGENTS.md"))
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("is not a file", out)

    def test_missing_guide_drops_the_build_link(self):
        os.remove(os.path.join(self.root, "ydb", "agents", "GUIDE.md"))
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 0, out)
        self.assertIn("build and test link is left out", out)
        self.assertNotIn("GUIDE.md", read(self.parts()["agents"]))
        self.assertNotIn("GUIDE.md", read(self.parts()["skill"]))

    def test_existing_agents_md_is_not_edited(self):
        write(os.path.join(self.target, "AGENTS.md"), "# Foo\n")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 0)
        self.assertIn("MANUAL STEP", out)
        self.assertEqual(read(os.path.join(self.target, "AGENTS.md")), "# Foo\n")

    def test_no_symlink_flag(self):
        code, _ = self.run_scaffold(self.target, "ydb-demo-skill", "--no-symlink")
        self.assertEqual(code, 0)
        self.assertFalse(os.path.lexists(self.parts()["link"]))

    def test_root_with_local_claude_dir_skips_symlink(self):
        os.mkdir(os.path.join(self.root, ".claude"))
        code, out = self.run_scaffold(self.root, "ydb-root-skill", "--dry-run")
        self.assertEqual(code, 0, out)
        self.assertIn("no symlink created", out)

    def test_description_whitespace_is_collapsed(self):
        self.run_scaffold(self.target, "ydb-demo-skill", "--description", "line one\nline two \"q\"")
        self.assertIn("description: \"line one line two 'q'\"", read(self.parts()["skill"]))

    def test_duplicate_name_is_conflict_and_similar_name_is_warning(self):
        other = os.path.join(self.root, "ydb", "other")
        os.makedirs(other)
        self.run_scaffold(other, "ydb-demo-skill")
        code, out = self.run_scaffold(self.target, "ydb-demo-skill")
        self.assertEqual(code, 1)
        self.assertIn("already used by", out)
        code, out = self.run_scaffold(self.target, "ydb-demo-skills", "--dry-run")
        self.assertEqual(code, 0)
        self.assertIn("looks like ydb-demo-skill", out)

    def test_bad_name_missing_dir_and_outside_root_are_usage_errors(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.run_scaffold(self.target, "Bad_Name")[0], 2)
            self.assertEqual(self.run_scaffold(self.target, "no-prefix")[0], 2)
            self.assertEqual(self.run_scaffold(os.path.join(self.root, "nope"), "ydb-x-skill")[0], 2)
            outside = tempfile.mkdtemp()
            try:
                self.assertEqual(self.run_scaffold(outside, "ydb-x-skill")[0], 2)
            finally:
                shutil.rmtree(outside)


if __name__ == "__main__":
    unittest.main()
