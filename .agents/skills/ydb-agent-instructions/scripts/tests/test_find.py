#!/usr/bin/env python3
"""Tests for find.py. Run: python3.9 -m unittest discover -s <this dir>"""
import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import check  # noqa: E402
import find  # noqa: E402


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


@unittest.skipUnless(shutil.which("git"), "git not installed")
class FindTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="find-test-")
        write(os.path.join(self.root, "AGENTS.md"), "# root\n")
        write(os.path.join(self.root, "ydb", "core", "foo", "AGENTS.md"), "# foo\n")
        write(os.path.join(self.root, "ydb", "core", "foo", ".agents", "skills", "ydb-foo-skill", "SKILL.md"), "pdisk here\n")
        write(os.path.join(self.root, "ydb", "core", "bar", "RULES.md"), "# bar\n")
        write(os.path.join(self.root, "ydb", "core", "bar", "rules", "coding.md"), "# coding\n")
        write(os.path.join(self.root, "ydb", "core", "bar", "README.md"), "About PDisk\n")
        write(os.path.join(self.root, "ydb", "docs", "en", "core", "contributor", "storage.md"), "PDISK internals\n")
        write(os.path.join(self.root, "ydb", "docs", "en", "core", "contributor", "toc_i.yaml"), "pdisk: yes\n")
        write(os.path.join(self.root, "ydb", "other.md"), "pdisk but not an instruction\n")
        write(os.path.join(self.root, "ydb", "core", ".hidden", "x.md"), "hidden\n")
        write(os.path.join(self.root, "contrib", "libs", "x", "AGENTS.md"), "vendored pdisk\n")
        write(os.path.join(self.root, "contrib", "libs", "x", ".agents", "skills", "typer", "SKILL.md"), "vendored\n")
        subprocess.run(["git", "init", "-q", self.root], check=True)
        subprocess.run(["git", "-C", self.root, "add", "-A"], check=True)

    def tearDown(self):
        shutil.rmtree(self.root)

    def run_find(self, *args):
        out = io.StringIO()
        err = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = find.main(list(args) + ["--root", self.root])
        return code, out.getvalue() + err.getvalue()

    def test_lists_instruction_files_outside_contrib(self):
        code, out = self.run_find()
        self.assertEqual(code, 0)
        for path in ("AGENTS.md", "ydb/core/foo/AGENTS.md", "ydb/core/foo/.agents/skills/ydb-foo-skill/SKILL.md", "ydb/core/bar/RULES.md",
                     "ydb/core/bar/rules/coding.md"):
            self.assertIn("  " + path + "\n", out)
        self.assertNotIn("contrib/", out)
        self.assertNotIn("files that mention", out)

    def test_topic_matches_markdown_only(self):
        code, out = self.run_find("PDisk")
        self.assertEqual(code, 0)
        mention = out.split("files that mention")[1]
        for path in ("ydb/core/bar/README.md", "ydb/docs/en/core/contributor/storage.md", "ydb/core/foo/.agents/skills/ydb-foo-skill/SKILL.md"):
            self.assertIn(path, mention)
        for absent in ("ydb/other.md", "toc_i.yaml", "contrib/"):
            self.assertNotIn(absent, mention)

    def test_skills_prints_names_directories_and_components(self):
        code, out = self.run_find("--skills")
        self.assertEqual(code, 0)
        self.assertEqual(out, "skills:\n  ydb-foo-skill\tydb/core/foo\ncomponents:\n  ydb/core/bar\n  ydb/core/foo\n  ydb/docs/en\n")

    def test_untracked_files_are_listed(self):
        write(os.path.join(self.root, "ydb", "new", "AGENTS.md"), "# new\n")
        write(os.path.join(self.root, ".gitignore"), "*\n!*.*\n!*/\n")
        write(os.path.join(self.root, "ydb", "new", "ignored"), "no extension, ignored\n")
        code, out = self.run_find()
        self.assertIn("  ydb/new/AGENTS.md\n", out)
        self.assertNotIn("ydb/new/ignored", out)

    def test_usage_errors_exit_2(self):
        outside = tempfile.mkdtemp()
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(find.main(["x", "--root", outside])[0] if False else find.main(["x", "--root", outside]), 2)
        finally:
            shutil.rmtree(outside)
        original = check.run_git
        check.run_git = lambda *args, **kwargs: None
        try:
            code, out = self.run_find()
        finally:
            check.run_git = original
        self.assertEqual(code, 2)
        self.assertIn("git is not installed", out)


if __name__ == "__main__":
    unittest.main()
