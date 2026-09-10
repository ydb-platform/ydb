#!/usr/bin/env python3
"""Tests for find.py. Run: python3.8 -m unittest discover -s <this dir>"""
import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
        write(os.path.join(self.root, "ydb", "core", "foo", ".agents", "skills", "x", "SKILL.md"), "pdisk here\n")
        write(os.path.join(self.root, "ydb", "core", "bar", "RULES.md"), "# bar\n")
        write(os.path.join(self.root, "ydb", "core", "bar", "rules", "coding.md"), "# coding\n")
        write(os.path.join(self.root, "ydb", "core", "bar", "README.md"), "About PDisk\n")
        write(os.path.join(self.root, "ydb", "docs", "en", "core", "contributor", "storage.md"), "PDISK internals\n")
        write(os.path.join(self.root, "ydb", "other.md"), "pdisk but not an instruction\n")
        write(os.path.join(self.root, "ydb", "agents", "GUIDE.md"), "shared guide, no topic\n")
        write(os.path.join(self.root, "ydb", "agents", "TESTS.md"), "how to run pdisk tests\n")
        write(os.path.join(self.root, "ydb", "agents", "notes.txt"), "pdisk, not markdown\n")
        write(os.path.join(self.root, "contrib", "libs", "x", "AGENTS.md"), "vendored pdisk\n")
        subprocess.run(["git", "init", "-q", self.root], check=True)
        subprocess.run(["git", "-C", self.root, "add", "-A"], check=True)

    def tearDown(self):
        shutil.rmtree(self.root)

    def run_find(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = find.main(list(args) + ["--root", self.root])
        return code, out.getvalue()

    def test_lists_instruction_files_outside_contrib(self):
        code, out = self.run_find()
        self.assertEqual(code, 0)
        for path in ("AGENTS.md", "ydb/core/foo/AGENTS.md", "ydb/core/foo/.agents/skills/x/SKILL.md", "ydb/core/bar/RULES.md", "ydb/core/bar/rules/coding.md", "ydb/agents/GUIDE.md", "ydb/agents/TESTS.md"):
            self.assertIn("  " + path + "\n", out)
        self.assertNotIn("notes.txt", out)
        self.assertNotIn("contrib/", out)
        self.assertNotIn("files that mention", out)

    def test_topic_matches_readme_docs_and_instructions_only(self):
        code, out = self.run_find("PDisk")
        self.assertEqual(code, 0)
        mention = out.split("files that mention")[1]
        self.assertIn("ydb/core/bar/README.md", mention)
        self.assertIn("ydb/docs/en/core/contributor/storage.md", mention)
        self.assertIn("ydb/core/foo/.agents/skills/x/SKILL.md", mention)
        self.assertIn("ydb/agents/TESTS.md", mention)
        self.assertNotIn("ydb/agents/GUIDE.md", mention)
        self.assertNotIn("ydb/other.md", mention)
        self.assertNotIn("contrib/", mention)

    def test_untracked_files_are_listed(self):
        write(os.path.join(self.root, "ydb", "new", "AGENTS.md"), "# new\n")
        write(os.path.join(self.root, "ydb", "new", "ignored"), "no extension, ignored by *\n")
        write(os.path.join(self.root, ".gitignore"), "*\n!*.*\n!*/\n")
        code, out = self.run_find()
        self.assertIn("  ydb/new/AGENTS.md\n", out)
        self.assertNotIn("ydb/new/ignored", out)

    def test_no_root_is_usage_error(self):
        outside = tempfile.mkdtemp()
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                code = find.main(["x", "--root", outside])
            self.assertEqual(code, 2)
        finally:
            shutil.rmtree(outside)


if __name__ == "__main__":
    unittest.main()
