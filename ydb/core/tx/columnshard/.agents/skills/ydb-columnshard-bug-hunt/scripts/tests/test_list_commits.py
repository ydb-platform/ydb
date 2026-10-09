#!/usr/bin/env python3
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import list_commits  # noqa: E402


def commit(repo, path, subject, date):
    full = os.path.join(repo, path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "a") as handle:
        handle.write(subject + "\n")
    env = dict(os.environ, GIT_AUTHOR_DATE=date, GIT_COMMITTER_DATE=date,
               GIT_AUTHOR_NAME="A", GIT_AUTHOR_EMAIL="a@x", GIT_COMMITTER_NAME="A", GIT_COMMITTER_EMAIL="a@x")
    subprocess.run(["git", "-C", repo, "add", path], check=True, env=env)
    subprocess.run(["git", "-C", repo, "commit", "-q", "-m", subject], check=True, env=env)


class ParseTest(unittest.TestCase):
    def test_parse_numstat(self):
        text = "3\t1\tydb/core/tx/columnshard/a.cpp\n5\t0\tydb/core/kqp/ut/olap/b_ut.cpp\n"
        self.assertEqual(list_commits.parse_numstat(text), (2, 8, 1, False, False))
        self.assertEqual(list_commits.parse_numstat("1\t0\tx/ut/a.cpp\n-\t-\tx/tests/b.bin\n"), (2, 1, 0, True, False))
        self.assertEqual(list_commits.parse_numstat(""), (0, 0, 0, False, False))
        reader = "1\t1\tydb/core/tx/columnshard/engines/reader/simple_reader/a.cpp\n"
        self.assertEqual(list_commits.parse_numstat(reader)[4], True)
        reader_test = "1\t1\tydb/core/tx/columnshard/engines/reader/ut/a_ut.cpp\n"
        self.assertEqual(list_commits.parse_numstat(reader_test)[4], False)

    def test_parse_day(self):
        self.assertEqual(str(list_commits.parse_day("2026-09-28")), "2026-09-28")
        with self.assertRaises(ValueError):
            list_commits.parse_day("not-a-date")

    def test_main_rejects_bad_dates(self):
        self.assertEqual(list_commits.main(["--repo", ".", "--ref", "HEAD", "--since", "x", "--until", "2026-10-02"]), 2)
        self.assertEqual(list_commits.main(["--repo", ".", "--ref", "HEAD", "--since", "2026-10-03", "--until", "2026-10-02"]), 2)

    def test_pr_number(self):
        self.assertEqual(list_commits.pr_number("Fix races in scans (#53382)"), "53382")
        self.assertEqual(list_commits.pr_number("No number"), "")


class RepoTest(unittest.TestCase):
    def test_period_and_paths(self):
        with tempfile.TemporaryDirectory() as repo:
            subprocess.run(["git", "init", "-q", repo], check=True)
            commit(repo, "ydb/core/tx/columnshard/a.cpp", "before (#1)", "2026-09-27T23:00:00+00:00")
            commit(repo, "ydb/core/tx/columnshard/a.cpp", "first day (#2)", "2026-09-28T00:30:00+00:00")
            commit(repo, "ydb/core/other/x.cpp", "other path (#3)", "2026-09-29T10:00:00+00:00")
            commit(repo, "ydb/core/tx/columnshard/ut/a_ut.cpp", "tests (#4)", "2026-10-02T23:30:00+00:00")
            commit(repo, "ydb/core/tx/columnshard/a.cpp", "after (#5)", "2026-10-03T00:30:00+00:00")
            rows = list_commits.list_commits(repo, "HEAD", "2026-09-28", "2026-10-02", ["ydb/core/tx/columnshard"])
            self.assertEqual([row[3] for row in rows], ["4", "2"])
            self.assertEqual(rows[0][7], "yes")
            self.assertEqual(rows[1][7], "no")
            self.assertEqual(rows[1][8], "no")
            self.assertEqual(rows[1][9], "first day (#2)")


if __name__ == "__main__":
    unittest.main()
