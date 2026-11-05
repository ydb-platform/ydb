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
    subprocess.run(["git", "-C", repo, "add", path], check=True)
    commit_index(repo, subject, date)


def commit_index(repo, subject, date):
    env = dict(os.environ, GIT_AUTHOR_DATE=date, GIT_COMMITTER_DATE=date,
               GIT_AUTHOR_NAME="A", GIT_AUTHOR_EMAIL="a@x", GIT_COMMITTER_NAME="A", GIT_COMMITTER_EMAIL="a@x")
    subprocess.run(["git", "-C", repo, "commit", "-q", "-m", subject], check=True, env=env)


class ParseTest(unittest.TestCase):
    def test_parse_numstat(self):
        text = "3\t1\tydb/core/tx/columnshard/a.cpp\x005\t0\tydb/core/kqp/ut/olap/b_ut.cpp\x00"
        self.assertEqual(list_commits.parse_numstat(text), (2, 8, 1, False, False))
        self.assertEqual(list_commits.parse_numstat("1\t0\tx/ut/a.cpp\x00-\t-\tx/tests/b.bin\x00"), (2, 1, 0, True, False))
        self.assertEqual(list_commits.parse_numstat(""), (0, 0, 0, False, False))
        reader = "1\t1\tydb/core/tx/columnshard/engines/reader/simple_reader/a.cpp\x00"
        self.assertEqual(list_commits.parse_numstat(reader)[4], True)
        reader_test = "1\t1\tydb/core/tx/columnshard/engines/reader/ut/a_ut.cpp\x00"
        self.assertEqual(list_commits.parse_numstat(reader_test)[4], False)

    def test_numstat_preserves_tabs_and_newlines_in_paths(self):
        text = "1\t0\tydb/core/tx/columnshard/engines/reader/tab\tand\nnewline.cpp\0"
        self.assertEqual(list_commits.parse_numstat(text), (1, 1, 0, False, True))

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

    def test_backdated_commit_does_not_hide_eligible_ancestor(self):
        with tempfile.TemporaryDirectory() as repo:
            subprocess.run(["git", "init", "-q", repo], check=True)
            path = "ydb/core/tx/columnshard/engines/reader/a.cpp"
            commit(repo, path, "eligible ancestor", "2026-09-29T12:00:00+00:00")
            commit(repo, path, "backdated middle", "2026-09-27T12:00:00+00:00")
            commit(repo, path, "eligible head", "2026-10-01T12:00:00+00:00")
            rows = list_commits.list_commits(repo, "HEAD", "2026-09-28", "2026-10-02", list_commits.DEFAULT_PATHS)
            self.assertEqual([row[9] for row in rows], ["eligible head", "eligible ancestor"])

    def test_renames_classify_both_paths(self):
        test_path = "ydb/core/kqp/ut/olap/common.cpp"
        reader_path = "ydb/core/tx/columnshard/engines/reader/common.cpp"
        cases = [
            (test_path, reader_path, "no", "yes"),
            (reader_path, test_path, "no", "yes"),
            (test_path, "ydb/core/tx/columnshard/ut/common.cpp", "yes", "no"),
        ]
        for old, new, tests_only, audit in cases:
            with self.subTest(old=old, new=new), tempfile.TemporaryDirectory() as repo:
                subprocess.run(["git", "init", "-q", repo], check=True)
                subprocess.run(["git", "-C", repo, "config", "diff.renames", "true"], check=True)
                commit(repo, old, "original helper", "2026-09-27T12:00:00+00:00")
                os.makedirs(os.path.dirname(os.path.join(repo, new)), exist_ok=True)
                subprocess.run(["git", "-C", repo, "mv", old, new], check=True)
                commit_index(repo, "move helper", "2026-09-29T12:00:00+00:00")
                rows = list_commits.list_commits(repo, "HEAD", "2026-09-28", "2026-10-02", list_commits.DEFAULT_PATHS)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0][4:9], ["1", "0", "0", tests_only, audit])


if __name__ == "__main__":
    unittest.main()
