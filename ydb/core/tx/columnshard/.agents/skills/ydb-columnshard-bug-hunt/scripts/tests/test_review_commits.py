#!/usr/bin/env python3
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import review_commits  # noqa: E402

LIST = (
    "aaa1111\t2026-09-28\tA\t1\t2\t3\t4\tno\tyes\tfirst (#1)\n"
    "bbb2222\t2026-09-29\tB\t2\t1\t1\t0\tyes\tno\ttests only (#2)\n"
    "ccc3333\t2026-09-30\tC\t3\t5\t6\t7\tno\tno\tthird (#3)\n"
)


class ReviewCommitsTest(unittest.TestCase):
    def test_read_commits_skips_tests_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "list.tsv")
            with open(path, "w") as handle:
                handle.write(LIST)
            self.assertEqual(review_commits.read_commits(path), ["aaa1111", "ccc3333"])

    def test_run_all_writes_every_prompt(self):
        seen = []

        def ask(reviewer, cwd, prompt, timeout):
            seen.append(prompt)
            return ("refused", "", "raw") if "ccc3333" in prompt and "Code review" in prompt else ("ok", "answer", "raw")

        with tempfile.TemporaryDirectory() as tmp:
            results = review_commits.run_all({"id": "fake"}, ["aaa1111", "ccc3333"], "/wt", tmp, 2, 10, ask)
            self.assertEqual(len(results), 4)
            self.assertIn(("ccc3333", "lifetime", "refused"), results)
            self.assertTrue(os.path.exists(os.path.join(tmp, "aaa1111-plain.md")))
            with open(os.path.join(tmp, "summary.tsv")) as handle:
                self.assertEqual(len(handle.read().splitlines()), 4)
        self.assertTrue(all("not security research" in prompt for prompt in seen))
        self.assertTrue(all("repository in /wt" in prompt for prompt in seen))

    def test_main_rejects_other_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_path = os.path.join(tmp, "env.json")
            with open(env_path, "w") as handle:
                json.dump({"main": {"harness": "claude", "model": "m"}, "reviewers": []}, handle)
            list_path = os.path.join(tmp, "list.tsv")
            with open(list_path, "w") as handle:
                handle.write(LIST)
            args = ["--commits", list_path, "--cwd", tmp, "--out", os.path.join(tmp, "out"), "--env", env_path]
            self.assertEqual(review_commits.main(["--main", "codex=x"] + args), 2)
            self.assertEqual(review_commits.main(["--main", "claude=m"] + args), 2)


if __name__ == "__main__":
    unittest.main()
