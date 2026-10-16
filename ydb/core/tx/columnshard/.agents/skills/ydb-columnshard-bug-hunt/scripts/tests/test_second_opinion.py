#!/usr/bin/env python3
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import second_opinion  # noqa: E402

ENV = {
    "reviewers": [
        {"id": "claude/opus", "ok": None, "same_as_main": True},
        {"id": "codex/a", "ok": False, "same_as_main": False},
        {"id": "codex/b", "ok": True, "same_as_main": False},
        {"id": "claude/sonnet", "ok": True, "same_as_main": False},
    ],
    "default_reviewer": "codex/b",
}


class PickTest(unittest.TestCase):
    def test_default(self):
        self.assertEqual(second_opinion.pick_reviewer(ENV, None)["id"], "codex/b")

    def test_explicit(self):
        self.assertEqual(second_opinion.pick_reviewer(ENV, "claude/sonnet")["id"], "claude/sonnet")

    def test_unusable_is_rejected(self):
        self.assertIsNone(second_opinion.pick_reviewer(ENV, "codex/a"))
        self.assertIsNone(second_opinion.pick_reviewer(ENV, "claude/opus"))

    def test_none_usable(self):
        self.assertIsNone(second_opinion.pick_reviewer({"reviewers": []}, None))


class TextTest(unittest.TestCase):
    def test_strip_ansi(self):
        self.assertEqual(second_opinion.strip_ansi("\x1b[1mcodex\x1b[0m"), "codex")

    def test_refusal(self):
        self.assertTrue(second_opinion.is_refusal("ERROR: This content was flagged for possible cybersecurity risk."))
        self.assertTrue(second_opinion.is_refusal("I can't help with that."))
        self.assertFalse(second_opinion.is_refusal("none found."))

    def test_framing_says_not_security(self):
        self.assertIn("not security research or penetration testing", second_opinion.FRAMING)


class AskTest(unittest.TestCase):
    def test_ask_reads_stdout(self):
        reviewer = {"id": "fake", "model": "m", "argv": [sys.executable, "-c", "import sys; print(sys.stdin.read()[-5:])"],
                    "stdin": True, "final": "stdout"}
        status, final, _ = second_opinion.ask(reviewer, os.getcwd(), "hello", 30)
        self.assertEqual(status, "ok")
        self.assertEqual(final, "hello")

    def test_ask_reads_last_file(self):
        script = "import sys; open(sys.argv[1], 'w').write('final answer'); print('noise')"
        reviewer = {"id": "fake", "model": "m", "argv": [sys.executable, "-c", script, "{last}"],
                    "stdin": False, "final": "last_file"}
        status, final, raw = second_opinion.ask(reviewer, os.getcwd(), "p", 30)
        self.assertEqual((status, final), ("ok", "final answer"))
        self.assertIn("noise", raw)

    def test_ask_missing_last_file_fails(self):
        reviewer = {"id": "fake", "model": "m", "argv": [sys.executable, "-c", "print('diagnostic only')"],
                    "stdin": False, "final": "last_file"}
        self.assertEqual(second_opinion.ask(reviewer, os.getcwd(), "p", 30)[0], "failed")

    def test_ask_missing_executable_fails(self):
        reviewer = {"id": "fake", "model": "m", "argv": ["no-such-review-cli-xyz"], "stdin": False, "final": "stdout"}
        self.assertEqual(second_opinion.ask(reviewer, os.getcwd(), "p", 30)[0], "failed")

    def test_env_matches_session(self):
        env = {"main": {"harness": "claude", "model": "m1"}}
        self.assertTrue(second_opinion.env_matches_session(env, ("claude", "m1")))
        self.assertFalse(second_opinion.env_matches_session(env, ("codex", "m2")))

    def test_ask_detects_refusal(self):
        reviewer = {"id": "fake", "model": "m",
                    "argv": [sys.executable, "-c", "print('flagged for possible cybersecurity risk')"],
                    "stdin": False, "final": "stdout"}
        self.assertEqual(second_opinion.ask(reviewer, os.getcwd(), "p", 30)[0], "refused")

    def test_main_without_env_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            prompt = os.path.join(tmp, "p.txt")
            with open(prompt, "w") as handle:
                handle.write("x")
            code = second_opinion.main(["--main", "claude=m", "--prompt-file", prompt, "--cwd", tmp,
                                        "--out", os.path.join(tmp, "o"), "--env", os.path.join(tmp, "missing.json")])
            self.assertEqual(code, 2)

    def test_main_writes_answer(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = {"main": {"harness": "claude", "model": "m"}, "reviewers": [{"id": "fake", "model": "m", "ok": True, "same_as_main": False,
                                  "argv": [sys.executable, "-c", "print('answer')"], "stdin": False,
                                  "final": "stdout"}], "default_reviewer": "fake"}
            env_path = os.path.join(tmp, "env.json")
            with open(env_path, "w") as handle:
                json.dump(env, handle)
            prompt = os.path.join(tmp, "p.txt")
            with open(prompt, "w") as handle:
                handle.write("x")
            out = os.path.join(tmp, "o")
            args = ["--prompt-file", prompt, "--cwd", tmp, "--out", out, "--env", env_path]
            self.assertEqual(second_opinion.main(["--main", "codex=other"] + args), 2)
            self.assertEqual(second_opinion.main(["--main", "claude=m"] + args), 0)
            with open(out) as handle:
                self.assertEqual(handle.read().strip(), "answer")


if __name__ == "__main__":
    unittest.main()
