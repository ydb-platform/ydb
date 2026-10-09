#!/usr/bin/env python3
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import detect_env  # noqa: E402

MAIN = ("claude", "claude-opus-5-5")


def which(name):
    return "/bin/" + name if name in ("codex", "claude") else None


def answer_ok(spec, model, cwd, prompt, timeout):
    return (1, "", "model not supported") if model == "broken" else (0, "OK", "")


class HelpersTest(unittest.TestCase):
    def test_codex_config_models(self):
        text = 'model = "a"\n[profiles.fast]\nmodel = "b"\nmodel = "a"\n'
        self.assertEqual(detect_env.codex_config_models(text), ["a", "b"])

    def test_same_as_main(self):
        cases = [
            ("claude", "opus", True),
            ("claude", "sonnet", False),
            ("opencode", "anthropic/claude-opus-5-5", True),
            ("codex", "opus", False),
        ]
        for harness, model, expected in cases:
            self.assertEqual(detect_env.same_as_main(harness, model, MAIN), expected, (harness, model))

    def test_smoke_ok(self):
        cases = [((0, "OK"), True), ((0, "ok."), True), ((1, "OK"), False), ((0, "NOT OK"), False)]
        for (code, text), expected in cases:
            self.assertEqual(detect_env.smoke_ok(code, text), expected, text)


class DetectTest(unittest.TestCase):
    def setUp(self):
        self.saved = detect_env.config_models
        detect_env.config_models = lambda harness: ["gpt-x"] if harness == "codex" else []

    def tearDown(self):
        detect_env.config_models = self.saved

    def test_reviewers_and_default(self):
        env = detect_env.detect(MAIN, [("codex", "broken")], which, answer_ok, 10, False, "/repo", ["sonnet"])
        state = {r["id"]: (r["ok"], r["same_as_main"]) for r in env["reviewers"]}
        self.assertEqual(state, {
            "codex/gpt-x": (True, False),
            "codex/broken": (False, False),
            "claude/opus": (None, True),
            "claude/sonnet": (True, False),
        })
        self.assertEqual(env["default_reviewer"], "codex/gpt-x")
        self.assertEqual(env["subagent_models"], ["sonnet"])

    def test_opus_is_preferred(self):
        env = detect_env.detect(("codex", "gpt-x"), [], which, answer_ok, 10, False, "/repo")
        self.assertEqual(env["default_reviewer"], "claude/opus")

    def test_skip_smoke_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "env.json")
            detect_env.main(["--main", "claude=m", "--skip-smoke", "--out", out])
            self.assertFalse(os.path.exists(out))


if __name__ == "__main__":
    unittest.main()
