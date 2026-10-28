#!/usr/bin/env python3
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

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

    def test_model_identity(self):
        cases = [
            (("codex", "gpt-6.1-sol"), ("codex", "gpt-6"), False),
            (("codex", "gpt-6"), ("codex", "gpt-6.1-sol"), False),
            (("codex", " GPT-6.1-SOL "), ("codex", "gpt-6.1-sol"), True),
            (("codex", "gpt-6.1-sol"), ("opencode", "openai/gpt-6.1-sol"), True),
            (("opencode", "openai/gpt-6.1-sol"), ("codex", "gpt-6.1-sol"), True),
            (("opencode", "openai/gpt-6.1-sol"), ("opencode", "openai/gpt-6"), False),
            (("opencode", "other/gpt-6"), ("codex", "gpt-6"), False),
            (("opencode", "gpt-6"), ("codex", "gpt-6"), False),
            (("claude", "claude-opus-5-4"), ("claude", "claude-opus-5-5"), False),
            (("claude", "claude-opus-5-5"), ("claude", "opus"), True),
            (("claude", "sonnet"), ("opencode", "anthropic/claude-sonnet-5-5"), True),
            (("claude", "haiku"), ("claude", "claude-haiku-5-5"), True),
            (("claude", "opus"), ("claude", "custom-opus-model"), False),
            (("claude", "opus"), ("claude", "claude-opus-5-5-custom"), False),
            (("opencode", "anthropic/opus"), ("claude", "claude-opus-5-5"), False),
        ]
        for (harness, model), main, expected in cases:
            with self.subTest(harness=harness, model=model, main=main):
                self.assertEqual(detect_env.same_as_main(harness, model, main), expected)
        self.assertFalse(detect_env.same_as_main("codex", "gpt-6", None))

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

    def test_distinct_model_prefix_is_probed(self):
        probed = []

        def runner(spec, model, cwd, prompt, timeout):
            probed.append(model)
            return 0, "OK", ""

        with patch.object(detect_env, "config_models", return_value=["gpt-6"]):
            env = detect_env.detect(
                ("codex", "gpt-6"), [("codex", "gpt-6.1-sol")],
                lambda name: "/bin/codex" if name == "codex" else None,
                runner, 10, False, "/repo",
            )
        self.assertEqual(probed, ["gpt-6.1-sol"])
        self.assertEqual(env["default_reviewer"], "codex/gpt-6.1-sol")
        state = {r["id"]: (r["ok"], r["same_as_main"]) for r in env["reviewers"]}
        self.assertEqual(state, {
            "codex/gpt-6": (None, True),
            "codex/gpt-6.1-sol": (True, False),
        })

    def test_skip_smoke_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "env.json")
            detect_env.main(["--main", "claude=m", "--skip-smoke", "--out", out])
            self.assertFalse(os.path.exists(out))


if __name__ == "__main__":
    unittest.main()
