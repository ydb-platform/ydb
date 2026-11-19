#!/usr/bin/env python3
"""Clone and fetch may use the probe proxy. Push stays on github.com and keeps the token."""

import importlib.util
import logging
import os
import subprocess
import unittest
from pathlib import Path
from unittest import mock


def _load_automerge():
    path = Path(__file__).resolve().parents[3] / "ydb" / "ci" / "rightlib" / "automerge.py"
    spec = importlib.util.spec_from_file_location("automerge_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


automerge = _load_automerge()


def _pr():
    pr = mock.Mock()
    pr.base.ref = "main"
    pr.number = 42
    pr.head.sha = "abc123head"
    pr.head.user.login = "someone"
    pr.head.ref = "feature"
    pr.labels = []
    return pr


def _merger():
    merger = automerge.PrAutomerger.__new__(automerge.PrAutomerger)
    merger.repo_name = "ydb-platform/ydb"
    merger.token = "secret-token"
    merger.logger = logging.getLogger("test_automerge_proxy")
    branch = mock.Mock()
    branch.commit.sha = "def456base"
    merger.repo = mock.Mock()
    merger.repo.get_branch.return_value = branch
    return merger


class FetchProxyArgsTest(unittest.TestCase):
    def test_empty_means_direct(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GH_FETCH_PROXY_URL", None)
            self.assertEqual(automerge.fetch_proxy_args(), [])

    def test_proxy_rewrites_only_this_command(self):
        with mock.patch.dict(os.environ, {"GH_FETCH_PROXY_URL": "https://gh-proxy.com/"}):
            self.assertEqual(automerge.fetch_proxy_args(), [
                "-c",
                "url.https://gh-proxy.com/https://github.com/.insteadOf=https://github.com/",
            ])


class GitMergeProxyTest(unittest.TestCase):
    def _run(self, env, rev_parse=None):
        calls = []
        rev_parse = rev_parse or {
            "origin/main": "def456base",
            "PR": "abc123head",
        }

        def run(args, **kwargs):
            calls.append(list(args))
            if "rev-parse" in args:
                rev = args[-1]
                out = (rev_parse.get(rev, "") + "\n").encode()
                return subprocess.CompletedProcess(args, 0, out, b"")
            return subprocess.CompletedProcess(args, 0, b"", b"")

        with mock.patch.dict(os.environ, env, clear=False):
            with mock.patch.object(automerge.subprocess, "run", side_effect=run):
                with mock.patch.object(automerge.shutil, "rmtree"):
                    with mock.patch.object(automerge.os, "chdir"):
                        self.assertTrue(_merger().git_merge_pr(_pr()))
        return calls

    def test_proxy_on_clone_and_fetch_only(self):
        calls = self._run({"GH_FETCH_PROXY_URL": "https://gh-proxy.com"})
        instead_of = "url.https://gh-proxy.com/https://github.com/.insteadOf=https://github.com/"
        clone = next(cmd for cmd in calls if "clone" in cmd)
        fetches = [cmd for cmd in calls if "fetch" in cmd]
        push = next(cmd for cmd in calls if cmd[-1:] == ["push"])
        config = next(cmd for cmd in calls if "config" in cmd)

        self.assertIn(instead_of, clone)
        self.assertIn("https://github.com/ydb-platform/ydb.git", clone)
        self.assertNotIn("secret-token", " ".join(clone))
        self.assertEqual(len(fetches), 2)
        for fetch in fetches:
            self.assertIn(instead_of, fetch)
            self.assertNotIn("secret-token", " ".join(fetch))
        self.assertNotIn(instead_of, push)
        self.assertNotIn("secret-token", " ".join(push))
        self.assertLess(calls.index(config), calls.index(push))
        self.assertIn("AUTHORIZATION: basic ", config[3])
        self.assertNotIn("gh-proxy.com", " ".join(config))

    def test_without_proxy_clone_has_no_token_in_the_url(self):
        env = os.environ.copy()
        env.pop("GH_FETCH_PROXY_URL", None)
        calls = self._run(env)
        clone = next(cmd for cmd in calls if "clone" in cmd)
        push = next(cmd for cmd in calls if cmd[-1:] == ["push"])
        self.assertNotIn("-c", clone)
        self.assertIn("https://github.com/ydb-platform/ydb.git", clone)
        self.assertNotIn("secret-token", " ".join(clone))
        self.assertEqual(push, ["git", "push"])

    def test_mismatched_base_tip_refetches_without_proxy(self):
        calls = []
        rev_values = {
            "origin/main": ["wrongbase", "def456base"],
            "PR": ["abc123head"],
        }

        def run(args, **kwargs):
            calls.append(list(args))
            if "rev-parse" in args:
                rev = args[-1]
                out = (rev_values[rev].pop(0) + "\n").encode()
                return subprocess.CompletedProcess(args, 0, out, b"")
            return subprocess.CompletedProcess(args, 0, b"", b"")

        with mock.patch.dict(os.environ, {"GH_FETCH_PROXY_URL": "https://gh-proxy.com"}, clear=False):
            with mock.patch.object(automerge.subprocess, "run", side_effect=run):
                with mock.patch.object(automerge.shutil, "rmtree"):
                    with mock.patch.object(automerge.os, "chdir"):
                        self.assertTrue(_merger().git_merge_pr(_pr()))

        base_fetches = [cmd for cmd in calls if "fetch" in cmd and "main" in cmd]
        self.assertEqual(len(base_fetches), 2)
        self.assertIn("insteadOf", " ".join(base_fetches[0]))
        self.assertNotIn("insteadOf", " ".join(base_fetches[1]))


if __name__ == "__main__":
    unittest.main()
