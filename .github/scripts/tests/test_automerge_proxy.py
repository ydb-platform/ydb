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
    pr.head.user.login = "someone"
    pr.head.ref = "feature"
    pr.labels = []
    return pr


def _merger():
    merger = automerge.PrAutomerger.__new__(automerge.PrAutomerger)
    merger.repo_name = "ydb-platform/ydb"
    merger.token = "secret-token"
    merger.logger = logging.getLogger("test_automerge_proxy")
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
    def _run(self, env):
        calls = []

        def run(args, **kwargs):
            calls.append(list(args))
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


if __name__ == "__main__":
    unittest.main()
