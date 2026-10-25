"""The guarded `ydb` CLI shim: it must never let the real CLI do anything but talk to mock endpoints."""
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest

if getattr(sys, "is_standalone_binary", False):
    import yatest.common
    SHIM = yatest.common.source_path("ydb/apps/bridge_skipper/ydbd_mock/scripts/ydb_shim.py")
else:
    SHIM = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts", "ydb_shim.py"))


class ShimTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(os.path.isfile(SHIM), "ydb_shim.py is required test data")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.run_dir = self._tmp.name
        with open(os.path.join(self.run_dir, "endpoint_map.json"), "w") as f:
            json.dump({"mock-pile1-1": "127.0.0.1:22100"}, f)
        self.fake = os.path.join(self.run_dir, "fake_ydb.sh")
        with open(self.fake, "w") as f:
            f.write('#!/bin/sh\necho "fake-ydb $@"\n')
        os.chmod(self.fake, os.stat(self.fake).st_mode | stat.S_IXUSR)

    def shim(self, *argv):
        env = dict(os.environ, YDBD_MOCK_RUN_DIR=self.run_dir, YDBD_MOCK_REAL_YDB=self.fake,
                   Y_PYTHON_ENTRY_POINT=":main")
        proc = subprocess.run([sys.executable, SHIM] + list(argv), env=env, capture_output=True, text=True,
                              timeout=10)
        return proc.returncode, proc.stdout, proc.stderr

    EP = ["-e", "grpc://mock-pile1-1:2135"]

    def test_a_mock_endpoint_is_translated_and_passed_on(self):
        code, out, err = self.shim(*self.EP, "admin", "cluster", "bridge", "list")
        self.assertEqual(code, 0, err)
        self.assertIn("grpc://127.0.0.1:22100", out)

    def test_the_bridge_update_command_is_not_the_cli_updater(self):
        code, out, err = self.shim(*self.EP, "admin", "cluster", "bridge", "update", "--set", "pile2=DISCONNECTED")
        self.assertEqual(code, 0, err)

    def test_foreign_endpoints_are_refused(self):
        code, _, _ = self.shim("-e", "grpc://example.com:2135", "monitoring", "healthcheck")
        self.assertEqual(code, 3)

    def test_compact_endpoint_option_cannot_hide_a_foreign_endpoint(self):
        code, _, _ = self.shim("-e", "grpc://mock-pile1-1:2135", "-egrpc://foreign:2135",
                               "monitoring", "healthcheck")
        self.assertEqual(code, 3)

    def test_grouped_short_options_cannot_hide_a_foreign_endpoint(self):
        code, _, _ = self.shim("-e", "grpc://mock-pile1-1:2135", "-vegrpc://foreign:2135",
                               "monitoring", "healthcheck")
        self.assertEqual(code, 3)

    def test_profiles_are_refused_in_every_spelling(self):
        for arg in (["-p", "dev"], ["--profile", "dev"], ["--profile=dev"], ["-pdev"], ["-p=dev"]):
            with self.subTest(arg=arg):
                code, _, _ = self.shim(*self.EP, *arg, "scheme", "ls")
                self.assertEqual(code, 3)

    def test_commands_that_change_the_cli_or_its_profiles_are_refused(self):
        ep = self.EP
        for argv in (ep + ["update"], ep + ["init"], ep + ["install"], ep + ["config", "profile", "list"],
                     ep + ["-d", "/Root", "update"], ["--assume-yes"] + ep + ["update"],
                     ep + ["--timeout", "5", "update"]):
            with self.subTest(argv=argv):
                self.assertEqual(self.shim(*argv)[0], 3)

    def test_without_an_endpoint_only_help_is_allowed(self):
        self.assertEqual(self.shim("scheme", "ls")[0], 3)
        self.assertEqual(self.shim("--help")[0], 0)


if __name__ == "__main__":
    unittest.main()
