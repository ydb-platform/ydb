#!/usr/bin/env python3
"""CI_DISABLE_YDB_QA kill-switch for Nebius / non-YC hosts."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

_SCRIPTS = Path(__file__).resolve().parents[4]
_UTILS_ANALYTICS = Path(__file__).resolve().parents[3] / "analytics"
sys.path.insert(0, str(_UTILS_ANALYTICS))
sys.path.insert(0, str(_SCRIPTS / "analytics"))

# ydb_wrapper imports the YDB SDK at module load; stub it for unit tests.
sys.modules.setdefault("ydb", MagicMock())

from collector.flush import flush_file, has_send_credentials  # noqa: E402
from collector.schema import is_ydb_qa_disabled as schema_disabled  # noqa: E402
from ydb_wrapper import YDBWrapper, is_ydb_qa_disabled  # noqa: E402


class YdbQaDisableTest(unittest.TestCase):
    def setUp(self):
        self._saved = {
            key: os.environ.get(key)
            for key in (
                "CI_DISABLE_YDB_QA",
                "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS",
                "ANALYTICS_YDB_CREDENTIALS",
                "YDB_QA_CONFIG",
            )
        }

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_is_ydb_qa_disabled_truthy(self):
        os.environ.pop("CI_DISABLE_YDB_QA", None)
        self.assertFalse(is_ydb_qa_disabled())
        self.assertFalse(schema_disabled())
        os.environ["CI_DISABLE_YDB_QA"] = "1"
        self.assertTrue(is_ydb_qa_disabled())
        self.assertTrue(schema_disabled())
        os.environ["CI_DISABLE_YDB_QA"] = "true"
        self.assertTrue(is_ydb_qa_disabled())

    def test_wrapper_skips_config_and_credentials(self):
        os.environ["CI_DISABLE_YDB_QA"] = "1"
        os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = "/tmp/fake-sa.json"
        with YDBWrapper(silent=True) as wrapper:
            self.assertEqual(wrapper.config_source, "disabled:CI_DISABLE_YDB_QA")
            self.assertFalse(wrapper.check_credentials())
            self.assertEqual(wrapper.database_endpoint, "")
            self.assertEqual(wrapper.database_path, "")

    def test_flush_keeps_local_batch_when_disabled(self):
        os.environ["CI_DISABLE_YDB_QA"] = "1"
        os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = "/tmp/fake-sa.json"
        self.assertFalse(has_send_credentials())
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    '{"name":"step","kind":"info","source":"job","run_id":1,'
                    '"span_id":"s1","event_ts":"2026-09-21T10:00:00Z"}\n'
                )
            with mock.patch("collector.flush._open_ydb_wrapper") as open_wrapper:
                uploaded = flush_file(path)
                open_wrapper.assert_not_called()
            self.assertEqual(uploaded, 0)


if __name__ == "__main__":
    unittest.main()
