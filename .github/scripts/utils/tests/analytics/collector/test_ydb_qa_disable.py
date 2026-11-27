#!/usr/bin/env python3
"""CI_DISABLE_YDB_QA: disabled wrapper must not open YDB; flush keeps the batch."""

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

sys.modules.setdefault("ydb", MagicMock())

from collector.flush import flush_file, has_send_credentials  # noqa: E402
from ydb_wrapper import YDBWrapper  # noqa: E402


class YdbQaDisableTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.dict(
            os.environ,
            {
                "CI_DISABLE_YDB_QA": "1",
                "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS": "/tmp/fake-sa.json",
            },
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_wrapper_disabled_skips_credentials(self):
        with YDBWrapper(silent=True) as wrapper:
            self.assertFalse(wrapper.check_credentials())

    def test_flush_keeps_local_batch_when_disabled(self):
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
            self.assertTrue(os.path.exists(path))
            self.assertFalse(os.path.exists(path + ".offset"))


if __name__ == "__main__":
    unittest.main()
