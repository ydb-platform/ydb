import importlib.util
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location(
    "sync", Path(__file__).resolve().parents[1] / "sync_github_data_to_inner.py")
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)


class SyncTest(unittest.TestCase):
    def test_project_duplicates_do_not_restore_old_labels(self):
        old = {"url": "https://github.com/ydb-platform/ydb/issues/1",
               "labels": '[{"name":"ai_review_in_process"}]', "state": "OPEN", "exported_at": 10}
        new = dict(old, labels=None, state="CLOSED", exported_at=20)
        for rows in ([old, new], [new, old]):
            copied = sync.prepare_rows(rows, 30)
            self.assertEqual(len(copied), 1)
            self.assertIsNone(copied[0]["labels"])
            self.assertEqual(copied[0]["state"], "CLOSED")
            self.assertEqual(copied[0]["synced_at"], 30)
            self.assertEqual(copied[0]["exported_at"], 20)

    def test_repository_urls_are_distinct_and_missing_urls_fail(self):
        rows = [{"url": "https://github.com/ydb-platform/%s/issues/1" % repo}
                for repo in ("ydb", "other")]
        self.assertEqual(len(sync.prepare_rows(rows, 30)), 2)
        with self.assertRaises(ValueError):
            sync.prepare_rows([{"url": None}], 30)

    def test_copy_includes_old_closed_and_unlabelled_rows_and_uses_batches(self):
        columns = Mock()
        sdk = SimpleNamespace(
            BulkUpsertColumns=lambda: columns,
            PrimitiveType=SimpleNamespace(Utf8="Utf8", Uint64="Uint64", Json="Json", Timestamp="Timestamp"),
            OptionalType=lambda t: t,
            retry_operation_sync=lambda callback: callback())
        source = Mock()
        source.get_table_path.return_value = "github_data/pull_requests"
        source.execute_scan_query.return_value = [
            {"url": "https://github.com/ydb-platform/ydb/pull/%d" % n,
             "pr_number": n, "state": "CLOSED", "labels": None,
             "exported_at": n} for n in range(5)]
        driver, pool = Mock(), Mock()
        pool.retry_operation_sync.side_effect = lambda callback: callback(Mock())
        with patch.dict(sys.modules, {"ydb": sdk}):
            self.assertEqual(sync.sync_table(source, driver, pool, "/inner", "pull_requests", 2), 5)
        query = source.execute_scan_query.call_args[0][0]
        self.assertNotIn("WHERE", query)
        self.assertIn("`pr_number`", query)
        batches = driver.table_client.bulk_upsert.call_args_list
        self.assertEqual([len(call.args[1]) for call in batches], [2, 2, 1])
        self.assertTrue(all(call.args[0] == "/inner/github_data/pull_requests" for call in batches))

    def test_destination_failure_is_not_reported_as_success(self):
        source, driver, pool = Mock(), Mock(), Mock()
        source.get_table_path.return_value = "github_data/issues"
        source.execute_scan_query.return_value = []
        pool.retry_operation_sync.side_effect = RuntimeError("permission denied")
        with patch.dict(sys.modules, {"ydb": Mock()}):
            with self.assertRaisesRegex(RuntimeError, "permission denied"):
                sync.sync_table(source, driver, pool, "/inner", "issues", 100)

    def test_missing_destination_token_fails_before_connecting(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(SystemExit) as error:
                sync.main([])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
