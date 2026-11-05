import importlib.util
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch


class SyncTest(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "sync", Path(__file__).resolve().parents[1] / "sync_github_data_to_inner.py")
        self.module = importlib.util.module_from_spec(spec)
        self.helpers = Mock()
        self.helpers.ydb_type_to_str.return_value = ("Json", "Json")
        self.sdk = Mock()
        with patch.dict(sys.modules, {"ydb": self.sdk, "data_mart_executor": self.helpers,
                                      "ydb_wrapper": Mock()}):
            spec.loader.exec_module(self.module)

    def test_both_tables_copy_all_rows_with_existing_helpers(self):
        source, destination = Mock(), Mock()
        source.get_table_path.side_effect = lambda table: "configured/" + table
        rows = [{"state": "CLOSED", "labels": None}, {"state": "OPEN", "labels": "[]"}]
        source.execute_scan_query_with_metadata.return_value = (rows, [("labels", "Json")])
        self.module.sync(source, destination)
        self.assertEqual(self.helpers.create_table.call_count, 2)
        self.assertEqual(destination.bulk_upsert_batches.call_count, 2)
        for table, number in self.module.TABLES:
            source.execute_scan_query_with_metadata.assert_any_call(
                f"SELECT * FROM `configured/{table}`;", query_name="sync_" + table)
            self.helpers.create_table.assert_any_call(
                destination, "github_data/" + table, [("labels", "Json")], "column",
                ["created_date"], ["created_date", number, "project_item_id"], None, None)
        self.assertTrue(all(call.args[1] is rows
                            for call in destination.bulk_upsert_batches.call_args_list))

    def test_write_failure_propagates(self):
        source, destination = Mock(), Mock()
        source.execute_scan_query_with_metadata.return_value = ([], [("labels", "Json")])
        destination.bulk_upsert_batches.side_effect = RuntimeError("write failed")
        with self.assertRaisesRegex(RuntimeError, "write failed"):
            self.module.sync(source, destination)

    def test_explicit_wrapper_config_and_credentials_do_not_change_cloud_environment(self):
        spec = importlib.util.spec_from_file_location(
            "wrapper", Path(__file__).resolve().parents[1] / "ydb_wrapper.py")
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"ydb": self.sdk}):
            spec.loader.exec_module(module)
        config = {"databases": {"main": {"endpoint": "grpc://inner:2135", "path": "/inner"}}}
        credentials = object()
        with patch.dict("os.environ", {"YDB_QA_CONFIG": "invalid cloud config"}, clear=True):
            with patch.object(module.YDBWrapper, "_get_cluster_version", return_value="test"):
                with module.YDBWrapper(config=config, credentials=credentials,
                                       enable_statistics=False, silent=True) as wrapper:
                    self.assertTrue(wrapper.check_credentials())
                    self.assertEqual(wrapper.database_path, "/inner")
                    self.assertIs(self.sdk.Driver.call_args.kwargs["credentials"], credentials)
                    self.sdk.credentials_from_env_variables.assert_not_called()
                    self.assertEqual(dict(os.environ),
                                     {"YDB_QA_CONFIG": "invalid cloud config"})

    def test_missing_token_fails_before_opening_database(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "YDB_INNER_TOKEN"):
                self.module.main()
        self.module.YDBWrapper.assert_not_called()


if __name__ == "__main__":
    unittest.main()
