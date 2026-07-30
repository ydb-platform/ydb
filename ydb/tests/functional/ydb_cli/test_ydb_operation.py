# -*- coding: utf-8 -*-

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import BaseCliTestWithDatabase
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

import logging

logger = logging.getLogger(__name__)


class TestSetNotNullOperationCli(BaseCliTestWithDatabase):
    """Tests for `ydb operation list|get|cancel|forget` CLI commands applied to Set Not Null operations."""

    @classmethod
    def get_cluster_configurator(cls):
        return KikimrConfigGenerator(extra_feature_flags=["enable_set_column_constraint"])

    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.pool = ydb.QuerySessionPool(cls.driver)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "pool") and cls.pool is not None:
            cls.pool.stop()
        super().teardown_class()

    def _create_table_and_data(self, table_path):
        self.pool.execute_with_retries(
            """
            CREATE TABLE `{0}` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
            """.format(table_path)
        )

    def _upsert_data(self, table_path):
        self.pool.execute_with_retries(
            """
            UPSERT INTO `{0}` (Key, Value) VALUES (1, "a"), (2, "b");
            """.format(table_path)
        )

    def _run_set_not_null(self, table_path):
        self.pool.execute_with_retries(
            """
            ALTER TABLE `{0}` ALTER COLUMN Value SET NOT NULL;
            """.format(table_path)
        )

    def test_list_empty(self, tmp_path):
        result = self.execute_ydb_cli_command(["operation", "list", "setnotnull"])
        assert result.exit_code == 0

    def test_list_after_set_not_null(self, tmp_path):
        table_path = self.root_dir + "/" + tmp_path.name

        self._create_table_and_data(table_path)
        self._upsert_data(table_path)
        self._run_set_not_null(table_path)

        result = self.execute_ydb_cli_command(["operation", "list", "setnotnull"])
        assert result.exit_code == 0
        assert table_path in result.stdout or "ydb://setnotnull" in result.stdout

    def test_get_set_not_null(self, tmp_path):
        table_path = self.root_dir + "/" + tmp_path.name

        self._create_table_and_data(table_path)
        self._upsert_data(table_path)
        self._run_set_not_null(table_path)

        list_result = self.execute_ydb_cli_command(["operation", "list", "setnotnull"])
        assert list_result.exit_code == 0

        operation_id = self._extract_first_operation_id(list_result.stdout)
        assert operation_id, "Could not find operation id in list output:\n" + list_result.stdout

        get_result = self.execute_ydb_cli_command(["operation", "get", operation_id])
        assert get_result.exit_code == 0

    def test_cancel_set_not_null(self, tmp_path):
        table_path = self.root_dir + "/" + tmp_path.name

        self._create_table_and_data(table_path)
        self._upsert_data(table_path)
        self._run_set_not_null(table_path)

        list_result = self.execute_ydb_cli_command(["operation", "list", "setnotnull"])
        operation_id = self._extract_first_operation_id(list_result.stdout)
        assert operation_id, "Could not find operation id in list output:\n" + list_result.stdout

        # Operation is already finished (SET NOT NULL completed synchronously from the
        # caller's perspective), so cancelling it must fail (mirrors the SDK-level check:
        # Cancel of a terminal op is not SUCCESS).
        cancel_result = self.execute_ydb_cli_command(
            ["operation", "cancel", operation_id], check_exit_code=False
        )
        assert cancel_result.exit_code != 0

        # The operation must still be gettable after a failed cancel attempt.
        get_result = self.execute_ydb_cli_command(["operation", "get", operation_id])
        assert get_result.exit_code == 0

    def test_forget_set_not_null(self, tmp_path):
        table_path = self.root_dir + "/" + tmp_path.name

        self._create_table_and_data(table_path)
        self._upsert_data(table_path)
        self._run_set_not_null(table_path)

        list_result = self.execute_ydb_cli_command(["operation", "list", "setnotnull"])
        operation_id = self._extract_first_operation_id(list_result.stdout)
        assert operation_id, "Could not find operation id in list output:\n" + list_result.stdout

        forget_result = self.execute_ydb_cli_command(["operation", "forget", operation_id])
        assert forget_result.exit_code == 0

        get_result = self.execute_ydb_cli_command(
            ["operation", "get", operation_id], check_exit_code=False
        )
        assert get_result.exit_code != 0

    @staticmethod
    def _extract_first_operation_id(list_output):
        for line in list_output.splitlines():
            line = line.strip()
            if "ydb://setnotnull" in line:
                for token in line.split():
                    if token.startswith("ydb://setnotnull"):
                        return token.strip(',"')
        return None
