# -*- coding: utf-8 -*-

import os
import logging
import pytest
import tempfile
import uuid

import yatest

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY enviroment variable is not specified")


class TestProfileWithInvalidCaFile:
    @classmethod
    def setup_class(cls):
        cls.profile_file = os.path.join(tempfile.gettempdir(), f"ydb_test_profile_{uuid.uuid4().hex}.yaml")
        cls.profile_name = "test"
        cls.invalid_ca_file = "/non/existent/ca/certificate.pem"
        result = cls.execute_ydb_cli_command(
            ["config", "profile", "create", cls.profile_name, "--ca-file", cls.invalid_ca_file]
        )
        assert result['exit_code'] == 0, f"Profile creation failed: {result['stderr']}"

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'profile_file') and os.path.exists(cls.profile_file):
            os.remove(cls.profile_file)

    @classmethod
    def execute_ydb_cli_command(cls, args, stdin=None):
        full_args = ["--profile-file", cls.profile_file] + args
        execution = yatest.common.execute([ydb_bin()] + full_args, stdin=stdin, check_exit_code=False)
        result = {
            'stdout': execution.std_out.decode('utf-8') if execution.std_out else '',
            'stderr': execution.std_err.decode('utf-8') if execution.std_err else '',
            'exit_code': execution.exit_code
        }
        logger.debug("Command: %s", full_args)
        logger.debug("Exit code: %d", result['exit_code'])
        logger.debug("stdout:\n%s", result['stdout'])
        logger.debug("stderr:\n%s", result['stderr'])
        return result

    def test_local_command_with_invalid_ca_file_succeeds(self):
        result = self.execute_ydb_cli_command(
            ["-p", self.profile_name, "config", "profile", "get", self.profile_name]
        )
        assert result['exit_code'] == 0, f"Local command failed with exit code {result['exit_code']}: {result['stderr']}"
        assert "ca-file" in result['stdout'].lower() or self.invalid_ca_file in result['stdout']

    def test_connection_command_with_invalid_ca_file_fails(self):
        result = self.execute_ydb_cli_command(
            ["-p", self.profile_name, "sql", "-s", "SELECT 1"]
        )
        assert result['exit_code'] != 0, "Connection command should fail with invalid CA file"
        error_output = result['stderr'] + result['stdout']
        assert "ca" in error_output.lower() or "certificate" in error_output.lower()

