# -*- coding: utf-8 -*-

import os
import sys
import pexpect
import pytest

from typing import List, Optional
from ydb.tests.functional.ydb_cli.ydb_cli_helpers import ydb_bin, BaseCliTestWithDatabase
from ydb.tests.oss.ydb_sdk_import import ydb


class BaseInteractiveTest(BaseCliTestWithDatabase):
    @pytest.fixture(autouse=True)
    def _tmp(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + tmp_path.name
        self.create_table(self.table_path)

    PROMPT_TIMEOUT = 15

    @staticmethod
    def _send_query(child, query: str):
        child.sendline(query)
        child.send('\r')

    @classmethod
    def spawn_interactive(cls, timeout: int = 15, extra_args: Optional[List[str]] = None, env_name: str = "YDB_CLI_BINARY", env: Optional[dict[str, str]] = None) -> pexpect.spawn:
        if extra_args is None:
            extra_args = []
        if env is None:
            env = os.environ.copy()

        env["TERM"] = "xterm-256color"
        child = pexpect.spawn(
            ydb_bin(env_name),
            ["--endpoint", cls.grpc_endpoint(), "--database", cls.root_dir] + extra_args,
            encoding="utf-8",
            timeout=timeout,
            env=env,
        )
        child.logfile_read = sys.stdout
        return child

    @classmethod
    def create_table(cls, table_path: str):
        session = cls.driver.table_client.session().create()
        session.create_table(
            table_path,
            ydb.TableDescription()
            .with_column(ydb.Column("id", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
            .with_column(ydb.Column("name", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
            .with_primary_keys("id"),
        )
