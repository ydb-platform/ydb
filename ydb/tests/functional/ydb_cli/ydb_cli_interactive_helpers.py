# -*- coding: utf-8 -*-

import os
import select
import signal
import sys
import tempfile
import time
import uuid
import pexpect
import pytest

from pexpect.spawnbase import SpawnBase
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

    @staticmethod
    def close_interactive(child, timeout: float = 60):
        """Wait for CLI shutdown, including LLVM's exit-time profile write.

        The caller must request exit first. Closing the PTY after just ``Bye!``
        can kill the process while it is still writing its coverage profile.
        A failed shutdown must remain a test failure, even during cleanup.
        """
        if child.closed:
            return
        original_error = sys.exc_info()[1]
        process = child.ptyproc

        def wait_for_exit(deadline, drain=False):
            # pexpect.expect()/isalive()/wait() can block when a live child
            # closes stdio. Reap with WNOHANG and drain only a readable PTY.
            while not process.terminated or (drain and not child.flag_eof):
                if not process.terminated:
                    pid, status = os.waitpid(process.pid, os.WNOHANG)
                    if pid:
                        process.status = status
                        process.terminated = True
                        process.exitstatus = os.WEXITSTATUS(status) if os.WIFEXITED(status) else None
                        process.signalstatus = os.WTERMSIG(status) if os.WIFSIGNALED(status) else None
                if process.terminated and (not drain or child.flag_eof):
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                interval = min(0.02, remaining)
                if drain and not child.flag_eof:
                    if select.select([child.child_fd], [], [], interval)[0]:
                        try:
                            # Preserve pexpect's decoding and output logs without
                            # spawn.read_nonblocking()'s blocking isalive() call.
                            SpawnBase.read_nonblocking(child, 65536)
                        except pexpect.EOF:
                            pass
                else:
                    # EOF does not imply process exit. Only poll after EOF or
                    # during forced cleanup; do not keep reading a closed PTY.
                    time.sleep(interval)
            return True

        failure = None
        cause = None
        try:
            deadline = time.monotonic() + timeout
            if not wait_for_exit(deadline, drain=True):
                failure = f"process shutdown did not finish within {timeout}s"
            else:
                child.close()
                if child.signalstatus is not None:
                    failure = f"Interactive CLI terminated by signal {child.signalstatus}"
                elif child.exitstatus != 0:
                    failure = f"Interactive CLI exit_code={child.exitstatus}, expected 0"
        except Exception as error:
            failure = str(error)
            cause = error

        if failure is None:
            return

        killed = False
        if not process.terminated:
            try:
                # Do not use pexpect.kill(): it calls the possibly blocking
                # isalive() before sending the signal.
                os.kill(process.pid, signal.SIGKILL)
                killed = True
            except ProcessLookupError:
                pass
            if not wait_for_exit(time.monotonic() + 5):
                # Even an uninterruptible process must not hang test cleanup.
                process.fileobj.close()
                process.closed = child.closed = True
                process.fd = child.child_fd = -1
        if not child.closed:
            child.close()
        detail = f"Interactive CLI shutdown failed: {failure}"
        if killed:
            detail += "; child was killed by signal 9, coverage may be incomplete"
        raise AssertionError(detail) from (original_error or cause)

    @staticmethod
    def _isolated_profile_file() -> str:
        """Return a unique path to a non-existent file for --profile-file.

        Without this, the CLI falls back to the user's default profile config
        (~/.ydb/config/config.yaml), which on developer machines may contain an
        active profile that overrides --endpoint/--database in the welcome
        message and leaks into other interactive tests. Pointing --profile-file
        to a non-existent path makes the CLI behave as if no profile config
        exists, regardless of the host environment.
        """
        return os.path.join(
            tempfile.gettempdir(),
            f"ydb_cli_test_no_profile_{os.getpid()}_{uuid.uuid4().hex}.yaml",
        )

    @classmethod
    def _with_profile_isolation(cls, extra_args: Optional[List[str]]) -> List[str]:
        """Inject --profile-file pointing to an empty path unless the caller already set it."""
        if extra_args is None:
            extra_args = []
        if "--profile-file" in extra_args:
            return list(extra_args)
        return ["--profile-file", cls._isolated_profile_file()] + list(extra_args)

    @classmethod
    def spawn_interactive(cls, timeout: int = 15, extra_args: Optional[List[str]] = None, env_name: str = "YDB_CLI_BINARY", env: Optional[dict[str, str]] = None) -> pexpect.spawn:
        extra_args = cls._with_profile_isolation(extra_args)
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
