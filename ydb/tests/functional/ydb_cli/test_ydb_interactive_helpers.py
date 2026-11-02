import io
import os
import signal
import sys
import textwrap
import time

import pexpect
import pytest
import yatest.common

from pexpect.spawnbase import SpawnBase
from ydb.tests.functional.ydb_cli.ydb_cli_interactive_helpers import BaseInteractiveTest


@pytest.fixture
def spawn_child():
    children = []

    def spawn(script, *args):
        child = pexpect.spawn(
            yatest.common.binary_path("contrib/tools/python3/bin/python3"),
            ["-u", "-c", textwrap.dedent(script), *map(str, args)],
            encoding="utf-8",
            timeout=5,
        )
        child.logfile_read = sys.stdout
        children.append(child)
        return child

    yield spawn

    for child in children:
        if child.closed:
            continue
        # Keep failing regression tests from leaving their synthetic children
        # behind. Avoid isalive(), which can block after the child closes stdio.
        if not child.ptyproc.terminated:
            try:
                os.kill(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        child.close(force=True)


def test_close_waits_for_exit_time_profile_write(spawn_child, tmp_path):
    report = tmp_path / "coverage.profraw"
    child = spawn_child(
        """
        import atexit
        import sys
        import time

        def write_profile():
            with open(sys.argv[1], "w") as report:
                report.write("header\\n")
                report.flush()
                time.sleep(0.3)
                report.write("complete\\n")
            print("shutdown-output" * 10000)

        atexit.register(write_profile)
        print("Bye!")
        """,
        report,
    )
    output = io.StringIO()
    child.logfile_read = output
    child.expect_exact("Bye!")

    BaseInteractiveTest.close_interactive(child, timeout=5)

    assert report.is_file(), output.getvalue()
    assert report.read_text() == "header\ncomplete\n", output.getvalue()
    assert "shutdown-output" * 10000 in output.getvalue()
    assert child.closed
    assert child.exitstatus == 0
    assert child.signalstatus is None


def test_close_accepts_previously_consumed_eof(spawn_child):
    child = spawn_child('print("Bye!")')
    # This prepares the already-consumed EOF, not a shutdown timeout check.
    child.expect(pexpect.EOF, timeout=60)

    BaseInteractiveTest.close_interactive(child, timeout=5)

    assert child.closed
    assert child.exitstatus == 0
    assert child.signalstatus is None


def test_close_rejects_nonzero_exit(spawn_child):
    child = spawn_child('import sys; print("Bye!"); sys.exit(7)')
    child.expect_exact("Bye!")

    with pytest.raises(AssertionError, match="exit_code=7"):
        BaseInteractiveTest.close_interactive(child, timeout=5)

    assert child.closed
    assert child.exitstatus == 7


def test_close_rejects_signal_exit(spawn_child):
    child = spawn_child(
        'import os, signal; print("Bye!"); os.kill(os.getpid(), signal.SIGTERM)'
    )
    child.expect_exact("Bye!")

    with pytest.raises(AssertionError, match="terminated by signal"):
        BaseInteractiveTest.close_interactive(child, timeout=5)

    assert child.closed
    assert child.signalstatus == signal.SIGTERM


@pytest.mark.parametrize("close_stdio", [False, True], ids=["running", "stdio_closed"])
def test_close_timeout_is_bounded_and_kills_child(spawn_child, close_stdio, monkeypatch):
    child = spawn_child(
        """
        import os
        import sys
        import time

        print("Bye!")
        if sys.argv[1] == "True":
            input()
            for fd in (0, 1, 2):
                os.close(fd)
        time.sleep(30)
        """,
        close_stdio,
    )
    child.expect_exact("Bye!")
    if close_stdio:
        child.sendline("close stdio")
    reads_after_eof = []
    original_read = SpawnBase.read_nonblocking

    def track_read(process, *args, **kwargs):
        reads_after_eof.append(process.flag_eof)
        return original_read(process, *args, **kwargs)

    monkeypatch.setattr(SpawnBase, "read_nonblocking", track_read)
    started = time.monotonic()

    with pytest.raises(AssertionError, match="killed by signal") as failure:
        BaseInteractiveTest.close_interactive(child, timeout=0.2)

    assert "process shutdown did not finish" in str(failure.value)
    assert not any(reads_after_eof)
    assert time.monotonic() - started < 5
    assert child.closed
    assert child.signalstatus == signal.SIGKILL


def test_close_reaps_child_after_terminal_read_error(spawn_child, monkeypatch):
    child = spawn_child('print("Bye!"); input(); print("shutdown output"); input()')
    child.expect_exact("Bye!")
    read_error = OSError("simulated PTY read failure")

    def fail_read(*args, **kwargs):
        raise read_error

    monkeypatch.setattr(SpawnBase, "read_nonblocking", fail_read)
    child.sendline("continue")

    with pytest.raises(AssertionError, match="simulated PTY read failure") as failure:
        BaseInteractiveTest.close_interactive(child, timeout=5)

    assert failure.value.__cause__ is read_error
    assert child.closed
    assert child.signalstatus == signal.SIGKILL


def test_close_preserves_original_test_failure(spawn_child):
    child = spawn_child('import time; print("Bye!"); time.sleep(30)')
    child.expect_exact("Bye!")
    original_error = AssertionError("original test failure")

    with pytest.raises(AssertionError, match="killed by signal") as failure:
        try:
            raise original_error
        finally:
            BaseInteractiveTest.close_interactive(child, timeout=0.2)

    assert failure.value.__cause__ is original_error
    assert child.closed


def test_close_is_idempotent(spawn_child):
    child = spawn_child('print("Bye!")')
    child.expect_exact("Bye!")
    BaseInteractiveTest.close_interactive(child, timeout=5)

    BaseInteractiveTest.close_interactive(child, timeout=0)

    assert child.closed
    assert child.exitstatus == 0
    assert child.signalstatus is None
