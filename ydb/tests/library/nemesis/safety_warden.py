# -*- coding: utf-8 -*-

import functools
import itertools
import logging
import subprocess
import tempfile
import time
import warnings
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta

from ydb.tests.library.nemesis.remote_execution import execute_command_with_output_on_hosts


logger = logging.getLogger()


# ---------------------------------------------------------------------------
# Base SafetyWarden
# ---------------------------------------------------------------------------


class SafetyWarden(metaclass=ABCMeta):

    def __init__(self, name):
        super(SafetyWarden, self).__init__()
        self.__name = name

    @abstractmethod
    def list_of_safety_violations(self):
        pass

    @property
    def name(self):
        return self.__name

    def __str__(self):
        return self.name


class AggregateSafetyWarden(SafetyWarden):
    def __init__(self, list_of_safety_wardens):
        super(AggregateSafetyWarden, self).__init__('AggregateSafetyWarden')
        self.__list_of_safety_wardens = list(list_of_safety_wardens)

    def list_of_safety_violations(self):
        all_safety_violations = []
        for warden in self.__list_of_safety_wardens:
            all_safety_violations.extend(
                warden.list_of_safety_violations()
            )
        return all_safety_violations


# ---------------------------------------------------------------------------
# CommandExecutor — strategy for running shell commands
# ---------------------------------------------------------------------------


class CommandExecutor(metaclass=ABCMeta):
    """Protocol: execute a shell command and return ``(retcode, list_of_lines)``."""

    @abstractmethod
    def execute_command(self, command, timeout=60):
        """
        Execute *command* (list of strings) and return ``(retcode, lines)``.

        Args:
            command: Command as a list of tokens (may contain shell pipes as literal ``|`` tokens).
            timeout: Maximum seconds to wait.

        Returns:
            Tuple of ``(return_code, list_of_output_lines)``.
        """
        pass


class LocalCommandExecutor(CommandExecutor):
    """Run a command locally via ``subprocess.Popen`` (no SSH)."""

    def execute_command(self, command, timeout=60):
        shell_cmd = " ".join(command)
        logger.info("LocalCommandExecutor: running %s", shell_cmd)
        list_of_lines = []
        try:
            with tempfile.TemporaryFile() as f_out, tempfile.TemporaryFile() as f_err:
                process = subprocess.Popen(shell_cmd, shell=True, stdout=f_out, stderr=f_err)
                start = time.time()
                while time.time() < start + timeout:
                    process.poll()
                    if process.returncode is not None:
                        break
                    time.sleep(0.5)
                else:
                    process.kill()
                    process.wait()

                f_out.flush()
                f_out.seek(0)
                list_of_lines = [line.decode("utf-8", errors="replace") for line in f_out.readlines()]
        except Exception as exc:
            logger.error("LocalCommandExecutor: failed: %s", exc)
            return 1, []

        return process.returncode, list_of_lines


class RemoteCommandExecutor(CommandExecutor):
    """Run a command on remote hosts via SSH (wraps ``execute_command_with_output_on_hosts``)."""

    def __init__(self, list_of_hosts, username=None):
        self._list_of_hosts = list_of_hosts
        self._username = username

    def execute_command(self, command, timeout=60):
        logger.info(
            "RemoteCommandExecutor: executing on hosts=%s, command=%s",
            self._list_of_hosts, command,
        )
        ret_code, output = execute_command_with_output_on_hosts(
            self._list_of_hosts, command, username=self._username, per_host_timeout=timeout,
        )
        return ret_code, output


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def split_in_chunks(list_of_lines, chunk_size):
    """
    >>> a = list(map(str, range(6)))
    >>> split_in_chunks(a, 3)
    ['0\\n1\\n2', '3\\n4\\n5']
    >>> split_in_chunks(a, 2)
    ['0\\n1', '2\\n3', '4\\n5']
    >>> a = list(map(str, range(7)))
    >>> split_in_chunks(a, 3)
    ['0\\n1\\n2', '3\\n4\\n5', '6']
    >>> split_in_chunks([], 3)
    []
    >>> split_in_chunks(['a', 'b'], 3)
    ['a\\nb']
    >>> split_in_chunks(['a', 'b'], 1)
    Traceback (most recent call last):
      ...
    AssertionError: chunk_size > 1 is False
    ...
    >>> split_in_chunks(['a', 'b'], 0)
    Traceback (most recent call last):
      ...
    AssertionError: chunk_size > 1 is False
    ...
    """
    assert chunk_size > 1, "chunk_size > 1 is False"

    it = iter(list_of_lines)

    def callee():
        ret = list(itertools.islice(it, 0, chunk_size))
        if ret:
            return ret
        return None

    return ['\n'.join(x) for x in iter(callee, None)]


def construct_list_of_grep_pattern_arguments(list_of_markers):
    """
    >>> construct_list_of_grep_pattern_arguments(['a', 'b'])
    ['-e', "'a'", '-e', "'b'"]
    """
    return list(functools.reduce(
        lambda x, y: x + y, [("-e", "'{}'".format(p)) for p in list_of_markers]
    ))


# ---------------------------------------------------------------------------
# CommandBasedSafetyWarden — composition-based replacement
# ---------------------------------------------------------------------------


class CommandBasedSafetyWarden(SafetyWarden):
    """
    Safety warden that delegates command execution to a ``CommandExecutor``.

    Subclasses build the command; the executor decides *how* to run it
    (locally, via SSH, etc.).
    """

    def __init__(self, name, executor, command, split_line_size=0):
        super(CommandBasedSafetyWarden, self).__init__(name)
        self._executor = executor
        self._command = command
        self._split_line_size = split_line_size

    def list_of_safety_violations(self):
        logger.info(
            "{me} executing command = {command}".format(
                me=self, command=self._command,
            )
        )
        ret_code, output = self._executor.execute_command(self._command)
        if self._split_line_size > 1:
            output = split_in_chunks(output, self._split_line_size)

        if output:
            return output
        else:
            return []


# ---------------------------------------------------------------------------
# Deprecated shim for backward compatibility
# ---------------------------------------------------------------------------


class AbstractRemoteCommandExecutionSafetyWarden(SafetyWarden):
    """
    .. deprecated::
        Use ``CommandBasedSafetyWarden`` with an explicit ``CommandExecutor`` instead.

    Backward-compatible shim that auto-creates a ``RemoteCommandExecutor``
    from ``list_of_hosts`` and ``username``.
    """

    def __init__(self, name, list_of_hosts, remote_command, username=None, split_line_size=0):
        warnings.warn(
            "AbstractRemoteCommandExecutionSafetyWarden is deprecated; "
            "use CommandBasedSafetyWarden with an explicit CommandExecutor",
            DeprecationWarning,
            stacklevel=2,
        )
        super(AbstractRemoteCommandExecutionSafetyWarden, self).__init__(name)
        self._delegate = CommandBasedSafetyWarden(
            name=name,
            executor=RemoteCommandExecutor(list_of_hosts, username=username),
            command=remote_command,
            split_line_size=split_line_size,
        )

    def list_of_safety_violations(self):
        return self._delegate.list_of_safety_violations()


# ---------------------------------------------------------------------------
# Concrete command-building wardens (composition-based)
# ---------------------------------------------------------------------------


class GrepLogFileForMarkers(CommandBasedSafetyWarden):
    def __init__(self, executor, log_file_name, list_of_markers, lines_after=10, only_count=False, cut=True):
        name = "GrepLogFileForMarkersSafetyWarden for markers = {markers}".format(
            markers=list_of_markers,
        )
        command = (
            [
                'grep',
                '-A', str(lines_after),
            ]
            + construct_list_of_grep_pattern_arguments(list_of_markers)
            + [
                log_file_name,
            ]
        )
        if cut:
            command.extend(
                [
                    '|',
                    'cut',
                    '-c',
                    '1-260'
                ]
            )

        if only_count:
            command.extend(
                [
                    '|', 'wc', '-l'
                ]
            )
        super(GrepLogFileForMarkers, self).__init__(
            name, executor, command=command, split_line_size=lines_after,
        )


class GrepGzippedLogFilesForMarkersSafetyWarden(CommandBasedSafetyWarden):
    def __init__(
            self, executor, log_file_pattern, list_of_markers, lines_after=1,
            modification_days=1, only_count=False, cut=True
    ):
        name = "GrepGzippedLogFilesForMarkersSafetyWarden for markers = {markers}".format(
            markers=list_of_markers,
        )

        if modification_days > 0:
            command = [
                'find',
                log_file_pattern,
                '-type',
                'f',
                '-mtime',
                '-{days}'.format(days=modification_days),
                '|',
                'xargs',
                'zcat',
            ]
        else:
            command = [
                'zcat',
                log_file_pattern,
            ]

        command.extend([
            '|',
            'grep',
            '-A', str(lines_after),
        ])

        command.extend(
            construct_list_of_grep_pattern_arguments(list_of_markers)
        )
        if cut:
            command.extend(
                [
                    '|',
                    'cut',
                    '-c',
                    '1-260'
                ]
            )

        if only_count:
            command.extend(
                [
                    '|', 'wc', '-l'
                ]
            )

        super(GrepGzippedLogFilesForMarkersSafetyWarden, self).__init__(
            name, executor, command=command, split_line_size=lines_after,
        )


class GrepJournalctlKernelForPatternsSafetyWarden(CommandBasedSafetyWarden):
    def __init__(self, executor, list_of_markers, lines_after=1, hours_back=24):
        name = "GrepJournalctlKernelForPatternsSafetyWarden for markers = {markers}".format(
            markers=list_of_markers,
        )
        since_value = '{hours} hours ago'.format(hours=hours_back)
        command = [
            'sudo', 'journalctl', '-k', '--no-pager', '--since', "'{since}'".format(since=since_value),
            '|',
            'grep',
            '-A', str(lines_after),
        ] + construct_list_of_grep_pattern_arguments(list_of_markers)

        super(GrepJournalctlKernelForPatternsSafetyWarden, self).__init__(
            name, executor, command=command, split_line_size=lines_after,
        )


# ---------------------------------------------------------------------------
# Standalone wardens (not command-based)
# ---------------------------------------------------------------------------


class UnifiedAgentVerifyFailedSafetyWarden(SafetyWarden):
    """
    Safety warden that checks for VERIFY failed errors in unified_agent logs.

    Uses unified_agent to search kikimr-start logs for VERIFY failed patterns.
    Based on logic from ydb/tests/library/stability/utils/collect_errors.py

    This warden runs locally (no SSH) and is designed for agent-side checks.

    Returns ALL VERIFY failed errors as raw violations list.
    Post-processing (deduplication, counting) should be done in warden_checker.
    """

    # Lines of context after each VERIFY failed match
    LINES_AFTER_MATCH = 25

    def __init__(self, hours_back=24):
        """
        Args:
            hours_back: How many hours back to search (default 24)
        """
        super(UnifiedAgentVerifyFailedSafetyWarden, self).__init__(
            'UnifiedAgentVerifyFailedSafetyWarden'
        )
        self._hours_back = hours_back

    def list_of_safety_violations(self):
        """
        Check unified_agent logs for VERIFY failed errors.

        Returns:
            List of violation strings, empty if no violations found.
            Each violation is a full stack trace (up to LINES_AFTER_MATCH lines).
            Returns ALL errors found, not limited.
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=self._hours_back)

        start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

        verify_pattern = 'VERIFY failed'
        violations = []

        try:
            # Get ALL VERIFY failed occurrences with full stack traces
            # No head limit - collect everything
            sample_cmd = (
                "ulimit -n 100500 2>/dev/null; "
                "unified_agent select -S '{start}' -U '{end}' -s kikimr-start 2>/dev/null | "
                "grep -i -A {lines} --no-group-separator '{pattern}' | "
                "sed '/{pattern}/i --'"
            ).format(start=start_str, end=end_str, lines=self.LINES_AFTER_MATCH, pattern=verify_pattern)
            sample_result = subprocess.run(
                sample_cmd, shell=True, capture_output=True, text=True, timeout=1200  # 20 minutes
            )
            if sample_result.returncode == 0 and sample_result.stdout.strip():
                # Split output by grep's "--" separator to get individual errors
                raw_output = sample_result.stdout.strip()
                error_blocks = raw_output.split('--\n')

                for block in error_blocks:
                    if block.strip():
                        # Add full stack trace as a single violation entry
                        violations.append(block.strip())

        except subprocess.TimeoutExpired:
            logger.warning("Timeout while checking unified_agent for VERIFY failed")
        except ValueError as e:
            logger.warning("Error parsing unified_agent output: {}".format(e))
        except FileNotFoundError:
            logger.warning("unified_agent not found")
        except Exception as e:
            logger.warning("Error checking unified_agent: {}".format(e))

        return violations
