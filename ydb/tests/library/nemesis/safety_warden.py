# -*- coding: utf-8 -*-

import functools
import itertools
import logging
import six
import subprocess
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from ydb.tests.library.nemesis.remote_execution import execute_command_with_output_on_hosts


logger = logging.getLogger()


if six.PY2:
    FileNotFoundError = IOError


class SafetyWarden(object):
    __metaclass__ = ABCMeta

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


class AbstractRemoteCommandExecutionSafetyWarden(SafetyWarden):

    def __init__(self, name, list_of_hosts, remote_command, username=None, split_line_size=0):
        super(AbstractRemoteCommandExecutionSafetyWarden, self).__init__(name)
        self.__list_of_hosts = list_of_hosts
        self.__remote_command = remote_command
        self.__username = username
        self.__split_line_size = split_line_size

    def list_of_safety_violations(self):
        logger.info(
            "{me} executing on hosts = {hosts}, command = {command}".format(
                me=self, hosts=self.__list_of_hosts, command=self.__remote_command
            )
        )
        ret_code, output = execute_command_with_output_on_hosts(
            self.__list_of_hosts, self.__remote_command, username=self.__username
        )
        if self.__split_line_size > 1:
            output = split_in_chunks(output, self.__split_line_size)

        if output:
            return output
        else:
            return []


class GrepLogFileForMarkers(AbstractRemoteCommandExecutionSafetyWarden):
    def __init__(self, targets, log_file_name, list_of_markers, lines_after=10, username=None, only_count=False, cut=True):
        name = "GrepLogFileForMarkersSafetyWarden for markers = {markers} on targets = {targets}".format(
            markers=list_of_markers, targets=targets
        )
        remote_command = (
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
            remote_command.extend(
                [
                    '|',
                    'cut',
                    '-c',
                    '1-260'
                ]
            )

        if only_count:
            remote_command.extend(
                [
                    '|', 'wc', '-l'
                ]
            )
        super(GrepLogFileForMarkers, self).__init__(
            name, targets, remote_command=remote_command, username=username, split_line_size=lines_after
        )


class GrepGzippedLogFilesForMarkersSafetyWarden(AbstractRemoteCommandExecutionSafetyWarden):
    def __init__(
            self, list_of_hosts, log_file_pattern, list_of_markers, lines_after=1, username=None,
            modification_days=1, only_count=False, cut=True
    ):
        name = "GrepGzippedLogFilesForMarkersSafetyWarden for markers = {markers} on targets = {targets}".format(
            markers=list_of_markers, targets=list_of_hosts
        )

        if modification_days > 0:
            remote_command = [
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
            remote_command = [
                'zcat',
                log_file_pattern,
            ]

        remote_command.extend([
            '|',
            'grep',
            '-A', str(lines_after),
        ])

        remote_command.extend(
            construct_list_of_grep_pattern_arguments(list_of_markers)
        )
        if cut:
            remote_command.extend(
                [
                    '|',
                    'cut',
                    '-c',
                    '1-260'
                ]
            )

        if only_count:
            remote_command.extend(
                [
                    '|', 'wc', '-l'
                ]
            )

        super(GrepGzippedLogFilesForMarkersSafetyWarden, self).__init__(
            name, list_of_hosts, remote_command=remote_command, username=username, split_line_size=lines_after
        )


class GrepDMesgForPatternsSafetyWarden(AbstractRemoteCommandExecutionSafetyWarden):
    def __init__(self, list_of_hosts, list_of_markers, lines_after=1, username=None):
        name = "GrepDMesgForPatternsSafetyWarden for markers = {markers} on targets = {targets}".format(
            markers=list_of_markers, targets=list_of_hosts
        )
        remote_command = [
            'dmesg', '-T',
            '|',
            'grep',
            '-A', str(lines_after),
        ] + construct_list_of_grep_pattern_arguments(list_of_markers)

        super(GrepDMesgForPatternsSafetyWarden, self).__init__(
            name, list_of_hosts, remote_command=remote_command, username=username, split_line_size=lines_after
        )


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
