# -*- coding: utf-8 -*-

import functools
import itertools
import logging
from abc import ABCMeta, abstractmethod
from ydb.tests.library.nemesis.remote_execution import execute_command_with_output_on_hosts


logger = logging.getLogger()


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
    def __init__(self, targets, log_file_name, list_of_markers, lines_after=10, username=None, only_count=False):
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
            modification_days=1, only_count=False,
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
