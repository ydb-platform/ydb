# -*- coding: utf-8 -*-
import logging
import random
from ydb.tests.library.nemesis.nemesis_core import AbstractNemesisNodeTerrorist
from ydb.tests.library.nemesis.remote_execution import execute_command_with_output

logger = logging.getLogger(__name__)

DEFAULT_KILL_ALL_CHANCE = 0.3


def kill_cmd(process, signal):
    return (
        "killall",
        "-s",
        str(signal),
        str(process)
    )


def kill_pid_cmd(pid, signal):
    return (
        'kill',
        '-s',
        str(signal),
        str(pid)
    )


class NemesisProcessKiller(AbstractNemesisNodeTerrorist):
    def __init__(
            self, cluster, processes_affected, act_interval, ssh_user=None, remote_sudo_user=None,
            kill_everything_chance=DEFAULT_KILL_ALL_CHANCE, signal='SIGKILL', processes_filters=None,
            timeout=3.0
    ):
        super(NemesisProcessKiller, self).__init__(cluster, act_interval, timeout=timeout,
                                                   ssh_user=ssh_user, remote_sudo_user=remote_sudo_user)
        self.__victims_procs = processes_affected
        self.__kill_all_chance = kill_everything_chance
        self._signal = signal

        self._command_for_victim = kill_cmd if processes_filters is None else kill_pid_cmd
        self.__process_filters = processes_filters

    def _commands_on_node(self, node=None):
        ret = []
        victim_candidates = self._get_victim(node)

        if random.random() < self.__kill_all_chance or not victim_candidates:
            real_victims = victim_candidates
        else:
            real_victims = [random.choice(victim_candidates), ]

        for prc in real_victims:
            ret.append(
                self._command_for_victim(prc, self._signal)
                )
        return ret

    def _get_victim(self, node=None):
        if self.__process_filters is None:
            return self.__victims_procs
        else:
            return self.__get_victims_by_filter(node)

    def __get_victims_by_filter(self, node):
        ps_command = ['ps', 'ax']
        result, output = execute_command_with_output(self._full_command(node, ps_command), timeout=self.timeout)
        if result:
            logger.error("Failed to fetch victim process on node {node}. Will not act this time".format(node=node))
            return []
        else:
            return map(
                lambda x: x.strip().split(' ')[0],
                self.__filter_output(output)
            )

    def __filter_output(self, output):
        lines_match = {}
        for i in range(len(output)):
            lines_match[i] = True

        for filt in self.__process_filters:
            for line_number, still_matching in lines_match.items():
                if still_matching:
                    if filt not in output[line_number]:
                        lines_match[line_number] = False

        return [output[i] for i in lines_match if lines_match[i]]


class NemesisProcessSuspender(NemesisProcessKiller):
    def __init__(
            self, cluster, processes_affected, act_interval, ssh_user=None, remote_sudo_user=None,
            kill_everything_chance=DEFAULT_KILL_ALL_CHANCE, processes_filters=None
    ):

        super(NemesisProcessSuspender, self).__init__(
            cluster, processes_affected, act_interval, ssh_user, remote_sudo_user,
            kill_everything_chance, signal='SIGSTOP', processes_filters=processes_filters
        )
        self.__to_unsuspend = []

    def __wake_all_suspended(self):
        for node, commands in self.__to_unsuspend:
            for command in commands:
                self._remote_exec_method(self._full_command(node, command), timeout=self.timeout)
        self.__to_unsuspend = []

    def _pre_inject_fault(self, node, commands):
        self.__wake_all_suspended()
        wake_commands = [self._command_for_victim(i[-1], signal="SIGCONT") for i in commands]
        self.__to_unsuspend.append((node, wake_commands))

    def extract_fault(self):
        self.__wake_all_suspended()


def fake_execute_command(command):
    print("{}".format(command))


class TestNemesisProcessSuspender(NemesisProcessSuspender):
    _remote_exec_method = fake_execute_command
    """
    >>> random.seed(123)
    >>> n = TestNemesisProcessSuspender(range(8), range(2), 1)
    >>> n.prepare_state()
    >>> n.inject_fault()
    ['ssh', 0, 'killall', '-s', 'SIGSTOP', '0']
    ['ssh', 0, 'killall', '-s', 'SIGSTOP', '1']
    >>> n.inject_fault()
    ['ssh', 0, 'killall', '-s', 'SIGCONT', '0']
    ['ssh', 0, 'killall', '-s', 'SIGCONT', '1']
    ['ssh', 3, 'killall', '-s', 'SIGSTOP', '0']
    ['ssh', 3, 'killall', '-s', 'SIGSTOP', '1']
    >>> n.inject_fault()
    ['ssh', 3, 'killall', '-s', 'SIGCONT', '0']
    ['ssh', 3, 'killall', '-s', 'SIGCONT', '1']
    ['ssh', 7, 'killall', '-s', 'SIGSTOP', '0']
    ['ssh', 7, 'killall', '-s', 'SIGSTOP', '1']
    >>> n.inject_fault()
    ['ssh', 7, 'killall', '-s', 'SIGCONT', '0']
    ['ssh', 7, 'killall', '-s', 'SIGCONT', '1']
    ['ssh', 4, 'killall', '-s', 'SIGSTOP', '1']
    >>> n.inject_fault()
    ['ssh', 4, 'killall', '-s', 'SIGCONT', '1']
    ['ssh', 1, 'killall', '-s', 'SIGSTOP', '0']
    >>> n.inject_fault()
    ['ssh', 1, 'killall', '-s', 'SIGCONT', '0']
    ['ssh', 1, 'killall', '-s', 'SIGSTOP', '0']
    ['ssh', 1, 'killall', '-s', 'SIGSTOP', '1']
    """
