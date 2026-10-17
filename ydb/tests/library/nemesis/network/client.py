#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import shlex

# noinspection PyUnresolvedReferences
from ydb.tests.library.nemesis.remote_execution import execute_command_with_output_single_host

logger = logging.getLogger()

# Wait for xtables lock instead of failing immediately (see ip6tables -w).
_XTABLES_WAIT = ('-w', '10')


def delete_args_from_iptables_save(dumped, match=None):
    """Turn ``iptables-save`` text into ``-D ...`` argv lists for statistic DROP rules.

    Snapshot-then-delete must not pipeline save into ``-D``: save holds the xtables lock,
    and a concurrent ``-D`` exits with "Another app is currently holding the xtables lock".
    """
    match_s = None if match is None else str(match)
    result = []
    for raw in dumped.splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or line.startswith('*') or line.startswith(':'):
            continue
        if line == 'COMMIT':
            continue
        if line.startswith('['):
            line = line.split(']', 1)[-1].strip()
        if '-A' not in line:
            continue
        if 'statistic' not in line and 'probability' not in line:
            continue
        if match_s is not None and match_s not in line:
            continue
        parts = shlex.split(line)
        try:
            idx = parts.index('-A')
        except ValueError:
            continue
        parts[idx] = '-D'
        result.append(parts)
    return result


class NetworkClient(object):
    def __init__(self, host, port=19001, ssh_username=None, ipv6=True):
        super(NetworkClient, self).__init__()
        if ipv6:
            self._iptables_bin = '/sbin/ip6tables'
            self._iptables_save_bin = '/sbin/ip6tables-save'
        else:
            self._iptables_bin = '/sbin/iptables'
            self._iptables_save_bin = '/sbin/iptables-save'

        self._port = str(port)
        self._host = host
        self._ssh_username = ssh_username

    def _iptables_cmd(self, *args):
        return ['sudo', self._iptables_bin] + list(_XTABLES_WAIT) + list(args)

    def drop_incoming_packets(self, probability=0.01):
        drop_incoming_command = self._iptables_cmd(
            '-A', 'YDB_FW', '-p', 'tcp', '--sport', self._port,
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP',
        )
        return self._exec_command(drop_incoming_command)

    def drop_outgoing_packets(self, probability=0.01):
        drop_outgoing_command = self._iptables_cmd(
            '-A', 'YDB_FW', '-p', 'tcp', '--dport', self._port,
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP',
        )
        return self._exec_command(drop_outgoing_command)

    def isolate_dns(self, probability=1.0):
        # Must stay on YDB_FW: cluster machinery reverts iptables changes outside that chain.
        drop_input = self._iptables_cmd(
            '-A', 'YDB_FW', '-p', 'udp', '--sport', '53',
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP',
        )

        drop_output = self._iptables_cmd(
            '-A', 'YDB_FW', '-p', 'udp', '--sport', '1024:65535', '--dport', '53',
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP',
        )

        for cmd in (drop_input, drop_output):
            retcode = self._exec_command(cmd)
            if retcode:
                logger.error("retcode %s for command %s", retcode, cmd)
                return retcode

        # Cache flush is best-effort: bind9 is often absent on YDB hosts.
        reset_cache = ['sudo', '/etc/init.d/bind9', 'restart']
        retcode = self._exec_command(reset_cache)
        if retcode:
            logger.warning("dns cache reset skipped (retcode %s for %s)", retcode, reset_cache)

        return 0

    def isolate_node(self):
        probability = 1.0
        self.drop_incoming_packets(probability)
        self.drop_outgoing_packets(probability)
        return

    def clear_all_drops(self, match=None):
        """Delete statistic/probability DROP rules, optionally filtered by ``match``.

        Pass ``match`` (e.g. port or ``53``) so Network extract does not wipe Dns rules.

        Saves the table first, then issues ``-D`` with ``-w`` so extract does not race
        the xtables lock held by ``iptables-save``.
        """
        retcode, dumped = self._run(['sudo', self._iptables_save_bin])
        if retcode:
            logger.error("retcode %s for %s-save", retcode, self._iptables_bin)
            return retcode

        failed = 0
        for args in delete_args_from_iptables_save(dumped, match=match):
            cmd = self._iptables_cmd(*args)
            rc = self._exec_command(cmd)
            if rc:
                logger.error("retcode %s deleting iptables rule %s", rc, args)
                failed = rc
        return failed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear_all_drops()
        return False

    def _run(self, command):
        retcode, output = execute_command_with_output_single_host(
            self._host, command, username=self._ssh_username
        )
        if isinstance(output, list):
            text = ''.join(output)
        else:
            text = output or ''
        return retcode, text

    def _exec_command(self, command):
        retcode, _unused = self._run(command)
        return retcode

    def __str__(self):
        return 'NetworkClient[{host}:{port}]'.format(host=self._host, port=self._port)
