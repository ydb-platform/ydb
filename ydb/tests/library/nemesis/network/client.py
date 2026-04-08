#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

# noinspection PyUnresolvedReferences
from ydb.tests.library.nemesis.remote_execution import execute_command_with_output_single_host

logger = logging.getLogger()


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

    def drop_incoming_packets(self, probability=0.01):
        drop_incoming_command = [
            'sudo', self._iptables_bin, '-A', 'INPUT', '-p', 'tcp', '--sport', self._port,
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP'
        ]
        return self._exec_command(drop_incoming_command)

    def drop_outgoing_packets(self, probability=0.01):
        drop_outgoing_command = [
            'sudo', self._iptables_bin, '-A', 'INPUT', '-p', 'tcp', '--dport', self._port,
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP'
        ]
        return self._exec_command(drop_outgoing_command)

    def isolate_dns(self, probability=1.0):
        drop_input = [
            'sudo', self._iptables_bin, '-A', 'INPUT', '-p', 'udp', '--sport', '53',
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP'
        ]

        drop_outout = [
            'sudo', self._iptables_bin, '-A', 'OUTPUT', '-p', 'udp', '--sport', '1024:65535', '--dport', '53',
            '-m', 'statistic', '--mode', 'random', '--probability', str(probability), '-j', 'DROP'
        ]

        reset_cache = [
            'sudo', '/etc/init.d/bind9', 'restart'
        ]

        for cmd in [drop_input, drop_outout, reset_cache]:
            retcode = self._exec_command(cmd)
            if retcode:
                logger.error("retcode %s for command %s", retcode, cmd)
                return retcode

        return 0

    def isolate_node(self):
        probability = 1.0
        self.drop_incoming_packets(probability)
        self.drop_outgoing_packets(probability)
        return

    def clear_all_drops(self):
        """
        $IP6TABLES_SAVE | grep -e statistic -e probability | sed -e "s/-A/-D/g" | while read line; do
            $IP6TABLES $line
        done
        """
        clear_all_drops_command = [
            'sudo', self._iptables_save_bin,
            '|', 'grep', '-e', 'statistic', '-e', 'probability',
            '|', 'sed', '-e', '"s/-A/-D/g"',
            '|', 'while', 'read', 'line', ';',
            'do',
            'sudo', self._iptables_bin, '$line', ';',
            'done'
        ]
        return self._exec_command(clear_all_drops_command)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear_all_drops()
        return False

    def _exec_command(self, command):
        retcode, output = execute_command_with_output_single_host(
            self._host, command, username=self._ssh_username
        )
        return retcode

    def __str__(self):
        return 'NetworkClient[{host}:{port}]'.format(host=self._host, port=self._port)
