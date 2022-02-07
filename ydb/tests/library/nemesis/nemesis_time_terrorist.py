# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import logging
from random import randint

from ydb.tests.library.nemesis.nemesis_core import AbstractNemesisNodeTerrorist
from ydb.tests.library.nemesis.remote_execution import execute_command_with_output

logger = logging.getLogger(__name__)


class NemesisTimeChanger(AbstractNemesisNodeTerrorist):
    def __init__(
            self, cluster, act_interval, ssh_user=None, remote_sudo_user=None,
            time_change_range=(-60, 60), timeout=3.0
    ):
        super(NemesisTimeChanger, self).__init__(
            cluster, act_interval, ssh_user=ssh_user, remote_sudo_user=remote_sudo_user, timeout=timeout
        )
        self.__change_range = time_change_range
        self.__get_time_cmd = ('date', '+%s')

    @staticmethod
    def set_time_cmd(time_to_set):
        time_str = '{:02d}:{:02d}:{:02d}'.format(time_to_set.hour, time_to_set.minute, time_to_set.second)
        return (
            'date', '+%T', '-s', time_str
        )

    def _commands_on_node(self, node=None):
        current_time = self.__get_curr_time(node)
        if current_time is None:
            return []
        else:
            delta = timedelta(seconds=randint(*self.__change_range))
            return self.set_time_cmd(current_time + delta)

    def __get_curr_time(self, node):
        result, output = execute_command_with_output(
            self._full_command(node, self.__get_time_cmd), timeout=self.timeout
        )
        if result:
            logger.error("Failed to fetch victim process on node {node}. Will not act this time".format(node=node))
            return None
        else:
            return datetime.fromtimestamp(int(output[0].strip()))
