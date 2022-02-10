#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

from ydb.tests.library.nemesis.safety_warden import SafetyWarden

from ydb.tests.library.harness.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.util import PDiskState


def fetch_pdisk_state_info(node, timeout_seconds=60):
    time_sleep_seconds = 5
    finish_time = time.time() + timeout_seconds
    swagger_client = SwaggerClient(node.host, port=node.mon_port)
    pdisk_state_info = None

    while pdisk_state_info is None and time.time() < finish_time:
        try:
            pdisk_info = swagger_client.pdisk_info(node.node_id)
        except IOError:
            continue
        pdisk_state_info = pdisk_info.get('PDiskStateInfo')

        if pdisk_state_info is not None:
            return pdisk_state_info
        time.sleep(time_sleep_seconds)

    raise IOError("Timeout exceeded timeout_seconds = {}".format(timeout_seconds))


class AllPDisksAreInValidStateSafetyWarden(SafetyWarden):

    def __init__(self, kikimr_cluster, valid_states=(), timeout_seconds=60):
        super(AllPDisksAreInValidStateSafetyWarden, self).__init__('AllPDisksAreInValidStateSafetyWarden')
        self.__timeout_seconds = timeout_seconds
        self.__kikimr_cluster = kikimr_cluster
        if not valid_states:
            valid_states = set([s.name for s in list(PDiskState) if s.is_valid_state])
        self.__valid_states = valid_states

    def list_of_safety_violations(self):
        ret = []
        for node_id, node in self.__kikimr_cluster.nodes.items():
            try:
                pdisk_state_info = fetch_pdisk_state_info(node, self.__timeout_seconds)
            except IOError as e:
                ret.append(
                    'Node = {node} is unavailable, exception = {exception}'.format(
                        node=node,
                        exception=e
                    )
                )
                continue

            actual_pdisk_id_set = set()
            for p in pdisk_state_info:
                pdisk_state = p.get('State')
                pdisk_id = p.get('PDiskId')
                pdisk_path = p.get('Path')
                actual_pdisk_id_set.add(pdisk_id)

                if pdisk_state not in self.__valid_states:
                    ret.append(
                        'On node = {node}, pdisk_id = {pdisk_id}, pdisk_path = {pdisk_path} is in invalid state, '
                        'current state = {state}'.format(
                            node=node, pdisk_id=pdisk_id, pdisk_path=pdisk_path, state=pdisk_state
                        )
                    )

        return ret
