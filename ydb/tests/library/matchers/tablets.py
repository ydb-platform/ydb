#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hamcrest import has_property, all_of, has_length, has_items, anything
from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.wrap_matcher import wrap_matcher

from ydb.tests.library.common.msgbus_types import EMessageStatus, MessageBusStatus


def all_tablets_are_created(tablets_to_create):
    return all_of(
        has_property('Status', MessageBusStatus.MSTATUS_OK),
        has_property(
            'CreateTabletResult',
            all_of(
                has_length(len(tablets_to_create)),
                has_items(has_property('Status', EMessageStatus.MESSAGE_OK))
            )
        )
    )


def is_balanced_list(lst, exact_values):
    _min, _max = min(lst), max(lst)
    if exact_values is not None:
        return _min == _max and wrap_matcher(exact_values).matches(_min)
    else:
        return _max - _min <= 1


class ExpectedTablets(object):
    def __init__(self, tablets_count=anything(), tablet_ids=()):
        self.tablets_count = wrap_matcher(tablets_count)
        self.tablet_ids = list(tablet_ids)


class TabletsBalanceMatcher(BaseMatcher):
    def __init__(self):
        super(TabletsBalanceMatcher, self).__init__()
        self.__all_balanced = False
        self.__special_nodes = {}
        self.__exact_count_on_all = None

    def all_balanced(self):
        self.__all_balanced = True
        return self

    def with_exact_count_tablets_on_all(self, exact_count):
        self.__exact_count_on_all = exact_count
        return self

    def with_x_tablets_on_node(self, node_id, num_tablets):
        self.__special_nodes.setdefault(node_id, ExpectedTablets())
        self.__special_nodes[node_id].tablets_count = num_tablets
        return self

    def with_exact_tablet_id_on_node(self, node_id, tablet_id):
        self.__special_nodes.setdefault(node_id, ExpectedTablets())
        self.__special_nodes[node_id].tablet_ids.append(tablet_id)
        return self

    def _matches(self, actual_tablets_per_node):
        not_special_nodes = actual_tablets_per_node.keys()

        for node_id, expected_tablets in self.__special_nodes.items():
            if node_id not in actual_tablets_per_node:
                return False
            if not expected_tablets.tablets_count.matches(len(actual_tablets_per_node[node_id])):
                return False

            for tablet_id in expected_tablets.tablet_ids:
                if tablet_id not in actual_tablets_per_node[node_id]:
                    return False
            not_special_nodes.remove(node_id)

        tablets_by_node = [len(actual_tablets_per_node[i]) for i in not_special_nodes]
        if self.__all_balanced:
            return is_balanced_list(
                tablets_by_node,
                self.__exact_count_on_all
            )
        elif self.__exact_count_on_all is not None:
            return self.__exact_count_on_all.matches(
                max(tablets_by_node)
            ) and self.__exact_count_on_all.matches(
                min(tablets_by_node)
            )

    def describe_to(self, description):
        if self.__all_balanced:
            description.append_text('Equally spread tablets across all nodes\n')
        if self.__exact_count_on_all is not None:
            description.append_text('With each node having \n')
            self.__exact_count_on_all.describe_to(description)
        for node_id, expected_tablets in self.__special_nodes.items():
            description.append_text('Except node %s which is expected to have\n' % str(node_id))
            description.append_text('\t num tablets: ')
            expected_tablets.tablets_count.describe_to(description)
            description.append_text('.')
            for tablet_id in expected_tablets.tablet_ids:
                description.append_text('\t tablet with id %s on it' % str(tablet_id))


def are_equally_spread_tablets():
    return TabletsBalanceMatcher().all_balanced()
