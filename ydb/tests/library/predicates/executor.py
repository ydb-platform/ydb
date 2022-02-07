#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger(__name__)


def externals_blobs_count(monitor, tablet_id):
    tablet_counters = monitor.tabletcounters(tablet_id)['TabletCounters']
    simple_counters = tablet_counters['ExecutorCounters']['CumulativeCounters']
    for counter in simple_counters:
        if counter['Name'] == 'DbAnnexItemsGrow':
            return int(counter['Value'])
    return 0


def external_blobs_is_present(monitor, tablets_ids):
    predicates = []
    for tablet_id in tablets_ids:
        value = externals_blobs_count(monitor, tablet_id)
        case = value > 0
        log_func = logger.debug if case else logger.error
        log_func(
            "tablet_id is %s TabletCounters.ExecutorCounters.CumulativeCounters.DbAnnexItemsGrow is %s",
            tablet_id,
            value
        )
        predicates.append(case)
    return all(predicates)
