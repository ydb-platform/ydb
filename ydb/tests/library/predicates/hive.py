#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from ydb.tests.library.common.types import TabletTypes

logger = logging.getLogger(__name__)


def hive_is_ready_all_tablets_started(monitors):
    """
    Waiting for special HIVE ready flag
    """
    hive_id = '0x%X' % TabletTypes.FLAT_HIVE.tablet_id_for(0)
    predicates = []
    for monitor in monitors:
        hive_done = monitor.sensor(counters='tablets', sensor='HIVE/StateDone',
                                   tabletid=hive_id, category='app')

        predicates.append(hive_done is not None and hive_done == 1)

    state_done = any(predicates)
    logger.debug("HIVE StateDone = " + str(predicates) + " hive_id = " + str(hive_id))
    return state_done
