#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

logger = logging.getLogger(__name__)


def kqp_alive_workers_count(monitors):
    alive_count = 0
    for monitor in monitors:
        alive_count += monitor.sensor(counters='kqp', sensor='Workers/Active')
    return alive_count


def cluster_kqp_alive_workers_count(cluster):
    return kqp_alive_workers_count([node.monitor for node in cluster.nodes.values()])
