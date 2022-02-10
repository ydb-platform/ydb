# -*- coding: utf-8 -*-
import logging
import six

logger = logging.getLogger(__name__)


def blobstorage_controller_has_started_on_some_node(monitors):
    """
    Waiting for simple sensor from blobstorage controller. This means it has started.
    """
    predicates = []
    for monitor in monitors:
        tx_all_counter = monitor.sensor(
            counters='tablets', sensor='Tx(all)', type='BSController',
            category='executor'
        )
        predicates.append(tx_all_counter is not None)
    return any(predicates)


def cluster_has_no_unsynced_vdisks(cluster):
    num_of_unsync_vdisks = 0
    for node in cluster.nodes.values():
        monitor = node.monitor
        sensors = monitor.get_by_name('SyncerUnsyncedDisks')
        for key, value in sensors:
            num_of_unsync_vdisks += value
    return num_of_unsync_vdisks == 0


def cluster_has_no_unreplicated_vdisks(cluster, max_count=0, logs=None):
    logs = logger if logs is None else logs
    num_of_unreplicated_vdisks = 0

    for node_id, node in six.iteritems(cluster.nodes):
        monitor = node.monitor
        if not monitor.has_actual_data():
            logs.error("Failed to collect sensors data for node: %s" % node_id)
            # there is no guarantee in such case that there is no un-replicated vdisks
            return False

        sensors = monitor.get_by_name('ReplUnreplicatedVDisks')
        for key, value in sensors:
            if value == 1:
                num_of_unreplicated_vdisks += 1
                logger.info(
                    "Found unreplicated vdisk on node host %s: %s",
                    node.host,
                    str(key)
                )

        logs.debug('Number nodes with un-replicated vdisks: %d' % num_of_unreplicated_vdisks)
    return num_of_unreplicated_vdisks <= max_count
