# -*- coding: utf-8 -*-
import abc
import random
import six

from ydb.tests.library.nemesis.nemesis_core import Nemesis
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.harness.kikimr_client import kikimr_client_factory
from ydb.tests.library.harness.kikimr_http_client import HiveClient
from ydb.tests.tools.nemesis.library.base import AbstractMonitoredNemesis


@six.add_metaclass(abc.ABCMeta)
class AbstractTabletByTypeNemesis(Nemesis, AbstractMonitoredNemesis):
    def __init__(self, tablet_type, cluster, schedule):
        AbstractMonitoredNemesis.__init__(self, scope='tablets')
        Nemesis.__init__(self, schedule=schedule)
        self.cluster = cluster
        self.__client = None
        self.__tablet_type = tablet_type
        self.__tablet_ids = []

    @property
    def tablet_type(self):
        return self.__tablet_type

    @property
    def client(self):
        if self.__client is None:
            self.__client = kikimr_client_factory(
                self.cluster.nodes[1].host, self.cluster.nodes[1].grpc_port, retry_count=10)
        return self.__client

    def extract_fault(self):
        pass

    @property
    def tablet_ids(self):
        return self.__tablet_ids

    def prepare_state(self):
        self.logger.info('Preparing state for nemesis = ' + str(self))
        response = self.client.tablet_state(self.__tablet_type)

        self.__tablet_ids = [
            info.TabletId for info in response.TabletStateInfo
        ]

    def __str__(self):
        return "{class_name}(tablet_type={tablet_type})".format(
            class_name=self.__class__.__name__,
            tablet_type=self.__tablet_type
        )


class KillSystemTabletByTypeNemesis(AbstractTabletByTypeNemesis):

    def __init__(self, tablet_type, cluster, schedule=(45, 90)):
        super(KillSystemTabletByTypeNemesis, self).__init__(tablet_type, cluster, schedule)
        self.__cluster = cluster
        self.__client = None
        self.__tablet_type = tablet_type

    def inject_fault(self):
        if self.tablet_ids:
            tablet_id = random.choice(self.tablet_ids)
            self.logger.info(
                "Killing {tablet_type}, tablet_id = {tablet_id}".format(
                    tablet_type=self.__tablet_type,
                    tablet_id=tablet_id
                )
            )
            try:
                self.client.tablet_kill(tablet_id)
                self.on_success_inject_fault()
            except RuntimeError:
                self.logger.error(
                    "Failed to kill {tablet_type}, tablet_id = {tablet_id}".format(
                        tablet_type=self.__tablet_type,
                        tablet_id=tablet_id
                    )
                )
        else:
            self.prepare_state()


class KillCoordinatorNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster):
        super(KillCoordinatorNemesis, self).__init__(TabletTypes.FLAT_TX_COORDINATOR, cluster)


class KillMediatorNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster):
        super(KillMediatorNemesis, self).__init__(TabletTypes.TX_MEDIATOR, cluster)


class KillDataShardNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(30, 60)):
        super(KillDataShardNemesis, self).__init__(TabletTypes.FLAT_DATASHARD, cluster, schedule)


class KillHiveNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(120, 240)):
        super(KillHiveNemesis, self).__init__(TabletTypes.FLAT_HIVE, cluster, schedule)


class KillBsControllerNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(300, 600)):
        super(KillBsControllerNemesis, self).__init__(TabletTypes.FLAT_BS_CONTROLLER, cluster, schedule)


class KillSchemeShardNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(120, 240)):
        super(KillSchemeShardNemesis, self).__init__(TabletTypes.FLAT_SCHEMESHARD, cluster, schedule)


class KillPersQueueNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(30, 60)):
        super(KillPersQueueNemesis, self).__init__(TabletTypes.PERSQUEUE, cluster, schedule)


class KillKeyValueNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(30, 60)):
        super(KillKeyValueNemesis, self).__init__(TabletTypes.KEYVALUEFLAT, cluster, schedule)


class KillTxAllocatorNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(120, 240)):
        super(KillTxAllocatorNemesis, self).__init__(TabletTypes.TX_ALLOCATOR, cluster, schedule)


class KillNodeBrokerNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(120, 240)):
        super(KillNodeBrokerNemesis, self).__init__(TabletTypes.NODE_BROKER, cluster, schedule)


class KillTenantSlotBrokerNemesis(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(120, 240)):
        super(KillTenantSlotBrokerNemesis, self).__init__(TabletTypes.TENANT_SLOT_BROKER, cluster, schedule)


class KillBlocktoreVolume(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(30, 60)):
        super(KillBlocktoreVolume, self).__init__(TabletTypes.BLOCKSTORE_VOLUME, cluster, schedule)


class KillBlocktorePartition(KillSystemTabletByTypeNemesis):
    def __init__(self, cluster, schedule=(30, 60)):
        super(KillBlocktorePartition, self).__init__(TabletTypes.BLOCKSTORE_PARTITION, cluster, schedule)


class KickTabletsFromNode(Nemesis, AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(150, 450)):
        Nemesis.__init__(self, schedule=schedule)
        AbstractMonitoredNemesis.__init__(self, scope='tablets')
        self.__nodes = cluster.nodes.values()
        self.hive = HiveClient(cluster.nodes[1].host, cluster.nodes[1].mon_port)

    def prepare_state(self):
        pass

    def inject_fault(self):
        if self.__nodes:
            try:
                node = random.choice(self.__nodes)
                node_id = node.node_id
                self.hive.block_node(node_id)
                self.hive.kick_tablets_from_node(node_id)
                self.hive.unblock_node(node_id)
                self.on_success_inject_fault()
            except Exception as e:
                self.logger.error(
                    "Failed to inject fault, %s", str(
                        e
                    )
                )

    def extract_fault(self):
        pass


class ChangeTabletGroupNemesis(AbstractTabletByTypeNemesis):

    def __init__(self, cluster, tablet_type, schedule=(30, 60), channels=()):
        super(ChangeTabletGroupNemesis, self).__init__(tablet_type=tablet_type, cluster=cluster, schedule=schedule)
        self.hive = HiveClient(cluster.nodes[1].host, cluster.nodes[1].mon_port)
        self.channels = channels

    def inject_fault(self):
        if self.tablet_ids:
            tablet_id = random.choice(self.tablet_ids)
            try:
                self.hive.change_tablet_group(tablet_id, channels=self.channels)
                self.on_success_inject_fault()
            except Exception as e:
                self.logger.error(
                    "Failed to inject fault, %s", str(
                        e
                    )
                )
        else:
            self.prepare_state()


class BulkChangeTabletGroupNemesis(AbstractTabletByTypeNemesis):
    def __init__(self, cluster, tablet_type, schedule=(60, 180), channels=(), percent=None):
        super(BulkChangeTabletGroupNemesis, self).__init__(tablet_type=tablet_type, cluster=cluster, schedule=schedule)
        self.hive = HiveClient(cluster.nodes[1].host, cluster.nodes[1].mon_port)
        self.channels = channels
        self.percent = percent

    def inject_fault(self):
        try:
            self.logger.info('Injecting fault for nemesis = ' + str(self))
            self.hive.change_tablet_group_by_tablet_type(self.tablet_type, percent=self.percent, channels=self.channels)
            self.on_success_inject_fault()
        except Exception as e:
            self.logger.error(
                "Failed to inject fault, %s", str(
                    e
                )
            )


class ReBalanceTabletsNemesis(Nemesis, AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(60, 180)):
        Nemesis.__init__(self, schedule=schedule)
        AbstractMonitoredNemesis.__init__(self)
        self.hive = HiveClient(cluster.nodes[1].host, cluster.nodes[1].mon_port)

    def prepare_state(self):
        pass

    def inject_fault(self):
        try:
            self.hive.rebalance_all_tablets()
            self.on_success_inject_fault()
        except Exception as e:
            self.logger.error(
                "Failed to inject fault, %s", str(
                    e
                )
            )

    def extract_fault(self):
        pass


def change_tablet_group_nemesis_list(cluster):
    result = []
    scale_per_cluster = max(int(len(cluster.nodes.values()) / 8), 1)
    for tablet_type in (TabletTypes.PERSQUEUE, TabletTypes.FLAT_DATASHARD, TabletTypes.KEYVALUEFLAT):
        for _ in range(scale_per_cluster):
            result.append(ChangeTabletGroupNemesis(cluster, tablet_type=tablet_type))
        result.append(BulkChangeTabletGroupNemesis(cluster, tablet_type))
    return result
