# Copied and adapted from ydb/tests/tools/nemesis/library/tablet.py (no tools.nemesis imports).

from __future__ import annotations

import random
import traceback
from typing import ClassVar, List, Optional

from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.types import TabletTypes

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


class ClusterKillTabletByTypeNemesis(MonitoredAgentActor):
    """Kill one random tablet of ``_tablet_type`` (tablet_state + tablet_kill)."""

    _tablet_type: ClassVar[TabletTypes]

    def __init__(self) -> None:
        super().__init__(scope="tablets")
        self._tablet_ids: List[int] = []
        self._client = None
        self._disabled = False

    def _grpc_client(self):
        if self._client is None:
            cluster = require_external_cluster()
            n = cluster.nodes[1]
            self._client = kikimr_client_factory(n.host, n.grpc_port, retry_count=10)
        return self._client

    def _prepare_state(self) -> None:
        self._logger.info("=== PREPARE_STATE START: %s ===", self.__class__.__name__)
        try:
            response = self._grpc_client().tablet_state(self._tablet_type)
            self._tablet_ids = [info.TabletId for info in response.TabletStateInfo]
            self._logger.info(
                "Found %d tablets of type %s", len(self._tablet_ids), self._tablet_type
            )
        except Exception:
            self._logger.error("prepare_state failed: %s", traceback.format_exc())
            self._disabled = True

    def inject_fault(self, payload=None) -> None:
        del payload
        if self._disabled:
            return
        self._logger.info("=== INJECT_FAULT START: %s ===", self.__class__.__name__)
        if self._tablet_ids:
            tablet_id = random.choice(self._tablet_ids)
            try:
                self._grpc_client().tablet_kill(tablet_id)
                self.on_success_inject_fault()
                self._logger.info("=== INJECT_FAULT SUCCESS ===")
            except Exception as e:
                self._logger.error("tablet_kill failed: %s", e)
                raise
        else:
            self._prepare_state()
            if self._tablet_ids:
                self.inject_fault()
            else:
                self.on_failed_inject_fault()
                self._logger.warning("No tablets of type %s", self._tablet_type)

    def extract_fault(self, payload=None) -> None:
        del payload
        self._logger.info("extract_fault (no-op recovery) %s", self.__class__.__name__)
        self.on_success_extract_fault()


class ClusterKillCoordinatorNemesis(ClusterKillTabletByTypeNemesis):
    """Kill FLAT_TX_COORDINATOR tablet."""

    _tablet_type = TabletTypes.FLAT_TX_COORDINATOR


class ClusterKillMediatorNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.TX_MEDIATOR


class ClusterKillDataShardNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.FLAT_DATASHARD


class ClusterKillHiveNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.FLAT_HIVE


class ClusterKillBsControllerNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.FLAT_BS_CONTROLLER


class ClusterKillSchemeShardNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.FLAT_SCHEMESHARD


class ClusterKillPersQueueNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.PERSQUEUE


class ClusterKillKeyValueNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.KEYVALUEFLAT


class ClusterKillTxAllocatorNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.TX_ALLOCATOR


class ClusterKillNodeBrokerNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.NODE_BROKER


class ClusterKillTenantSlotBrokerNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.TENANT_SLOT_BROKER


class ClusterKillBlockstoreVolumeNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.BLOCKSTORE_VOLUME


class ClusterKillBlockstorePartitionNemesis(ClusterKillTabletByTypeNemesis):
    _tablet_type = TabletTypes.BLOCKSTORE_PARTITION


class ClusterChangeTabletGroupNemesis(MonitoredAgentActor):
    """change_tablet_group for one tablet id (after tablet_state)."""

    def __init__(self, tablet_type: TabletTypes, channels: tuple = ()) -> None:
        super().__init__(scope="tablets")
        self._tablet_type = tablet_type
        self._channels = channels
        self._tablet_ids: List[int] = []
        self._client = None
        self._hive = None
        self._disabled = False

    def _grpc_client(self):
        if self._client is None:
            cluster = require_external_cluster()
            n = cluster.nodes[1]
            self._client = kikimr_client_factory(n.host, n.grpc_port, retry_count=10)
        return self._client

    def _hive_client(self):
        if self._hive is None:
            cluster = require_external_cluster()
            from ydb.tests.library.clients.kikimr_http_client import HiveClient

            n = cluster.nodes[1]
            self._hive = HiveClient(n.host, n.mon_port)
        return self._hive

    def _prepare_state(self) -> None:
        try:
            response = self._grpc_client().tablet_state(self._tablet_type)
            self._tablet_ids = [info.TabletId for info in response.TabletStateInfo]
        except Exception:
            self._logger.error("prepare_state failed: %s", traceback.format_exc())
            self._disabled = True

    def inject_fault(self, payload=None) -> None:
        del payload
        if self._disabled:
            return
        if self._tablet_ids:
            tablet_id = random.choice(self._tablet_ids)
            try:
                self._hive_client().change_tablet_group(tablet_id, channels=self._channels)
                self.on_success_inject_fault()
            except Exception as e:
                self._logger.error("change_tablet_group failed: %s", e)
                raise
        else:
            self._prepare_state()
            if self._tablet_ids:
                self.inject_fault()

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()


class ClusterBulkChangeTabletGroupNemesis(MonitoredAgentActor):
    """change_tablet_group_by_tablet_type."""

    def __init__(self, tablet_type: TabletTypes, *, percent: Optional[int] = None, channels: tuple = ()) -> None:
        super().__init__(scope="tablets")
        self._tablet_type = tablet_type
        self._percent = percent
        self._channels = channels
        self._hive = None

    def _hive_client(self):
        if self._hive is None:
            cluster = require_external_cluster()
            from ydb.tests.library.clients.kikimr_http_client import HiveClient

            n = cluster.nodes[1]
            self._hive = HiveClient(n.host, n.mon_port)
        return self._hive

    def inject_fault(self, payload=None) -> None:
        del payload
        try:
            self._hive_client().change_tablet_group_by_tablet_type(
                self._tablet_type, percent=self._percent, channels=self._channels
            )
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("bulk change tablet group failed: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self.on_success_extract_fault()
