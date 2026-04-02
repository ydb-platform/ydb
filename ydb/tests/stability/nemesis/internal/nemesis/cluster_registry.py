"""Register app-local cluster nemesis actors (wire ids = public type names, no ``Lib`` prefix)."""

from __future__ import annotations

from typing import Any, Type

from ydb.tests.library.common.types import TabletTypes
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.pinned_first_host_planner import (
    PinnedFirstHostPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.serial_staggered_planner import (
    SerialStaggeredInjectPlanner,
)
from ydb.tests.stability.nemesis.internal.nemesis.runners.cluster_disk import (
    ClusterSafelyBreakDiskNemesis,
    ClusterSafelyCleanupDisksNemesis,
)
from ydb.tests.stability.nemesis.internal.nemesis.runners.cluster_hive import (
    ClusterKickTabletsFromNodeNemesis,
    ClusterReBalanceTabletsNemesis,
)
from ydb.tests.stability.nemesis.internal.nemesis.runners.cluster_node import (
    ClusterKillNodeDaemonNemesis,
    ClusterKillSlotDaemonNemesis,
    ClusterRollingUpdateNemesis,
    ClusterSerialKillNodeNemesis,
    ClusterSerialKillSlotsNemesis,
    ClusterStopStartNodeNemesis,
    ClusterSuspendNodeNemesis,
)
from ydb.tests.stability.nemesis.internal.nemesis.runners.cluster_tablets import (
    ClusterBulkChangeTabletGroupNemesis,
    ClusterChangeTabletGroupNemesis,
    ClusterKillBlockstorePartitionNemesis,
    ClusterKillBlockstoreVolumeNemesis,
    ClusterKillBsControllerNemesis,
    ClusterKillCoordinatorNemesis,
    ClusterKillDataShardNemesis,
    ClusterKillHiveNemesis,
    ClusterKillKeyValueNemesis,
    ClusterKillMediatorNemesis,
    ClusterKillNodeBrokerNemesis,
    ClusterKillPersQueueNemesis,
    ClusterKillSchemeShardNemesis,
    ClusterKillTenantSlotBrokerNemesis,
    ClusterKillTxAllocatorNemesis,
)

_CHANGE_TABLET_TYPES = (TabletTypes.PERSQUEUE, TabletTypes.FLAT_DATASHARD, TabletTypes.KEYVALUEFLAT)

_UI_GROUP = "ClusterChaos"
_TABLET_UI_GROUP = "ClusterTablets"
_DATACENTER_UI_GROUP = "DatacenterChaos"
_BRIDGE_UI_GROUP = "BridgePileChaos"


def _pinned_planner_factory(nemesis_type_key: str) -> PinnedFirstHostPlanner:
    return PinnedFirstHostPlanner(nemesis_type_key)


def _serial_staggered_node_planner_factory(nemesis_type_key: str) -> SerialStaggeredInjectPlanner:
    return SerialStaggeredInjectPlanner(nemesis_type_key, target_kind="node")


def _serial_staggered_slot_planner_factory(nemesis_type_key: str) -> SerialStaggeredInjectPlanner:
    return SerialStaggeredInjectPlanner(nemesis_type_key, target_kind="slot")


# (type key, actor class, schedule_sec)
_KILL_TABLET_SPECS: tuple[tuple[str, Type[Any], int], ...] = (
    ("KillCoordinatorNemesis", ClusterKillCoordinatorNemesis, 180),
    ("KillHiveNemesis", ClusterKillHiveNemesis, 180),
    ("KillBsControllerNemesis", ClusterKillBsControllerNemesis, 300),
    ("KillNodeBrokerNemesis", ClusterKillNodeBrokerNemesis, 180),
    ("KillSchemeShardNemesis", ClusterKillSchemeShardNemesis, 180),
    ("KillMediatorNemesis", ClusterKillMediatorNemesis, 180),
    ("KillTxAllocatorNemesis", ClusterKillTxAllocatorNemesis, 180),
    ("KillKeyValueNemesis", ClusterKillKeyValueNemesis, 180),
    ("KillTenantSlotBrokerNemesis", ClusterKillTenantSlotBrokerNemesis, 180),
    ("KillPersQueueNemesis", ClusterKillPersQueueNemesis, 90),
    ("KillDataShardNemesis", ClusterKillDataShardNemesis, 90),
    ("KillBlockstoreVolumeNemesis", ClusterKillBlockstoreVolumeNemesis, 90),
    ("KillBlockstorePartitionNemesis", ClusterKillBlockstorePartitionNemesis, 90),
)


def cluster_nemesis_type_entries() -> dict[str, dict[str, Any]]:
    """Entries merged into ``catalog.NEMESIS_TYPES``."""
    out: dict[str, dict[str, Any]] = {}

    for wire, cls, sched in _KILL_TABLET_SPECS:
        out[wire] = {
            "runner": cls(),
            "schedule": sched,
            "ui_group": _TABLET_UI_GROUP,
        }

    out["KickTabletsFromNodeNemesis"] = {
        "runner": ClusterKickTabletsFromNodeNemesis(),
        "schedule": 200,
        "ui_group": _TABLET_UI_GROUP,
    }
    out["ReBalanceTabletsNemesis"] = {
        "runner": ClusterReBalanceTabletsNemesis(),
        "schedule": 120,
        "ui_group": _TABLET_UI_GROUP,
    }

    # One scheduled type per tablet kind is enough (orchestrator picks hosts at random per tick).
    for tt in _CHANGE_TABLET_TYPES:
        out[f"ChangeTabletGroup_{tt.name}"] = {
            "runner": ClusterChangeTabletGroupNemesis(tt, channels=()),
            "schedule": 120,
            "ui_group": _TABLET_UI_GROUP,
        }
        out[f"BulkChangeTabletGroup_{tt.name}"] = {
            "runner": ClusterBulkChangeTabletGroupNemesis(tt, percent=10, channels=()),
            "schedule": 180,
            "ui_group": _TABLET_UI_GROUP,
        }

    out["KillSlotDaemonNemesis"] = {
        "runner": ClusterKillSlotDaemonNemesis(),
        "schedule": 120,
        "ui_group": _UI_GROUP,
    }
    out["KillNodeDaemonNemesis"] = {
        "runner": ClusterKillNodeDaemonNemesis(),
        "schedule": 180,
        "ui_group": _UI_GROUP,
    }

    out["SerialKillNodeNemesis"] = {
        "runner": ClusterSerialKillNodeNemesis(),
        "schedule": 300,
        "ui_group": _UI_GROUP,
        "planner_factory": _serial_staggered_node_planner_factory,
    }
    out["SerialKillSlotsNemesis"] = {
        "runner": ClusterSerialKillSlotsNemesis(),
        "schedule": 300,
        "ui_group": _UI_GROUP,
        "planner_factory": _serial_staggered_slot_planner_factory,
    }

    for wire, cls, sched in (
        ("SafelyBreakDiskNemesis", ClusterSafelyBreakDiskNemesis, 400),
        ("SafelyCleanupDisksNemesis", ClusterSafelyCleanupDisksNemesis, 400),
        ("RollingUpdateClusterNemesis", ClusterRollingUpdateNemesis, 120),
        ("StopStartNodeNemesis", ClusterStopStartNodeNemesis, 400),
        ("SuspendNodeNemesis", ClusterSuspendNodeNemesis, 800),
    ):
        out[wire] = {
            "runner": cls(),
            "schedule": sched,
            "ui_group": _UI_GROUP,
            "planner_factory": _pinned_planner_factory,
        }

    out.update(_topology_conditional_entries())
    return out


def _topology_conditional_entries() -> dict[str, dict[str, Any]]:
    """Datacenter / bridge pile actors only if ``cluster.yaml`` advertises the layout."""
    from ydb.tests.stability.nemesis.internal.nemesis.runners.yaml_gates import (
        yaml_has_bridge_piles_section,
        yaml_has_multi_datacenter,
    )
    from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import cluster_yaml_path

    path = cluster_yaml_path()
    extra: dict[str, dict[str, Any]] = {}

    if yaml_has_multi_datacenter(path):
        from ydb.tests.stability.nemesis.internal.nemesis.runners.datacenter import (
            ClusterDataCenterIptablesBlockPortsNemesis,
            ClusterDataCenterRouteUnreachableNemesis,
            ClusterDataCenterStopNodesNemesis,
        )

        for wire, cls, sched in (
            ("DataCenterStopNodesNemesis", ClusterDataCenterStopNodesNemesis, 600),
            ("DataCenterRouteUnreachableNemesis", ClusterDataCenterRouteUnreachableNemesis, 600),
            ("DataCenterIptablesBlockPortsNemesis", ClusterDataCenterIptablesBlockPortsNemesis, 600),
        ):
            extra[wire] = {
                "runner": cls(),
                "schedule": sched,
                "ui_group": _DATACENTER_UI_GROUP,
                "planner_factory": _pinned_planner_factory,
            }

    if yaml_has_bridge_piles_section(path):
        from ydb.tests.stability.nemesis.internal.nemesis.runners.bridge_pile import (
            ClusterBridgePileIptablesBlockPortsNemesis,
            ClusterBridgePileRouteUnreachableNemesis,
            ClusterBridgePileStopNodesNemesis,
        )

        for wire, cls, sched in (
            ("BridgePileStopNodesNemesis", ClusterBridgePileStopNodesNemesis, 600),
            ("BridgePileRouteUnreachableNemesis", ClusterBridgePileRouteUnreachableNemesis, 600),
            ("BridgePileIptablesBlockPortsNemesis", ClusterBridgePileIptablesBlockPortsNemesis, 600),
        ):
            extra[wire] = {
                "runner": cls(),
                "schedule": sched,
                "ui_group": _BRIDGE_UI_GROUP,
                "planner_factory": _pinned_planner_factory,
            }

    return extra
