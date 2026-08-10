"""
All nemesis type entries for the catalog.

Core nemesis (network, node kill, time skew), cluster-specific entries (tablet
kills, daemon kills, disk operations, rolling updates), and topology-conditional
entries (datacenter / bridge pile scenarios).

Topology-conditional entries are registered only when ``cluster.yaml``
advertises the corresponding sections; see ``runners.yaml_gates``.
"""

from __future__ import annotations

from typing import Any, Type

from ydb.tests.library.common.types import TabletTypes
from ydb.tests.stability.nemesis.internal.nemesis.runners import (
    ClusterBulkChangeTabletGroupNemesis,
    ClusterChangeTabletGroupNemesis,
    ClusterKickTabletsFromNodeNemesis,
    ClusterKillBlockstorePartitionNemesis,
    ClusterKillBlockstoreVolumeNemesis,
    ClusterKillBsControllerNemesis,
    ClusterKillCoordinatorNemesis,
    ClusterKillDataShardNemesis,
    ClusterKillHiveNemesis,
    ClusterKillKeyValueNemesis,
    ClusterKillMediatorNemesis,
    ClusterKillNodeBrokerNemesis,
    ClusterKillNodeDaemonNemesis,
    ClusterKillPersQueueNemesis,
    ClusterKillSchemeShardNemesis,
    ClusterKillSlotDaemonNemesis,
    ClusterKillTenantSlotBrokerNemesis,
    ClusterKillTxAllocatorNemesis,
    ClusterReBalanceTabletsNemesis,
    ClusterRollingRestartNemesis,
    ClusterSafelyBreakDiskNemesis,
    ClusterSafelyCleanupDisksNemesis,
    ClusterSerialKillNodeNemesis,
    ClusterSerialKillSlotsNemesis,
    ClusterStopStartNodeNemesis,
    ClusterSuspendNodeNemesis,
    # ClusterHardRebootHostNemesis,
    KillNodeNemesis,
    NetworkNemesis,
    DnsNemesis,
    TimeSkewNemesis,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.network_planner import (
    DnsNemesisPlanner,
    NetworkNemesisPlanner,
    TimeSkewNemesisPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.pinned_first_host_planner import (
    PinnedFirstHostPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.serial_staggered_planner import (
    SerialStaggeredInjectPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.topology_fanout_planner import (
    BridgePileFanoutPlanner,
    DataCenterFanoutPlanner,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.rolling_restart_planner import (
    RollingRestartNemesisPlanner
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    GuardMode,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import TargetKind

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CHANGE_TABLET_TYPES = (TabletTypes.PERSQUEUE, TabletTypes.FLAT_DATASHARD, TabletTypes.KEYVALUEFLAT)

_UI_GROUP = "ClusterChaos"
_TABLET_UI_GROUP = "ClusterTablets"
_DATACENTER_UI_GROUP = "DatacenterChaos"
_BRIDGE_UI_GROUP = "BridgePileChaos"


# ---------------------------------------------------------------------------
# Planner factories
# ---------------------------------------------------------------------------


def _pinned_planner_factory(nemesis_type_key: str) -> PinnedFirstHostPlanner:
    return PinnedFirstHostPlanner(nemesis_type_key)


def _serial_staggered_node_planner_factory(nemesis_type_key: str) -> SerialStaggeredInjectPlanner:
    return SerialStaggeredInjectPlanner(nemesis_type_key, target_kind="node")


def _serial_staggered_slot_planner_factory(nemesis_type_key: str) -> SerialStaggeredInjectPlanner:
    return SerialStaggeredInjectPlanner(nemesis_type_key, target_kind="slot")


def _dc_fanout_planner_factory(nemesis_type_key: str) -> DataCenterFanoutPlanner:
    return DataCenterFanoutPlanner(nemesis_type_key)


def _bridge_pile_fanout_planner_factory(nemesis_type_key: str) -> BridgePileFanoutPlanner:
    return BridgePileFanoutPlanner(nemesis_type_key)


# ---------------------------------------------------------------------------
# Kill-tablet specs (type key, actor class, schedule_sec)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def all_nemesis_type_entries() -> dict[str, dict[str, Any]]:
    """All nemesis type entries for ``catalog.NEMESIS_TYPES``."""
    out: dict[str, dict[str, Any]] = {}

    # --- core nemesis (network / node / time skew) --------------------------
    out["NetworkNemesis"] = {
        "runner": NetworkNemesis(),
        "schedule": 200,
        "ui_group": "NetworkNemesis",
        "planner_cls": NetworkNemesisPlanner,
        "target_kind": TargetKind.HOST,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
        "supports_manual": False,
        # Toggle: probe extracts after auto_recovery_sec (clear iptables drops).
        "recovery": "extract",
        "auto_recovery_sec": 120,
    }
    out["KillNodeNemesis"] = {
        "runner": KillNodeNemesis(),
        "schedule": 200,
        "ui_group": "NodeNemesis",
        "target_kind": TargetKind.NODE,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
        # Self-healing (SIGKILL + systemd); probe releases on endpoint + storage GREEN.
    }
    out["DnsNemesis"] = {
        "runner": DnsNemesis(),
        "schedule": 120,
        "ui_group": "NetworkNemesis",
        "planner_cls": DnsNemesisPlanner,
        "target_kind": TargetKind.HOST,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
        "supports_manual": False,
        # Toggle: probe extracts after auto_recovery_sec (clear iptables DNS drops).
        "recovery": "extract",
        "auto_recovery_sec": 120,
    }

    out["ClusterRollingRestartNemesis"] = {
        "runner": ClusterRollingRestartNemesis(),
        "schedule": 200,
        "ui_group": _UI_GROUP,
        "planner_factory": lambda nemesis_type_key, params=None: RollingRestartNemesisPlanner(
            **(params or {})
        ),
        "target_kind": TargetKind.NODE,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
        "supports_manual": False,
        "params": [
            {
                "name": "nodes_per_step",
                "label": "Nodes per step",
                "type": "int",
                "default": 2,
                "min": 1,
                "description": "Max nodes restarted per scheduled tick.",
            },
            {
                "name": "use_storage_nodes",
                "label": "Use storage nodes",
                "type": "bool",
                "default": False,
                "description": "If checked, restart storage nodes; otherwise compute nodes.",
            },
            {
                "name": "node_downtime_sec",
                "label": "Node downtime (sec)",
                "type": "int",
                "default": 60,
                "min": 1,
                "description": "How long the agent keeps each node down.",
            },
        ],
    }
    out["TimeSkewNemesis"] = {
        "runner": TimeSkewNemesis(),
        "schedule": 400,
        "ui_group": "NetworkNemesis",
        "planner_cls": TimeSkewNemesisPlanner,
        "target_kind": TargetKind.HOST,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
        "supports_manual": False,
        # Toggle: probe extracts after auto_recovery_sec (re-enable ntp).
        "recovery": "extract",
        "auto_recovery_sec": 120,
    }

    # --- tablet chaos (BYPASS: not counted against the failure-model budget) ---
    for wire, cls, sched in _KILL_TABLET_SPECS:
        out[wire] = {
            "runner": cls(),
            "schedule": sched,
            "ui_group": _TABLET_UI_GROUP,
            "target_kind": TargetKind.TABLET,
            "impact_scope": ImpactScope.NODE,
            "guard_mode": GuardMode.BYPASS,
        }

    out["KickTabletsFromNodeNemesis"] = {
        "runner": ClusterKickTabletsFromNodeNemesis(),
        "schedule": 200,
        "ui_group": _TABLET_UI_GROUP,
        "target_kind": TargetKind.NODE,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
    }
    out["ReBalanceTabletsNemesis"] = {
        "runner": ClusterReBalanceTabletsNemesis(),
        "schedule": 120,
        "ui_group": _TABLET_UI_GROUP,
        "target_kind": TargetKind.TABLET,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.BYPASS,
    }

    # One scheduled type per tablet kind is enough (orchestrator picks hosts at random per tick).
    for tt in _CHANGE_TABLET_TYPES:
        out[f"ChangeTabletGroup_{tt.name}"] = {
            "runner": ClusterChangeTabletGroupNemesis(tt, channels=()),
            "schedule": 120,
            "ui_group": _TABLET_UI_GROUP,
            "target_kind": TargetKind.TABLET,
            "impact_scope": ImpactScope.NODE,
            "guard_mode": GuardMode.BYPASS,
        }
        out[f"BulkChangeTabletGroup_{tt.name}"] = {
            "runner": ClusterBulkChangeTabletGroupNemesis(tt, percent=None, channels=()),
            "schedule": 180,
            "ui_group": _TABLET_UI_GROUP,
            "target_kind": TargetKind.TABLET,
            "impact_scope": ImpactScope.NODE,
            "guard_mode": GuardMode.BYPASS,
        }

    # --- daemon kills (self-healing; probe releases by HC) ------------------
    out["KillSlotDaemonNemesis"] = {
        "runner": ClusterKillSlotDaemonNemesis(),
        "schedule": 120,
        "ui_group": _UI_GROUP,
        "target_kind": TargetKind.SLOT,
        "impact_scope": ImpactScope.SLOT,
        "guard_mode": GuardMode.FULL,
    }
    out["KillNodeDaemonNemesis"] = {
        "runner": ClusterKillNodeDaemonNemesis(),
        "schedule": 180,
        "ui_group": _UI_GROUP,
        "target_kind": TargetKind.NODE,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
    }

    # --- serial kills -------------------------------------------------------
    out["SerialKillNodeNemesis"] = {
        "runner": ClusterSerialKillNodeNemesis(),
        "schedule": 300,
        "ui_group": _UI_GROUP,
        "planner_factory": _serial_staggered_node_planner_factory,
        # Injects exactly the candidates it is handed, so the boundary scheduler may drive it.
        "boundary_safe": True,
        "target_kind": TargetKind.NODE,
        "impact_scope": ImpactScope.NODE,
        "guard_mode": GuardMode.FULL,
    }
    out["SerialKillSlotsNemesis"] = {
        "runner": ClusterSerialKillSlotsNemesis(),
        "schedule": 300,
        "ui_group": _UI_GROUP,
        "planner_factory": _serial_staggered_slot_planner_factory,
        "boundary_safe": True,
        "target_kind": TargetKind.SLOT,
        "impact_scope": ImpactScope.SLOT,
        "guard_mode": GuardMode.FULL,
    }

    # --- disk / rolling / stop-start / suspend (toggle: extract then HC confirm) ---
    for wire, cls, sched, scope, tkind, extra in (
        ("SafelyBreakDiskNemesis", ClusterSafelyBreakDiskNemesis, 400, ImpactScope.DISK, TargetKind.DISK,
         {"recovery": "extract", "auto_recovery_sec": 120}),
        # Obliterate: confirm waits out full re-replication.
        ("SafelyCleanupDisksNemesis", ClusterSafelyCleanupDisksNemesis, 400, ImpactScope.DISK, TargetKind.DISK,
         {"recovery": "extract", "auto_recovery_sec": 90, "confirm_timeout_sec": 7200}),
        # ("RollingUpdateClusterNemesis", ClusterRollingUpdateNemesis, 120, ImpactScope.NODE, TargetKind.NODE, {}),
        ("StopStartNodeNemesis", ClusterStopStartNodeNemesis, 400, ImpactScope.NODE, TargetKind.NODE,
         {"recovery": "extract", "auto_recovery_sec": 90}),
        # SIGSTOP never self-heals — needs extract.
        ("SuspendNodeNemesis", ClusterSuspendNodeNemesis, 800, ImpactScope.NODE, TargetKind.NODE,
         {"recovery": "extract", "auto_recovery_sec": 90}),
    ):
        out[wire] = {
            "runner": cls(),
            "schedule": sched,
            "ui_group": _UI_GROUP,
            "planner_factory": _pinned_planner_factory,
            "target_kind": tkind,
            "impact_scope": scope,
            "guard_mode": GuardMode.FULL,
            **extra,
        }

    # --- host reboot --------------------------------------------------------
    # out["HardRebootHostNemesis"] = {
    #     "runner": ClusterHardRebootHostNemesis(),
    #     "schedule": 700,
    #     "ui_group": _UI_GROUP,
    #      should be executed on any host except the first
    # }

    # --- topology-conditional (datacenter / bridge pile) --------------------
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
        # StopNodes only: it is a self-recovering pulse. The route/iptables/DNS variants need an
        # explicit extract and don't fit the inject-only path.
        from ydb.tests.stability.nemesis.internal.nemesis.runners import (
            ClusterDataCenterStopNodesNemesis,
        )

        extra["DataCenterStopNodesNemesis"] = {
            "runner": ClusterDataCenterStopNodesNemesis(),
            "schedule": 600,
            "ui_group": _DATACENTER_UI_GROUP,
            "planner_factory": _dc_fanout_planner_factory,
            "target_kind": TargetKind.DATACENTER,
            "impact_scope": ImpactScope.DATACENTER,
            "guard_mode": GuardMode.FULL,  # reserves the whole realm
            # Self-healing pulse; probe waits for DC endpoints + storage GREEN.
            "supports_manual": False,
        }

    if yaml_has_bridge_piles_section(path):
        from ydb.tests.stability.nemesis.internal.nemesis.runners import (
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
                "planner_factory": _bridge_pile_fanout_planner_factory,
                "target_kind": TargetKind.PILE,
                "impact_scope": ImpactScope.PILE,
                "guard_mode": GuardMode.BYPASS,
                "supports_manual": False,
            }

    return extra
