"""Synthetic SelfCheck with YDB's data-quorum BRIDGE_GROUP visibility.

This models one representative group, not the tablet/whiteboard collection path.
Besides group parts it emits the server's three other issue kinds the skipper reads: BLUE
`Replication in progress` VDISK reasons of stage-2 piles, PILE_* state storage issues and
HEALTHCHECK_STATUS on request. Messages are the server's; reason chains are shallower.
"""
import binascii
import typing

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.public.api.protos import ydb_monitoring_pb2

StatusFlag = ydb_monitoring_pb2.StatusFlag
SelfCheck = ydb_monitoring_pb2.SelfCheck

STORAGE = "storage"
CONNECTIVITY = "connectivity"
POOL_NAME = "/Root:mock"
PROXY_GROUP_ID = "1"
ISSUE_LEVEL = 5
HEALTHCHECK_MESSAGES = {
    "YELLOW": "Healthcheck response size exceeds 30 MB",
    "ORANGE": "Healthcheck response size exceeds 40 MB",
    "RED": "Healthcheck response size exceeds 50 MB and will be truncated",
}


def blame(instance, node_index: int) -> typing.Dict[str, str]:
    """Pile name -> cause, for the node `node_index` of this instance's pile.

    Manual blame comes from the node's faults. Automatic blame (policy `auto_blame_dead_piles`)
    uses the BSC host's connectivity, shared by every responder that reaches BSC.
    BSC-backed SelfCheck evaluates BRIDGE_GROUP parts in stored SYNCHRONIZED
    and NOT_SYNCHRONIZED_2 states.
    Manual blame uses the same visibility filter; raw response overrides may bypass it.
    The BSC host never blames its own pile automatically. Fallback has no BRIDGE_GROUP parts.
    """
    faults = instance.node_faults[node_index]
    if not bsc_available(instance):
        return {}
    out: typing.Dict[str, str] = {}
    for name in sorted(faults.blame_connectivity):
        out[name] = CONNECTIVITY
    for name in sorted(faults.blame_storage):
        out[name] = STORAGE
    state = instance.replica.state
    if instance.policies.auto_blame_dead_piles:
        host = state.promoted
        host_links = instance.reach.last_connected_piles.get(host, [])
        for p, name in enumerate(instance.names):
            if p == host or name in out:
                continue
            connected = (instance.connected(p) if host == instance.index else
                         host_links[p] if p < len(host_links) else True)
            if not connected and model.requires_data_quorum(state.per_pile[p]):
                out[name] = CONNECTIVITY
    return {name: cause for name, cause in out.items()
            if model.requires_data_quorum(state.per_pile[instance.names.index(name)])}


def bsc_available(instance) -> bool:
    """BSC follows PROMOTED, which equals PRIMARY when there is no pending promotion."""
    host = instance.state.promoted
    return instance.connected(host) and instance.has_static_quorum(host)


def _fallback(instance, node, result) -> None:
    result.issue_log.add(id="%d-bsc-missing" % node.node_id, type="STORAGE", status=StatusFlag.RED,
                         message="System tablet BSC didn't provide information", level=2)
    faults = instance.node_faults[node.index_in_pile]
    for p, name in enumerate(instance.names):
        if name not in instance.state.registered_names(instance.names):
            continue
        outcomes = (instance.node_health() if p == instance.index else
                    instance.reach.last_node_health.get(p, []) if instance.reach.reachable(p) else [])
        refused = outcomes.count("refused")
        silent = outcomes.count("silent") + instance.topo.nodes_per_pile[name] - len(outcomes)
        # This mock has one static part per pile, using the configured count as its quorum approximation.
        red = (name in faults.blame_storage or name in faults.blame_connectivity or
               len(outcomes) - refused < instance.topo.static_quorum[name])
        if not outcomes:
            red = name in faults.blame_storage or name in faults.blame_connectivity
        if not red and not silent:
            continue
        issue = _issue(node, name, p, STORAGE)
        issue.type = "STORAGE_GROUP"
        issue.status = StatusFlag.RED if red else StatusFlag.GREY
        issue.message = "Group failed" if red else "Group has disks with no data"
        result.issue_log.append(issue)


def _issue(node, pile_name: str, pile_index: int, cause: str):
    issue = ydb_monitoring_pb2.IssueLog(
        id="%d-bridge-group-%s-%s" % (node.node_id, pile_name, cause),
        type="BRIDGE_GROUP",
        status=StatusFlag.RED,
        level=ISSUE_LEVEL,
        message="Group failed",
        listed=1,
        count=1,
    )
    issue.location.storage.pool.name = POOL_NAME
    issue.location.storage.pool.group.id.append(str(pile_index + 1))
    issue.location.storage.pool.group.pile.name = pile_name
    return issue


def _pile_issues(instance, node, result) -> int:
    """PILE_* issues of SYNCHRONIZED piles, one ring per static node; returns the worst status.

    The server attaches ring and node reason chains; the mock omits them.
    """
    worst = StatusFlag.GREEN
    for p, name in enumerate(instance.names):
        if instance.state.stored(p) != model.Stored.SYNCHRONIZED:
            continue
        nodes = instance.topo.nodes_per_pile[name]
        down = nodes - instance.connected_static_nodes(p)
        if down == 0:
            continue
        if down > (nodes - 1) // 2:
            status, message = StatusFlag.RED, "There is not enough functional rings"
        elif down > 1:
            status, message = StatusFlag.YELLOW, "Multiple rings have unavailable replicas"
        else:
            status, message = StatusFlag.BLUE, "One ring has unavailable replicas"
        for kind in ("STATE_STORAGE", "SCHEME_BOARD", "BOARD"):
            issue = result.issue_log.add(id="%d-pile-%s-%s" % (node.node_id, kind.lower(), name),
                                         type="PILE_" + kind, status=status, level=2, message=message)
            issue.location.compute.state_storage.pile.name = name
        worst = max(worst, status)
    return worst


def build(instance, node) -> ydb_monitoring_pb2.SelfCheckResult:
    faults = instance.node_faults[node.index_in_pile]
    blamed = blame(instance, node.index_in_pile)
    style = instance.policies.issue_style
    result = ydb_monitoring_pb2.SelfCheckResult()
    children = []
    fallback = not bsc_available(instance)
    if fallback:
        _fallback(instance, node, result)
    pile_status = _pile_issues(instance, node, result)
    for name in sorted(blamed):
        issue = _issue(node, name, instance.names.index(name), blamed[name])
        result.issue_log.append(issue)
        children.append(issue)
    for p, name in enumerate(instance.names):
        if fallback or name in blamed or instance.state.stored(p) != model.Stored.NOT_SYNCHRONIZED_2:
            continue
        issue = _issue(node, name, p, "replicating")
        vdisk = ydb_monitoring_pb2.IssueLog(id="%d-vdisk-%s-replicating" % (node.node_id, name), type="VDISK",
                                            status=StatusFlag.BLUE, level=ISSUE_LEVEL + 1,
                                            message="Replication in progress")
        issue.reason.append(vdisk.id)
        result.issue_log.append(issue)
        result.issue_log.append(vdisk)
        children.append(issue)
    synced = {instance.names[i] for i, state in enumerate(instance.replica.state.per_pile)
              if state == model.Stored.SYNCHRONIZED}
    overall = StatusFlag.GREEN
    if blamed:
        overall = StatusFlag.RED if synced.issubset(blamed) else StatusFlag.ORANGE
    if len(children) > len(blamed):
        overall = max(overall, StatusFlag.ORANGE)
    if style == "realistic" and children:
        evaluated = {instance.names[i] for i, state in enumerate(instance.replica.state.per_pile)
                     if model.requires_data_quorum(state)}
        result.issue_log.append(_parent_issue(node, children, overall, evaluated.issubset(blamed)))
    status = max(overall, pile_status)
    if fallback:
        overall = StatusFlag.RED
        result.self_check_result = SelfCheck.EMERGENCY
    elif faults.self_check is not None:
        result.self_check_result = SelfCheck.Result.Value(faults.self_check)
    elif status == StatusFlag.RED:
        result.self_check_result = SelfCheck.EMERGENCY
    elif status == StatusFlag.ORANGE:
        result.self_check_result = SelfCheck.MAINTENANCE_REQUIRED
    elif status in (StatusFlag.BLUE, StatusFlag.YELLOW):
        result.self_check_result = SelfCheck.DEGRADED
    else:
        result.self_check_result = SelfCheck.GOOD
    result.location.id = node.node_id
    result.location.host = node.label
    result.location.port = node.grpc_port
    result.location.pile.name = node.pile_name
    # Explicit diagnostic coverage is required to distinguish healthy storage
    # from an incomplete SelfCheck response with no reported issues.
    database = result.database_status.add(name="/Root", overall=overall)
    database.storage.overall = overall
    pool = database.storage.pools.add(id=POOL_NAME, overall=overall)
    pool.groups.add(id=PROXY_GROUP_ID, overall=overall)
    if faults.healthcheck_status is not None:
        if faults.healthcheck_status == "RED":
            for i in reversed(range(len(result.issue_log))):
                if result.issue_log[i].type == "VDISK":
                    del result.issue_log[i]
        message = HEALTHCHECK_MESSAGES[faults.healthcheck_status]
        result.issue_log.add(id="%s-%04x" % (faults.healthcheck_status, binascii.crc_hqx(message.encode(), 0)),
                             type="HEALTHCHECK_STATUS", level=1,
                             status=StatusFlag.Status.Value(faults.healthcheck_status), message=message)
    return result


def _parent_issue(node, children, overall, all_evaluated_blamed):
    """Optional STORAGE_GROUP parent that lists the BRIDGE_GROUP ids in `reason`.

    Real RenameMergingIssues copies pile attribution only for a single reason,
    even when merging is off. The parent retains the proxy group ID, not a part's
    ID. The status is RED only when every SYNCHRONIZED pile's copy is blamed.
    Wording covers all evaluated copies. Unlike production's single-child rewrite,
    the mock retains aggregate wording.
    Clients must use status, type and pile location, not message text.
    """
    parent = ydb_monitoring_pb2.IssueLog(
        id="%d-storage-group" % node.node_id,
        type="STORAGE_GROUP",
        status=overall,
        level=ISSUE_LEVEL - 1,
        message="Group dead in all piles" if all_evaluated_blamed else "Group dead in some piles",
        listed=len(children),
        count=len(children),
    )
    for child in children:
        parent.reason.append(child.id)
    parent.location.storage.pool.name = POOL_NAME
    parent.location.storage.pool.group.id.append(PROXY_GROUP_ID)
    if len(children) == 1:
        parent.location.storage.pool.group.pile.CopyFrom(children[0].location.storage.pool.group.pile)
    return parent
