"""Synthetic SelfCheck with YDB's synchronized-only BRIDGE_GROUP visibility.

This models one representative group, not the tablet/whiteboard collection path.
"""
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


def blame(instance, node_index: int) -> typing.Dict[str, str]:
    """Pile name -> cause, for the node `node_index` of this instance's pile.

    Manual blame comes from the node's faults. Automatic blame (policy `auto_blame_dead_piles`)
    is every other SYNCHRONIZED pile that this instance sees as not connected.
    Real SelfCheck does not evaluate BRIDGE_GROUP parts in other stored states,
    including NOT_SYNCHRONIZED_2 despite that state's data-quorum requirement.
    Manual blame uses the same visibility filter; raw response overrides may bypass it.
    A node never blames its own pile automatically.
    """
    faults = instance.node_faults[node_index]
    out: typing.Dict[str, str] = {}
    for name in sorted(faults.blame_connectivity):
        out[name] = CONNECTIVITY
    for name in sorted(faults.blame_storage):
        out[name] = STORAGE
    state = instance.replica.state
    if instance.policies.auto_blame_dead_piles:
        for p, name in enumerate(instance.names):
            if p == instance.index or name in out:
                continue
            if not instance.connected(p) and state.per_pile[p] == model.Stored.SYNCHRONIZED:
                out[name] = CONNECTIVITY
    return {name: cause for name, cause in out.items()
            if state.per_pile[instance.names.index(name)] == model.Stored.SYNCHRONIZED}


def _issue(node, pile_name: str, pile_index: int, cause: str):
    issue = ydb_monitoring_pb2.IssueLog(
        id="%d-bridge-group-%s-%s" % (node.node_id, pile_name, cause),
        type="BRIDGE_GROUP",
        status=StatusFlag.RED,
        level=ISSUE_LEVEL,
        message=("storage of pile %s is unavailable" if cause == STORAGE
                 else "connectivity with pile %s is lost") % pile_name,
        listed=1,
        count=1,
    )
    issue.location.storage.pool.name = POOL_NAME
    issue.location.storage.pool.group.id.append(str(pile_index + 1))
    issue.location.storage.pool.group.pile.name = pile_name
    return issue


def build(instance, node) -> ydb_monitoring_pb2.SelfCheckResult:
    faults = instance.node_faults[node.index_in_pile]
    blamed = blame(instance, node.index_in_pile)
    style = instance.policies.issue_style
    result = ydb_monitoring_pb2.SelfCheckResult()
    children = []
    for name, cause in blamed.items():
        issue = _issue(node, name, instance.names.index(name), cause)
        result.issue_log.append(issue)
        children.append(issue)
    synced = {instance.names[i] for i, state in enumerate(instance.replica.state.per_pile)
              if state == model.Stored.SYNCHRONIZED}
    overall = StatusFlag.GREEN
    if blamed:
        overall = StatusFlag.RED if synced.issubset(blamed) else StatusFlag.ORANGE
    if style == "realistic" and children:
        result.issue_log.append(_parent_issue(node, children, overall))
    if faults.self_check is not None:
        result.self_check_result = SelfCheck.Result.Value(faults.self_check)
    elif overall == StatusFlag.RED:
        result.self_check_result = SelfCheck.EMERGENCY
    elif overall == StatusFlag.ORANGE:
        result.self_check_result = SelfCheck.MAINTENANCE_REQUIRED
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
    return result


def _parent_issue(node, children, overall):
    """Optional STORAGE_GROUP parent that lists the BRIDGE_GROUP ids in `reason`.

    Real RenameMergingIssues copies pile attribution only for a single reason,
    even when merging is off. The parent retains the proxy group ID, not a part's
    ID. The status is RED only when every SYNCHRONIZED pile's copy is blamed.
    """
    parent = ydb_monitoring_pb2.IssueLog(
        id="%d-storage-group" % node.node_id,
        type="STORAGE_GROUP",
        status=overall,
        level=ISSUE_LEVEL - 1,
        message="Group dead in all piles" if overall == StatusFlag.RED else "Group dead in some piles",
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
