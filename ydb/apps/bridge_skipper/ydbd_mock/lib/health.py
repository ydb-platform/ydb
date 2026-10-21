"""SelfCheck result of one node: BRIDGE_GROUP issues for blamed piles, shaped for the demo's filter.

The demo keeps an issue when `"GROUP" in type`, `status == "RED"` and
`location.storage.pool.group.pile.name` is set.
"""
import typing

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.public.api.protos import ydb_monitoring_pb2

StatusFlag = ydb_monitoring_pb2.StatusFlag
SelfCheck = ydb_monitoring_pb2.SelfCheck

STORAGE = "storage"
CONNECTIVITY = "connectivity"
POOL_NAME = "/Root:mock"
ISSUE_LEVEL = 5


def blame(instance, node_index: int) -> typing.Dict[str, str]:
    """Pile name -> cause, for the node `node_index` of this instance's pile.

    Manual blame comes from the node's faults. Automatic blame (policy `auto_blame_dead_piles`)
    is every other pile that this instance sees as not connected and whose stored state
    requires data quorum. A pile that is already DISCONNECTED is not blamed, and a node
    never blames its own pile automatically.
    """
    faults = instance.node_faults[node_index]
    out: typing.Dict[str, str] = {}
    for name in sorted(faults.blame_connectivity):
        out[name] = CONNECTIVITY
    for name in sorted(faults.blame_storage):
        out[name] = STORAGE
    if instance.policies.auto_blame_dead_piles:
        state = instance.replica.state
        for p, name in enumerate(instance.names):
            if p == instance.index or name in out:
                continue
            if not instance.connected(p) and model.requires_data_quorum(state.per_pile[p]):
                out[name] = CONNECTIVITY
    return out


def _issue(node, pile_name: str, pile_index: int, cause: str, style: str):
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
        issue = _issue(node, name, instance.names.index(name), cause, style)
        result.issue_log.append(issue)
        children.append(issue)
    if style == "realistic" and children:
        result.issue_log.append(_parent_issue(instance, node, children))
    if faults.self_check is not None:
        result.self_check_result = SelfCheck.Result.Value(faults.self_check)
    elif blamed:
        result.self_check_result = SelfCheck.EMERGENCY
    else:
        result.self_check_result = SelfCheck.GOOD
    result.location.id = node.node_id
    result.location.host = node.label
    result.location.port = node.grpc_port
    result.location.pile.name = node.pile_name
    return result


def _parent_issue(instance, node, children):
    """Optional STORAGE_GROUP parent that lists the BRIDGE_GROUP ids in `reason`.

    Real RenameMergingIssues copies the child's pile name into the parent even when
    merging is off. The status is RED only when every SYNCHRONIZED pile's copy is blamed.
    """
    state = instance.replica.state
    synced = [instance.names[i] for i in range(len(instance.names)) if state.per_pile[i] == model.Stored.SYNCHRONIZED]
    blamed_names = {c.location.storage.pool.group.pile.name for c in children}
    all_dead = bool(synced) and all(name in blamed_names for name in synced)
    parent = ydb_monitoring_pb2.IssueLog(
        id="%d-storage-group" % node.node_id,
        type="STORAGE_GROUP",
        status=StatusFlag.RED if all_dead else StatusFlag.ORANGE,
        level=ISSUE_LEVEL - 1,
        message="Group dead in all piles" if all_dead else "Group dead in some piles",
        listed=len(children),
        count=len(children),
    )
    for child in children:
        parent.reason.append(child.id)
    first = children[0]
    parent.location.storage.pool.name = POOL_NAME
    parent.location.storage.pool.group.id.extend(first.location.storage.pool.group.id)
    parent.location.storage.pool.group.pile.name = first.location.storage.pool.group.pile.name
    return parent
