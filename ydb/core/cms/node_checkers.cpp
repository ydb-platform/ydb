#include "node_checkers.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NCms {

#define NCH_LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)
#define NCH_LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)

TNodesLimitsCounterBase::ENodeState INodesChecker::NodeState(NKikimrCms::EState state) {
    switch (state) {
        case NKikimrCms::UP:
            return NODE_STATE_UP;
        case NKikimrCms::UNKNOWN:
            return NODE_STATE_UNSPECIFIED;
        case NKikimrCms::DOWN:
            return NODE_STATE_DOWN;
        case NKikimrCms::RESTART:
            return NODE_STATE_RESTART;
        default:
            Y_ABORT("Unknown EState");
    }
}

void TNodesCounterBase::AddNode(ui32 nodeId) {
    if (NodeToState.contains(nodeId)) {
        return;
    }
    NodeToState[nodeId] = NODE_STATE_UNSPECIFIED;
}

void TNodesCounterBase::UpdateNode(ui32 nodeId, NKikimrCms::EState state) {
    if (!NodeToState.contains(nodeId)) {
        AddNode(nodeId);
    }

    if (NodeToState[nodeId] == NODE_STATE_DOWN) {
        --DownNodesCount;
    }

    if (IsNodeLocked(nodeId)) {
        --LockedNodesCount;
    }

    const auto nodeState = NodeState(state);
    NodeToState[nodeId] = nodeState;

    if (nodeState == NODE_STATE_RESTART || nodeState == NODE_STATE_LOCKED) {
        ++LockedNodesCount;
    }

    if (nodeState == NODE_STATE_DOWN) {
        ++DownNodesCount;
    }
}

bool TNodesCounterBase::IsNodeLocked(ui32 nodeId) const {
    Y_ABORT_UNLESS(NodeToState.contains(nodeId));
    return NodeToState.at(nodeId) == NODE_STATE_RESTART || NodeToState.at(nodeId) == NODE_STATE_LOCKED;
}   

void TNodesCounterBase::LockNode(ui32 nodeId) {
    Y_ABORT_UNLESS(!IsNodeLocked(nodeId));

    ++LockedNodesCount;
    if (NodeToState[nodeId] == NODE_STATE_DOWN) {
        NodeToState[nodeId] = NODE_STATE_RESTART;
        --DownNodesCount;
    } else {
        NodeToState[nodeId] = NODE_STATE_LOCKED;
    }
}

void TNodesCounterBase::UnlockNode(ui32 nodeId) {
    Y_ABORT_UNLESS(IsNodeLocked(nodeId));
   
    --LockedNodesCount;
    if (NodeToState[nodeId] == NODE_STATE_RESTART) {
        NodeToState[nodeId] = NODE_STATE_DOWN;
        ++DownNodesCount;
    } else {
        NodeToState[nodeId] = NODE_STATE_UP;
    }
}

const THashMap<ui32, INodesChecker::ENodeState>& TNodesCounterBase::GetNodeToState() const {
    return NodeToState;
}

bool TNodesLimitsCounterBase::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, TString& reason) const {
    Y_ABORT_UNLESS(NodeToState.contains(nodeId));
    auto nodeState = NodeToState.at(nodeId);

    bool isForceRestart = mode == NKikimrCms::MODE_FORCE_RESTART;

    NCH_LOG_D("Checking Node: "
            << nodeId << ", with state: " << nodeState
            << ", with limit: " << DisabledNodesLimit
            << ", with ratio limit: " << DisabledNodesRatioLimit
            << ", locked nodes: " << LockedNodesCount
            << ", down nodes: " << DownNodesCount);

    switch (nodeState) {
        case NODE_STATE_UP:
            break;
        case NODE_STATE_UNSPECIFIED:
        case NODE_STATE_LOCKED:
        case NODE_STATE_RESTART:
            reason = TStringBuilder() << ReasonPrefix(nodeId)
                << ": node state: '" << nodeState << "'";
            return false;
        case NODE_STATE_DOWN:
            // Allow to maintain down/unavailable node
            return true;
    }

    // Always allow at least one node
    if (LockedNodesCount + DownNodesCount == 0) {
        return true;
    }

    if (isForceRestart && !LockedNodesCount) {
        return true;
    }

    const auto disabledNodes = LockedNodesCount + DownNodesCount + 1;

    if (DisabledNodesLimit > 0 && disabledNodes > DisabledNodesLimit) {
        reason = TStringBuilder() << ReasonPrefix(nodeId)
            << ": too many unavailable nodes."
            << " Locked: " << LockedNodesCount
            << ", down: " << DownNodesCount
            << ", limit: " << DisabledNodesLimit;
        return false;
    }

    if (DisabledNodesRatioLimit > 0 && (disabledNodes * 100 > NodeToState.size() * DisabledNodesRatioLimit)) {
        reason = TStringBuilder() << ReasonPrefix(nodeId)
            << ": too many unavailable nodes."
            << " Locked: " << LockedNodesCount
            << ", down: " << DownNodesCount
            << ", total: " << NodeToState.size()
            << ", limit: " << DisabledNodesRatioLimit << "%";
        return false;
    }

    return true;
}

bool TSysTabletsNodesCounter::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, TString& reason) const {
    Y_ABORT_UNLESS(NodeToState.contains(nodeId));
    auto nodeState = NodeToState.at(nodeId);

    NCH_LOG_D("Checking limits for sys tablet: " << NKikimrConfig::TBootstrap_ETabletType_Name(TabletType)
            << ", on node: " << nodeId
            << ", with state: " << nodeState
            << ", locked nodes: " << LockedNodesCount
            << ", down nodes: " << DownNodesCount);

    switch (nodeState) {
        case NODE_STATE_UP:
            break;
        case NODE_STATE_UNSPECIFIED:
        case NODE_STATE_LOCKED:
        case NODE_STATE_RESTART:
            reason = TStringBuilder() << "Cannot lock node '" << nodeId << "'"
                << ": node state: '" << nodeState << "'";
            return false;
        case NODE_STATE_DOWN:
            // Allow to maintain down/unavailable node
            return true;
    }

    const auto tabletNodes = NodeToState.size();
    if (tabletNodes < 1) {
        return true;
    }

    const auto disabledNodes = LockedNodesCount + DownNodesCount + 1;
    ui32 limit = 0;

    switch (mode) {
        case NKikimrCms::MODE_FORCE_RESTART:
            return true;
        case NKikimrCms::MODE_MAX_AVAILABILITY:
            limit = NodeToState.size() / 2;
            if (disabledNodes * 2 <= tabletNodes) {
                return true;
            }
            break;
        case NKikimrCms::MODE_KEEP_AVAILABLE:
            limit = NodeToState.size() - 1;
            if (disabledNodes < tabletNodes) {
                return true;
            }
            break;
        default:
            Y_ABORT("Unknown availability mode");
    }

    reason = TStringBuilder() << "Cannot lock node '" << nodeId << "'"
        << ": tablet '" << NKikimrConfig::TBootstrap_ETabletType_Name(TabletType) << "'"
        << " has too many unavailable nodes."
        << " Locked: " << LockedNodesCount
        << ", down: " << DownNodesCount
        << ", limit: " << limit;
    return false;
}

} // namespace NKikimr::NCms
