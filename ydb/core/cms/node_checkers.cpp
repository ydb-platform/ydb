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
    const ui32 pileId = (*NodeIdToPileId)[nodeId];
    if (!NodeToState.contains(nodeId)) {
        AddNode(nodeId);
    }

    if (NodeToState[nodeId] == NODE_STATE_DOWN) {
        --DownNodesCount[pileId];
    }

    if (IsNodeLocked(nodeId)) {
        --LockedNodesCount[pileId];
    }

    const auto nodeState = NodeState(state);
    NodeToState[nodeId] = nodeState;

    if (nodeState == NODE_STATE_RESTART || nodeState == NODE_STATE_LOCKED) {
        ++LockedNodesCount[pileId];
    }

    if (nodeState == NODE_STATE_DOWN) {
        ++DownNodesCount[pileId];
    }
}

bool TNodesCounterBase::IsNodeLocked(ui32 nodeId) const {
    Y_ABORT_UNLESS(NodeToState.contains(nodeId));
    return NodeToState.at(nodeId) == NODE_STATE_RESTART || NodeToState.at(nodeId) == NODE_STATE_LOCKED;
}   

void TNodesCounterBase::LockNode(ui32 nodeId) {
    Y_ABORT_UNLESS(!IsNodeLocked(nodeId));
    const ui32 pileId = (*NodeIdToPileId)[nodeId];
    ENodeState& state = NodeToState[nodeId];

    ++LockedNodesCount[pileId];
    if (state == NODE_STATE_DOWN) {
        state = NODE_STATE_RESTART;
        --DownNodesCount[pileId];
    } else {
        state = NODE_STATE_LOCKED;
    }
}

void TNodesCounterBase::UnlockNode(ui32 nodeId) {
    Y_ABORT_UNLESS(IsNodeLocked(nodeId));
    const ui32 pileId = (*NodeIdToPileId)[nodeId];
    ENodeState& state = NodeToState[nodeId];
   
    --LockedNodesCount[pileId];
    if (state == NODE_STATE_RESTART) {
        state = NODE_STATE_DOWN;
        ++DownNodesCount[pileId];
    } else {
        state = NODE_STATE_UP;
    }
}

const THashMap<ui32, INodesChecker::ENodeState>& TNodesCounterBase::GetNodeToState() const {
    return NodeToState;
}

bool TNodesLimitsCounterBase::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, TReason& reason) const {
    Y_ABORT_UNLESS(NodeToState.contains(nodeId));
    const ui32 pileId = (*NodeIdToPileId)[nodeId];

    auto it = LockedNodesCount.find(pileId);
    const ui32 lockedNodesCount = (it != LockedNodesCount.end()) ? it->second : 0;

    it = DownNodesCount.find(pileId);
    const ui32 downNodesCount = (it != DownNodesCount.end()) ? it->second : 0;
    
    auto nodeState = NodeToState.at(nodeId);

    NCH_LOG_D("Checking Node: "
            << nodeId << ", with state: " << nodeState
            << ", with limit: " << DisabledNodesLimit
            << ", with ratio limit: " << DisabledNodesRatioLimit
            << ", locked nodes: " << lockedNodesCount
            << ", down nodes: " << downNodesCount);

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

    if (mode == NKikimrCms::MODE_FORCE_RESTART) {
        return true;
    }

    // Always allow at least one node
    if (lockedNodesCount + downNodesCount == 0) {
        return true;
    }

    const auto disabledNodes = lockedNodesCount + downNodesCount + 1;

    if (DisabledNodesLimit > 0 && disabledNodes > DisabledNodesLimit) {
        reason = TReason(
            TStringBuilder() << ReasonPrefix(nodeId)
            << ": too many unavailable nodes."
            << " Locked: " << lockedNodesCount
            << ", down: " << downNodesCount
            << ", limit: " << DisabledNodesLimit,
            DisabledNodesLimitReachedReasonType()
        );
        return false;
    }

    if (DisabledNodesRatioLimit > 0 && (disabledNodes * 100 > NodeToState.size() * DisabledNodesRatioLimit)) {
        reason = TReason(
            TStringBuilder() << ReasonPrefix(nodeId)
            << ": too many unavailable nodes."
            << " Locked: " << lockedNodesCount
            << ", down: " << downNodesCount
            << ", total: " << NodeToState.size()
            << ", limit: " << DisabledNodesRatioLimit << "%",
            DisabledNodesLimitReachedReasonType()
        );
        return false;
    }

    return true;
}

bool TSysTabletsNodesCounter::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, TReason& reason) const {
    Y_ABORT_UNLESS(NodeToState.contains(nodeId));
    const ui32 pileId = (*NodeIdToPileId)[nodeId];

    auto it = LockedNodesCount.find(pileId);
    const ui32 lockedNodesCount = (it != LockedNodesCount.end()) ? it->second : 0;

    it = DownNodesCount.find(pileId);
    const ui32 downNodesCount = (it != DownNodesCount.end()) ? it->second : 0;

    auto nodeState = NodeToState.at(nodeId);

    NCH_LOG_D("Checking limits for sys tablet: " << NKikimrConfig::TBootstrap_ETabletType_Name(TabletType)
            << ", on node: " << nodeId
            << ", with state: " << nodeState
            << ", locked nodes: " << lockedNodesCount
            << ", down nodes: " << downNodesCount);

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

    const auto disabledNodes = lockedNodesCount + downNodesCount + 1;
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

    reason = TReason(
        TStringBuilder() << "Cannot lock node '" << nodeId << "'"
        << ": tablet '" << NKikimrConfig::TBootstrap_ETabletType_Name(TabletType) << "'"
        << " has too many unavailable nodes."
        << " Locked: " << lockedNodesCount
        << ", down: " << downNodesCount
        << ", limit: " << limit,
        TReason::EType::SysTabletsNodeLimitReached
    );
    return false;
}

} // namespace NKikimr::NCms
