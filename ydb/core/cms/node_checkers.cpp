#include "node_checkers.h"
#include "priority_lock.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NCms {

#define NCH_LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)
#define NCH_LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)

INodesChecker::TLock::TLock(i32 priority)
    : Priority(priority)
{}

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
    if (Nodes.contains(nodeId)) {
        return;
    }
    Nodes[nodeId] = { .State = NODE_STATE_UNSPECIFIED };
}

void TNodesCounterBase::UpdateNode(ui32 nodeId, NKikimrCms::EState state) {
    if (!Nodes.contains(nodeId)) {
        AddNode(nodeId);
    }

    if (Nodes[nodeId].State == NODE_STATE_DOWN) {
        --DownNodesCount;
    }

    if (Nodes[nodeId].State == NODE_STATE_RESTART || Nodes[nodeId].State == NODE_STATE_LOCKED) {
        --LockedNodesCount;
    }

    const auto nodeState = NodeState(state);
    Nodes[nodeId].State = nodeState;

    if (nodeState == NODE_STATE_RESTART || nodeState == NODE_STATE_LOCKED) {
        ++LockedNodesCount;
    }

    if (nodeState == NODE_STATE_DOWN) {
        ++DownNodesCount;
    }
}

bool TNodesCounterBase::IsNodeLocked(ui32 nodeId, i32 priority) const {
    Y_ABORT_UNLESS(Nodes.contains(nodeId));
    const auto& node = Nodes.at(nodeId);
    return HasSameOrHigherPriorityLock(node.Locks, priority);
}

void TNodesCounterBase::LockNode(ui32 nodeId, i32 priority) {
    Y_ABORT_UNLESS(!IsNodeLocked(nodeId, priority));

    auto& node = Nodes[nodeId];

    if (node.Locks.empty()) {
        ++LockedNodesCount;
        if (node.State == NODE_STATE_DOWN) {
            node.State = NODE_STATE_RESTART;
            --DownNodesCount;
        } else {
            node.State = NODE_STATE_LOCKED;
        }
    }

    AddPriorityLock(node.Locks, TLock(priority));
}

void TNodesCounterBase::UnlockNode(ui32 nodeId, i32 priority) {
    auto& node = Nodes[nodeId];

    RemovePriorityLocks(node.Locks, priority);

    if (node.Locks.empty()) {
        --LockedNodesCount;
        if (node.State == NODE_STATE_RESTART) {
            node.State = NODE_STATE_DOWN;
            ++DownNodesCount;
        } else {
            node.State = NODE_STATE_UP;
        }
    }
}

const THashMap<ui32, INodesChecker::TNodeInfo>& TNodesCounterBase::GetNodes() const {
    return Nodes;
}

bool TNodesLimitsCounterBase::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, i32 priority, TReason& reason) const {
    Y_ABORT_UNLESS(Nodes.contains(nodeId));
    auto nodeState = Nodes.at(nodeId).State;

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
            reason = TStringBuilder() << ReasonPrefix(nodeId)
                << ": node state: '" << nodeState << "'";
            return false;
        case NODE_STATE_LOCKED:
        case NODE_STATE_RESTART:
            if (IsNodeLocked(nodeId, priority)) {
                reason = TStringBuilder() << ReasonPrefix(nodeId)
                    << ": node state: '" << nodeState << "'";
                return false;
            } else {
                // Allow to maintain nodes that locked by lower priority locks
                return true;
            }
        case NODE_STATE_DOWN:
            // Allow to maintain down/unavailable node
            return true;
    }

    if (mode == NKikimrCms::MODE_FORCE_RESTART) {
        return true;
    }

    // Always allow at least one node
    if (LockedNodesCount + DownNodesCount == 0) {
        return true;
    }

    const auto disabledNodes = LockedNodesCount + DownNodesCount + 1;

    if (DisabledNodesLimit > 0 && disabledNodes > DisabledNodesLimit) {
        reason = TReason(
            TStringBuilder() << ReasonPrefix(nodeId)
            << ": too many unavailable nodes."
            << " Locked: " << LockedNodesCount
            << ", down: " << DownNodesCount
            << ", limit: " << DisabledNodesLimit,
            DisabledNodesLimitReachedReasonType()
        );
        return false;
    }

    if (DisabledNodesRatioLimit > 0 && (disabledNodes * 100 > Nodes.size() * DisabledNodesRatioLimit)) {
        reason = TReason(
            TStringBuilder() << ReasonPrefix(nodeId)
            << ": too many unavailable nodes."
            << " Locked: " << LockedNodesCount
            << ", down: " << DownNodesCount
            << ", total: " << Nodes.size()
            << ", limit: " << DisabledNodesRatioLimit << "%",
            DisabledNodesLimitReachedReasonType()
        );
        return false;
    }

    return true;
}

bool TSysTabletsNodesCounter::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, i32 priority, TReason& reason) const {
    Y_ABORT_UNLESS(Nodes.contains(nodeId));
    auto nodeState = Nodes.at(nodeId).State;

    NCH_LOG_D("Checking limits for sys tablet: " << NKikimrConfig::TBootstrap_ETabletType_Name(TabletType)
            << ", on node: " << nodeId
            << ", with state: " << nodeState
            << ", locked nodes: " << LockedNodesCount
            << ", down nodes: " << DownNodesCount);

    switch (nodeState) {
        case NODE_STATE_UP:
            break;
        case NODE_STATE_UNSPECIFIED:
            reason = TStringBuilder() << "Cannot lock node '" << nodeId << "'"
                << ": node state: '" << nodeState << "'";
            return false;
        case NODE_STATE_LOCKED:
        case NODE_STATE_RESTART:
            if (IsNodeLocked(nodeId, priority)) {
                reason = TStringBuilder() << "Cannot lock node '" << nodeId << "'"
                    << ": node state: '" << nodeState << "'";
                return false;
            } else {
                // Allow to maintain nodes that locked by lower priority locks
                return true;
            }
        case NODE_STATE_DOWN:
            // Allow to maintain down/unavailable node
            return true;
    }

    const auto tabletNodes = Nodes.size();
    if (tabletNodes < 1) {
        return true;
    }

    const auto disabledNodes = LockedNodesCount + DownNodesCount + 1;
    ui32 limit = 0;

    switch (mode) {
        case NKikimrCms::MODE_FORCE_RESTART:
            return true;
        case NKikimrCms::MODE_MAX_AVAILABILITY:
            limit = Nodes.size() / 2;
            if (disabledNodes * 2 <= tabletNodes) {
                return true;
            }
            break;
        case NKikimrCms::MODE_KEEP_AVAILABLE:
            limit = Nodes.size() - 1;
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
        << " Locked: " << LockedNodesCount
        << ", down: " << DownNodesCount
        << ", limit: " << limit,
        TReason::EType::SysTabletsNodeLimitReached
    );
    return false;
}

} // namespace NKikimr::NCms
