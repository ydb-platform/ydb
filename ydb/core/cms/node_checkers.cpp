#include "node_checkers.h"
#include "priority_lock.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NCms {

#define NCH_LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)
#define NCH_LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)

TNodeLockContext::TNodeLockContext(i32 priority, const TString& requestId, NKikimrCms::EAvailabilityMode mode)
    : Priority(priority)
    , RequestId(requestId)
    , Mode(mode)
{}

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

void TNodesCounterBase::LockNode(ui32 nodeId, const TNodeLockContext& ctx) {
    Y_ABORT_UNLESS(!IsNodeLocked(nodeId, ctx.Priority));

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

    AddPriorityLock(node.Locks, TLock(ctx.Priority));
}

void TNodesCounterBase::UnlockNode(ui32 nodeId, const TNodeLockContext& ctx) {
    auto& node = Nodes[nodeId];

    RemovePriorityLocks(node.Locks, ctx.Priority);

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

bool TNodesLimitsCounterBase::TryToLockNode(ui32 nodeId, const TNodeLockContext& ctx, TReason& reason) const {
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
            if (IsNodeLocked(nodeId, ctx.Priority)) {
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

    if (ctx.Mode == NKikimrCms::MODE_FORCE_RESTART) {
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

void TSysTabletsNodesCounter::LockNode(ui32 nodeId, const TNodeLockContext& ctx) {
    TNodesCounterBase::LockNode(nodeId, ctx);

    Y_DEBUG_ABORT_UNLESS(!ctx.RequestId.empty());
    ++LockedByRequests[ctx.RequestId];
}

void TSysTabletsNodesCounter::UnlockNode(ui32 nodeId, const TNodeLockContext& ctx) {
    TNodesCounterBase::UnlockNode(nodeId, ctx);

    Y_DEBUG_ABORT_UNLESS(!ctx.RequestId.empty());
    auto it = LockedByRequests.find(ctx.RequestId);
    Y_ABORT_UNLESS(it != LockedByRequests.end());
    if (--it->second == 0) {
        LockedByRequests.erase(it);
    }
}

bool TSysTabletsNodesCounter::TryToLockNode(ui32 nodeId, const TNodeLockContext& ctx, TReason& reason) const {
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
            if (IsNodeLocked(nodeId, ctx.Priority)) {
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

    const ui32 maxAvailabilityLimit = Nodes.size() / 2;
    const ui32 keepAvailableLimit = Nodes.size() - 1;

    const bool maxAvailabilityOk = disabledNodes * 2 <= tabletNodes;
    const bool keepAvailableOk = disabledNodes < tabletNodes;

    switch (ctx.Mode) {
        case NKikimrCms::MODE_FORCE_RESTART:
            return true;
        case NKikimrCms::MODE_MAX_AVAILABILITY:
            limit = maxAvailabilityLimit;
            if (maxAvailabilityOk) {
                return true;
            }
            break;
        case NKikimrCms::MODE_KEEP_AVAILABLE:
            limit = keepAvailableLimit;
            if (keepAvailableOk) {
                return true;
            }
            break;
        case NKikimrCms::MODE_SMART_AVAILABILITY:
            limit = maxAvailabilityLimit;
            if (maxAvailabilityOk) {
                return true;
            }

            if (!LockedByRequests.contains(ctx.RequestId)) {
                limit = keepAvailableLimit;
                if (keepAvailableOk) {
                    return true;
                }
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
