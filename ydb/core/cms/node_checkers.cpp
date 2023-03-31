#include "node_checkers.h"
#include "util/string/cast.h"

#include <ydb/core/protos/cms.pb.h>

namespace NKikimr::NCms {

#define NCH_LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CMS, "[Checker] " << stream)
#define NCH_LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CMS, "[Checker] " << stream)

TNodesStateBase::ENodeState TNodesStateBase::NodeState(NKikimrCms::EState state) {
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
            Y_FAIL("Unknown EState");
    }
}

void TNodesStateBase::AddNode(ui32 nodeId) {
    if (NodeToState.contains(nodeId)) {
        return;
    }
    NodeToState[nodeId] = NODE_STATE_UNSPECIFIED;
}

void TNodesStateBase::UpdateNode(ui32 nodeId, NKikimrCms::EState state) {
    if (!NodeToState.contains(nodeId)) {
        AddNode(nodeId);
    }

    if (NodeToState[nodeId] == NODE_STATE_DOWN) {
        --DownNodesCount;
    }

    if (NodeToState[nodeId] == NODE_STATE_LOCKED ||
        NodeToState[nodeId] == NODE_STATE_RESTART) {
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

void TNodesStateBase::LockNode(ui32 nodeId) {
    Y_VERIFY(NodeToState.contains(nodeId));

    ++LockedNodesCount;
    if (NodeToState[nodeId] == NODE_STATE_DOWN) {
        NodeToState[nodeId] = NODE_STATE_RESTART;
        --DownNodesCount;
    } else {
        NodeToState[nodeId] = NODE_STATE_LOCKED;
    }
}

void TNodesStateBase::UnlockNode(ui32 nodeId) {
    Y_VERIFY(NodeToState.contains(nodeId));

    --LockedNodesCount;
    if (NodeToState[nodeId] == NODE_STATE_RESTART) {
        NodeToState[nodeId] = NODE_STATE_DOWN;
        ++DownNodesCount;
    } else {
        NodeToState[nodeId] = NODE_STATE_UP;
    }
}

bool TNodesStateBase::TryToLockNode(ui32 nodeId, bool isForceRestart) {
    Y_VERIFY(NodeToState.contains(nodeId));

    auto nodeState = NodeToState[nodeId];

    NCH_LOG_D("Checking Node: "
            << nodeId << ", with state: " << ToString(nodeState) 
            << ", with limit: " << DisabledNodesLimit
            << ", with ratio limit: " << DisabledNodesRatioLimit
            << ", locked nodes: " << LockedNodesCount
            << ", down nodes: " << DownNodesCount);

    // Allow to maintain down/unavailable node
    if (nodeState == NODE_STATE_DOWN) {
        return true;
    }

    if (nodeState == NODE_STATE_RESTART ||
        nodeState == NODE_STATE_LOCKED ||
        nodeState == NODE_STATE_UNSPECIFIED) {

        return false;
    }

    // Always allow at least one node
    if (LockedNodesCount + DownNodesCount == 0) {
        return true;
    }

    if (isForceRestart && !LockedNodesCount) {
        return true;
    }

    if (DisabledNodesLimit > 0 &&
        (LockedNodesCount + DownNodesCount + 1 > DisabledNodesLimit)) {
        return false;
    }

    if (DisabledNodesRatioLimit > 0 &&
        ((LockedNodesCount + DownNodesCount + 1) * 100 >
         (NodeToState.size() * DisabledNodesRatioLimit))) {
        return false;
    }

    return true;
}


} // namespace NKikimr::NCms
