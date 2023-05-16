#include "node_checkers.h"
#include "util/system/yassert.h"
#include "ydb/public/api/protos/draft/ydb_maintenance.pb.h"

#include <ydb/core/protos/cms.pb.h>

#include <util/string/cast.h>

namespace NKikimr::NCms {

#define NCH_LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)
#define NCH_LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::CMS, "[Nodes Counter] " << stream)

using namespace Ydb::Maintenance;

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
            Y_FAIL("Unknown EState");
    }
}

void TNodesCounterBase::AddNode(ui32 nodeId) {
    if (NodeToState.contains(nodeId)) {
        return;
    }
    NodeToState[nodeId].State = NODE_STATE_UNSPECIFIED;
}

void TNodesCounterBase::UpdateNode(ui32 nodeId, NKikimrCms::EState state) {
    if (!NodeToState.contains(nodeId)) {
        AddNode(nodeId);
    }

    if (NodeToState[nodeId].State == NODE_STATE_DOWN) {
        --DownNodesCount;
    }

    if (NodeToState[nodeId].State == NODE_STATE_LOCKED ||
        NodeToState[nodeId].State == NODE_STATE_RESTART) {
        --LockedNodesCount;
    }

    const auto nodeState = NodeState(state);
    NodeToState[nodeId].State = nodeState;

    if (nodeState == NODE_STATE_RESTART || nodeState == NODE_STATE_LOCKED) {
        ++LockedNodesCount;
    }

    if (nodeState == NODE_STATE_DOWN) {
        ++DownNodesCount;
    }
}

void TNodesCounterBase::LockNode(ui32 nodeId) {
    Y_VERIFY(NodeToState.contains(nodeId));

    ++LockedNodesCount;
    if (NodeToState[nodeId].State == NODE_STATE_DOWN) {
        NodeToState[nodeId].State = NODE_STATE_RESTART;
        --DownNodesCount;
    } else {
        NodeToState[nodeId].State = NODE_STATE_LOCKED;
    }
}

void TNodesCounterBase::UnlockNode(ui32 nodeId) {
    Y_VERIFY(NodeToState.contains(nodeId));

    --LockedNodesCount;
    if (NodeToState[nodeId].State == NODE_STATE_RESTART) {
        NodeToState[nodeId].State = NODE_STATE_DOWN;
        ++DownNodesCount;
    } else {
        NodeToState[nodeId].State = NODE_STATE_UP;
    }
}

void TNodesCounterBase::EmplaceTask(const ui32 nodeId, i32 priority, ui64 order, const std::string& taskUId) {
    auto& priorities = NodeToState[nodeId].Priorities;
    auto it = priorities.lower_bound(TNodeState::TTaskPriority(priority, order, ""));

    if (it != priorities.end() && (it->Order == order && it->Priority == priority)) {
        if (it->TaskUId == taskUId) {
            return;
        }
        Y_FAIL("Task with the same priority and order already exists");
    } else {
        priorities.emplace_hint(it, priority, order, taskUId);
    }

    NodesWithScheduledTasks.insert(nodeId);
}

void TNodesCounterBase::RemoveTask(const std::string& taskUId) {
    auto taskUIdsEqual = [&taskUId](const TNodeState::TTaskPriority &p) {
        return p.TaskUId == taskUId;
    };

    TVector<ui32> NodesToRemove;
    for (auto nodeId : NodesWithScheduledTasks) {
        auto& nodeState = NodeToState[nodeId];
        auto it = std::find_if(nodeState.Priorities.begin(),
                               nodeState.Priorities.end(), taskUIdsEqual);
        if (it == nodeState.Priorities.end()) {
            continue;
        }

        nodeState.Priorities.erase(it);

        if (nodeState.Priorities.empty()) {
            NodesToRemove.push_back(nodeId);
        }
    }

    for (auto nodeId : NodesToRemove) {    
        NodesWithScheduledTasks.erase(nodeId);
    }
}

ActionState::ActionReason TNodesLimitsCounterBase::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, i32 priority, ui64 order) const {
    Y_VERIFY(NodeToState.contains(nodeId));

    const auto& nodeState = NodeToState.at(nodeId);
    const auto taskPriority = TNodeState::TTaskPriority(priority, order, "");

    if (!nodeState.Priorities.empty() && (taskPriority < *nodeState.Priorities.rbegin())) {
        return ActionState::ACTION_REASON_LOW_PRIORITY;
    }

    if (nodeState.State == NODE_STATE_RESTART ||
        nodeState.State == NODE_STATE_LOCKED ||
        nodeState.State == NODE_STATE_UNSPECIFIED) {

        return ActionState::ACTION_REASON_ALREADY_LOCKED;
    }

    ui32 priorityLockedCount = 0;
    for (auto id : NodesWithScheduledTasks) {
        Y_VERIFY(!NodeToState.at(id).Priorities.empty());

        if (taskPriority < *NodeToState.at(id).Priorities.rbegin()) {
            ++priorityLockedCount;
        }
    }

    ui32 downNodes = nodeState.State == NODE_STATE_DOWN ? DownNodesCount - 1 : DownNodesCount;
    // Always allow at least one node
    if (LockedNodesCount + downNodes + priorityLockedCount == 0) {
        return ActionState::ACTION_REASON_OK;
    }

    bool isForceRestart = mode == NKikimrCms::MODE_FORCE_RESTART;

    if (isForceRestart && !LockedNodesCount) {
        return ActionState::ACTION_REASON_OK;
    }

    if (DisabledNodesLimit > 0 &&
        (LockedNodesCount + downNodes + priorityLockedCount + 1 > DisabledNodesLimit)) {
        return ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED;
    }

    if (DisabledNodesRatioLimit > 0 &&
        ((LockedNodesCount + downNodes + priorityLockedCount + 1) * 100 > (NodeToState.size() * DisabledNodesRatioLimit))) {
        return ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED;
    }

    return ActionState::ACTION_REASON_OK;
}

ActionState::ActionReason TSysTabletsNodesCounter::TryToLockNode(ui32 nodeId, NKikimrCms::EAvailabilityMode mode, i32 priority, ui64 order) const  {
    Y_VERIFY(NodeToState.contains(nodeId));

    const auto& nodeState = NodeToState.at(nodeId);
    const auto taskPriority = TNodeState::TTaskPriority(priority, order, "");

    if (!nodeState.Priorities.empty() && (taskPriority < *nodeState.Priorities.rbegin())) {
        return ActionState::ACTION_REASON_LOW_PRIORITY;
    }

    if (nodeState.State == NODE_STATE_RESTART ||
        nodeState.State == NODE_STATE_LOCKED ||
        nodeState.State == NODE_STATE_UNSPECIFIED) {

        return ActionState::ACTION_REASON_ALREADY_LOCKED;
    }

    ui32 priorityLockedCount = 0;
    for (auto id : NodesWithScheduledTasks) {
        Y_VERIFY(!NodeToState.at(id).Priorities.empty());

        if (taskPriority < *NodeToState.at(id).Priorities.rbegin()) {
            ++priorityLockedCount;
        }
    }

    ui32 downNodes = nodeState.State == NODE_STATE_DOWN ? DownNodesCount - 1 : DownNodesCount;
    ui32 tabletNodes = NodeToState.size();
    switch (mode) {
        case NKikimrCms::MODE_MAX_AVAILABILITY:
            if (tabletNodes > 1 && (downNodes + LockedNodesCount + priorityLockedCount + 1) * 2 > tabletNodes){
                return ActionState::ACTION_REASON_SYS_TABLETS_NODE_LIMIT_REACHED;
            }
            break;
        case NKikimrCms::MODE_KEEP_AVAILABLE:
            if (tabletNodes > 1 && (downNodes + LockedNodesCount + priorityLockedCount + 1) > tabletNodes - 1) {
                return ActionState::ACTION_REASON_SYS_TABLETS_NODE_LIMIT_REACHED;
            }
            break;
        case NKikimrCms::MODE_FORCE_RESTART:
            break;
        default:
            Y_FAIL("Unknown availability mode");
    }

    return ActionState::ACTION_REASON_OK;
}

std::string TTenantLimitsCounter::ReadableReason(ui32 nodeId, NKikimrCms::EAvailabilityMode mode,
                                                 ActionState::ActionReason reason) const {
    Y_UNUSED(mode);

    std::stringstream readableReason;

    if (reason == ActionState::ACTION_REASON_OK) {
        readableReason << "Action is OK";
        return readableReason.str();
    }

    readableReason << "Cannot lock node: " << nodeId;

    switch (reason) {
        case ActionState::ACTION_REASON_ALREADY_LOCKED:
            readableReason << "Node is already locked";
            break;
        case ActionState::ACTION_REASON_LOW_PRIORITY:
            readableReason << "Task with higher priority in progress: " << (*NodeToState.at(nodeId).Priorities.rbegin()).TaskUId;
            break;
        case ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED:
            readableReason << ". Too many locked nodes for tenant " << TenantName
                   << "; locked: " << LockedNodesCount
                   << "; down: " << DownNodesCount
                   << "; total: " << NodeToState.size()
                   << "; limit: " << DisabledNodesLimit
                   << "; ratio limit: " << DisabledNodesRatioLimit << "%";
            break;
        default:
            Y_FAIL("Unexpected reason");
            break;
    }
    return readableReason.str();
}

std::string TClusterLimitsCounter::ReadableReason(ui32 nodeId, NKikimrCms::EAvailabilityMode mode,
                                                  ActionState::ActionReason reason) const {
    Y_UNUSED(mode);

    std::stringstream readableReason;

    if (reason == ActionState::ACTION_REASON_OK) {
        readableReason << "Action is OK";
        return readableReason.str();
    }

    if (mode == NKikimrCms::MODE_FORCE_RESTART) {
            return readableReason.str();
    }

    readableReason << "Cannot lock node: " << nodeId;

    switch (reason) {
        case ActionState::ACTION_REASON_ALREADY_LOCKED:
            readableReason << "Node is already locked";
            break;
        case ActionState::ACTION_REASON_LOW_PRIORITY:
            readableReason << "Task with higher priority in progress: " << (*NodeToState.at(nodeId).Priorities.rbegin()).TaskUId;
            break;
        case ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED:
            readableReason << ". Too many locked nodes in cluster"
                   << "; locked: " << LockedNodesCount
                   << "; down: " << DownNodesCount
                   << "; total: " << NodeToState.size()
                   << "; limit: " << DisabledNodesLimit
                   << "; ratio limit: " << DisabledNodesRatioLimit << "%";
            break;
        default:
            Y_FAIL("Unexpected reason");
            break;
    }

    return readableReason.str();
}


std::string TSysTabletsNodesCounter::ReadableReason(ui32 nodeId, NKikimrCms::EAvailabilityMode mode,
                                                    ActionState::ActionReason reason) const {
    std::stringstream readableReason;

    if (reason == ActionState::ACTION_REASON_OK) {
        readableReason << "Action is OK";
        return readableReason.str();
    }

    if (mode == NKikimrCms::MODE_FORCE_RESTART) {
            return readableReason.str();
    }

    switch (reason) {
        case ActionState::ACTION_REASON_ALREADY_LOCKED:
            readableReason << "Node is already locked";
            break;
        case ActionState::ACTION_REASON_LOW_PRIORITY:
            readableReason << "Task with higher priority in progress: " << (*NodeToState.at(nodeId).Priorities.rbegin()).TaskUId;
            break;
        case ActionState::ACTION_REASON_SYS_TABLETS_NODE_LIMIT_REACHED:
            readableReason << "Cannot lock node: " << nodeId << ". Tablet "
                   << NKikimrConfig::TBootstrap_ETabletType_Name(TabletType)
                   << " has too many unavailable nodes. Locked: "
                   << LockedNodesCount << ". Down: " << DownNodesCount;

            if (mode == NKikimrCms::MODE_MAX_AVAILABILITY) {
                readableReason << ". Limit: " << NodeToState.size() / 2 << " (50%)";
            }

            if (mode == NKikimrCms::MODE_KEEP_AVAILABLE) {
                readableReason << ". Limit: " << NodeToState.size() - 1;
            }
            break;
        default:
            Y_FAIL("Unexpected reason");

    }

    return readableReason.str();
}
} // namespace NKikimr::NCms
