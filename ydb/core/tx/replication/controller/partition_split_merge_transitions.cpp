#include "partition_split_merge_transitions.h"

#include <util/generic/algorithm.h>

namespace NKikimr::NReplication::NController {

void TPartitionSplitMergeTransitions::StartTransition(const TWorkerId& parent, const TVector<TWorkerId>& children) {
    if (Statuses.emplace(parent, EStatus::WaitingForChildren).second) {
        WaitingParents.emplace(parent, children);
    }
}

const THashMap<TWorkerId, TVector<TWorkerId>>& TPartitionSplitMergeTransitions::GetWaitingParents() const {
    return WaitingParents;
}

void TPartitionSplitMergeTransitions::RequestParentRemoval(const TVector<TWorkerId>& parents) {
    for (const auto& parent : parents) {
        Statuses[parent] = EStatus::ParentRemovalRequested;
        WaitingParents.erase(parent);
    }
}

void TPartitionSplitMergeTransitions::CompleteTransition(const TWorkerId& parent) {
    Statuses[parent] = EStatus::Completed;
    WaitingParents.erase(parent);
}

bool TPartitionSplitMergeTransitions::IsParentRemovalRequested(const TWorkerId& parent) const {
    return HasStatus(parent, EStatus::ParentRemovalRequested);
}

bool TPartitionSplitMergeTransitions::IsTransitionCompleted(const TWorkerId& parent) const {
    return HasStatus(parent, EStatus::Completed);
}

TVector<TWorkerId> TPartitionSplitMergeTransitions::ClearTarget(ui64 replicationId, ui64 targetId) {
    TVector<TWorkerId> removedParents;
    EraseNodesIf(Statuses, [&](const auto& item) {
        const auto& [parent, status] = item;
        if (parent.ReplicationId() != replicationId || parent.TargetId() != targetId) {
            return false;
        }
        if (status != EStatus::WaitingForChildren) {
            removedParents.push_back(parent);
        }
        WaitingParents.erase(parent);
        return true;
    });
    return removedParents;
}

void TPartitionSplitMergeTransitions::ForgetTransitions(const TVector<TWorkerId>& parents) {
    for (const auto& parent : parents) {
        Statuses.erase(parent);
        WaitingParents.erase(parent);
    }
}

void TPartitionSplitMergeTransitions::Clear() {
    Statuses.clear();
    WaitingParents.clear();
}

bool TPartitionSplitMergeTransitions::HasStatus(const TWorkerId& parent, EStatus status) const {
    const auto it = Statuses.find(parent);
    return it != Statuses.end() && it->second == status;
}

}
