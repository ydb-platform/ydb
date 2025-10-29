#include "blob_checker_planner.h"
#include "impl.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/util/fast_lookup_unique_list.h>
 
namespace NKikimr {
namespace NBsController {

/////////////////////////////////////////////////////////////////////////////////////
/// TBlobCheckerPlanner implementation

class TBlobCheckerPlanner::TImpl {
public:
    TImpl(TDuration periodicity, ui32 groupCount)
        : Periodicity(periodicity)
        , GroupCount(groupCount)
    {}

    void EnqueueCheck(const TGroupId groupId, std::vector<ui32> nodes) {
        if (IsCheckEnqueued(groupId)) {
            // This group is already planned to scan, no need to re-enqueue it
            return;
        }

        ui32 lockedNodeCount = nodes.size();
        TLockInfo& lock = LocksByGroup[groupId];

        for (const ui32 nodeId : nodes) {
            TFastLookupUniqueList<TGroupId>& lockQueue = NodeLockQueues[nodeId];

            if (lockQueue.Contains(groupId)) {
                // multiple VDisks of this group on the same node, lock already acquired
                --lockedNodeCount;
            } else {
                // if we are the only group to lock this node, we are not locked
                lockedNodeCount -= lockQueue.IsEmpty();
                lockQueue.PushBack(groupId);
                lock.UsedNodes.insert(nodeId);
            }
        }

        if (lockedNodeCount == 0) {
            GroupsAllowedToScan.PushBack(groupId);
        }

        lock.LockedNodeCount = lockedNodeCount;
    }

    void EnqueueCheck(const TBlobStorageGroupInfo* groupInfo) {
        if (!groupInfo) {
            Y_DEBUG_ABORT();
            return;
        }

        std::vector<ui32> nodes;
        nodes.reserve(groupInfo->Type.BlobSubgroupSize());
        std::transform(
            groupInfo->VDisksBegin(),
            groupInfo->VDisksEnd(),
            std::back_inserter(nodes),
            [&](const auto& it) {
                return groupInfo->GetActorId(it.OrderNumber).NodeId();
            }
        );

        EnqueueCheck(groupInfo->GroupID, std::move(nodes));
    }

    void EnqueueCheck(const TBlobStorageController::TGroupInfo* groupInfo) {
        if (!groupInfo) {
            Y_DEBUG_ABORT();
            return;
        }

        std::vector<ui32> nodes;
        nodes.reserve(groupInfo->VDisksInGroup.size());
        std::transform(
            groupInfo->VDisksInGroup.begin(),
            groupInfo->VDisksInGroup.end(),
            std::back_inserter(nodes),
            [&](const auto& it) {
                return it->VSlotId.NodeId;
            }
        );

        EnqueueCheck(groupInfo->ID, std::move(nodes));
    }

    bool DequeueCheck(const TGroupId groupId) {
        const auto it = LocksByGroup.find(groupId);
        if (it == LocksByGroup.end()) {
            // no info about locks, so scan for this group is not enqueued
            return false;
        }

        GroupsAllowedToScan.Erase(groupId);

        for (ui32 lockedNodeId : it->second.UsedNodes) {
            TFastLookupUniqueList<TGroupId>& lockQueue = NodeLockQueues[lockedNodeId];
            Y_ABORT_UNLESS(!lockQueue.IsEmpty());
            const TGroupId lockingGroupId = lockQueue.Front();
            lockQueue.Erase(groupId);
            if (lockingGroupId == groupId) {
                // Dequeued group was the one locking node, unlock next group if possible
                if (lockQueue.IsEmpty()) {
                    // nothing to unlock
                    continue;
                } else {
                    const TGroupId nextGroupId = lockQueue.Front();
                    TLockInfo& nextLocks = LocksByGroup[nextGroupId];
                    if (--nextLocks.LockedNodeCount == 0) {
                        GroupsAllowedToScan.PushBack(nextGroupId);
                    }
                }
            }
        }

        LocksByGroup.erase(it);
        return true;
    }

    void ResetState() {
        LocksByGroup.clear();
        GroupsAllowedToScan.Clear();
        NodeLockQueues.clear();

        TimeWasted = TDuration::Zero();
        LastPlannedTimestamp = TMonotonic::Zero();
    }

    void SetGroupCount(ui32 groupCount) {
        GroupCount = groupCount;
    }

    void SetPeriodicity(TDuration newPeriodicity) {
        Periodicity = newPeriodicity;
        TimeWasted = std::max(TimeWasted, Periodicity);
    }

    TMonotonic GetNextAllowedCheckTimestamp(TMonotonic now) {
        auto [delay, acceleratedTime] = GetAdjustedDelay();
        if (LastPlannedTimestamp + delay < now) {
            // last planned request was too long ago, allow next scan right now
            // and accelerate further requests to compensate wasted time
            TDuration wasted = now - (LastPlannedTimestamp + delay);
            if (LastPlannedTimestamp != TMonotonic::Zero()) {
                TimeWasted = std::max(Periodicity, TimeWasted + wasted);
            }
            LastPlannedTimestamp = now;
        } else {
            LastPlannedTimestamp += delay;
            if (TimeWasted > acceleratedTime) {
                TimeWasted -= acceleratedTime;
            } else {
                TimeWasted = TDuration::Zero();
            }
        }
        return LastPlannedTimestamp;
    }

    std::optional<TGroupId> ObtainNextGroupToCheck() {
        if (GroupsAllowedToScan.IsEmpty()) {
            return std::nullopt;
        }
        return GroupsAllowedToScan.ExtractFront();
    }

public:
    std::pair<TDuration, TDuration> GetAdjustedDelay() {
        if (Periodicity == TDuration::Zero()) {
            return { TDuration::Zero(), TDuration::Zero() };
        }

        TDuration targetDelay = GetTargetDelay();
        float accelerationRatio = (TimeWasted / Periodicity) + 1;
        TDuration adjustedDelay = targetDelay / accelerationRatio;
        return std::pair<TDuration, TDuration>(adjustedDelay, targetDelay - adjustedDelay);
    }

    TDuration GetTargetDelay() {
        return Periodicity / std::max(static_cast<ui32>(1), GroupCount);
    }

    bool IsCheckEnqueued(TGroupId groupId) const {
        return GroupsAllowedToScan.Contains(groupId) || LocksByGroup.contains(groupId);
    }

private:
    struct TLockInfo {
        ui32 LockedNodeCount;
        std::unordered_set<ui32> UsedNodes;

        TLockInfo() {
            UsedNodes.reserve(MaxExpectedNodesInGroup);
        }
    };

private:
    constexpr static ui32 MaxExpectedNodesInGroup = 9;

    TDuration Periodicity;
    ui32 GroupCount;

    TMonotonic LastPlannedTimestamp = TMonotonic::Zero();
    TDuration TimeWasted = TDuration::Zero();

    TFastLookupUniqueList<TGroupId> GroupsAllowedToScan;
    std::unordered_map<TGroupId, TLockInfo> LocksByGroup;
    std::unordered_map<ui32, TFastLookupUniqueList<TGroupId>> NodeLockQueues;
};

/////////////////////////////////////////////////////////////////////////////////////

TBlobCheckerPlanner::TBlobCheckerPlanner(TDuration periodicity, ui32 groupCount)
    : Impl(new TImpl(periodicity, groupCount))
{}

TBlobCheckerPlanner::~TBlobCheckerPlanner() {}

template<>
void TBlobCheckerPlanner::EnqueueCheck<TBlobStorageGroupInfo>(const TBlobStorageGroupInfo* groupInfo) {
    Impl->EnqueueCheck(groupInfo);
}

template<>
void TBlobCheckerPlanner::EnqueueCheck<TBlobStorageController::TGroupInfo>(
        const TBlobStorageController::TGroupInfo* groupInfo) {
    Impl->EnqueueCheck(groupInfo);
}

bool TBlobCheckerPlanner::DequeueCheck(TGroupId groupId) {
    return Impl->DequeueCheck(groupId);
}

void TBlobCheckerPlanner::ResetState() {
    Impl->ResetState();
}

TMonotonic TBlobCheckerPlanner::GetNextAllowedCheckTimestamp(TMonotonic now) {
    return Impl->GetNextAllowedCheckTimestamp(now);
}

std::optional<TGroupId> TBlobCheckerPlanner::ObtainNextGroupToCheck() {
    return Impl->ObtainNextGroupToCheck();
}

void TBlobCheckerPlanner::SetGroupCount(ui32 groupCount) {
    Impl->SetGroupCount(groupCount);
}
void TBlobCheckerPlanner::SetPeriodicity(TDuration newPeriodicity) {
    Impl->SetPeriodicity(newPeriodicity);
}

} // namespace NBsController
} // namespace NKikimr
