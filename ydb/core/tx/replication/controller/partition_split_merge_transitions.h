#pragma once

#include <ydb/core/tx/replication/common/worker_id.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NKikimr::NReplication::NController {

class TPartitionSplitMergeTransitions {
    enum class EStatus {
        WaitingForChildren,
        ParentRemovalRequested,
        Completed,
    };

public:
    void StartTransition(const TWorkerId& parent, const TVector<TWorkerId>& children);
    const THashMap<TWorkerId, TVector<TWorkerId>>& GetWaitingParents() const;
    void RequestParentRemoval(const TVector<TWorkerId>& parents);
    void CompleteTransition(const TWorkerId& parent);

    bool IsParentRemovalRequested(const TWorkerId& parent) const;
    bool IsTransitionCompleted(const TWorkerId& parent) const;

    TVector<TWorkerId> ClearTarget(ui64 replicationId, ui64 targetId);
    void ForgetTransitions(const TVector<TWorkerId>& parents);
    void Clear();

private:
    bool HasStatus(const TWorkerId& parent, EStatus status) const;

    THashMap<TWorkerId, EStatus> Statuses;
    THashMap<TWorkerId, TVector<TWorkerId>> WaitingParents;
};

}
