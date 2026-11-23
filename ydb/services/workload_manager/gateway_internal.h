#pragma once

#include <ydb/library/actors/core/actorid.h>
#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>

#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>
#include <util/generic/string.h>

#include <memory>

namespace NKikimr::NWorkloadManager::NPrivate {

///
/// Snapshot of the workload manager state. Immutable once published.
///
struct TSnapshot {
    TResourcePoolMapPtr Pools;
    std::shared_ptr<const TResourcePoolClassifierSnapshot> Classifiers;
    NActors::TActorId StateActorId;
    bool EnableResourcePools = false;
    bool EnableResourcePoolsOnServerless = false;

    bool IsResourcePoolsEnabled(const TString& databaseId) const;
};

using TSnapshotPtr = TTrueAtomicSharedPtr<TSnapshot>;

///
/// Server-side implementation of IGateway. Created in the initializer and
/// stored in `AppData()->WorkloadManagerGateway`. State actor writes
/// snapshots via `PublishSnapshot`; consumers call `TryCreateQueryClassifier`.
/// All state, including the state actor id, is read from the published snapshot.
///
class TWorkloadManagerGateway: public IGateway {
public:
    void OnUnregistered() {
        Snapshot_.atomic_store(TSnapshotPtr());
    }

    void PublishSnapshot(TSnapshotPtr snapshot) {
        Snapshot_.atomic_store(std::move(snapshot));
    }

    std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(const TString& databaseId, TClassifyContext context) override;

    TSnapshotPtr GetSnapshot() const {
        return Snapshot_;
    }

private:
    TSnapshotPtr Snapshot_;
};

}   // namespace NKikimr::NWorkloadManager::NPrivate
