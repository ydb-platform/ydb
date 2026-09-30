#pragma once

#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <memory>


namespace NKikimr::NWorkloadManager::NPrivate {

struct TDatabaseInfo {
    bool Serverless = false;
    Ydb::StatusIds::StatusCode FetchStatus = Ydb::StatusIds::SUCCESS;
    TString FetchMessage;
};

///
/// Snapshot of the workload manager state. Immutable once published.
///
struct TSnapshot {
    TResourcePoolMapPtr Pools;
    std::shared_ptr<const TResourcePoolClassifierSnapshot> Classifiers;
    THashMap<TString, TDatabaseInfo> Databases;
    bool EnableResourcePools = false;
    bool EnableResourcePoolsOnServerless = false;
    bool ClassifierMetadataInitialized = false;

    bool IsResourcePoolsEnabled(const TString& databaseId) const {
        if (!EnableResourcePools) {
            return false;
        }
        const auto it = Databases.find(databaseId);
        if (it == Databases.end()) {
            return false;
        }
        if (it->second.FetchStatus != Ydb::StatusIds::SUCCESS) {
            return false;
        }
        return EnableResourcePoolsOnServerless || !it->second.Serverless;
    }
};

using TSnapshotPtr = TTrueAtomicSharedPtr<TSnapshot>;

///
/// Server-side implementation of IGateway. Created in the initializer and
/// stored in `AppData()->WorkloadManagerGateway`. Cache actor writes
/// snapshots via `PublishSnapshot`; consumers call `TryCreateQueryClassifier`.
///
class TWorkloadManagerGateway : public IGateway {
public:
    void OnRegistered(NActors::TActorId stateActorId) {
        StateActorId_ = stateActorId;
    }

    void PublishSnapshot(TSnapshotPtr snapshot) {
        Snapshot_.atomic_store(std::move(snapshot));
    }

    std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(
        const TString& databaseId, TClassifyContext context) override;

    TReadyInfo EnsureReady(const TString& databaseId) override;

    void SubscribeOnReady(const TString& databaseId,
                          NActors::TActorId subscriber, ui64 cookie) override;

    void Warmup(const TString& databasePath) override;

    TSnapshotPtr GetSnapshot() const {
        return Snapshot_;
    }

    NActors::TActorId GetStateActorId() const {
        return StateActorId_;
    }

private:
    void DoWarmupRequest(const TString& databaseId);

    TSnapshotPtr Snapshot_;
    // Written once from the state actor thread in OnRegistered() before the first
    // PublishSnapshot(); readers reach it only after loading a non-null snapshot.
    NActors::TActorId StateActorId_;
};

}
