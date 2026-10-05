#pragma once

#include <ydb/services/workload_manager/gateway.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <memory>


namespace NKikimr::NWorkloadManager::NPrivate {

inline constexpr TStringBuf WORKLOAD_MANAGER_NOT_READY_MESSAGE = "Workload manager is not ready for the database, please retry";

enum class EDatabaseState {
    Pending,
    Ready,
    Failed,
    TimedOut,
    Unsupported,
};

enum class EMetadataState {
    Pending,
    Ready,
    TimedOut,
};

struct TDatabaseInfo {
    EDatabaseState State = EDatabaseState::Pending;
    bool Serverless = false;
    Ydb::StatusIds::StatusCode FailureStatus = Ydb::StatusIds::SUCCESS;
    TString FailureMessage;
};

///
/// Snapshot of the workload manager state. Immutable once published.
///
struct TSnapshot {
    TResourcePoolMapPtr Pools;
    std::shared_ptr<const TResourcePoolClassifierSnapshot> Classifiers;
    THashMap<TString, TDatabaseInfo> Databases;
    THashSet<TString> ReadyPaths;
    NActors::TActorId StateActorId;
    bool EnableResourcePools = false;
    bool EnableResourcePoolsOnServerless = false;
    EMetadataState Metadata = EMetadataState::Pending;

    bool IsResourcePoolsEnabled(const TString& databaseId) const {
        if (!EnableResourcePools) {
            return false;
        }
        const auto it = Databases.find(databaseId);
        if (it == Databases.end()) {
            return false;
        }
        if (it->second.State != EDatabaseState::Ready) {
            return false;
        }
        return EnableResourcePoolsOnServerless || !it->second.Serverless;
    }

    bool NeedsWarmup(const TString& databasePath) const {
        return EnableResourcePools && !ReadyPaths.contains(databasePath);
    }
};

using TSnapshotPtr = TTrueAtomicSharedPtr<TSnapshot>;

///
/// Server-side implementation of IGateway. Created in the initializer and
/// stored in `AppData()->WorkloadManagerGateway`. State actor writes
/// snapshots via `PublishSnapshot`; consumers call `TryCreateQueryClassifier`.
/// All state, including the state actor id, is read from the published snapshot.
///
class TWorkloadManagerGateway : public IGateway {
public:
    void OnUnregistered() {
        Snapshot_.atomic_store(TSnapshotPtr());
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

private:
    static void DoWarmupRequest(const NActors::TActorId& stateActorId, const TString& databaseId);

    TSnapshotPtr Snapshot_;
};

}
