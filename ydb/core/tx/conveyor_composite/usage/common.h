#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <util/generic/hash.h>

namespace NKikimr::NConveyorComposite {
using ITask = NConveyor::ITask;
class TCPULimitsConfig;

class TWorkloadManagerQueryIdentity {
private:
    YDB_READONLY_DEF(TString, DatabaseId);
    YDB_READONLY_DEF(TString, PoolId);
    YDB_READONLY(ui64, QueryId, 0);

public:
    struct THash {
        size_t operator()(const TWorkloadManagerQueryIdentity& identity) const {
            return CombineHashes(
                CombineHashes(::THash<TString>()(identity.GetDatabaseId()), ::THash<TString>()(identity.GetPoolId())),
                ::THash<ui64>()(identity.GetQueryId()));
        }
    };

    TWorkloadManagerQueryIdentity() = default;

    TWorkloadManagerQueryIdentity(TString databaseId, TString poolId, const ui64 queryId)
        : DatabaseId(std::move(databaseId))
        , PoolId(std::move(poolId))
        , QueryId(queryId) {
    }

    bool operator==(const TWorkloadManagerQueryIdentity&) const = default;
};

enum class ESpecialTaskCategory {
    Insert = 0 /* "insert" */,
    Compaction = 1 /* "compaction" */,
    Normalizer = 2 /* "normalizer" */,
    Scan = 3 /* "scan" */,
    Deduplication = 4 /* "deduplication" */
};

class TProcessGuard: TNonCopyable {
private:
    const ESpecialTaskCategory Category;
    const TString ScopeId;
    const ui64 ExternalProcessId;
    static inline TAtomicCounter InternalCounter = 0;
    const ui64 InternalProcessId = InternalCounter.Inc();
    bool Finished = false;
    std::optional<NActors::TActorId> ServiceActorId;

public:
    ui64 GetInternalProcessId() const {
        return InternalProcessId;
    }

    explicit TProcessGuard(const ESpecialTaskCategory category, const TString& scopeId, const ui64 externalProcessId,
        const TCPULimitsConfig& cpuLimits, const std::optional<NActors::TActorId>& actorId);

    bool SendTaskToExecute(const std::shared_ptr<ITask>& task) const;

    void Finish();

    TProcessGuard(TProcessGuard&& other)
        : Category(other.Category)
        , ScopeId(other.ScopeId)
        , ExternalProcessId(other.ExternalProcessId)
        , InternalProcessId(other.InternalProcessId)
        , Finished(other.Finished)
        , ServiceActorId(std::move(other.ServiceActorId)) {
        other.Finished = true;
        other.ServiceActorId.reset();
    }

    ~TProcessGuard() {
        if (!Finished) {
            Finish();
        }
    }
};

class TWorkloadManagerQueryGuard: TNonCopyable {
private:
    const TWorkloadManagerQueryIdentity Identity;
    bool Finished = false;
    std::optional<NActors::TActorId> ServiceActorId;

public:
    explicit TWorkloadManagerQueryGuard(
        TWorkloadManagerQueryIdentity identity, const std::optional<NActors::TActorId>& actorId);

    void Finish();

    TWorkloadManagerQueryGuard(TWorkloadManagerQueryGuard&& other)
        : Identity(other.Identity)
        , Finished(other.Finished)
        , ServiceActorId(std::move(other.ServiceActorId)) {
        other.Finished = true;
        other.ServiceActorId.reset();
    }

    ~TWorkloadManagerQueryGuard() {
        if (!Finished) {
            Finish();
        }
    }
};

}   // namespace NKikimr::NConveyorComposite
