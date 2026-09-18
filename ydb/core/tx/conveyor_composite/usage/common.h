#pragma once
#include <ydb/core/tx/conveyor_composite/common/category.h>
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

class TProcessGuard: TNonCopyable {
private:
    const ESpecialTaskCategory Category;
    const TString ScopeId;
    const ui64 ExternalProcessId;
    static inline TAtomicCounter InternalCounter = 0;
    const ui64 InternalProcessId = InternalCounter.Inc();
    const std::optional<TWorkloadManagerQueryIdentity> WorkloadManagerQueryIdentity;
    bool Finished = false;
    std::optional<NActors::TActorId> ServiceActorId;

public:
    ui64 GetInternalProcessId() const {
        return InternalProcessId;
    }

    explicit TProcessGuard(const ESpecialTaskCategory category, const TString& scopeId, const ui64 externalProcessId,
        const TCPULimitsConfig& cpuLimits, const std::optional<NActors::TActorId>& actorId,
        std::optional<TWorkloadManagerQueryIdentity> workloadManagerQueryIdentity = std::nullopt);

    const std::optional<TWorkloadManagerQueryIdentity>& GetWorkloadManagerQueryIdentity() const {
        return WorkloadManagerQueryIdentity;
    }

    bool SendTaskToExecute(const std::shared_ptr<ITask>& task) const;

    void Finish();

    TProcessGuard(TProcessGuard&& other)
        : Category(other.Category)
        , ScopeId(other.ScopeId)
        , ExternalProcessId(other.ExternalProcessId)
        , InternalProcessId(other.InternalProcessId)
        , WorkloadManagerQueryIdentity(other.WorkloadManagerQueryIdentity)
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

}   // namespace NKikimr::NConveyorComposite
