#pragma once
#include <ydb/core/tx/conveyor_composite/common/category.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <util/generic/hash.h>

namespace NKikimr::NConveyorComposite {
using ITask = NConveyor::ITask;
class TCPULimitsConfig;

struct TSchedulerQueryIdentity {
    TString DatabaseId;
    TString PoolId;
    ui64 QueryId = 0;

        bool operator==(const TSchedulerQueryIdentity&) const;
        bool IsDefault() const;
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
        const TCPULimitsConfig& cpuLimits, const std::optional<NActors::TActorId>& actorId,
        const std::optional<TSchedulerQueryIdentity>& schedulerQueryIdentity = std::nullopt);

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

}   // namespace NKikimr::NConveyorComposite

template <>
struct THash<NKikimr::NConveyorComposite::TSchedulerQueryIdentity> {
    size_t operator()(const NKikimr::NConveyorComposite::TSchedulerQueryIdentity& identity) const {
        return CombineHashes(
            CombineHashes(THash<TString>{}(identity.DatabaseId), THash<TString>{}(identity.PoolId)),
            THash<ui64>{}(identity.QueryId));
    }
};
