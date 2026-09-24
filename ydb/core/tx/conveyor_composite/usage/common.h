#pragma once
#include <ydb/core/kqp/runtime/scheduler/fwd.h>
#include <ydb/core/tx/conveyor_composite/common/category.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <util/generic/hash.h>

namespace NKikimr::NConveyorComposite {
using ITask = NConveyor::ITask;
class TCPULimitsConfig;

struct TSchedulerQueryIdentity {
    ui64 QueryId;
    bool IsServiceQuery = false;

    bool operator==(const TSchedulerQueryIdentity&) const = default;
};

inline constexpr TSchedulerQueryIdentity kServiceQueryIdentity{0, true};

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
        ui64 txId = 0, const std::optional<NKqp::NScheduler::NHdrf::TFullPoolId>& schedulerPool = std::nullopt);

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
        return CombineHashes(THash<ui64>{}(identity.QueryId), THash<bool>{}(identity.IsServiceQuery));
    }
};
