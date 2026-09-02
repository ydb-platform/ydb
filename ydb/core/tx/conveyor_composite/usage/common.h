#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NConveyorComposite {
using ITask = NConveyor::ITask;
class TCPULimitsConfig;

enum class ESpecialTaskCategory {
    Insert = 0 /* "insert" */,
    Compaction = 1 /* "compaction" */,
    Normalizer = 2 /* "normalizer" */,
    Scan = 3 /* "scan" */,
    Deduplication = 4 /* "deduplication" */
};

struct TWorkloadContext {
    TString DatabaseId;
    TString PoolId;
    ui64 QueryId = 0;

    bool IsDefined() const {
        return DatabaseId && PoolId && QueryId;
    }

    bool operator==(const TWorkloadContext&) const = default;
};

class TProcessGuard: TNonCopyable {
private:
    const ESpecialTaskCategory Category;
    const TString ScopeId;
    const ui64 ExternalProcessId;
    static inline TAtomicCounter InternalCounter = 0;
    const ui64 InternalProcessId = InternalCounter.Inc();
    const TWorkloadContext WorkloadContext;
    bool Finished = false;
    std::optional<NActors::TActorId> ServiceActorId;

public:
    ui64 GetInternalProcessId() const {
        return InternalProcessId;
    }

    explicit TProcessGuard(const ESpecialTaskCategory category, const TString& scopeId, const ui64 externalProcessId,
        const TCPULimitsConfig& cpuLimits, const std::optional<NActors::TActorId>& actorId,
        TWorkloadContext workloadContext = {});

    bool SendTaskToExecute(const std::shared_ptr<ITask>& task) const;

    void Finish();

    TProcessGuard(TProcessGuard&& other)
        : Category(other.Category)
        , ScopeId(other.ScopeId)
        , ExternalProcessId(other.ExternalProcessId)
        , InternalProcessId(other.InternalProcessId)
        , WorkloadContext(std::move(other.WorkloadContext))
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
