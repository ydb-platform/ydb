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

    void Finish();

    TProcessGuard(TProcessGuard&& other)
        : Category(other.Category)
        , ScopeId(other.ScopeId)
        , ExternalProcessId(other.ExternalProcessId)
        , ServiceActorId(other.ServiceActorId) {
        other.ServiceActorId.reset();
    }

    ~TProcessGuard() {
        if (!Finished) {
            Finish();
        }
    }
};

}   // namespace NKikimr::NConveyorComposite
