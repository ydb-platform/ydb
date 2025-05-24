#pragma once
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NConveyorComposite {
using ITask = NConveyor::ITask;

enum class ESpecialTaskCategory {
    Insert = 0 /* "insert" */,
    Compaction = 1 /* "compaction" */,
    Normalizer = 2 /* "normalizer" */,
    Scan = 3 /* "scan" */
};

class TProcessGuard: TNonCopyable {
private:
    const ESpecialTaskCategory Category;
    const TString ScopeId;
    const ui64 ProcessId;
    bool Finished = false;
    const std::optional<NActors::TActorId> ServiceActorId;

public:
    ui64 GetProcessId() const {
        return ProcessId;
    }

    explicit TProcessGuard(
        const ESpecialTaskCategory category, const TString& scopeId, const ui64 processId, const std::optional<NActors::TActorId>& actorId)
        : Category(category)
        , ScopeId(scopeId)
        , ProcessId(processId)
        , ServiceActorId(actorId) {
    }

    void Finish();

    ~TProcessGuard() {
        if (!Finished) {
            Finish();
        }
    }
};

}   // namespace NKikimr::NConveyorComposite
