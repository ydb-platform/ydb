#pragma once
#include "counters.h"
#include "process.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>

#include <ydb/library/accessor/validator.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TManager {
private:
    const TConfig Config;
    const TString Name;
    const std::shared_ptr<TCounters> Signals;
    const NActors::TActorId OwnerActorId;
    THashMap<ui64, TProcessMemory> Processes;
    std::shared_ptr<TStageFeatures> DefaultStage;
    TIdsControl ProcessIds;

    void TryAllocateWaiting();
    void RefreshSignals() const {
        Signals->ProcessesCount->Set(Processes.size());
    }

    TProcessMemory& GetProcessMemoryVerified(const ui64 internalProcessId) {
        auto it = Processes.find(internalProcessId);
        AFL_VERIFY(it != Processes.end());
        return it->second;
    }

    TProcessMemory* GetProcessMemoryByExternalIdOptional(const ui64 externalProcessId);

    TProcessMemory* GetProcessMemoryOptional(const ui64 internalProcessId) {
        auto it = Processes.find(internalProcessId);
        if (it != Processes.end()) {
            return &it->second;
        } else {
            return nullptr;
        }
    }

public:
    TManager(const NActors::TActorId& ownerActorId, const TConfig& config, const TString& name, const std::shared_ptr<TCounters>& signals,
        const std::shared_ptr<TStageFeatures>& defaultStage)
        : Config(config)
        , Name(name)
        , Signals(signals)
        , OwnerActorId(ownerActorId)
        , DefaultStage(defaultStage)
    {
    }

    void RegisterGroup(const ui64 externalProcessId, const ui64 externalGroupId);
    void UnregisterGroup(const ui64 externalProcessId, const ui64 externalGroupId);

    void RegisterProcess(const ui64 externalProcessId, const std::vector<std::shared_ptr<TStageFeatures>>& stages);
    void UnregisterProcess(const ui64 externalProcessId);

    void RegisterAllocation(const ui64 externalProcessId, const ui64 externalGroupId, const std::shared_ptr<IAllocation>& task,
        const std::optional<ui32>& stageIdx);
    void UnregisterAllocation(const ui64 externalProcessId, const ui64 allocationId);
    void UpdateAllocation(const ui64 externalProcessId, const ui64 allocationId, const ui64 volume);

    bool IsEmpty() const {
        return Processes.empty();
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
