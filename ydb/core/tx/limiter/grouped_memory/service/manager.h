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
    std::map<TProcessMemoryUsage, TProcessMemory*> ProcessesOrdered;
    std::shared_ptr<TStageFeatures> DefaultStage;
    TIdsControl ProcessIds;

    void TryAllocateWaiting();
    void RefreshSignals() const {
        Signals->ProcessesCount->Set(Processes.size());
    }

    class TOrderedProcessesGuard {
    private:
        bool Released = false;
        TProcessMemory& Process;
        std::map<TProcessMemoryUsage, TProcessMemory*>* Processes;
        TProcessMemoryUsage Start;

    public:
        TOrderedProcessesGuard(TProcessMemory& process, std::map<TProcessMemoryUsage, TProcessMemory*>& processes)
            : Process(process)
            , Processes(&processes)
            , Start(Process.BuildUsageAddress()) {
            AFL_VERIFY(Processes->contains(Start));
        }

        ~TOrderedProcessesGuard() {
            if (Released) {
                return;
            }
            AFL_VERIFY(Processes->erase(Start))("start", Start.DebugString());
            AFL_VERIFY(Processes->emplace(Process.BuildUsageAddress(), &Process).second);
        }

        void Release() {
            AFL_VERIFY(!Released);
            Released = true;
        }
    };

    TOrderedProcessesGuard BuildProcessOrderGuard(TProcessMemory& process) {
        return TOrderedProcessesGuard(process, ProcessesOrdered);
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
        , DefaultStage(defaultStage) {
    }

    void RegisterGroup(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId);
    void UnregisterGroup(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId);

    void RegisterProcessScope(const ui64 externalProcessId, const ui64 externalScopeId);
    void UnregisterProcessScope(const ui64 externalProcessId, const ui64 externalScopeId);

    void RegisterProcess(const ui64 externalProcessId, const std::vector<std::shared_ptr<TStageFeatures>>& stages);
    void UnregisterProcess(const ui64 externalProcessId);

    void RegisterAllocation(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId,
        const std::shared_ptr<IAllocation>& allocation, const std::optional<ui32>& stageIdx);
    void UnregisterAllocation(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 allocationId);
    void AllocationUpdated(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 allocationId);

    void SetMemoryConsumptionUpdateFunction(std::function<void(ui64)> func);
    void UpdateMemoryLimits(const ui64 limit, const std::optional<ui64>& hardLimit);

    bool IsEmpty() const {
        return Processes.empty();
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
