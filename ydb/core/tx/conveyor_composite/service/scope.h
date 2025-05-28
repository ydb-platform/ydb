#pragma once
#include "common.h"
#include "process.h"

namespace NKikimr::NConveyorComposite {

class TProcessScope: public TNonCopyable {
private:
    YDB_READONLY_DEF(std::shared_ptr<TCPUUsage>, CPUUsage);
    TCPUGroup::TPtr ScopeLimits;
    THashMap<ui64, TProcess> Processes;

public:
    double GetWeight() const {
        return ScopeLimits->GetWeight();
    }

    ui32 GetProcessesCount() const {
        return Processes.size();
    }
    bool HasTasks() const;

    TWorkerTask ExtractTaskWithPrediction();

    void DoQuant(const TMonotonic newStart);

    void UpdateLimits(const TCPULimitsConfig& processCpuLimits) {
        ScopeLimits->SetCPUThreadsLimit(processCpuLimits.GetCPUGroupThreadsLimitDef(256));
        ScopeLimits->SetWeight(processCpuLimits.GetWeight());
    }

    void PutTaskResult(TWorkerTaskResult&& result) {
        const ui64 id = result.GetProcessId();
        if (auto* process = MutableProcessOptional(id)) {
            process->PutTaskResult(std::move(result));
        }
    }

    TProcessScope(TCPUGroup::TPtr&& limits, const std::shared_ptr<TCPUUsage>& categoryScope)
        : CPUUsage(std::make_shared<TCPUUsage>(categoryScope))
        , ScopeLimits(std::move(limits)) {
    }

    TProcess& MutableProcessVerified(const ui64 processId) {
        auto it = Processes.find(processId);
        AFL_VERIFY(it != Processes.end());
        return it->second;
    }

    TProcess* MutableProcessOptional(const ui64 processId) {
        auto it = Processes.find(processId);
        if (it != Processes.end()) {
            return &it->second;
        } else {
            return nullptr;
        }
    }

    void RegisterProcess(const ui64 processId) {
        TProcess process(processId, CPUUsage);
        AFL_VERIFY(Processes.emplace(processId, std::move(process)).second);
        ScopeLimits->IncProcesses();
    }

    bool UnregisterProcess(const ui64 processId) {
        AFL_VERIFY(Processes.erase(processId));
        return ScopeLimits->DecProcesses();
    }
};

}   // namespace NKikimr::NConveyorComposite
