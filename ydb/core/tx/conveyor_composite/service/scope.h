#pragma once
#include "common.h"
#include "process.h"

namespace NKikimr::NConveyorComposite {

class TProcessScope: public TNonCopyable {
private:
    YDB_READONLY(std::shared_ptr<TCPUUsage>, CPUUsage, std::make_shared<TCPUUsage>());
    TCPUGroup::TPtr ScopeLimits;
    THashMap<ui64, TProcess> Processes;
    YDB_READONLY(double, Weight, 1);

public:
    ui32 GetProcessesCount() const {
        return Processes.size();
    }
    bool HasTasks() const;

    TWorkerTask ExtractTaskWithPrediction();

    void DoQuant(const TMonotonic newStart);

    void UpdateLimits(TCPUGroup::TPtr&& limits) {
        ScopeLimits = std::move(limits);
    }

    void PutTaskResult(TWorkerTaskResult&& result) {
        const ui64 id = result.GetProcessId();
        MutableProcessVerified(id).PutTaskResult(std::move(result));
    }

    TProcessScope(TCPUGroup::TPtr&& limits)
        : ScopeLimits(std::move(limits)) {
    }

    TProcess& MutableProcessVerified(const ui64 processId) {
        auto it = Processes.find(processId);
        AFL_VERIFY(it != Processes.end());
        return it->second;
    }

    void RegisterProcess(const ui64 processId) {
        TProcess process(processId);
        AFL_VERIFY(Processes.emplace(processId, std::move(process)).second);
        ScopeLimits->IncProcesses();
    }

    bool UnregisterProcess(const ui64 processId) {
        AFL_VERIFY(Processes.erase(processId));
        return ScopeLimits->DecProcesses();
    }
};

}   // namespace NKikimr::NConveyorComposite
