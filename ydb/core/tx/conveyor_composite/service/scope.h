#pragma once
#include "common.h"
#include "process.h"

namespace NKikimr::NConveyorComposite {

class TProcessScope: public TNonCopyable {
private:
    YDB_READONLY_DEF(std::shared_ptr<TCPUUsage>, CPUUsage);
    std::shared_ptr<TPositiveControlInteger> WaitingTasksCount;
    TCPUGroup::TPtr ScopeLimits;

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

    TProcessScope(TCPUGroup::TPtr&& limits, const std::shared_ptr<TCPUUsage>& categoryScope)
        : CPUUsage(std::make_shared<TCPUUsage>(categoryScope))
        , ScopeLimits(std::move(limits)) {
    }

};

}   // namespace NKikimr::NConveyorComposite
