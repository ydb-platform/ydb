#pragma once
#include "common.h"

#include <ydb/core/tx/conveyor_composite/usage/config.h>

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NConveyorComposite {

class TProcessScope: public TNonCopyable, public NColumnShard::TMonitoringObjectsCounter<TProcessScope> {
private:
    YDB_READONLY_DEF(TString, ScopeId);
    YDB_READONLY_DEF(std::shared_ptr<TCPUUsage>, CPUUsage);
    TCPUGroup::TPtr ScopeLimits;
    TPositiveControlInteger CountInFlight;
    TPositiveControlInteger CountProcesses;

public:
    void IncProcesses() {
        CountProcesses.Inc();
    }

    bool DecProcesses() {
        return CountProcesses.Dec() == 0;
    }

    ui32 GetCountInFlight() const {
        return CountInFlight.Val();
    }

    bool CheckToRun() const {
        return CountInFlight.Val() + 1 <= std::ceil(ScopeLimits->GetCPUThreadsLimit());
    }

    void IncInFlight() {
        AFL_VERIFY(CountInFlight.Inc() <= std::ceil(ScopeLimits->GetCPUThreadsLimit()))("in_flight", CountInFlight.Inc())(
                                            "limit", ScopeLimits->GetCPUThreadsLimit());
    }

    void DecInFlight() {
        CountInFlight.Dec();
    }

    double GetWeight() const {
        return ScopeLimits->GetWeight();
    }

    void UpdateLimits(const TCPULimitsConfig& processCpuLimits) {
        ScopeLimits->SetCPUThreadsLimit(processCpuLimits.GetCPUGroupThreadsLimitDef(256));
        ScopeLimits->SetWeight(processCpuLimits.GetWeight());
    }

    TProcessScope(const TString& scopeId, TCPUGroup::TPtr&& limits, const std::shared_ptr<TCPUUsage>& categoryScope)
        : ScopeId(scopeId)
        , CPUUsage(std::make_shared<TCPUUsage>(categoryScope))
        , ScopeLimits(std::move(limits)) {
    }
};

}   // namespace NKikimr::NConveyorComposite
