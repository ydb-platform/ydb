#pragma once
#include "common.h"

#include <ydb/core/tx/conveyor_composite/usage/config.h>

namespace NKikimr::NConveyorComposite {

class TProcessScope: public TNonCopyable {
private:
    YDB_READONLY_DEF(TString, ScopeId);
    YDB_READONLY_DEF(std::shared_ptr<TCPUUsage>, CPUUsage);
    TCPUGroup::TPtr ScopeLimits;

public:
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
