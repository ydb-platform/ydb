#pragma once

#include <ydb/core/protos/counters_pq.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {
namespace NPQ {


class TWorkingTimeCounter {
private:
    bool IsInWorkingState;
    ::NMonitoring::TDynamicCounters::TCounterPtr WorkingTimeMicroSec;
    TInstant LastUpdateTimestamp;
public:

    TWorkingTimeCounter(::NMonitoring::TDynamicCounters::TCounterPtr counter)
    : IsInWorkingState(false)
    , WorkingTimeMicroSec(counter)
    {}

    void UpdateState(bool state) {
        IsInWorkingState = state;
    }

    void UpdateWorkingTime(const TInstant now) {
        if (!WorkingTimeMicroSec) //no counter
            return;
        if (IsInWorkingState && LastUpdateTimestamp > TInstant::Zero()) {
            TDuration res = now - LastUpdateTimestamp;
            (*WorkingTimeMicroSec) += res.MicroSeconds();
            LastUpdateTimestamp += res;
        } else {
            LastUpdateTimestamp = now;
        }
    }

    void SetCounter(::NMonitoring::TDynamicCounterPtr counter,
                    const TVector<std::pair<TString, TString>>& subgroups,
                    const std::tuple<TString, TString, bool>& expiring) {
        for (const auto& subgroup : subgroups) {
           counter = counter->GetSubgroup(subgroup.first, subgroup.second);
        }

        WorkingTimeMicroSec = counter->GetExpiringNamedCounter(std::get<0>(expiring),
                                                               std::get<1>(expiring),
                                                               std::get<2>(expiring));
    }
};

} //NPQ
} //NKikimr
