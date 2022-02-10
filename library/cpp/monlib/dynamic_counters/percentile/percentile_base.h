#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/string/printf.h>

namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Percentile tracker for monitoring
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TPercentileBase : public TThrRefBase {
    using TPercentile = std::pair<float, NMonitoring::TDynamicCounters::TCounterPtr>;
    using TPercentiles = TVector<TPercentile>;

    TPercentiles Percentiles;

    void Initialize(const TIntrusivePtr<NMonitoring::TDynamicCounters> &counters, const TVector<float> &thresholds,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public) {
        Percentiles.reserve(thresholds.size());
        for (size_t i = 0; i < thresholds.size(); ++i) {
            Percentiles.emplace_back(thresholds[i],
                counters->GetNamedCounter("percentile", Sprintf("%.1f", thresholds[i] * 100.f), false, visibility));
        }
    }

    void Initialize(const TIntrusivePtr<NMonitoring::TDynamicCounters> &counters, TString group, TString subgroup,
            TString name, const TVector<float> &thresholds,
            TCountableBase::EVisibility visibility = TCountableBase::EVisibility::Public) {
        auto subCounters = counters->GetSubgroup(group, subgroup)->GetSubgroup("sensor", name);
        Initialize(subCounters, thresholds, visibility);
    }
};

} // NMonitoring
