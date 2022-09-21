#pragma once

#include "defs.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

    class TThroughputMeter
    {
        const ui64 UpdateDivisionFactor;
        const ui64 WindowUpdateTimeoutNs;
        TVector<std::pair<float, ::NMonitoring::TDynamicCounters::TCounterPtr>> Counters;

        ui64 CumTimeNs = 0;
        ui64 CumBytes = 0;
        THPTimer Timer;
        ui64 WindowNs = 0;
        TVector<ui64> Histogram;

    public:
        TThroughputMeter(ui64 updateDivisionFactor, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
                const TString& group, const TString& subgroup, const TString& name, const TVector<float>& thresholds,
                const TDuration& windowUpdateTimeout = TDuration::MilliSeconds(10))
            : UpdateDivisionFactor(updateDivisionFactor)
            , WindowUpdateTimeoutNs(windowUpdateTimeout.NanoSeconds())
        {
            TIntrusivePtr<::NMonitoring::TDynamicCounters> histGroup =
                counters->GetSubgroup(group, subgroup)->GetSubgroup("sensor", name);
            for (float threshold : thresholds) {
                Counters.emplace_back(threshold, histGroup->GetNamedCounter("percentile", Sprintf("%.1f", threshold * 100)));
            }
        }

        void Count(ui64 bytes) {
            const i64 span = Max<i64>(0, Timer.PassedReset() * 1000000000);

            WindowNs += span;
            while (WindowNs >= WindowUpdateTimeoutNs) {
                const ui64 rate = CumTimeNs ? (1000000000 * CumBytes / CumTimeNs) : 0;
                Histogram.push_back(rate);

                if (UpdateDivisionFactor) {
                    CumBytes /= UpdateDivisionFactor;
                    CumTimeNs /= UpdateDivisionFactor;
                } else {
                    CumBytes = 0;
                    CumTimeNs = 0;
                }

                WindowNs -= WindowUpdateTimeoutNs;
            }

            // count passed bytes and time
            CumBytes += bytes;
            CumTimeNs += span;
        }

        void UpdateHistogram() {
            Count(0);
            std::sort(Histogram.begin(), Histogram.end());
            if (!Histogram) {
                Histogram.push_back(0);
            }
            const size_t maxIndex = Histogram.size() - 1;
            for (const auto& p : Counters) {
                const float threshold = p.first;
                const auto& counter = p.second;
                const size_t index = Min<size_t>(maxIndex, maxIndex * threshold);
                *counter = Histogram[index];
            }
            Histogram.clear();
        }
    };

} // NKikimr
