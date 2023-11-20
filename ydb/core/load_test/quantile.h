#pragma once

#include "defs.h"
#include "time_series.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

    template<typename T>
    class TQuantileTracker : public TTimeSeries<T>
    {
        using TItem = typename TTimeSeries<T>::TItem;
        using TTimeSeries<T>::Items;

        using TPercentile = std::pair<float, ::NMonitoring::TDynamicCounters::TCounterPtr>;
        using TPercentiles = TVector<TPercentile>;

        TPercentiles Percentiles;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
        ::NMonitoring::TDynamicCounters::TCounterPtr Samples;

        struct TCompareTimestamp {
            bool operator ()(const TItem& x, TMonotonic y) {
                return x.Timestamp < y;
            }
        };

    public:
        using TTimeSeries<T>::TTimeSeries;

        TQuantileTracker(TDuration lifetime, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                const TString& metric, const TVector<float>& percentiles)
            : TTimeSeries<T>(lifetime)
            , Counters(counters)
        {
            Y_ABORT_UNLESS(Counters);
            Samples = Counters->GetCounter("samples", false);
            for (auto perc : percentiles) {
                auto subgroup = Counters->GetSubgroup("percentile", Sprintf("%.1f", perc * 100.f));
                Percentiles.emplace_back(perc, subgroup->GetCounter(metric, false));
            }
        }

        void CalculateQuantiles() const {
            Y_ABORT_UNLESS(Counters);

            *Samples = Items.size();

            if (Items.empty()) {
                for (auto& perc : Percentiles) {
                    *perc.second = T();
                }
                return;
            }

            // create a vector of values matching time criterion
            TVector<T> values;
            values.reserve(Items.size());
            for (const TItem &item : Items) {
                values.push_back(item.Value);
            }

            // sort and calculate quantiles
            std::sort(values.begin(), values.end());
            const size_t maxIndex = values.size() - 1;
            for (auto& perc : Percentiles) {
                const size_t index = Min<size_t>(maxIndex, maxIndex  * perc.first);
                *perc.second = values[index];
            }
        }

        bool CalculateQuantiles(size_t count, const size_t *numerators, size_t denominator, T *res,
                size_t *numSamples = nullptr, TDuration *interval = nullptr) const {
            if (numSamples) {
                *numSamples = Items.size();
                if (interval) {
                    *interval = Items ? Items.back().Timestamp - Items.front().Timestamp : TDuration::Zero();
                }
            }

            // create a vector of values matching time criterion
            TVector<T> values;
            values.reserve(Items.size());
            for (const TItem &item : Items) {
                values.push_back(item.Value);
            }

            // if there are no values, return false meaning we can't get adequate results
            if (values.empty()) {
                std::fill(res, res + count, T());
                return false;
            }

            // sort and calculate quantiles
            std::sort(values.begin(), values.end());
            size_t maxIndex = values.size() - 1;
            while (count--) {
                const size_t numerator = *numerators++;
                Y_ABORT_UNLESS(numerator >= 0 && numerator <= denominator);
                const size_t index = maxIndex * numerator / denominator;
                *res++ = values[index];
            }
            return true;
        }
    };

} // NKikimr
