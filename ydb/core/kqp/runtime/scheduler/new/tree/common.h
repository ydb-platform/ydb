#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/types.h>

#include <optional>

namespace NKikimr::NKqp::NScheduler::NHdrf {

    static constexpr ui64 Infinity() {
        return std::numeric_limits<ui64>::max();
    }

    struct TStaticAttributes {
        std::optional<ui64> Limit;
        std::optional<ui64> Guarantee;
        std::optional<double> Weight;

        inline auto GetLimit() const {
            return Limit.value_or(Infinity());
        }

        inline auto GetGuarantee() const {
            return Guarantee.value_or(0);
        }

        inline auto GetWeight() const {
            return Weight.value_or(1);
        }

        inline void Update(const TStaticAttributes& other) {
            if (other.Limit) {
                Limit = other.Limit;
            }
            if (other.Guarantee) {
                Guarantee = other.Guarantee;
            }
            if (other.Weight) {
                Weight = other.Weight;
            }
        }
    };

    struct TPoolCounters {
        NMonitoring::TDynamicCounters::TCounterPtr Limit;
        NMonitoring::TDynamicCounters::TCounterPtr Guarantee;
        NMonitoring::TDynamicCounters::TCounterPtr Demand;
        NMonitoring::TDynamicCounters::TCounterPtr Usage;
        NMonitoring::TDynamicCounters::TCounterPtr Throttle;
        NMonitoring::TDynamicCounters::TCounterPtr FairShare;
        NMonitoring::TDynamicCounters::TCounterPtr InFlight;
        NMonitoring::TDynamicCounters::TCounterPtr Waiting;
        NMonitoring::TDynamicCounters::TCounterPtr InFlightExtra;
        NMonitoring::TDynamicCounters::TCounterPtr UsageExtra;
        NMonitoring::THistogramPtr                 Delay;
    };

} // namespace NKikimr::NKqp::NScheduler::NHdrf
