#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

/**
 * The aggregator for additive metric values.
 */
class TAdditiveMetricAggregator {
public:
    /**
     * Start a new aggregation process for the given target counter.
     *
     * @param[in] targetCounter The target counter for the aggregation
     */
    explicit TAdditiveMetricAggregator(
        NMonitoring::TDynamicCounters::TCounterPtr targetCounter
    ) noexcept
        : Value(0)
        , TargetCounter(targetCounter)
    {
    }

    /**
     * Finish the current aggregation process and update the target counter value.
     */
    ~TAdditiveMetricAggregator() noexcept {
        TargetCounter->Set(Value);
    }

    /**
     * Aggregate the current counter value with the value of the given source counter.
     *
     * @param[in] sourceCounter The source counter to aggregate
     */
    void AggregateValue(NMonitoring::TDynamicCounters::TCounterPtr sourceCounter) noexcept {
        Value += sourceCounter->Val();
    }

private:
    /**
     * The aggregated value.
     *
     * @note To avoid exposing intermediate values to the metric collectors,
     *       the aggregated value is stored here during the aggregation
     *       and copied to the actual counter only at the end.
     */
    ui64 Value;

    /**
     * The target counter, which will be updated after the aggregation is complete.
     */
    NMonitoring::TDynamicCounters::TCounterPtr TargetCounter;
};

/**
 * The aggregator for histogram metric values.
 */
class THistogramMetricAggregator {
public:
    /**
     * Start a new aggregation process for the given target counter.
     *
     * @param[in] targetCounter The target counter for the aggregation
     */
    explicit THistogramMetricAggregator(NMonitoring::THistogramPtr targetCounter) noexcept
        : TargetCounter(targetCounter)
    {
        TargetCounter->Reset();
    }

    /**
     * Finish the current aggregation process and update the target counter value.
     */
    ~THistogramMetricAggregator() noexcept {
        // The target histogram is updated in-place, so nothing to do here
    }

    /**
     * Aggregate the current counter value with the value of the given source counter.
     *
     * @param[in] sourceCounter The source counter to aggregate
     */
    void AggregateValue(NMonitoring::THistogramPtr sourceCounter);

private:
    /**
     * The target counter, which will be updated after the aggregation is complete.
     *
     * @note This counter is updated in-place, which will cause the histogram value
     *       "to flicker" and to drop to zero values until the aggregation is complete.
     *       Unfortunately, there is no way to avoid this - this is the limitation
     *       of the histogram interface.
     */
    NMonitoring::THistogramPtr TargetCounter;
};

} // Namespace NKikimr
