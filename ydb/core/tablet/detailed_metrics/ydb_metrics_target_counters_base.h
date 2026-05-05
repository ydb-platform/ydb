#pragma once

#include "ydb_metrics_aggregator.h"

#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

/**
 * The base class for all classes, which work with target counters
 * for the YDB metrics (for example, table.datashard.*).
 *
 * @tparam SimpleDesc The function, which returns the enum description for simple counters
 * @tparam CumulativeDesc The function, which returns the enum description for cumulative counters
 * @tparam PercentileDesc The function, which returns the enum description for percentile counters
 * @tparam ParseSourceCounters Indicates whether to parse the SourceCounters fields
 */
template <const NProtoBuf::EnumDescriptor* SimpleDesc(),
          const NProtoBuf::EnumDescriptor* CumulativeDesc(),
          const NProtoBuf::EnumDescriptor* PercentileDesc(),
          bool ParseSourceCounters>
class TYdbMetricsTargetCountersBase {
protected:
    using TSimpleCountersOpts = NAux::TAppParsedOpts<SimpleDesc, ParseSourceCounters>;
    using TCumulativeCountersOpts = NAux::TAppParsedOpts<CumulativeDesc, ParseSourceCounters>;
    using TPercentileCountersOpts = NAux::TAppParsedOpts<PercentileDesc, ParseSourceCounters>;

    static const TSimpleCountersOpts* SimpleCountersOpts() {
        return NAux::GetAppOpts<SimpleDesc, ParseSourceCounters>();
    }

    static const TCumulativeCountersOpts* CumulativeCountersOpts() {
        return NAux::GetAppOpts<CumulativeDesc, ParseSourceCounters>();
    }

    static const TPercentileCountersOpts* PercentileCountersOpts() {
        return NAux::GetAppOpts<PercentileDesc, ParseSourceCounters>();
    }

    /**
     * Create an explicit histogram with the bucket boundaries defined
     * in the corresponding protobuf enum definition.
     *
     * @param[in] counterOptions The parsed enum options for the target counters
     * @param[in] targetCounterGroup The counter group where the target counters are created
     * @param[in] index The index for the corresponding enum value in the protobuf file
     * @param[in] name The name of the histogram to create
     *
     * @return The corresponding explicit histogram counter
     */
    static NMonitoring::THistogramPtr CreateExplicitHistogram(
        const TPercentileCountersOpts* counterOptions,
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        size_t index,
        const char* name
    ) {
        // Use the specified range boundaries for the histogram buckets
        const auto& allRanges = counterOptions->GetRanges(index);

        NMonitoring::TBucketBounds bucketBounds;
        bucketBounds.reserve(allRanges.size());

        for (const auto& range : allRanges) {
            bucketBounds.push_back(range.RangeVal);
        }

        return targetCounterGroup->GetNamedHistogram(
            "name",
            name,
            NMonitoring::ExplicitHistogram(bucketBounds),
            false /* derivative */
        );
    }

    /**
     * Create all target counters of the given counter type (simple, cumulative, percentile).
     *
     * @tparam TCounterOptions The type of the parsed enum options for target counters
     * @tparam TTargetCounters The type of the container with the target counters
     * @tparam CreateTargetCounter The function, which creates the given target counter
     *
     * @param[in] counterOptions The parsed enum options for the target counters
     * @param[in] targetCounterGroup The counter group where the target counters are created
     * @param[in,out] targetCounters The container where the target counters will be saved
     */
    template <
        class TCounterOptions,
        class TTargetCounters,
        decltype(TTargetCounters::value_type::TargetCounter) CreateTargetCounter(
            const TCounterOptions* counterOptions,
            NMonitoring::TDynamicCounterPtr targetCounterGroup,
            size_t index,
            const char* name
        )
    >
    void CreateTargetCountersForCounterType(
        const TCounterOptions* counterOptions,
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        TTargetCounters& targetCounters
    ) {
        targetCounters.reserve(counterOptions->Size);

        for (size_t i = 0; i < counterOptions->Size; ++i) {
            targetCounters.emplace_back().TargetCounter = CreateTargetCounter(
                counterOptions,
                targetCounterGroup,
                i,
                counterOptions->GetNames()[i]
            );
        }
    }
};

} // namespace NKikimr
