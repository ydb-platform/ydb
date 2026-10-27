#pragma once

#include "ydb_metrics_mapper.h"

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NKikimr {

    enum class ECumulativeHistoryPolicy {
        DiscardOnSourceRemoval,
        RetainOnSourceRemoval,
    };

    /**
     * The aggregator for the YDB metrics (for example, table.datashard.*),
     * which combines the same metrics from different sources into a single target value.
     *
     * @note Typically, this class is responsible for maintaining detailed metrics
     *       for the given tablet type. The detailed metrics are defined in the corresponding
     *       .proto file and are divided into 3 groups: simple, cumulative and histogram
     *       counters. This class creates exactly one target counter for each metric,
     *       defined in the .proto file. For example, the metrics may look like this:
     *
     *           * table.datashard.foo
     *           * table.datashard.bar
     *           * table.datashard.baz
     *
     *       The target counters always carry the Aggregate names.
     *
     *       In addition, this class takes any number of groups with source counters.
     *       Each source group defines the same metrics under the names of its own scope
     *       (see EYdbMetricNameScope): an Aggregate source as table.datashard.foo,
     *       a Partition source as table.datashard.partition.foo. A follower source may omit
     *       the LeaderOnly metrics; every other name must exist.
     *
     *       This class aggregates each metric across all source groups into the target counter
     *       of the Aggregate name. For example, it adds table.datashard.foo of every Aggregate
     *       source and table.datashard.partition.foo of every Partition source into
     *       table.datashard.foo of the target group.
     *
     *       In other words, this class takes M groups of N source counters
     *       and aggregates them into a single group of N counters.
     */
    class TYdbMetricsAggregator: public TThrRefBase {
    public:
        /**
         * Add a new group of source counters to the given group of target counters.
         *
         * @warning The target counters are NOT automatically recalculated by this
         *          function. To update the target counters to account for the new values,
         *          the RecalculateAllTargetCounters() function must be called explicitly
         *          after adding all the necessary source groups.
         *
         * @warning All source counters must exist in the given source counter group
         *          when this function is called.
         *
         * @param[in] sourceGroupId The ID of the source group to add
         * @param[in] sourceCounterGroup The counter group where the source counters are looked up
         * @param[in] isFollowerSource When true, skip LeaderOnly metrics when looking up source counters
         * @param[in] sourceNameScope The scope for the source metric names; determines which names
         *            are looked up in the sourceCounterGroup (Aggregate or Partition).
         *            The target counters always use Aggregate scope.
         */
        virtual void AddSourceCountersGroup(
            const TString& sourceGroupId,
            NMonitoring::TDynamicCounterPtr sourceCounterGroup,
            bool isFollowerSource = false,
            EYdbMetricNameScope sourceNameScope = EYdbMetricNameScope::Aggregate) = 0;

        /**
         * Remove an existing group of source counters from the given group of target counters.
         *
         * @warning The target counters are NOT automatically recalculated by this
         *          function. To update the target counters to account for the removed values,
         *          the RecalculateAllTargetCounters() function must be called explicitly
         *          after removing all the necessary source groups.
         *
         * @param[in] sourceGroupId The ID of the source group to remove
         *
         * @note RetainOnSourceRemoval saves the source's current cumulative values.
         *       Publish any pending source updates before calling this function.
         *       Reusing the source ID starts a new contribution in addition to that history.
         */
        virtual void RemoveSourceCountersGroup(const TString& sourceGroupId) = 0;

        /**
         * Recalculate the values of all target counters by aggregating the values
         * of the corresponding source counters.
         */
        virtual void RecalculateAllTargetCounters() = 0;
    };

    using TYdbMetricsAggregatorPtr = TIntrusivePtr<TYdbMetricsAggregator>;

    /**
     * Create an instance of the TYdbMetricsAggregator for metrics for the given tablet type.
     *
     * @note The target counters will be created in the given target group immediately.
     *
     * @param[in] tabletType The tablet type for which to create the TYdbMetricsAggregator class
     * @param[in] targetCounterGroup The counter group where the target (aggregated) counters are created
     * @param[in] cumulativeHistoryPolicy Whether removed sources retain their scalar cumulative
     *            contributions for the lifetime of this aggregator. Simple and histogram counters
     *            always aggregate only current sources.
     *
     * @return The corresponding instance of the TYdbMetricsAggregator class
     */
    TYdbMetricsAggregatorPtr CreateYdbMetricsAggregatorByTabletType(
        TTabletTypes::EType tabletType,
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        ECumulativeHistoryPolicy cumulativeHistoryPolicy = ECumulativeHistoryPolicy::DiscardOnSourceRemoval);

} // namespace NKikimr
