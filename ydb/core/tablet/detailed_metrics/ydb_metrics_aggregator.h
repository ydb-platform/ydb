#pragma once

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NKikimr {

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
 *       In addition, this class takes any number of groups with source counters.
 *       Each source group is assumed to define exactly the same counters
 *       (table.datashard.foo, table.datashard.bar and so on).
 *
 *       This class aggregates each counter across all source groups into the corresponding
 *       target counter. In the example above, it takes the table.datashard.foo counter
 *       across all source groups, adds all values together and stores the result
 *       in the table.datashard.foo counter in the target group using the same name.
 *       This process is repeated for table.datashard.bar and table.datashard.baz.
 *
 *       In other words, this class takes M groups of N source counters
 *       and aggregates them into a single group of N counters.
 *
 * @note Instances of this class can be stacked on top of each other. For example,
 *       one instance may be used to aggregate all detailed metrics from all followers
 *       (and the leader) into a single set of counters corresponding
 *       to the entire partition. Then another instance of this class may be used
 *       to aggregate all detailed metrics from all partitions into a single set
 *       of counters corresponding to the entire table.
 */
class TYdbMetricsAggregator : public TThrRefBase {
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
     */
    virtual void AddSourceCountersGroup(
        const TString& sourceGroupId,
        NMonitoring::TDynamicCounterPtr sourceCounterGroup
    ) = 0;

    /**
     * Remove an existing group of source counters from the given group of target counters.
     *
     * @warning The target counters are NOT automatically recalculated by this
     *          function. To update the target counters to account for the removed values,
     *          the RecalculateAllTargetCounters() function must be called explicitly
     *          after removing all the necessary source groups.
     *
     * @param[in] sourceGroupId The ID of the source group to remove
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
 *
 * @return The corresponding instance of the TYdbMetricsAggregator class
 */
TYdbMetricsAggregatorPtr CreateYdbMetricsAggregatorByTabletType(
    TTabletTypes::EType tabletType,
    NMonitoring::TDynamicCounterPtr targetCounterGroup
);


} // namespace NKikimr
