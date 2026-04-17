#pragma once

#include <util/generic/string.h>

#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/testlib/basics/runtime.h>

namespace NKikimr {

namespace NDetailedMetricsTests {

/**
 * Normalizes the given JSON to be well formatted with all keys sorted.
 *
 * @warning This function sorts only maps by the key value. It does not sort
 *          items in arrays at all. Luckily, all counters and groups in TDynamicCounters
 *          are stored in SORTED maps, which means that the array of sensors
 *          is always inherently sorted in a stable order. This makes it safe
 *          to compare sensor arrays directly without sorting them.
 *
 * @param[in] jsonString The JSON to normalize (as a string)
 * @param[in] removeSensorsByLabels If specified, sensors with the given labels are removed
 *                                  from the given JSON (the key is the label name,
 *                                  the value is the set of label values to remove)
 *
 * @return The corresponding normalized JSON
 */
TString NormalizeJson(
    const TString& jsonString,
    const std::unordered_map<TString, std::unordered_set<TString>>& removeSensorsByLabels = {}
);

/**
 * Create and initialize the Tablet Counters Aggregator actor.
 *
 * @param[in] runtime The test runtime
 * @param[in] forFollowers Indicates if the aggregator should process leaders or followers data
 *
 * @return The ID of the Tablet Counters Aggregator actor
 */
NActors::TActorId InitializeTabletCountersAggregator(
    NActors::TTestBasicRuntime& runtime,
    bool forFollowers
);

/**
 * Send low level metrics to the Tablet Counters Aggregator for the given DataShard tablet.
 *
 * @note This function generates all metric values with an offset of 10000.
 *       The lower 3 digits are supposed to be used for tablet IDs. For example,
 *       using non-overlapping values like 1, 20, 300 allows tests to verify
 *       that metric values from different tablet IDs are correctly aggregated
 *       correctly (1 + 20 + 300 = 123).
 *
 * @param[in] runtime The test runtime
 * @param[in] aggregatorId The ID of the Tablet Counters Aggregator actor
 * @param[in] edgeActorId The ID of the edge actor used for sending all messages
 * @param[in] tabletId The ID of the DataShard tablet to send the metrics for
 * @param[in] followerId The ID of the follower to send the metrics for
 * @param[in] cpuLoadPercentage The CPU load percentage to report for this tablet
 * @param[in] tableMetricsConfig The metrics configuration for the table (if needed)
 * @param[in] metricsOffset The offset for all metrics values (if not set, tabletId is used)
 */
void SendDataShardMetrics(
    NActors::TTestBasicRuntime& runtime,
    const NActors::TActorId& aggregatorId,
    const NActors::TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId,
    ui32 cpuLoadPercentage,
    TEvTabletCounters::TTableMetricsConfig* tableMetricsConfig = nullptr,
    std::optional<ui64> metricsOffset = {}
);

/**
 * Send a message to the Tablet Counters Aggregator to forget the given Data Shard tablet.
 *
 * @param[in] runtime The test runtime
 * @param[in] aggregatorId The ID of the Tablet Counters Aggregator actor
 * @param[in] edgeActorId The ID of the edge actor used for sending all messages
 * @param[in] tabletId The ID of the DataShard tablet to forget
 * @param[in] followerId The ID of the follower to forget
 */
void SendForgetDataShardTablet(
    NActors::TTestBasicRuntime& runtime,
    const NActors::TActorId& aggregatorId,
    const NActors::TActorId& edgeActorId,
    ui64 tabletId,
    ui32 followerId
);

} // namespace NDetailedMetricsTests

} // namespace NKikimr
