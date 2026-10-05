#pragma once

#include <ydb/core/base/tablet_types.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

namespace NKikimr {

/**
 * Scope for the metric names: whether the published name is the aggregate name
 * (the whole-table rollup, and also what the ydb/ydb_serverless database-wide
 * groups publish) or the partition-level leaf name.
 */
enum class EYdbMetricNameScope {
    /**
     * Aggregate name, as defined in the .proto. This is the scope used for
     * the table rollup and for database-wide groups.
     */
    Aggregate,
    /**
     * Partition-level leaf name. The name has ".partition." inserted
     * after the family prefix (e.g., "table.datashard." -> "table.datashard.partition.").
     * This ensures the name alone decides the population, so a SUM never mixes
     * the rollup with its own leaves.
     */
    Partition,
};

/**
 * Build the published metric name for the given scope.
 *
 * For Aggregate, returns the protoName unchanged.
 * For Partition, inserts "partition." right after the family prefix, the way PQ pairs
 * an object-level topic.<m> with its per-partition topic.partition.<m>
 * (topic.write.bytes -> topic.partition.write.bytes). The PQ family prefix is one
 * segment ("topic."), the DataShard one is two ("table.datashard."), so the segment
 * goes after the second dot:
 * "table.datashard.row_count" -> "table.datashard.partition.row_count",
 * "table.datashard.read.rows" -> "table.datashard.partition.read.rows".
 *
 * @note This deviates from the design document, which keeps the same name at every
 *       level: with one name, a label-superset SUM would add the rollup to its own leaves.
 *
 * @param[in] protoName The metric name as defined in the .proto file
 * @param[in] scope The scope (Aggregate or Partition)
 * @return The published metric name for the given scope
 */
TString MakeYdbMetricName(TStringBuf protoName, EYdbMetricNameScope scope);

/**
 * The mapper from tablet/executor metrics to the corresponding YDB metrics
 * (for example, table.datashard.*).
 */
class TYdbMetricsMapper : public TThrRefBase {
public:
    /**
     * Transfer values from the source counters to the corresponding target counters.
     */
    virtual void TransferCounterValues() = 0;
};

using TYdbMetricsMapperPtr = TIntrusivePtr<TYdbMetricsMapper>;

/**
 * Create an instance of the TYdbMetricsMapper for metrics for the given tablet type.
 *
 * @note The target counters will be created in the given target group immediately.
 *       The source counters will be looked up in the source group only when needed.
 *       If source counters are not present when the counters need to be transferred,
 *       no values will be transferred and no errors will be generated. In this case,
 *       the source counters will be looked up again during the next transfer attempt.
 *       The above statement applies only if all counters for the given source
 *       tablet type are missing (no updates received yet). However, if some source counters
 *       are already present and some source counters are not (at least one update
 *       has already been received), then the missing counters will be ignored.
 *
 *       This allows the source counters to be created asynchronously at some point
 *       in the future. Once the source counters are created, the target counters
 *       will be populated with the corresponding values.
 *
 * @param[in] tabletType The tablet type for which to create the TYdbMetricsMapper class
 * @param[in] targetCounterGroup The counter group where the target (mapped) counters are created
 * @param[in] sourceCounterGroup The counter group where the source counters are looked up
 * @param[in] nameScope The scope for the published metric names (Aggregate or Partition);
 *            defaults to Aggregate to keep the ydb/ydb_serverless callers byte-identical
 * @param[in] isFollowerSource When true, the mapper creates no target for LeaderOnly metrics
 *            (followers never write, so those would be series that are always 0)
 *
 * @return The corresponding instance of the TYdbMetricsMapper class
 */
TYdbMetricsMapperPtr CreateYdbMetricsMapperByTabletType(
    TTabletTypes::EType tabletType,
    NMonitoring::TDynamicCounterPtr targetCounterGroup,
    NMonitoring::TDynamicCounterPtr sourceCounterGroup,
    EYdbMetricNameScope nameScope = EYdbMetricNameScope::Aggregate,
    bool isFollowerSource = false
);

} // namespace NKikimr
