#pragma once

#include "detailed_metrics_descriptor.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/sys_view.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {

/**
 * The public (YDB) metrics of one bucket (a TABLE partial or a PARTITION leaf),
 * created from the descriptor of the tablet type the same way as the YDB metrics
 * mapper creates its target counters (see TYdbMetricsTargetCountersBase): the same
 * names, the same "name" label, the same derivative flags and the same bucket bounds.
 *
 * The index in Gauges, Rates and Histograms is the index of the metric
 * in the descriptor (the enum value and the wire slot).
 *
 * @note Every metric, which is not skipped as LeaderOnly, gets its target,
 *       even if its specification failed the validation: the rollup
 *       (TYdbMetricsAggregator) looks up every such series by name.
 *       A histogram, whose bounds cannot make an explicit histogram (no bounds,
 *       unsorted bounds or more than NMonitoring::HISTOGRAM_MAX_BUCKETS_COUNT bounds),
 *       gets the single placeholder bound 0 instead of its bounds.
 */
struct TPublicTargets {
    /**
     * Create the target counters in the given group.
     *
     * @param[in] descriptor The descriptor of the tablet type
     * @param[in] group The counter group where the target counters are created
     * @param[in] scope The scope for the published metric names: Partition for a leaf,
     *            Aggregate for a TABLE partial
     * @param[in] skipLeaderOnly When true (a follower leaf), the LeaderOnly metrics
     *            get no target and their slots stay null
     */
    TPublicTargets(
        const TDetailedMetricsDescriptor& descriptor,
        NMonitoring::TDynamicCounterPtr group,
        EYdbMetricNameScope scope,
        bool skipLeaderOnly);

    /**
     * The gauges (not derivative), null for a skipped LeaderOnly metric.
     */
    TVector<NMonitoring::TDynamicCounters::TCounterPtr> Gauges;

    /**
     * The rates (derivative), null for a skipped LeaderOnly metric.
     */
    TVector<NMonitoring::TDynamicCounters::TCounterPtr> Rates;

    /**
     * The explicit histograms over the public bounds (+Inf is implicit),
     * null for a skipped LeaderOnly metric.
     */
    TVector<NMonitoring::THistogramPtr> Histograms;
};

/**
 * The state of one public metrics bucket in the SysView Processor (a TABLE partial
 * or a PARTITION leaf), which combines the public metric values reported by the nodes
 * and publishes them to its targets.
 *
 * Every node reports the public values of the bucket as NKikimrSysView::TDbCounters,
 * slot i of Simple, Cumulative and Histogram being the metric i of the descriptor:
 * - a gauge is Simple[i], the absolute value on the node, every report replaces
 *   the previous one of the node; a leaf takes the maximum over the nodes (a partition
 *   move overlaps two nodes for a while), a TABLE partial sums the nodes up (takes
 *   the maximum, if the metric is CombineByMax); a missing slot is zero;
 * - a rate is a sparse (i, delta) pair in Cumulative, the deltas of every node are
 *   summed up (modulo 2^64) for as long as the bucket lives, a removed node keeps
 *   its contribution;
 * - a level histogram (TMetricSpec::IsLevel) is Histogram[i] marked NonDerivative,
 *   sparse (bucket, count) pairs of the full current value on the node, every report
 *   replaces the previous one of the node (a missing or an empty entry is an empty
 *   histogram), the nodes are summed up;
 * - an increment histogram is Histogram[i], not marked NonDerivative, sparse
 *   (bucket, delta) pairs, which are summed up the same way as the rates.
 *
 * Whether a histogram holds a level or increments is decided by the descriptor,
 * not by the NonDerivative mark of the payload:
 * - an unmarked payload of a StaticLevel histogram (a delta, which cannot be applied
 *   without the baseline it was taken against) clears the level of the node;
 * - any other payload, whose mark disagrees with the descriptor, is ignored.
 * Both cases add a warning once per bucket (see TakeWarnings()).
 *
 * The payload comes from a remote node, so it is never trusted: the extra slots,
 * pairs and buckets are ignored, an odd tail of pairs is ignored, the bucket counts
 * of the payload are clamped by the descriptor, nothing is sized by the payload
 * and nothing aborts.
 *
 * @note The bucket depends on the descriptor and the wire format only (no actor code),
 *       the descriptor must outlive the bucket.
 */
class TPublicBucket {
public:
    /**
     * Create the bucket and its targets.
     *
     * @param[in] descriptor The descriptor of the tablet type
     * @param[in] group The counter group where the targets are created
     * @param[in] isPartitionBucket True for a PARTITION leaf (Partition scope names,
     *            gauges take the maximum over the nodes), false for a TABLE partial
     *            (Aggregate scope names, gauges are summed up over the nodes)
     * @param[in] skipLeaderOnly True for a follower leaf: no targets for the LeaderOnly metrics
     */
    TPublicBucket(
        const TDetailedMetricsDescriptor& descriptor,
        NMonitoring::TDynamicCounterPtr group,
        bool isPartitionBucket,
        bool skipLeaderOnly);

    /**
     * Apply the public values of the bucket reported by the given node.
     *
     * @param[in] nodeId The node, which reported the values
     * @param[in] values The public values (see the class comment for the encoding)
     */
    void Apply(ui32 nodeId, const NKikimrSysView::TDbCounters& values);

    /**
     * Remove the gauges and the level histograms of the given node.
     * The rates and the increment histograms of the node are kept.
     *
     * @param[in] nodeId The node to remove
     *
     * @return True if no node is left in the bucket
     */
    bool DropNode(ui32 nodeId);

    /**
     * Publish the current values of the bucket to its targets.
     */
    void Publish();

    /**
     * Take the warnings about the payloads added since the previous call
     * (at most one per kind of problem for the lifetime of the bucket).
     */
    TVector<TString> TakeWarnings();

    /**
     * @return The targets of the bucket
     */
    const TPublicTargets& GetTargets() const {
        return Targets;
    }

    /**
     * @return The number of bytes held by the bucket itself, its heap allocations
     *         included, but the monlib counters of its targets excluded
     */
    size_t GetAllocatedBytes() const;

private:
    /**
     * The latest absolute values reported by one node.
     */
    struct TNodeLevels {
        ui32 NodeId = 0;

        /**
         * The gauges, the index is the metric.
         */
        TVector<ui64> Gauges;

        /**
         * The bucket counts of the level histograms, the index is the metric
         * (empty for an increment histogram).
         */
        TVector<TVector<ui64>> LevelHists;
    };

    TNodeLevels& GetOrAddNode(ui32 nodeId);

    void ApplyHistogram(
        ui32 nodeId,
        size_t index,
        const NKikimrSysView::TDbCounters::THistogram& histogram,
        TNodeLevels& node);

    const TDetailedMetricsDescriptor* const Desc;
    const bool IsPartitionBucket;

    /**
     * The sum of the deltas of every rate, the index is the metric.
     */
    TVector<ui64> RateTotals;

    /**
     * The sum of the deltas of every increment histogram, the index is the metric
     * (empty for a level histogram).
     */
    TVector<TVector<ui64>> IncrementTotals;

    /**
     * The latest absolute values of every node, which reports the bucket (usually one).
     */
    TVector<TNodeLevels> PerNode;

    TPublicTargets Targets;

    TVector<TString> Warnings;
    bool WarnedUnmarkedLevel = false;
    bool WarnedMarkMismatch = false;
};

} // namespace NKikimr
