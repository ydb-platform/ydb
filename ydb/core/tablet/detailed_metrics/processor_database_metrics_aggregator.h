#pragma once

#include "detailed_metrics_binding.h"
#include "detailed_metrics_tree.h"

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {

    class TProcessorDatabaseMetricsAggregator: public TThrRefBase {
    public:
        virtual void ApplyFromNode(
            ui32 nodeId,
            bool isFollowerRole,
            const NProtoBuf::RepeatedPtrField<NKikimrSysView::TDetailedTableCounters>& tables) = 0;

        virtual void DropNode(ui32 nodeId) = 0;

        // Folds or restores the leaves first
        virtual void RecalculateAllCounters() = 0;

        /**
         * Sets the limit on the estimated series bytes, applied by the next RecalculateAllCounters.
         *
         * @param[in] prefixLabels The labels above targetCounterGroup, written in every series
         */
        virtual void SetOutputLimit(ui64 maxBytes, const NDetailedMetrics::TSubgroupPath& prefixLabels) = 0;

        // The estimated bytes of the unfolded output: the labels and values of its series, not their encoding
        virtual ui64 GetEstimatedOutputBytes() const = 0;
    };

    // RecalculateAllCounters calls with leaves in a row that fit, before a folded database publishes its leaves
    constexpr ui32 DETAILED_OUTPUT_RESTORE_TICKS = 12;

    using TProcessorDatabaseMetricsAggregatorPtr = TIntrusivePtr<TProcessorDatabaseMetricsAggregator>;

    using TDetailedMetricsDescriptorGetter = const TDetailedMetricsDescriptor* (*)(TTabletTypes::EType);

    /**
     * Creates an aggregator that publishes rolled-up and per-partition detailed metrics.
     *
     * The targetCounterGroup is attached under the SysView Processor's host="" / [monitoring_project_id] /
     * database and filled with the public counters, by metric name:
     *
     *     name=table.datashard.<m>
     *       table=T                                        rollup: leaders at TABLE, leaders + followers
     *                                                      at PARTITION, leader-only metrics from leaders only
     *     name=table.datashard.partition.<m>
     *       table=T / tablet_id=N / follower_id=0          every metric
     *       table=T / tablet_id=N / follower_id=F (F>0)    leader-only metrics absent
     *
     * Every TABLE partial and leaf feeds the public rollup, and leaves are published under tablet_id/follower_id.
     *
     * The aggregators of one budgetScope share the limit of SetOutputLimit on the estimated bytes of their series:
     * labels and values, without the overhead of a scrape format.
     * Rollups always publish. Over the limit, the databases with the most leaf bytes fold: their leaves keep
     * feeding the rollup unpublished. Each database decides in RecalculateAllCounters, assuming that the ones
     * with more leaf bytes fold too, so the output can stay over the limit until each has recalculated.
     * A database folded by the limit publishes its leaves again at 80% of the limit, after
     * DETAILED_OUTPUT_RESTORE_TICKS RecalculateAllCounters in a row; while between 80% and the limit, it stays
     * folded. A scoped aggregator starts folded and publishes within the limit after the same ticks.
     *
     * @param[in] budgetScope One budget per scope (a node's ydb_detailed group); null: private, starts unfolded
     * @param[in] getDescriptor Replaced by the tests only
     */
    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        NMonitoring::TDynamicCounterPtr budgetScope = nullptr,
        TDetailedMetricsDescriptorGetter getDescriptor = &GetDetailedMetricsDescriptor);

} // namespace NKikimr
