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

        virtual void RecalculateAllCounters() = 0;

        /**
         * Sets the output limit of the shared budget and folds at once if over it.
         *
         * @param[in] restorePercent A budget fold restores at or below this percent of maxBytes
         * @param[in] prefixLabels The labels above targetCounterGroup, written in every series
         */
        virtual void SetOutputLimit(
            ui64 maxBytes, ui64 restorePercent, const NDetailedMetrics::TSubgroupPath& prefixLabels) = 0;

        // Folds at once if over the limit; never restores
        virtual void FitOutput() = 0;

        // The estimated JSON bytes of the unfolded output
        virtual ui64 GetEstimatedOutputBytes() const = 0;
    };

    // RecalculateAllCounters calls in a row that fit, before a folded database restores
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
     * The aggregators of one budgetScope share the limit of SetOutputLimit on their estimated JSON output.
     * Over it, the databases with the most leaf bytes fold: their leaves keep feeding the rollup unpublished.
     * Folding is immediate; restoring takes DETAILED_OUTPUT_RESTORE_TICKS fitting RecalculateAllCounters
     * in a row. A scoped aggregator starts folded.
     *
     * @param[in] getDescriptor Replaced by the tests only
     * @param[in] budgetScope One budget per scope (a node's ydb_detailed group); null: private, starts unfolded
     */
    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        TDetailedMetricsDescriptorGetter getDescriptor = &GetDetailedMetricsDescriptor,
        NMonitoring::TDynamicCounterPtr budgetScope = nullptr);

} // namespace NKikimr
