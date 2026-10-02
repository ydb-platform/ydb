#pragma once

#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet/tablet_counters.h>

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
    };

    using TProcessorDatabaseMetricsAggregatorPtr = TIntrusivePtr<TProcessorDatabaseMetricsAggregator>;

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
     * The aggregator keeps only the public metric values of every TABLE partial and leaf (see TPublicBucket),
     * no low level counters: the low level counters reported by the nodes are converted into the public
     * metric values of the descriptor of the tablet type (see TDetailedMetricsDescriptor) as they arrive.
     *
     * @param[in] targetCounterGroup The counter group for the public counters
     * @param[in] databasePath The path of the database, the table paths of the reports are relative to it
     * @param[in] executorCountersTemplate The Executor counters, whose layout the nodes report
     *            (the application counters come from the tablet type), must not be null
     */
    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        THolder<TTabletCountersBase> executorCountersTemplate);

} // namespace NKikimr
