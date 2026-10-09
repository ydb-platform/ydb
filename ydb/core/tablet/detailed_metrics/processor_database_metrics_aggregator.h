#pragma once

#include "detailed_metrics_binding.h"

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
    };

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
     * @param[in] getDescriptor Replaced by the tests only
     */
    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        TDetailedMetricsDescriptorGetter getDescriptor = &GetDetailedMetricsDescriptor);

} // namespace NKikimr
