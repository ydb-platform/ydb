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

    TProcessorDatabaseMetricsAggregatorPtr CreateProcessorDatabaseMetricsAggregator(
        NMonitoring::TDynamicCounterPtr rawCounterGroup,
        NMonitoring::TDynamicCounterPtr targetCounterGroup,
        const TString& databasePath,
        THolder<TTabletCountersBase> executorCountersTemplate);

} // namespace NKikimr
