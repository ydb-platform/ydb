#include "query_metrics.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/processor_scan.h>

namespace NKikimr::NSysView {

using namespace NActors;

template <>
void SetField<0>(NKikimrSysView::TQueryMetricsKey& key, ui64 value) {
    key.SetIntervalEndUs(value);
}

template <>
void SetField<1>(NKikimrSysView::TQueryMetricsKey& key, ui32 value) {
    key.SetRank(value);
}

struct TQueryMetricsExtractorsMap :
    public std::unordered_map<NTable::TTag, TExtractorFunc<NKikimrSysView::TQueryMetricsEntry>>
{
    using S = Schema::QueryMetrics;
    using E = NKikimrSysView::TQueryMetricsEntry;

    TQueryMetricsExtractorsMap() {
        insert({S::IntervalEnd::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetKey().GetIntervalEndUs());
        }});
        insert({S::Rank::ColumnId, [] (const E& entry) {
            return TCell::Make<ui32>(entry.GetKey().GetRank());
        }});
        insert({S::QueryText::ColumnId, [] (const E& entry) {
            const auto& text = entry.GetQueryText();
            return TCell(text.data(), text.size());
        }});
        insert({S::Count::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetMetrics().GetCount());
        }});

#define ADD_METRICS(FieldName, MetricsName)                                             \
        insert({S::Sum ## FieldName::ColumnId, [] (const E& entry) {                    \
            return TCell::Make<ui64>(entry.GetMetrics().Get ## MetricsName().GetSum()); \
        }});                                                                            \
        insert({S::Min ## FieldName::ColumnId, [] (const E& entry) {                    \
            return TCell::Make<ui64>(entry.GetMetrics().Get ## MetricsName().GetMin()); \
        }});                                                                            \
        insert({S::Max ## FieldName::ColumnId, [] (const E& entry) {                    \
            return TCell::Make<ui64>(entry.GetMetrics().Get ## MetricsName().GetMax()); \
        }});

        ADD_METRICS(CPUTime, CpuTimeUs);
        ADD_METRICS(Duration, DurationUs);
        ADD_METRICS(ReadRows, ReadRows);
        ADD_METRICS(ReadBytes, ReadBytes);
        ADD_METRICS(UpdateRows, UpdateRows);
        ADD_METRICS(UpdateBytes, UpdateBytes);
        ADD_METRICS(DeleteRows, DeleteRows);
        ADD_METRICS(RequestUnits, RequestUnits);
#undef ADD_METRICS
    }
};

THolder<NActors::IActor> CreateQueryMetricsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    using TQueryMetricsScan = TProcessorScan<
        NKikimrSysView::TQueryMetricsEntry,
        NKikimrSysView::TEvGetQueryMetricsRequest,
        NKikimrSysView::TEvGetQueryMetricsResponse,
        TEvSysView::TEvGetQueryMetricsRequest,
        TEvSysView::TEvGetQueryMetricsResponse,
        TQueryMetricsExtractorsMap,
        ui64,
        ui32
    >;

    return MakeHolder<TQueryMetricsScan>(ownerId, scanId, tableId, tableRange, columns,
        NKikimrSysView::METRICS_ONE_MINUTE);
}

} // NKikimr::NSysView
