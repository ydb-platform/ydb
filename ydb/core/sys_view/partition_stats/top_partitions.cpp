#include "top_partitions.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/processor_scan.h>

namespace NKikimr::NSysView {

using namespace NActors;

template <>
void SetField<0>(NKikimrSysView::TTopPartitionsKey& key, ui64 value) {
    key.SetIntervalEndUs(value);
}

template <>
void SetField<1>(NKikimrSysView::TTopPartitionsKey& key, ui32 value) {
    key.SetRank(value);
}

struct TTopPartitionsExtractorMap :
    public std::unordered_map<NTable::TTag, TExtractorFunc<NKikimrSysView::TTopPartitionsEntry>>
{
    using S = Schema::TopPartitions;
    using E = NKikimrSysView::TTopPartitionsEntry;

    TTopPartitionsExtractorMap() {
        insert({S::IntervalEnd::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetKey().GetIntervalEndUs());
        }});
        insert({S::Rank::ColumnId, [] (const E& entry) {
            return TCell::Make<ui32>(entry.GetKey().GetRank());
        }});
        insert({S::TabletId::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetInfo().GetTabletId());
        }});
        insert({S::Path::ColumnId, [] (const E& entry) {
            const auto& text = entry.GetInfo().GetPath();
            return TCell(text.data(), text.size());
        }});
        insert({S::PeakTime::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetInfo().GetPeakTimeUs());
        }});
        insert({S::CPUCores::ColumnId, [] (const E& entry) {
            return TCell::Make<double>(entry.GetInfo().GetCPUCores());
        }});
        insert({S::NodeId::ColumnId, [] (const E& entry) {
            return TCell::Make<ui32>(entry.GetInfo().GetNodeId());
        }});
        insert({S::DataSize::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetInfo().GetDataSize());
        }});
        insert({S::RowCount::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetInfo().GetRowCount());
        }});
        insert({S::IndexSize::ColumnId, [] (const E& entry) {
            return TCell::Make<ui64>(entry.GetInfo().GetIndexSize());
        }});
        insert({S::InFlightTxCount::ColumnId, [] (const E& entry) {
            return TCell::Make<ui32>(entry.GetInfo().GetInFlightTxCount());
        }});
    }
};

THolder<NActors::IActor> CreateTopPartitionsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns)
{
    using TTopPartitionsScan = TProcessorScan<
        NKikimrSysView::TTopPartitionsEntry,
        NKikimrSysView::TEvGetTopPartitionsRequest,
        NKikimrSysView::TEvGetTopPartitionsResponse,
        TEvSysView::TEvGetTopPartitionsRequest,
        TEvSysView::TEvGetTopPartitionsResponse,
        TTopPartitionsExtractorMap,
        ui64,
        ui32
    >;

    auto viewName = tableId.SysViewInfo;

    if (viewName == TopPartitions1MinuteName) {
        return MakeHolder<TTopPartitionsScan>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_PARTITIONS_ONE_MINUTE);

    } else if (viewName == TopPartitions1HourName) {
        return MakeHolder<TTopPartitionsScan>(ownerId, scanId, tableId, tableRange, columns,
            NKikimrSysView::TOP_PARTITIONS_ONE_HOUR);
    }
    return {};
}

} // NKikimr::NSysView
