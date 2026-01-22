#include "merge.h"
#include "private_events.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TFiltersBuilder {
private:
    THashMap<ui64, NArrow::TColumnFilter> Filters;
    YDB_READONLY(ui64, RowsAdded, 0);
    YDB_READONLY(ui64, RowsSkipped, 0);
    bool IsDone = false;

    void AddImpl(const ui64 sourceId, const bool value) {
        auto* findFilter = Filters.FindPtr(sourceId);
        AFL_VERIFY(findFilter);
        findFilter->Add(value);
    }

public:
    void AddRecord(const NArrow::NMerger::TBatchIterator& cursor) {
        AddImpl(cursor.GetSourceId(), true);
        ++RowsAdded;
    }

    void SkipRecord(const NArrow::NMerger::TBatchIterator& cursor) {
        AddImpl(cursor.GetSourceId(), false);
        ++RowsSkipped;
    }

    void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& /*schema*/) const {
    }

    bool IsBufferExhausted() const {
        return false;
    }

    THashMap<ui64, NArrow::TColumnFilter>&& ExtractFilters() && {
        AFL_VERIFY(!IsDone);
        IsDone = true;
        return std::move(Filters);
    }

    void AddSource(const ui64 sourceId) {
        AFL_VERIFY(!IsDone);
        AFL_VERIFY(Filters.emplace(sourceId, NArrow::TColumnFilter::BuildAllowFilter()).second);
    }
};

void TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("task", "build_duplicate_filters")("info", DebugString());
    auto columnData = ColumnData.ExtractDataByPortion(Context.GetGlobalContext().GetColumns());

    NArrow::NMerger::TMergePartialStream merger(
        Context.GetGlobalContext().GetPKSchema(), nullptr, false, GetVersionColumnNames(), ScanSnapshotBatch, MinUncommittedSnapshotBatch);
    for (const auto& [portionId, data] : columnData) {
        merger.AddSource(data, nullptr, NArrow::NMerger::TIterationOrder::Forward(0), portionId);
    }

    THashMap<TDuplicateMapInfo, NArrow::TColumnFilter> filters;
    for (const auto& interval : Context.GetIntervals()) {
        for (auto&& [portionId, filter] : BuildFiltersOnInterval(interval, merger, columnData)) {
            AFL_VERIFY(filters.emplace(TDuplicateMapInfo(Context.GetGlobalContext().GetMaxVersion(), TIntervalBordersView(interval.GetBegin().MakeView(), interval.GetEnd().MakeView()), portionId), std::move(filter)).second);
        }
    }

    TActivationContext::AsActorContext().Send(Context.GetGlobalContext().GetOwner(),
        new NPrivate::TEvFilterConstructionResult(std::move(filters), Context.GetGlobalContext().MakeResultInFlightGuard()));
    Context.GetExecutor()->ScheduleNext(Context.ExtractGlobalContext());
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    TActivationContext::AsActorContext().Send(Context.GetGlobalContext().GetOwner(),
        new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(reason), Context.GetGlobalContext().MakeResultInFlightGuard()));
}

THashMap<ui64, NArrow::TColumnFilter> TBuildDuplicateFilters::BuildFiltersOnInterval(const TIntervalInfo& interval,
    NArrow::NMerger::TMergePartialStream& merger, const THashMap<ui64, std::shared_ptr<NArrow::TGeneralContainer>>& columnData) {
    merger.SkipToBound(*interval.GetBegin().GetKey(), !interval.GetBegin().GetIsLast());

    AFL_VERIFY(!interval.IsEmpty());
    if (interval.IsExclusive()) {
        THashMap<ui64, NArrow::TColumnFilter> result;
        for (const auto& [portionId, _] : columnData) {
            result.emplace(portionId, NArrow::TColumnFilter::BuildAllowFilter());
        }
        const ui64 recordsOnInterval = merger.SkipToBound(*interval.GetEnd().GetKey(), !interval.GetEnd().GetIsLast());
        NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();
        filter.Add(true, recordsOnInterval);
        result.insert_or_assign(interval.GetExclusivePortionId(), std::move(filter));
        Context.GetGlobalContext().GetCounters()->OnRowsMerged(0, 0, recordsOnInterval);
        return result;
    }

    TFiltersBuilder filtersBuilder;
    for (const auto& [portionId, _] : columnData) {
        filtersBuilder.AddSource(portionId);
    }
    merger.PutControlPoint(*interval.GetEnd().GetKey(), false);
    merger.DrainToControlPoint(filtersBuilder, interval.GetEnd().GetIsLast());
    Context.GetGlobalContext().GetCounters()->OnRowsMerged(filtersBuilder.GetRowsAdded(), filtersBuilder.GetRowsSkipped(), 0);
    return std::move(filtersBuilder).ExtractFilters();
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
