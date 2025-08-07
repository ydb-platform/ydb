#include "merge.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

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
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("task", "build_duplicate_filters");
    NArrow::NMerger::TMergePartialStream merger(Context->GetPKSchema(), nullptr, false, IIndexInfo::GetSnapshotColumnNames(), std::nullopt);
    merger.PutControlPoint(Context->GetMaxPK().BuildSortablePosition(), false);
    TFiltersBuilder filtersBuilder;
    for (const auto& [portionId, data] : ColumnData.ExtractDataByPortion()) {
        auto position = NArrow::NMerger::TRWSortableBatchPosition(data, 0, Context->GetPKSchema()->field_names(), {}, false);
        const auto findBound = NArrow::NMerger::TSortableBatchPosition::FindBound(
            position, 0, data->GetRecordsCount() - 1, Context->GetMinPK().BuildSortablePosition(), false);
        if (findBound) {
            merger.AddSource(data, nullptr, NArrow::NMerger::TIterationOrder::Forward(findBound->GetPosition()), portionId);
            filtersBuilder.AddSource(portionId);
        }
    }
    merger.DrainToControlPoint(filtersBuilder, true);
    Context->GetCounters()->OnRowsMerged(filtersBuilder.GetRowsAdded(), filtersBuilder.GetRowsSkipped(), 0);

    THashMap<ui64, NArrow::TColumnFilter> filtersBySource = std::move(filtersBuilder).ExtractFilters();
    THashMap<TDuplicateMapInfo, NArrow::TColumnFilter> filters;

    auto* result = filtersBySource.FindPtr(Context->GetRequest()->Get()->GetSourceId());
    AFL_VERIFY(result);
    Context->SetFilter(std::move(*result));
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    Context->Abort(reason);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
