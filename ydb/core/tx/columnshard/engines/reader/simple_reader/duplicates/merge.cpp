#include "merge.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/events.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TFiltersBuilder {
private:
    THashMap<ui64, NArrow::TColumnFilter> Filters;
    YDB_READONLY(ui64, RowsAdded, 0);
    YDB_READONLY(ui64, RowsSkipped, 0);

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
        return std::move(Filters);
    }

    void AddSource(const ui64 sourceId) {
        AFL_VERIFY(Filters.emplace(sourceId, NArrow::TColumnFilter::BuildAllowFilter()).second);
    }
};

void TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NArrow::NMerger::TMergePartialStream merger(PKSchema, nullptr, false, VersionColumnNames, MaxVersion);
    merger.PutControlPoint(Finish.BuildSortablePosition(), false);
    TFiltersBuilder filtersBuilder;
    for (const auto& [interval, data] : SourcesById) {
        merger.AddSource(data->GetData(), nullptr, NArrow::NMerger::TIterationOrder::Forward(interval.GetOffset()), interval.GetSourceId());
        filtersBuilder.AddSource(interval.GetSourceId());
    }
    merger.DrainToControlPoint(filtersBuilder, IncludeFinish);
    Counters.OnRowsMerged(filtersBuilder.GetRowsAdded(), filtersBuilder.GetRowsSkipped(), 0);

    THashMap<ui64, NArrow::TColumnFilter> filtersBySource = std::move(filtersBuilder).ExtractFilters();
    THashMap<TDuplicateMapInfo, NArrow::TColumnFilter> filters;
    for (auto&& [interval, data] : SourcesById) {
        NArrow::TColumnFilter* findFilter = filtersBySource.FindPtr(interval.GetSourceId());
        AFL_VERIFY(findFilter);
        filters.emplace(interval, std::move(*findFilter));
    }

    AFL_VERIFY(Owner);
    TActivationContext::AsActorContext().Send(Owner, new TEvFilterConstructionResult(std::move(filters)));
    Owner = TActorId();
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    AFL_VERIFY(Owner);
    TActivationContext::AsActorContext().Send(Owner, new TEvFilterConstructionResult(TConclusionStatus::Fail(reason)));
    Owner = TActorId();
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
