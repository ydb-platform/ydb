#include "merge.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

void TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NArrow::NMerger::TMergePartialStream merger(PKSchema, nullptr, false, VersionColumnNames, MaxVersion);
    merger.PutControlPoint(Finish.BuildSortablePosition(), false);
    TFiltersBuilder filtersBuilder;
    for (const auto& [id, source] : SourcesById) {
        merger.AddSource(
            source.GetBatch()->GetData(), NArrow::NMerger::TIterationOrder::Forward(source.GetOffset()), nullptr, id);
        filtersBuilder.AddSource(id);
    }
    merger.DrainToControlPoint(filtersBuilder, IncludeFinish);
    THashMap<ui64, NArrow::TColumnFilter> filters = std::move(filtersBuilder).ExtractFilters();
    AFL_VERIFY(filters.size() == SourcesById.size())("filters", filters.size())("sources", SourcesById.size());
    AFL_VERIFY(Callback);
    Callback->OnResult(std::move(filters));
    Callback.reset();
    Counters.OnRowsMerged(filtersBuilder.GetRowsAdded(), filtersBuilder.GetRowsSkipped(), 0);
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    AFL_VERIFY(Callback);
    Callback->OnFailure(reason);
    Callback.reset();
}

TBuildDuplicateFilters::TBuildDuplicateFilters(const std::shared_ptr<arrow::Schema>& sortingSchema,
    const std::optional<NArrow::NMerger::TCursor>& maxVersion, const NArrow::TSimpleRow& finish, const bool includeFinish,
    const NColumnShard::TDuplicateFilteringCounters& counters, std::unique_ptr<ISubscriber>&& callback)
    : PKSchema(sortingSchema)
    , VersionColumnNames(IIndexInfo::GetSnapshotColumnNames())
    , Counters(counters)
    , MaxVersion(maxVersion)
    , Finish(finish)
    , IncludeFinish(includeFinish)
    , Callback(std::move(callback)) {
    AFL_VERIFY(Callback);
    AFL_VERIFY(finish.GetSchema()->Equals(sortingSchema));
}
}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
