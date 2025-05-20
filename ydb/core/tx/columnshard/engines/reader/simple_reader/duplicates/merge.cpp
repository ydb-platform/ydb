#include "manager.h"
#include "merge.h"

namespace NKikimr::NOlap::NReader::NSimple {

void TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NArrow::NMerger::TMergePartialStream merger(PKSchema, nullptr, false, VersionColumnNames, MaxVersion);
    TFiltersBuilder filtersBuilder;
    for (const auto& [id, source] : SourcesById) {
        merger.AddSource(source.GetData().GetData()->GetData(), source.GetData().GetOffset(),
            source.GetData().GetOffset() + source.GetData().GetRowsCount(), source.GetFilter(), id);
        filtersBuilder.AddSource(id);
    }
    merger.DrainAll(filtersBuilder);
    THashMap<ui64, NArrow::TColumnFilter> filters = std::move(filtersBuilder).ExtractFilters();
    AFL_VERIFY(filters.size() == SourcesById.size())("filters", filters.size())("sources", SourcesById.size());
    for (const auto& [id, source] : SourcesById) {
        AFL_VERIFY(source.GetData().GetRowsCount() == TValidator::CheckNotNull(filters.FindPtr(id))->GetRecordsCount().value_or(0))(
                                                          "data", source.GetData().GetRowsCount())(
                                                          "filter", filters.FindPtr(id)->GetRecordsCount().value_or(0));
    }
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

}   // namespace NKikimr::NOlap::NReader::NSimple
