#include "manager.h"
#include "merge.h"

namespace NKikimr::NOlap::NReader {

TConclusionStatus TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NArrow::NMerger::TMergePartialStream merger(PKSchema, nullptr, false, VersionColumnNames);
    TFiltersBuilder filtersBuilder;
    for (const auto& [id, source] : SourcesById) {
        merger.AddSource(source.GetData(), source.GetFilter(), id);
        filtersBuilder.AddSource(id);
    }
    merger.DrainAll(filtersBuilder);
    THashMap<ui64, NArrow::TColumnFilter> filters = std::move(filtersBuilder).ExtractFilters();
    AFL_VERIFY(filters.size() == SourcesById.size())("filters", filters.size())("sources", SourcesById.size());
    for (const auto& [id, source] : SourcesById) {
        AFL_VERIFY(source.GetData()->GetRecordsCount() == TValidator::CheckNotNull(filters.FindPtr(id))->GetRecordsCount().value_or(0))(
                                                              "data", source.GetData()->GetRecordsCount())(
                                                              "filter", filters.FindPtr(id)->GetRecordsCount().value_or(0));
    }
    TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterIntervalResult(std::move(filters), IntervalIdx));
    return TConclusionStatus::Success();
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterIntervalResult(TConclusionStatus::Fail(reason), IntervalIdx));
}

}   // namespace NKikimr::NOlap::NReader
