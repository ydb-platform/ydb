#include "manager.h"
#include "merge.h"

namespace NKikimr::NOlap::NReader {

TConclusionStatus TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NArrow::NMerger::TMergePartialStream merger(PKSchema, nullptr, false, VersionColumnNames);
    for (ui64 i = 0; i < Sources.size(); ++i) {
        const auto& source = Sources[i];
        merger.AddSource(source.GetData(), source.GetFilter(), source.GetSourceId());
    }
    TFiltersBuilder filtersBuilder;
    merger.DrainAll(filtersBuilder);
    THashMap<ui64, NArrow::TColumnFilter> filters = std::move(filtersBuilder).ExtractFilters();
    AFL_VERIFY(filters.size() == Sources.size())("filters", filters.size())("sources", Sources.size());
    for (const auto& source : Sources) {
        AFL_VERIFY(source.GetData()->GetRecordsCount() == TValidator::CheckNotNull(filters.FindPtr(source.GetSourceId()))->GetRecordsCountVerified())(
                "data", source.GetData()->GetRecordsCount())("filter", filters.FindPtr(source.GetSourceId())->GetRecordsCountVerified());
    }
    TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterIntervalResult(std::move(filters), IntervalIdx));
    return TConclusionStatus::Success();
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterIntervalResult(TConclusionStatus::Fail(reason), IntervalIdx));
}

}   // namespace NKikimr::NOlap::NReader
