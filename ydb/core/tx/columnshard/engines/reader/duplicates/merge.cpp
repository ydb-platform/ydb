#include "manager.h"
#include "merge.h"

namespace NKikimr::NOlap::NReader {

TConclusionStatus TBuildDuplicateFilters::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NArrow::NMerger::TMergePartialStream merger(PKSchema, nullptr, false, VersionColumnNames);
    for (ui64 i = 0; i < Sources.size(); ++i) {
        const auto& source = Sources[i];
        merger.AddSource(source.GetData(), source.GetFilter(), i);
    }
    TFiltersBuilder filtersBuilder(Sources.size());
    merger.DrainAll(filtersBuilder);
    std::vector<NArrow::TColumnFilter> filters = std::move(filtersBuilder).ExtractFilters();
    AFL_VERIFY(filters.size() == Sources.size());
    // TODO: avoid copying filters
    THashMap<ui64, NArrow::TColumnFilter> result;
    for (ui64 i = 0; i < filters.size(); ++i) {
        AFL_VERIFY(Sources[i].GetData()->GetRecordsCount() == filters[i].GetRecordsCountVerified())(
                                                                  "data", Sources[i].GetData()->GetRecordsCount())(
                                                                  "filter", filters[i].GetRecordsCountVerified());
        result.emplace(Sources[i].GetSourceId(), std::move(filters[i]));
    }
    TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterIntervalResult(std::move(result), IntervalIdx));
    return TConclusionStatus::Success();
}

void TBuildDuplicateFilters::DoOnCannotExecute(const TString& reason) {
    TActorContext::AsActorContext().Send(Owner, new TEvDuplicateFilterIntervalResult(TConclusionStatus::Fail(reason), IntervalIdx));
}

}   // namespace NKikimr::NOlap::NReader
