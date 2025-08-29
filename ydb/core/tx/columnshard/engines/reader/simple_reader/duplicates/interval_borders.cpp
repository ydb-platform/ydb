#include "interval_borders.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/splitter.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

std::vector<TPortionsSlice> TFindIntervalBorders::FindForSource(const NArrow::TSimpleRow& keyStart, const NArrow::TSimpleRow& keyEnd, const std::shared_ptr<arrow::Schema>& schema)
{
    THashMap<ui64, NArrow::TFirstLastSpecialKeys> borders;
    borders.reserve(DataByPortion.size());
    for (const auto& [portionId, _] : DataByPortion) {
        const auto& portion = GetPortionVerified(portionId);
        borders.emplace(
            portionId, NArrow::TFirstLastSpecialKeys(portion->IndexKeyStart(), portion->IndexKeyEnd(), portion->IndexKeyStart().GetSchema()));
    }

    TColumnDataSplitter splitter(
        borders, NArrow::TFirstLastSpecialKeys(keyStart, keyEnd, schema));

    std::vector<TPortionsSlice> slices;
    slices.reserve(splitter.NumIntervals());
    for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
        slices.emplace_back(TPortionsSlice(splitter.GetIntervalFinish(i))).Reserve(DataByPortion.size());
    }

    for (const auto& [id, data] : DataByPortion) {
        auto intervals = splitter.SplitPortion(data);
        AFL_VERIFY(intervals.size() == splitter.NumIntervals());
        for (ui64 i = 0; i < splitter.NumIntervals(); ++i) {
            slices[i].AddRange(id, intervals[i]);
        }
    }

    return slices;
}

void TFindIntervalBorders::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    const auto& mainSource = GetPortionVerified(Context->GetRequest()->Get()->GetSourceId());
    std::vector<TPortionsSlice> mainResult = FindForSource(mainSource->IndexKeyStart(), mainSource->IndexKeyEnd(), mainSource->IndexKeyStart().GetSchema());

    std::vector<std::pair<TPortionInfo::TConstPtr, std::vector<TPortionsSlice>>> additionalResults;
    const auto& additionalSources = Context->GetAdditionalSources();
    if (additionalSources.size() > 0) {
        additionalResults.reserve(additionalSources.size());
        for (const auto& additionalSource : Context->GetAdditionalSources()) {
            if (additionalSource == mainSource) {
                continue;
            }
            additionalResults.emplace_back(additionalSource, FindForSource(additionalSource->IndexKeyStart(), additionalSource->IndexKeyEnd(), additionalSource->IndexKeyStart().GetSchema()));
        }
    }

    AFL_VERIFY(AllocationGuard);
    AFL_VERIFY(Owner);
    TActivationContext::AsActorContext().Send(Owner, new NPrivate::TEvFindIntervalsResult(std::move(mainResult), Context, std::move(AllocationGuard), std::move(DataByPortion), std::move(additionalResults)));
    Owner = TActorId();
}

void TFindIntervalBorders::DoOnCannotExecute(const TString& reason) {
    AFL_VERIFY(Owner);
    TActivationContext::AsActorContext().Send(Owner, new NPrivate::TEvFindIntervalsResult(TConclusionStatus::Fail(reason), Context, std::move(AllocationGuard), std::move(DataByPortion), {}));
    Owner = TActorId();
}

} // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering