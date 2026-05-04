#include "merge.h"
#include "private_events.h"

#include <ydb/core/formats/arrow/reader/merger.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

TMergeContext::TMergeContext(std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters, const bool reversed, const std::shared_ptr<TPortionStore>& portions, const std::map<ui32, std::shared_ptr<arrow::Field>>& fetchingColumns)
    : Merger(std::move(merger))
    , Counters(std::move(counters))
    , IsReversed(reversed)
    , Portions(portions)
    , FetchingColumns(fetchingColumns) {
}

TMergeBorders::TMergeBorders(const TActorId& owner, const std::shared_ptr<TMergeContext>& context, const TEvBordersConstructionResult::TPtr& event, const std::vector<NArrow::TSimpleRow>& readyBorders)
    : Owner(owner)
    , Context(context)
    , Event(event)
    , ReadyBorders(readyBorders) {
}

void TMergeBorders::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    auto columnData = Event->Get()->Result->ExtractDataByPortion(Context->FetchingColumns);
    for (const auto& [portionId, data] : columnData) {
        Context->Merger->AddSource(data, nullptr, Context->IsReversed ? NArrow::NMerger::TIterationOrder::Reversed(0) : NArrow::NMerger::TIterationOrder::Forward(0), portionId);
        Context->FiltersBuilder.AddSource(portionId, Context->Portions->GetPortionVerified(portionId)->GetRecordsCount());
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("component", "duplicates_manager")
            ("event", "TMergeBorders::DoExecute")
            ("type", "add_source")
            ("portion_id", portionId)
            ("records_count", data->GetRecordsCount())
            ("builder", Context->FiltersBuilder.DebugString());
    }

    AFL_VERIFY(Context->FiltersBuilder.CountSources() > 0 || ReadyBorders.empty());

    for (const auto& readyBorder: ReadyBorders) {
        Context->Merger->PutControlPoint(readyBorder.BuildSortablePosition(Context->IsReversed), false);
        Context->Merger->DrainToControlPoint(Context->FiltersBuilder, true);
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)
            ("component", "duplicates_manager")
            ("event", "TMergeBorders::DoExecute")
            ("type", "drain")
            ("border", readyBorder.BuildSortablePosition(Context->IsReversed).DebugString())
            ("builder", Context->FiltersBuilder.DebugString());
    }

    Context->Counters->OnRowsMerged(Context->FiltersBuilder.GetRowsAdded() - Context->PrevRowsAdded, Context->FiltersBuilder.GetRowsSkipped() - Context->PrevRowsSkipped, 0);
    Context->PrevRowsAdded = Context->FiltersBuilder.GetRowsAdded();
    Context->PrevRowsSkipped = Context->FiltersBuilder.GetRowsSkipped();

    TActivationContext::AsActorContext().Send(Owner,
        std::make_unique<TEvMergeBordersResult>(std::move(Event.Get()->Get()->Context), Context->FiltersBuilder.ExtractReadyFilters(), TConclusionStatus::Success()));
}

void TMergeBorders::DoOnCannotExecute(const TString& reason) {
    TActivationContext::AsActorContext().Send(Owner,
        std::make_unique<TEvMergeBordersResult>(std::move(Event.Get()->Get()->Context), THashMap<ui64, NArrow::TColumnFilter>{}, TConclusionStatus::Fail(reason)));
}

TString TMergeBorders::GetTaskClassIdentifier() const {
    return "BUILD_DUPLICATE_FILTERS";
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
