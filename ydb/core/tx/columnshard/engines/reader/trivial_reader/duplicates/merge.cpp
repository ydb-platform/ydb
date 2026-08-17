#include "merge.h"
#include "private_events.h"

#include <ydb/core/formats/arrow/reader/merger.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

TMergeContext::TMergeContext(std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters, const bool reversed,
    const std::shared_ptr<TPortionStore>& portions, const std::map<ui32, std::shared_ptr<arrow::Field>>& fetchingColumns,
    const std::shared_ptr<const TAtomicCounter>& abortionFlag, std::shared_ptr<arrow::Schema> pkSchema,
    std::vector<std::string> versionColumnNames, NArrow::NMerger::TCursor maxVersion, NArrow::NMerger::TCursor minUncommittedVersion)
    : Counters(std::move(counters))
    , IsReversed(reversed)
    , Portions(portions)
    , FetchingColumns(fetchingColumns)
    , AbortionFlag(abortionFlag)
    , PKSchema(std::move(pkSchema))
    , VersionColumnNames(std::move(versionColumnNames))
    , MaxVersion(std::move(maxVersion))
    , MinUncommittedVersion(std::move(minUncommittedVersion))
{
    AFL_VERIFY(!!PKSchema);
}

std::unique_ptr<NArrow::NMerger::TMergePartialStream> TMergeContext::MakeMerger() const {
    return std::make_unique<NArrow::NMerger::TMergePartialStream>(
        PKSchema, nullptr, IsReversed, VersionColumnNames, MaxVersion, MinUncommittedVersion);
}

TMergeBorders::TMergeBorders(const TActorId& owner, const std::shared_ptr<TMergeContext>& context, TMergeRuntimeState&& state,
    const TEvBordersConstructionResult::TPtr& event, const std::vector<NArrow::TSimpleRow>& readyBorders)
    : Owner(owner)
    , Context(context)
    , State(std::move(state))
    , Event(event)
    , ReadyBorders(readyBorders)
{
}

void TMergeBorders::SendResult(THashMap<ui64, NArrow::TColumnFilter>&& readyFilters, TConclusionStatus&& conclusion) {
    TActivationContext::AsActorContext().Send(Owner, std::make_unique<TEvMergeBordersResult>(std::move(Event.Get()->Get()->Context),
                                                         std::move(State), std::move(readyFilters), std::move(conclusion)));
}

void TMergeBorders::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    auto sendFailure = [&](const TString& reason) {
        SendResult(THashMap<ui64, NArrow::TColumnFilter>{}, TConclusionStatus::Fail(reason));
    };

    if (Context->IsAborted()) {
        sendFailure("duplicate filter merge aborted");
        return;
    }

    auto& result = Event->Get()->Result;
    if (result.IsFail()) {
        sendFailure(TStringBuilder() << "duplicate filter merge event holds error: " << result.GetErrorMessage());
        return;
    }

    // Fresh merger per task: progressive state is carried via OpenBatches/FiltersBuilder, not the stream.
    auto merger = Context->MakeMerger();
    for (const auto& [portionId, data] : State.OpenBatches) {
        const ui64 start = State.FiltersBuilder.GetProcessedRows(portionId);
        merger->AddSource(data, nullptr,
            Context->IsReversed ? NArrow::NMerger::TIterationOrder::Reversed(start) : NArrow::NMerger::TIterationOrder::Forward(start),
            portionId);
        YDB_LOG_TRACE("",
            {"component", "duplicates_manager"},
            {"event", "TMergeBorders::DoExecute"},
            {"type", "readd_open_source"},
            {"portionId", portionId},
            {"start", start},
            {"recordsCount", data->GetRecordsCount()},
            {"builder", State.FiltersBuilder.DebugString()});
    }

    auto columnData = result.MutableResult().ExtractDataByPortion(Context->FetchingColumns);
    for (const auto& [portionId, data] : columnData) {
        const ui64 expectedRecordsCount = Context->Portions->GetPortionVerified(portionId)->GetRecordsCount();
        if (data->GetRecordsCount() != expectedRecordsCount) {
            sendFailure(TStringBuilder() << "duplicate filter column data records mismatch for portion " << portionId
                                         << ": meta=" << expectedRecordsCount << ", fetched=" << data->GetRecordsCount());
            return;
        }
        AFL_VERIFY(State.OpenBatches.emplace(portionId, data).second)("portionId", portionId);
        merger->AddSource(data, nullptr,
            Context->IsReversed ? NArrow::NMerger::TIterationOrder::Reversed(0) : NArrow::NMerger::TIterationOrder::Forward(0), portionId);
        State.FiltersBuilder.AddSource(portionId, expectedRecordsCount);
        YDB_LOG_TRACE("",
            {"component", "duplicates_manager"},
            {"event", "TMergeBorders::DoExecute"},
            {"type", "add_source"},
            {"portionId", portionId},
            {"recordsCount", data->GetRecordsCount()},
            {"builder", State.FiltersBuilder.DebugString()});
    }

    if (!(State.FiltersBuilder.CountSources() > 0 || ReadyBorders.empty())) {
        sendFailure("duplicate filter merge has ready borders but no sources");
        return;
    }

    for (const auto& readyBorder : ReadyBorders) {
        merger->PutControlPoint(readyBorder.BuildSortablePosition(Context->IsReversed), false);
        if (!merger->DrainToControlPoint(State.FiltersBuilder, true)) {
            sendFailure(TStringBuilder() << "cannot drain duplicate filter merger to control point "
                                         << readyBorder.BuildSortablePosition(Context->IsReversed).DebugString());
            return;
        }
        YDB_LOG_TRACE("",
            {"component", "duplicates_manager"},
            {"event", "TMergeBorders::DoExecute"},
            {"type", "drain"},
            {"border", readyBorder.BuildSortablePosition(Context->IsReversed).DebugString()},
            {"builder", State.FiltersBuilder.DebugString()});
    }

    Context->Counters->OnRowsMerged(
        State.FiltersBuilder.GetRowsAdded() - State.PrevRowsAdded, State.FiltersBuilder.GetRowsSkipped() - State.PrevRowsSkipped, 0);
    State.PrevRowsAdded = State.FiltersBuilder.GetRowsAdded();
    State.PrevRowsSkipped = State.FiltersBuilder.GetRowsSkipped();

    auto readyFilters = State.FiltersBuilder.ExtractReadyFilters();
    for (const auto& [portionId, _] : readyFilters) {
        AFL_VERIFY(State.OpenBatches.erase(portionId))("portionId", portionId);
    }
    AFL_VERIFY(State.OpenBatches.size() == State.FiltersBuilder.CountSources());
    SendResult(std::move(readyFilters), TConclusionStatus::Success());
}

void TMergeBorders::DoOnCannotExecute(const TString& reason) {
    SendResult(THashMap<ui64, NArrow::TColumnFilter>{}, TConclusionStatus::Fail(reason));
}

TString TMergeBorders::GetTaskClassIdentifier() const {
    return "BUILD_DUPLICATE_FILTERS";
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
