#include "merge.h"
#include "private_events.h"

#include <ydb/core/formats/arrow/reader/merger.h>

#include <ydb/library/actors/core/log.h>

#include <util/system/thread.h>

#include <atomic>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

namespace {
std::atomic<ui64> MergeTaskSeq{ 0 };
}

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
    , TaskId(MergeTaskSeq.fetch_add(1) + 1)
{
    State.BumpGeneration();
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_ctor")("task_id", TaskId)("this", (ui64)this)(
        "owner", Owner.ToString())("state", State.DebugString())("ready_borders", ReadyBorders.size())("open_batches", State.OpenBatches.size())(
        "aborted", Context && Context->IsAborted());
}

void TMergeBorders::SendResult(THashMap<ui64, NArrow::TColumnFilter>&& readyFilters, TConclusionStatus&& conclusion) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_send_result")("task_id", TaskId)(
        "this", (ui64)this)("owner", Owner.ToString())("ok", conclusion.IsSuccess())("error",
        conclusion.IsSuccess() ? TString() : conclusion.GetErrorMessage())("state", State.DebugString())("filters", readyFilters.size());
    TActivationContext::AsActorContext().Send(Owner, std::make_unique<TEvMergeBordersResult>(std::move(Event.Get()->Get()->Context),
                                                         std::move(State), std::move(readyFilters), std::move(conclusion)));
}

void TMergeBorders::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    auto sendFailure = [&](const TString& reason) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_fail")("task_id", TaskId)("this", (ui64)this)(
            "reason", reason)("state", State.DebugString());
        SendResult(THashMap<ui64, NArrow::TColumnFilter>{}, TConclusionStatus::Fail(reason));
    };

    if (Context->IsAborted()) {
        sendFailure("duplicate filter merge aborted");
        return;
    }

    if (!Event || !Event->Get()) {
        sendFailure(TStringBuilder() << "duplicate filter merge event is null task_id=" << TaskId);
        return;
    }
    auto& result = Event->Get()->Result;
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_result_state")("task_id", TaskId)(
        "this", (ui64)this)("event_ptr", (ui64)Event->Get())("success", result.IsSuccess())("fail", result.IsFail());
    if (result.IsFail()) {
        sendFailure(TStringBuilder() << "duplicate filter merge event holds error: " << result.GetErrorMessage());
        return;
    }
    if (!result.IsSuccess()) {
        sendFailure(TStringBuilder() << "duplicate filter merge event conclusion is valueless/corrupt task_id=" << TaskId);
        return;
    }

    auto merger = Context->MakeMerger();
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_enter")("task_id", TaskId)("this", (ui64)this)(
        "tid", TThread::CurrentThreadId())("owner", Owner.ToString())("state", State.DebugString())("ready_borders", ReadyBorders.size())(
        "merger_ptr", (ui64)merger.get())("open_batches", State.OpenBatches.size())("aborted", Context && Context->IsAborted());

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
    TStringBuilder portionIds;
    for (const auto& [portionId, data] : columnData) {
        if (!portionIds.empty()) {
            portionIds << ",";
        }
        portionIds << portionId;
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

    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_sources")("task_id", TaskId)("this", (ui64)this)(
        "portions", portionIds)("sources", columnData.size())("ready_borders", ReadyBorders.size())("merger_ptr", (ui64)merger.get())(
        "open_batches", State.OpenBatches.size())("state", State.DebugString());

    if (!(State.FiltersBuilder.CountSources() > 0 || ReadyBorders.empty())) {
        sendFailure("duplicate filter merge has ready borders but no sources");
        return;
    }

    ui32 borderIdx = 0;
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
        ++borderIdx;
    }
    Y_UNUSED(borderIdx);

    Context->Counters->OnRowsMerged(
        State.FiltersBuilder.GetRowsAdded() - State.PrevRowsAdded, State.FiltersBuilder.GetRowsSkipped() - State.PrevRowsSkipped, 0);
    State.PrevRowsAdded = State.FiltersBuilder.GetRowsAdded();
    State.PrevRowsSkipped = State.FiltersBuilder.GetRowsSkipped();

    auto readyFilters = State.FiltersBuilder.ExtractReadyFilters();
    for (const auto& [portionId, _] : readyFilters) {
        AFL_VERIFY(State.OpenBatches.erase(portionId))("portionId", portionId);
    }
    AFL_VERIFY(State.OpenBatches.size() == State.FiltersBuilder.CountSources());
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_exit")("task_id", TaskId)("this", (ui64)this)(
        "tid", TThread::CurrentThreadId())("merger_ptr", (ui64)merger.get())("state", State.DebugString())("filters", readyFilters.size());
    SendResult(std::move(readyFilters), TConclusionStatus::Success());
}

void TMergeBorders::DoOnCannotExecute(const TString& reason) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("component", "df_diag")("event", "df_merge_cannot_execute")("task_id", TaskId)(
        "this", (ui64)this)("reason", reason)("state", State.DebugString());
    SendResult(THashMap<ui64, NArrow::TColumnFilter>{}, TConclusionStatus::Fail(reason));
}

TString TMergeBorders::GetTaskClassIdentifier() const {
    return "BUILD_DUPLICATE_FILTERS";
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
